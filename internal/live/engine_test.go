package live

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadclient"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/tencentcloud"
)

const testNode = "11111111-1111-1111-1111-111111111111"
const testInstance = "ins-test1"
const testGroup = "asg-test1"
const testRegion = "ap-chengdu"
const testRequest = "11111111-2222-3333-4444-555555555555"
const testActivity = "asa-test1"

func copyJSON[T any](v T) T {
	b, _ := json.Marshal(v)
	var out T
	_ = json.Unmarshal(b, &out)
	return out
}

type fakeCloud struct {
	mu                          sync.Mutex
	snap                        tencentcloud.Snapshot
	activity                    tencentcloud.Activity
	writes                      []string
	observeCalls, activityCalls int
	growUnknown, removeUnknown  bool
	observeFailure              bool
	// Called immediately before a write. The real durable journal must already
	// contain the corresponding intent; a mere in-memory phase is insufficient.
	beforeWrite func(string) error
}

func baseline() tencentcloud.Snapshot {
	return tencentcloud.Snapshot{At: time.Now().UTC(), Region: testRegion,
		Group:   tencentcloud.Group{ID: testGroup, Min: 0, Max: 1, Desired: 0, Count: 0, Enabled: "ENABLED", Activity: "NOT_IN_ACTIVITY", Status: "NORMAL", RetryPolicy: "NO_RETRY", LaunchConfiguration: "asc-test1"},
		Members: []tencentcloud.Member{}, VMs: map[string]tencentcloud.VM{}, Policies: []tencentcloud.Policy{},
		LaunchConfiguration: tencentcloud.LaunchConfiguration{ID: "asc-test1", Status: "NORMAL", ChargeType: "POSTPAID_BY_HOUR", InstanceType: "S5.SMALL2", ImageID: "img-test1", VersionNumber: 1, InstanceTypes: []string{}, SecurityGroupIDs: []string{"sg-test1"}}}
}
func newCloud() *fakeCloud {
	return &fakeCloud{snap: baseline(), activity: tencentcloud.Activity{ID: testActivity, GroupID: testGroup, Status: "RUNNING", RelatedInstances: []tencentcloud.RelatedInstance{{ID: testInstance, Status: "RUNNING"}}}}
}
func (f *fakeCloud) addMember() {
	f.snap.Group.Count = 1
	f.snap.Group.Desired = 1
	f.snap.Members = []tencentcloud.Member{{ID: testInstance, CreationType: "AUTO_CREATION", Lifecycle: "IN_SERVICE", AddedAt: time.Now().UTC().Format(time.RFC3339)}}
	f.snap.VMs[testInstance] = tencentcloud.VM{ID: testInstance, State: "RUNNING", ChargeType: "POSTPAID_BY_HOUR", Returned: true}
}
func (f *fakeCloud) Observe(_ context.Context, group, tracked string) (tencentcloud.Snapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.observeFailure {
		return tencentcloud.Snapshot{}, errors.New("raw-secret-should-not-leak")
	}
	if group != testGroup {
		return tencentcloud.Snapshot{}, errors.New("raw-secret-should-not-leak")
	}
	f.observeCalls++
	out := copyJSON(f.snap)
	out.At = time.Now().UTC()
	if tracked != "" {
		if _, ok := out.VMs[tracked]; !ok {
			out.VMs[tracked] = tencentcloud.VM{ID: tracked}
		}
	}
	return out, nil
}
func (f *fakeCloud) SetDesired(_ context.Context, group string, count int64) (tencentcloud.WriteResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if group != testGroup || count != 1 {
		return tencentcloud.WriteResult{}, errors.New("unexpected target")
	}
	if f.beforeWrite != nil {
		if err := f.beforeWrite("grow-intent"); err != nil {
			return tencentcloud.WriteResult{}, err
		}
	}
	f.writes = append(f.writes, "desired1")
	f.addMember()
	out := tencentcloud.WriteResult{Attempted: true, RequestID: testRequest, Action: "ModifyDesiredCapacity", Unknown: f.growUnknown}
	if f.growUnknown {
		return out, errors.New("raw-secret-should-not-leak")
	}
	return out, nil
}
func (f *fakeCloud) RemoveExact(_ context.Context, group, id string) (tencentcloud.WriteResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if group != testGroup || id != testInstance {
		return tencentcloud.WriteResult{}, errors.New("wrong exact instance")
	}
	if f.beforeWrite != nil {
		if err := f.beforeWrite("remove-intent"); err != nil {
			return tencentcloud.WriteResult{}, err
		}
	}
	f.writes = append(f.writes, "remove:"+id)
	f.snap.Group.Desired = 0
	// Deliberately idle before deletion. The engine must still await activity,
	// membership and exact CVM evidence rather than this convenient flag.
	f.snap.Group.Activity = "NOT_IN_ACTIVITY"
	out := tencentcloud.WriteResult{Attempted: true, RequestID: testRequest, ActivityID: testActivity, Action: "RemoveInstances", Unknown: f.removeUnknown}
	if f.removeUnknown {
		return out, errors.New("raw-secret-should-not-leak")
	}
	return out, nil
}
func (f *fakeCloud) Activity(_ context.Context, group, id string) (tencentcloud.Activity, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.activityCalls++
	if group != testGroup || id != testActivity {
		return tencentcloud.Activity{}, errors.New("wrong activity")
	}
	return copyJSON(f.activity), nil
}
func (f *fakeCloud) complete() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.activity.Status = "SUCCESSFUL"
	f.activity.RelatedInstances[0].Status = "SUCCESSFUL"
	f.snap.Group.Count = 0
	f.snap.Group.Desired = 0
	f.snap.Members = []tencentcloud.Member{}
	f.snap.VMs = map[string]tencentcloud.VM{}
}
func (f *fakeCloud) fail() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.activity.Status = "FAILED"
	f.activity.RelatedInstances[0].Status = "FAILED"
}
func (f *fakeCloud) writeCount() int { f.mu.Lock(); defer f.mu.Unlock(); return len(f.writes) }

type fakeNomad struct {
	mu                           sync.Mutex
	node                         nomadclient.Node
	missing                      bool
	drainUnknown, restoreUnknown bool
	writes                       []string
	beforeWrite                  func(string) error
}

func newNomad() *fakeNomad {
	return &fakeNomad{node: nomadclient.Node{Identity: nomadclient.Identity{NodeID: testNode, InstanceID: testInstance, GroupID: testGroup, Region: testRegion}, Status: "ready", Eligibility: "eligible"}}
}

func (f *fakeNomad) TasksStopped(context.Context, string, bool) (bool, error) { return true, nil }
func (f *fakeNomad) ResolveMembers(_ context.Context, group, region string, ids []string) ([]nomadclient.Node, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.missing {
		return nil, errors.New("raw-secret-should-not-leak")
	}
	if group != testGroup || region != testRegion || len(ids) != 1 || ids[0] != testInstance {
		return nil, errors.New("wrong mapping scope")
	}
	return []nomadclient.Node{copyJSON(f.node)}, nil
}
func (f *fakeNomad) ReadNode(_ context.Context, id string) (nomadclient.Node, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if id != testNode {
		return nomadclient.Node{}, errors.New("wrong node")
	}
	return copyJSON(f.node), nil
}
func (f *fakeNomad) StartOwnedDrain(_ context.Context, want nomadclient.Identity, op string, _ time.Duration) (nomadclient.MutationResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.beforeWrite != nil {
		if err := f.beforeWrite("drain-intent"); err != nil {
			return nomadclient.MutationResult{}, err
		}
	}
	if want != f.node.Identity {
		return nomadclient.MutationResult{}, errors.New("wrong identity")
	}
	f.writes = append(f.writes, "drain")
	f.node.Drain = true
	f.node.HasDrainStrategy = true
	f.node.Eligibility = "ineligible"
	f.node.LastDrain = &nomadclient.DrainRecord{Owner: drainOwner, OperationID: op, Status: "draining"}
	n := copyJSON(f.node)
	out := nomadclient.MutationResult{After: &n, WriteAttempted: true, Unknown: f.drainUnknown, NodeModifyIndex: 1}
	if f.drainUnknown {
		return out, errors.New("raw-secret-should-not-leak")
	}
	return out, nil
}
func (f *fakeNomad) RestoreOwnedDrain(_ context.Context, want nomadclient.Identity, op string) (nomadclient.MutationResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.beforeWrite != nil {
		if err := f.beforeWrite("restore-intent"); err != nil {
			return nomadclient.MutationResult{}, err
		}
	}
	if want != f.node.Identity || f.node.LastDrain == nil || f.node.LastDrain.Owner != drainOwner || f.node.LastDrain.OperationID != op {
		return nomadclient.MutationResult{}, errors.New("foreign ownership")
	}
	f.writes = append(f.writes, "restore")
	f.node.Drain = false
	f.node.HasDrainStrategy = false
	f.node.Eligibility = "eligible"
	f.node.LastDrain.Status = "canceled"
	n := copyJSON(f.node)
	out := nomadclient.MutationResult{After: &n, WriteAttempted: true, Unknown: f.restoreUnknown, NodeModifyIndex: 2}
	if f.restoreUnknown {
		return out, errors.New("raw-secret-should-not-leak")
	}
	return out, nil
}
func (f *fakeNomad) drained() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.node.Drain = false
	f.node.HasDrainStrategy = false
	f.node.LastDrain.Status = "complete"
}
func (f *fakeNomad) writeCount() int { f.mu.Lock(); defer f.mu.Unlock(); return len(f.writes) }

func config(t *testing.T) Config {
	t.Helper()
	return Config{Dir: filepath.Join(t.TempDir(), "journal"), GroupID: testGroup, Region: testRegion, ReconcileInterval: time.Hour, DrainDeadline: time.Minute}
}
func openTest(t *testing.T, cfg Config, c *fakeCloud, n *fakeNomad) *Engine {
	t.Helper()
	e, err := Open(cfg, c, n)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(e.Close)
	hook := func(phase string) error {
		b, err := os.ReadFile(filepath.Join(cfg.Dir, "live.json"))
		if err != nil {
			return err
		}
		var s State
		if err = json.Unmarshal(b, &s); err != nil {
			return err
		}
		if s.Pending == nil || s.Pending.Phase != phase {
			return errors.New("write preceded durable intent")
		}
		return nil
	}
	c.beforeWrite = hook
	n.beforeWrite = hook
	return e
}
func step(t *testing.T, e *Engine) {
	t.Helper()
	if err := e.reconcileOnce(); err != nil {
		t.Fatal(err)
	}
}
func grow(t *testing.T, e *Engine) {
	t.Helper()
	changed, err := e.Scale(1)
	if err != nil || !changed {
		t.Fatalf("grow: %v %v", changed, err)
	}
	step(t, e)
}
func startShrink(t *testing.T, e *Engine, n *fakeNomad) {
	t.Helper()
	changed, err := e.Scale(0)
	if err != nil || !changed {
		t.Fatalf("shrink: %v %v", changed, err)
	}
	n.drained()
	step(t, e)
	s, _, _ := e.Snapshot()
	if s.Pending == nil || s.Pending.Phase != "removing" {
		t.Fatalf("not removing: %+v", s.Pending)
	}
}

func TestExactGrowDrainRemoveAndTwoIndependentAbsences(t *testing.T) {
	c, n := newCloud(), newNomad()
	e := openTest(t, config(t), c, n)
	grow(t, e)
	s, _, _ := e.Snapshot()
	if len(s.Members) != 1 || s.LastEvent != "expansion-complete" {
		t.Fatalf("grow not proven: %+v", s)
	}
	startShrink(t, e, n)
	// A duplicate target never repeats a drain or cloud mutation.
	if changed, err := e.Scale(0); changed || !errors.Is(err, ErrBusy) {
		t.Fatalf("pending replay: %v %v", changed, err)
	}
	step(t, e)
	s, ready, _ := e.Snapshot()
	if s.Pending == nil || ready || len(s.Members) != 1 {
		t.Fatal("idle group was mistaken for real deletion")
	}
	c.complete()
	step(t, e)
	s, _, _ = e.Snapshot()
	if s.Pending == nil || s.Pending.AbsentObservations != 1 {
		t.Fatal("one absence completed deletion")
	}
	step(t, e)
	step(t, e)
	s, ready, err := e.Snapshot()
	if err != nil || !ready || s.Pending != nil || len(s.Members) != 0 || s.LastEvent != "deletion-complete" {
		t.Fatalf("not completed: %+v %v", s, err)
	}
	if c.writeCount() != 2 || n.writeCount() != 1 {
		t.Fatal("unexpected replay or restoration")
	}
}

func TestUnregisteredMembersAreCountedAndUnready(t *testing.T) {
	c, n := newCloud(), newNomad()
	c.addMember()
	n.missing = true
	e := openTest(t, config(t), c, n)
	s, ready, err := e.Snapshot()
	if len(s.Members) != 1 || ready || err != nil || s.ErrorCode != "NomadMemberMappingUnavailable" {
		t.Fatalf("unregistered capacity hidden: %+v %v", s, err)
	}
	changed, scaleErr := e.Scale(0)
	if changed || scaleErr == nil || c.writeCount() != 0 || n.writeCount() != 0 {
		t.Fatal("unregistered node was mutated")
	}
	if strings.Contains(scaleErr.Error(), "raw-secret") {
		t.Fatal("raw error leaked")
	}
}

func TestMappingStatusKeepsWorkerBlockedAndCloudFailuresRemainErrors(t *testing.T) {
	c, n := newCloud(), newNomad()
	cfg := config(t)
	e := openTest(t, cfg, c, n)
	grow(t, e)
	if changed, err := e.Scale(0); !changed || err != nil {
		t.Fatal(err)
	}
	n.drained()
	n.mu.Lock()
	n.missing = true
	n.mu.Unlock()
	// The outward Status representation changes; the workflow observation
	// must still fail and prevent a ready-to-remove owned drain advancing.
	refreshErr := e.reconcileOnce()
	if refreshErr == nil {
		t.Fatal("mapping failure no longer blocks workflow refresh")
	}
	s, ready, err := e.Snapshot()
	if err != nil || ready || len(s.Members) != 1 || s.Pending.Phase != "draining" || c.writeCount() != 1 {
		t.Fatal("mapping failure hid count or advanced deletion")
	}
	c.mu.Lock()
	c.observeFailure = true
	c.mu.Unlock()
	refreshErr = e.reconcileOnce()
	if refreshErr == nil {
		t.Fatal("cloud failure not reported")
	}
	s, ready, err = e.Snapshot()
	if err == nil || ready || s.ErrorCode != "CloudObservationFailed" {
		t.Fatal("stale cloud membership was presented as valid unready Status")
	}
	c.mu.Lock()
	c.observeFailure = false
	c.snap.VMs[testInstance] = tencentcloud.VM{ID: "ins-wrong1"}
	c.mu.Unlock()
	refreshErr = e.reconcileOnce()
	if refreshErr == nil {
		t.Fatal("incomplete cloud snapshot accepted")
	}
	if _, ready, err = e.Snapshot(); err == nil || ready {
		t.Fatal("incomplete cloud snapshot was hidden as a Nomad-only failure")
	}
}

func TestKnownFailureRestoresOnlyOwnedSurvivorAndRequiresManualReview(t *testing.T) {
	c, n := newCloud(), newNomad()
	e := openTest(t, config(t), c, n)
	grow(t, e)
	startShrink(t, e, n)
	c.fail()
	step(t, e)
	s, ready, err := e.Snapshot()
	if err != nil || ready || !s.ManualReview || s.Pending != nil || s.Desired != 0 || n.writeCount() != 2 {
		t.Fatalf("unsafe restore completion: %+v %v", s, err)
	}
	// Even if a later external reconciliation restores D=1, this engine must
	// not silently resume cloud writes after a failed-delete restoration.
	c.mu.Lock()
	c.snap.Group.Desired = 1
	c.mu.Unlock()
	step(t, e)
	if changed, err := e.Scale(0); changed || !errors.Is(err, ErrNotReady) {
		t.Fatalf("manual-review gate bypassed: %v %v", changed, err)
	}
	if c.writeCount() != 2 {
		t.Fatal("restoration changed desired or replayed delete")
	}
}

func TestFailureEvidenceDoesNotRestoreForeignDrainOrMissingCVM(t *testing.T) {
	for _, mode := range []string{"foreign-owner", "other-operation", "running-activity", "related-not-failed", "vm-not-running", "protected-foreign-owner"} {
		t.Run(mode, func(t *testing.T) {
			c, n := newCloud(), newNomad()
			e := openTest(t, config(t), c, n)
			grow(t, e)
			startShrink(t, e, n)
			c.fail()
			switch mode {
			case "foreign-owner":
				n.mu.Lock()
				n.node.LastDrain.Owner = "operator"
				n.mu.Unlock()
			case "other-operation":
				n.mu.Lock()
				n.node.LastDrain.OperationID = "another-operation"
				n.mu.Unlock()
			case "running-activity":
				c.mu.Lock()
				c.activity.Status = "RUNNING"
				c.mu.Unlock()
			case "related-not-failed":
				c.mu.Lock()
				c.activity.RelatedInstances[0].Status = "RUNNING"
				c.mu.Unlock()
			case "vm-not-running":
				c.mu.Lock()
				vm := c.snap.VMs[testInstance]
				vm.State = "TERMINATING"
				c.snap.VMs[testInstance] = vm
				c.mu.Unlock()
			case "protected-foreign-owner":
				n.mu.Lock()
				n.node.LastDrain.Owner = "operator"
				n.mu.Unlock()
				c.mu.Lock()
				c.snap.Members[0].Protected = true
				c.mu.Unlock()
			}
			step(t, e)
			if n.writeCount() != 1 || c.writeCount() != 2 {
				t.Fatal("insufficient failure evidence caused restoration or cloud replay")
			}
			s, ready, _ := e.Snapshot()
			if s.Pending == nil || ready {
				t.Fatal("uncertain failure was marked complete")
			}
		})
	}
}

func TestUnknownCloudWriteSurvivesRestartWithoutReplay(t *testing.T) {
	for _, mode := range []string{"grow", "remove"} {
		t.Run(mode, func(t *testing.T) {
			c, n := newCloud(), newNomad()
			cfg := config(t)
			e := openTest(t, cfg, c, n)
			if mode == "grow" {
				c.growUnknown = true
				if changed, err := e.Scale(1); !changed || !errors.Is(err, ErrUnknown) {
					t.Fatalf("missing grow unknown: %v %v", changed, err)
				}
			} else {
				grow(t, e)
				c.removeUnknown = true
				if changed, err := e.Scale(0); !changed || err != nil {
					t.Fatal("drain failed")
				}
				n.drained()
				err := e.reconcileOnce()
				if !errors.Is(err, ErrUnknown) {
					t.Fatalf("missing remove unknown: %v", err)
				}
			}
			writes, nomadWrites := c.writeCount(), n.writeCount()
			e.Close()
			reopened, err := Open(cfg, c, n)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			step(t, reopened)
			s, ready, err := reopened.Snapshot()
			if ready || !errors.Is(err, ErrUnknown) || s.Pending == nil || !s.Pending.Unknown {
				t.Fatal("unknown outcome was silently resolved")
			}
			if changed, err := reopened.Scale(0); changed || !errors.Is(err, ErrUnknown) {
				t.Fatal("unknown outcome accepted replay")
			}
			if c.writeCount() != writes || n.writeCount() != nomadWrites {
				t.Fatal("unknown write was replayed on restart")
			}
			if mode == "remove" {
				c.mu.Lock()
				reads := c.activityCalls
				c.mu.Unlock()
				if reads == 0 || s.Pending.ActivityID != testActivity {
					t.Fatal("partial activity evidence was lost instead of read-only reconciliation")
				}
			}
			b, _ := json.Marshal(s)
			if strings.Contains(string(b), "raw-secret") {
				t.Fatal("raw error leaked to journal")
			}
		})
	}
}

func TestUnknownNomadDrainOrRestoreNeverReplaysOnRestart(t *testing.T) {
	for _, mode := range []string{"drain", "restore"} {
		t.Run(mode, func(t *testing.T) {
			c, n := newCloud(), newNomad()
			cfg := config(t)
			e := openTest(t, cfg, c, n)
			grow(t, e)
			if mode == "drain" {
				n.drainUnknown = true
				if changed, err := e.Scale(0); !changed || !errors.Is(err, ErrUnknown) {
					t.Fatalf("missing drain unknown: %v %v", changed, err)
				}
			} else {
				startShrink(t, e, n)
				c.fail()
				n.restoreUnknown = true
				err := e.reconcileOnce()
				if !errors.Is(err, ErrUnknown) {
					t.Fatalf("missing restore unknown: %v", err)
				}
			}
			writes, nomadWrites := c.writeCount(), n.writeCount()
			e.Close()
			reopened, err := Open(cfg, c, n)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			step(t, reopened)
			if _, ready, err := reopened.Snapshot(); ready || !errors.Is(err, ErrUnknown) {
				t.Fatal("unknown Nomad mutation became ready")
			}
			if c.writeCount() != writes || n.writeCount() != nomadWrites {
				t.Fatal("unknown Nomad mutation was replayed")
			}
		})
	}
}

func TestIntentCrashPhasesNeverIssueWritesOnRestart(t *testing.T) {
	for _, phase := range []string{"grow-intent", "drain-intent", "remove-intent", "restore-intent"} {
		t.Run(phase, func(t *testing.T) {
			c, n := newCloud(), newNomad()
			cfg := config(t)
			e := openTest(t, cfg, c, n)
			e.mu.Lock()
			op := &Operation{ID: "live-" + strings.Repeat("a", 32), Kind: "grow", Target: 1, Phase: phase, StartedAt: time.Now().UTC()}
			if phase != "grow-intent" {
				op.Kind = "shrink"
				op.Target = 0
				op.InstanceID = testInstance
				op.NodeID = testNode
			}
			if phase == "restore-intent" {
				op.RequestID = testRequest
				op.ActivityID = testActivity
				op.ActivityStatus = "FAILED"
				op.InstanceResult = "FAILED"
			}
			e.state.Pending = op
			e.state.Sequence = 1
			err := e.save()
			e.mu.Unlock()
			if err != nil {
				t.Fatal(err)
			}
			e.Close()
			reopened, err := Open(cfg, c, n)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			step(t, reopened)
			s, ready, err := reopened.Snapshot()
			if ready || !errors.Is(err, ErrUnknown) || s.Pending.Phase != "unknown" {
				t.Fatal("intent crash was treated as permission to replay")
			}
			if c.writeCount() != 0 || n.writeCount() != 0 {
				t.Fatal("intent crash replayed mutation")
			}
		})
	}
}

func TestProtectedNodeAndForeignDrainRefuseInitialScaleIn(t *testing.T) {
	for _, mode := range []string{"cloud-protected", "cvm-protected", "foreign-drain", "wrong-group"} {
		t.Run(mode, func(t *testing.T) {
			c, n := newCloud(), newNomad()
			c.addMember()
			switch mode {
			case "cloud-protected":
				c.snap.Members[0].Protected = true
			case "cvm-protected":
				vm := c.snap.VMs[testInstance]
				vm.Protected = true
				c.snap.VMs[testInstance] = vm
			case "foreign-drain":
				n.node.Drain = true
				n.node.Eligibility = "ineligible"
				n.node.LastDrain = &nomadclient.DrainRecord{Owner: "operator", OperationID: "foreign", Status: "complete"}
			case "wrong-group":
				n.node.GroupID = "asg-other"
			}
			e := openTest(t, config(t), c, n)
			if changed, err := e.Scale(0); changed || err == nil || c.writeCount() != 0 || n.writeCount() != 0 {
				t.Fatal("protected identity/state was mutated")
			}
		})
	}
}

func TestUnsafeGroupRefusedAtOpen(t *testing.T) {
	for _, mode := range []string{"min", "max", "retry", "policy", "lc-status", "lc-charge"} {
		t.Run(mode, func(t *testing.T) {
			c, n := newCloud(), newNomad()
			switch mode {
			case "min":
				c.snap.Group.Min = 1
			case "max":
				c.snap.Group.Max = 2
			case "retry":
				c.snap.Group.RetryPolicy = "INCREMENTAL_INTERVALS"
			case "policy":
				c.snap.PolicyCount = 1
				c.snap.Policies = []tencentcloud.Policy{{ID: "asp-test1", Type: "SIMPLE"}}
			case "lc-status":
				c.snap.LaunchConfiguration.Status = "SECURITY_GROUP_ABNORMAL"
			case "lc-charge":
				c.snap.LaunchConfiguration.ChargeType = "PREPAID"
			}
			e, err := Open(config(t), c, n)
			if e != nil {
				e.Close()
			}
			if err == nil || c.writeCount() != 0 || n.writeCount() != 0 {
				t.Fatal("unsafe group opened")
			}
		})
	}
}

func TestJournalBindingLockAndPersistenceFailureStopWrites(t *testing.T) {
	c, n := newCloud(), newNomad()
	cfg := config(t)
	e := openTest(t, cfg, c, n)
	if another, err := Open(cfg, c, n); err == nil {
		another.Close()
		t.Fatal("second writer acquired lock")
	}
	// Replacing the journal with a directory causes rename failure regardless
	// of whether the remote test runner happens to execute as root.
	if err := os.Remove(filepath.Join(cfg.Dir, "live.json")); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(cfg.Dir, "live.json"), 0700); err != nil {
		t.Fatal(err)
	}
	if changed, err := e.Scale(1); changed || err == nil || c.writeCount() != 0 {
		t.Fatal("persistence failure allowed cloud write")
	}
	if _, ready, err := e.Snapshot(); ready || err == nil {
		t.Fatal("persistence failure remained ready")
	}
	if changed, err := e.Scale(1); changed || err == nil || c.writeCount() != 0 {
		t.Fatal("persistence failure did not latch")
	}
	e.Close()
	// A clean journal cannot be reused for another group or drain policy.
	c2, n2 := newCloud(), newNomad()
	cfg2 := config(t)
	e2 := openTest(t, cfg2, c2, n2)
	e2.Close()
	cfg2.GroupID = "asg-other"
	if opened, err := Open(cfg2, c2, n2); err == nil {
		opened.Close()
		t.Fatal("journal group binding bypassed")
	}
	cfg2.GroupID = testGroup
	cfg2.DrainDeadline = 2 * time.Minute
	if opened, err := Open(cfg2, c2, n2); err == nil {
		opened.Close()
		t.Fatal("journal drain policy binding bypassed")
	}
}

func TestIndependentWorkerFinishesWithoutStatusOrScalePolling(t *testing.T) {
	c, n := newCloud(), newNomad()
	cfg := config(t)
	cfg.ReconcileInterval = 10 * time.Millisecond
	e := openTest(t, cfg, c, n)
	if changed, err := e.Scale(1); !changed || err != nil {
		t.Fatal(err)
	}
	// Wait on the internal state only: Snapshot is neither a reconciler nor
	// the source of progress. The ticker must advance the accepted operation.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		e.mu.Lock()
		done := e.state.Pending == nil && e.state.LastEvent == "expansion-complete"
		e.mu.Unlock()
		if done {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("independent worker did not finish")
}
