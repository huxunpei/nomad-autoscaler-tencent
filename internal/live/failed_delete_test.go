package live

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadclient"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/tencentcloud"
)

type failedDeleteNomad struct {
	*multiNomad
	dir             string
	refuse, loseAck bool
	afterRestore    func()
}

func (n *failedDeleteNomad) RestoreOwnedDrain(ctx context.Context, id nomadclient.Identity, op string) (nomadclient.MutationResult, error) {
	raw, err := os.ReadFile(filepath.Join(n.dir, "live.json"))
	var s State
	if err != nil || json.Unmarshal(raw, &s) != nil || s.Pending == nil || s.Pending.ID != op || s.Pending.Phase != "failed-delete" || !failureTerminal(s.Pending.ActivityStatus) || s.Pending.InstanceResult != "FAILED" || s.Pending.ActivityID == "" {
		return nomadclient.MutationResult{}, errors.New("restore preceded durable deletion failure")
	}
	if n.refuse {
		return nomadclient.MutationResult{}, errors.New("temporary prewrite failure")
	}
	result, err := n.multiNomad.RestoreOwnedDrain(ctx, id, op)
	if n.afterRestore != nil {
		f := n.afterRestore
		n.afterRestore = nil
		f()
	}
	if n.loseAck {
		return nomadclient.MutationResult{WriteAttempted: true, Unknown: true}, errors.New("lost restore response")
	}
	return result, err
}

func submittedFailure(t *testing.T, e *Engine, c *multiCloud) string {
	t.Helper()
	if _, err := e.Scale(1); err != nil {
		t.Fatal(err)
	}
	id := e.state.Pending.InstanceID
	step(t, e)
	c.terminal(false)
	for i := range c.snap.Members {
		if c.snap.Members[i].ID == id {
			c.snap.Members[i].Lifecycle = "TERMINATION_FAILED"
		}
	}
	return id
}

func TestSubmittedFailureRestoreSurvivesRestartWithoutManualReview(t *testing.T) {
	for _, mode := range []string{"normal", "before-write", "lost-ack", "after-write-persist-failure"} {
		t.Run(mode, func(t *testing.T) {
			cfg, c, n, e := groupContractFixture(t, 2, 0, 2, false)
			e.Close()
			probe := &failedDeleteNomad{multiNomad: n, dir: cfg.Dir}
			e = openMulti(t, cfg, c, probe)
			id := submittedFailure(t, e, c)
			switch mode {
			case "before-write":
				probe.refuse = true
			case "lost-ack":
				probe.loseAck = true
			case "after-write-persist-failure":
				probe.afterRestore = func() {
					p := filepath.Join(cfg.Dir, "live.json")
					if err := os.Rename(p, p+".saved"); err != nil {
						t.Fatal(err)
					}
					if err := os.Mkdir(p, 0700); err != nil {
						t.Fatal(err)
					}
				}
			}
			err := e.reconcileOnce()
			if mode == "after-write-persist-failure" {
				if err == nil {
					t.Fatal("ignored persistence failure")
				}
				p := filepath.Join(cfg.Dir, "live.json")
				if err := os.Remove(p); err != nil {
					t.Fatal(err)
				}
				if err := os.Rename(p+".saved", p); err != nil {
					t.Fatal(err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			e.Close()
			probe.refuse = false
			e = openMulti(t, cfg, c, probe)
			step(t, e)
			if e.state.Pending == nil || e.state.Pending.Phase != "failed-delete" || e.state.Pending.DrainOutcome != "restored" || e.state.Pending.Unknown || e.state.ManualReview || e.state.LastCompletedAt != 0 || n.nodes[id].Eligibility != "eligible" {
				t.Fatalf("restoration was not resumed: state=%+v node=%+v", e.state, n.nodes[id])
			}
			if len(n.writes) != 2 || c.writeCount() != 1 {
				t.Fatal("duplicated restoration or cloud write", n.writes, c.writes)
			}
			if done, err := e.completionState(e.state.Batch.ID); done || err == nil || err.Error() != "ScaleActionFailed" {
				t.Fatal("lost failure result", done, err)
			}
			for i := 0; i < 3; i++ {
				step(t, e)
			}
			if _, ready, _ := e.Snapshot(); ready {
				t.Fatal("restoring scheduling was treated as cloud settlement")
			}
			if changed, err := e.Scale(2); changed || !errors.Is(err, ErrBusy) {
				t.Fatal("new action overlapped unresolved deletion", changed, err)
			}
			e.Close()
			e = openMulti(t, cfg, c, probe)
			if e.state.Pending.DrainOutcome != "restored" || len(n.writes) != 2 {
				t.Fatal("lost durable readback")
			}
		})
	}
}

func TestSubmittedFailureRestoreIndependentOfUnrelatedGroupGuards(t *testing.T) {
	for _, mode := range []string{"other-unmapped", "disabled", "launch-config", "in-activity", "protected"} {
		t.Run(mode, func(t *testing.T) {
			cfg, c, n, e := groupContractFixture(t, 2, 0, 2, true)
			id := submittedFailure(t, e, c)
			switch mode {
			case "other-unmapped":
				delete(n.nodes, "ins-multi1")
			case "disabled":
				c.snap.Group.Enabled = "DISABLED"
			case "launch-config":
				c.snap.LaunchConfiguration.VersionNumber++
			case "in-activity":
				c.snap.Group.Activity = "IN_ACTIVITY"
			case "protected":
				v := c.snap.VMs[id]
				v.Protected = true
				c.snap.VMs[id] = v
				c.snap.Members[0].Protected = true
			}
			e.Close()
			e = openMulti(t, cfg, c, n)
			_ = e.reconcileOnce()
			if n.nodes[id].Eligibility != "eligible" || e.state.Pending == nil || e.state.Pending.DrainOutcome != "restored" || e.state.ManualReview || c.writeCount() != 1 {
				t.Fatal("unrelated guard prevented restoration or cleared cloud watch")
			}
		})
	}
}

func TestSubmittedFailureRestoreRequiresExactFailureAndSurvivor(t *testing.T) {
	for _, mode := range []string{"foreign-owner", "foreign-operation", "changed-identity", "vm-terminating", "member-left", "manual-member", "invalid-snapshot", "read-error", "activity-running", "instance-running", "wrong-activity"} {
		t.Run(mode, func(t *testing.T) {
			_, c, n, e := groupContractFixture(t, 2, 0, 2, false)
			id := submittedFailure(t, e, c)
			node := copyJSON(n.nodes[id])
			snap := copyJSON(c.snap)
			activity := copyJSON(c.activities[e.state.Pending.ActivityID])
			switch mode {
			case "foreign-owner":
				v := copyJSON(node)
				v.LastDrain.Owner = "operator"
				n.nodes[id] = v
			case "foreign-operation":
				v := copyJSON(node)
				v.LastDrain.OperationID = "another"
				n.nodes[id] = v
			case "changed-identity":
				v := copyJSON(node)
				v.InstanceID = "ins-other"
				n.nodes[id] = v
			case "vm-terminating":
				v := c.snap.VMs[id]
				v.State = "TERMINATING"
				c.snap.VMs[id] = v
			case "member-left":
				c.snap.Members = c.snap.Members[1:]
				c.snap.Group.Count = 1
			case "manual-member":
				c.snap.Members[0].CreationType = "MANUAL_ATTACH"
			case "invalid-snapshot":
				v := c.snap.VMs[id]
				v.ID = "ins-other"
				c.snap.VMs[id] = v
			case "read-error":
				c.observeFailure = true
			case "activity-running":
				a := copyJSON(activity)
				a.Status = "RUNNING"
				c.activities[a.ID] = a
			case "instance-running":
				a := copyJSON(activity)
				a.RelatedInstances[0].Status = "RUNNING"
				c.activities[a.ID] = a
			case "wrong-activity":
				a := copyJSON(activity)
				a.RelatedInstances[0].ID = "ins-other"
				c.activities[a.ID] = a
			}
			_ = e.reconcileOnce()
			if len(n.writes) != 1 || c.writeCount() != 1 || e.state.Pending == nil {
				t.Fatal("unsafe restore or lost pending")
			}
			n.nodes[id] = node
			c.snap = snap
			c.activities[activity.ID] = activity
			c.observeFailure = false
			step(t, e)
			if e.state.Pending == nil || e.state.Pending.DrainOutcome != "restored" || n.nodes[id].Eligibility != "eligible" {
				t.Fatal("could not resume after observation recovered")
			}
		})
	}
}

func TestSubmittedFailurePreservesSuccessfulChildrenAndDoesNotDrainNext(t *testing.T) {
	_, c, n, e := groupContractFixture(t, 3, 0, 3, false)
	if _, err := e.Scale(0); err != nil {
		t.Fatal(err)
	}
	completeMultiDelete(t, e, c)
	step(t, e)
	step(t, e)
	id := e.state.Pending.InstanceID
	c.terminal(false)
	step(t, e)
	if e.state.Batch == nil || len(e.state.Batch.Results) != 1 || e.state.Batch.Results[0].ActivityStatus != "SUCCESSFUL" || e.state.Pending == nil || e.state.Pending.DrainOutcome != "restored" || e.state.LastCompletedAt != 0 || e.state.ManualReview {
		t.Fatal("lost prior success or reported partial batch success")
	}
	for i := 0; i < 3; i++ {
		step(t, e)
	}
	if n.nodes[id].Eligibility != "eligible" || len(n.writes) != 3 || c.writeCount() != 2 {
		t.Fatal("advanced to another selected node")
	}
	// Even an idle/count=desired snapshot is not proof no cloud retry remains.
	c.snap.Group.Desired = c.snap.Group.Count
	step(t, e)
	if _, ready, _ := e.Snapshot(); ready {
		t.Fatal("one matching capacity snapshot released pending deletion")
	}
}

type instantFailedDeleteCloud struct{ *multiCloud }

func (c *instantFailedDeleteCloud) RemoveExact(ctx context.Context, g, id string) (tencentcloud.WriteResult, error) {
	result, err := c.multiCloud.RemoveExact(ctx, g, id)
	c.terminal(false)
	return result, err
}
func TestSubmittedFailureScaleAndWaitReturnsFailureWithRecoveryPending(t *testing.T) {
	cfg, c, n, e := groupContractFixture(t, 2, 0, 2, false)
	e.Close()
	cfg.StabilizationInterval = time.Millisecond
	probe := &failedDeleteNomad{multiNomad: n, dir: cfg.Dir, refuse: true}
	e = openMulti(t, cfg, &instantFailedDeleteCloud{c}, probe)
	if changed, err := e.ScaleAndWait(1, 3); !changed || err == nil || err.Error() != "ScaleActionFailed" {
		t.Fatal("failed activity was not returned as failure", changed, err)
	}
	if e.state.Pending == nil || e.state.Pending.Phase != "failed-delete" || e.state.ManualReview {
		t.Fatal("recovery obligation lost")
	}
	probe.refuse = false
	step(t, e)
	if e.state.Pending.DrainOutcome != "restored" || e.state.LastCompletedAt != 0 {
		t.Fatal("background recovery did not restore survivor")
	}
}

func TestSubmittedFailureRestoredRecordDoesNotUndoLaterOperatorDrain(t *testing.T) {
	_, c, n, e := groupContractFixture(t, 2, 0, 2, false)
	id := submittedFailure(t, e, c)
	step(t, e)
	v := n.nodes[id]
	v.Eligibility = "ineligible"
	v.LastDrain = &nomadclient.DrainRecord{Owner: "operator", OperationID: "operator", Status: "draining"}
	n.nodes[id] = v
	step(t, e)
	if len(n.writes) != 2 || n.nodes[id].Eligibility != "ineligible" || c.writeCount() != 1 {
		t.Fatal("recovery overwrote a later operator decision")
	}
}

type failureCheckpointCloud struct {
	*multiCloud
	checkpoint func()
}

func (c *failureCheckpointCloud) Activity(ctx context.Context, g, id string) (tencentcloud.Activity, error) {
	a, err := c.multiCloud.Activity(ctx, g, id)
	if c.checkpoint != nil {
		f := c.checkpoint
		c.checkpoint = nil
		f()
	}
	return a, err
}
func TestSubmittedFailureMustPersistBeforeRestoring(t *testing.T) {
	cfg, c, n, e := groupContractFixture(t, 2, 0, 2, false)
	e.Close()
	probe := &failureCheckpointCloud{multiCloud: c}
	e = openMulti(t, cfg, probe, n)
	submittedFailure(t, e, c)
	path := filepath.Join(cfg.Dir, "live.json")
	probe.checkpoint = func() {
		if err := os.Rename(path, path+".saved"); err != nil {
			t.Fatal(err)
		}
		if err := os.Mkdir(path, 0700); err != nil {
			t.Fatal(err)
		}
	}
	if err := e.reconcileOnce(); err == nil {
		t.Fatal("failure decision was not durable")
	}
	if len(n.writes) != 1 || c.writeCount() != 1 {
		t.Fatal("restore occurred before durable failure")
	}
	e.Close()
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(path+".saved", path); err != nil {
		t.Fatal(err)
	}
	e = openMulti(t, cfg, probe, n)
	step(t, e)
	if e.state.Pending == nil || e.state.Pending.DrainOutcome != "restored" || len(n.writes) != 2 {
		t.Fatal("restart did not recover durable removal")
	}
}

func TestSubmittedFailureKnownActivityResolvesUnknownOrLegacyRestore(t *testing.T) {
	for _, phase := range []string{"unknown-remove", "restore-intent", "unknown-restore"} {
		t.Run(phase, func(t *testing.T) {
			cfg, c, n, e := groupContractFixture(t, 2, 0, 2, false)
			id := submittedFailure(t, e, c)
			op := e.state.Pending
			op.Phase, op.Unknown = "unknown", true
			if phase != "unknown-remove" {
				op.ActivityStatus, op.InstanceResult = "FAILED", "FAILED"
				op.NodeModifyIndex = 0 // Older lost-ACK path discarded the prior index.
				if phase == "restore-intent" {
					op.Phase, op.Unknown = "restore-intent", false
				}
				v := n.nodes[id]
				v.Eligibility = "eligible"
				v.LastDrain.Status = "canceled"
				n.nodes[id] = v
				n.writes = append(n.writes, "restore:"+id)
			}
			if err := e.save(); err != nil {
				t.Fatal(err)
			}
			e.Close()
			e = openMulti(t, cfg, c, n)
			step(t, e)
			if e.state.Pending == nil || e.state.Pending.Phase != "failed-delete" || e.state.Pending.DrainOutcome != "restored" || e.state.Pending.Unknown || e.state.ManualReview || len(n.writes) != 2 || c.writeCount() != 1 {
				t.Fatal("known terminal failure did not resume safely")
			}
			e.Close()
			e = openMulti(t, cfg, c, n)
		})
	}
}

func TestSubmittedFailureRejectsInvalidDurableRecovery(t *testing.T) {
	for _, mode := range []string{"successful-activity", "running-instance", "no-activity", "unknown", "bad-drain-outcome", "absence-counter", "abort-reason"} {
		t.Run(mode, func(t *testing.T) {
			cfg, c, n, e := groupContractFixture(t, 2, 0, 2, false)
			submittedFailure(t, e, c)
			step(t, e)
			if e.state.Pending == nil {
				t.Fatal("missing recovery")
			}
			op := e.state.Pending
			switch mode {
			case "successful-activity":
				op.ActivityStatus = "SUCCESSFUL"
			case "running-instance":
				op.InstanceResult = "RUNNING"
			case "no-activity":
				op.ActivityID = ""
			case "unknown":
				op.Unknown = true
			case "bad-drain-outcome":
				op.DrainOutcome = "not-submitted"
			case "absence-counter":
				op.AbsentObservations = 1
			case "abort-reason":
				op.AbortReason = "drain-completion-timeout"
			}
			if err := e.save(); err != nil {
				t.Fatal(err)
			}
			e.Close()
			reopened, err := Open(cfg, c, n)
			if err == nil {
				reopened.Close()
				t.Fatal("invalid recovery state accepted")
			}
			if len(n.writes) != 2 || c.writeCount() != 1 {
				t.Fatal("open mutated external state")
			}
		})
	}
}
