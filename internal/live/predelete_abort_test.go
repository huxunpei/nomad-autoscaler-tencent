package live

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadclient"
)

func TestPreDeleteAbortRestoresSurvivorAndAllowsNextAction(t *testing.T) {
	cfg, c, n, e := groupContractFixture(t, 2, 0, 2, true)
	if _, err := e.Scale(1); err != nil {
		t.Fatal(err)
	}
	id := e.state.Pending.InstanceID
	c.snap.Group.Min = 2
	step(t, e)
	s, _, _ := e.Snapshot()
	if c.writeCount() != 0 || n.nodes[id].Eligibility != "eligible" || s.Pending != nil || s.Batch != nil || s.ManualReview || s.LastBatch == nil || s.LastBatch.Phase != "failed" || s.LastCompletedAt != 0 {
		t.Fatalf("pre-delete abort did not restore and finish as failure: node=%+v state=%+v", n.nodes[id], s)
	}
	e.Close()
	c.snap.Group.Min = 0
	e = openMulti(t, cfg, c, n)
	if changed, err := e.Scale(1); !changed || err != nil {
		t.Fatalf("next evaluation remains blocked: %v %v", changed, err)
	}
	completeMultiDelete(t, e, c)
	if c.writeCount() != 1 {
		t.Fatal("duplicate deletion")
	}
}

func TestPreDeleteAbortBetweenChildrenPreservesSuccessfulDeletion(t *testing.T) {
	cfg, c, n, e := groupContractFixture(t, 3, 0, 3, false)
	if _, err := e.Scale(0); err != nil {
		t.Fatal(err)
	}
	completeMultiDelete(t, e, c)
	c.snap.Group.Min = 2
	step(t, e)
	s, _, _ := e.Snapshot()
	if s.Pending != nil || s.Batch != nil || s.LastBatch == nil || s.LastBatch.Phase != "failed" || s.ManualReview || s.LastCompletedAt != 0 || c.writeCount() != 1 || len(n.writes) != 1 {
		t.Fatalf("batch remained locked or drained another child after limits changed: %+v writes=%v", s, n.writes)
	}
	if len(s.LastBatch.Results) != 2 || s.LastBatch.Results[0].ActivityStatus != "SUCCESSFUL" {
		t.Fatal("lost successful child result")
	}
	e.Close()
	e = openMulti(t, cfg, c, n)
	c.snap.Group.Min = 0
	if changed, err := e.Scale(1); !changed || err != nil {
		t.Fatal("failed batch blocked next action", changed, err)
	}
	e.Close()
	e = openMulti(t, cfg, c, n)
	if e.state.Pending == nil {
		t.Fatal("new action did not survive reopening with a prior aborted batch")
	}
}

type abortNomad struct {
	*multiNomad
	dir             string
	loseAck, refuse bool
	beforeRead      func()
	afterRestore    func()
}

func TestPreDeleteAbortDisabledBetweenChildren(t *testing.T) {
	cfg, c, n, e := groupContractFixture(t, 3, 0, 3, false)
	if _, err := e.Scale(0); err != nil {
		t.Fatal(err)
	}
	completeMultiDelete(t, e, c)
	c.snap.Group.Enabled = "DISABLED"
	step(t, e)
	s, ready, err := e.Snapshot()
	if err != nil || ready || s.Pending != nil || s.Batch != nil || s.LastBatch == nil || s.LastBatch.Phase != "failed" ||
		s.LastCompletedAt != 0 || c.writeCount() != 1 || len(n.writes) != 1 || len(s.LastBatch.Results) != 2 || s.LastBatch.Results[0].ActivityStatus != "SUCCESSFUL" {
		t.Fatalf("disabled group advanced a batch or lost the completed child: %+v err=%v", s, err)
	}
	e.Close()
	c.snap.Group.Enabled = "ENABLED"
	e = openMulti(t, cfg, c, n)
	if changed, err := e.Scale(1); !changed || err != nil {
		t.Fatal("re-enabled group did not resume normal evaluation", changed, err)
	}
}

func (n *abortNomad) ReadNode(ctx context.Context, id string) (nomadclient.Node, error) {
	if n.beforeRead != nil {
		f := n.beforeRead
		n.beforeRead = nil
		f()
	}
	return n.multiNomad.ReadNode(ctx, id)
}
func (n *abortNomad) RestoreOwnedDrain(ctx context.Context, id nomadclient.Identity, op string) (nomadclient.MutationResult, error) {
	raw, err := os.ReadFile(filepath.Join(n.dir, "live.json"))
	var s State
	if err != nil || json.Unmarshal(raw, &s) != nil || s.Pending == nil || s.Pending.ID != op || s.Pending.Phase != "aborting" || s.Pending.AbortReason == "" || s.Pending.RequestID != "" || s.Pending.ActivityID != "" {
		return nomadclient.MutationResult{}, errors.New("restore preceded durable abort decision")
	}
	if n.refuse {
		return nomadclient.MutationResult{}, errors.New("temporary read failure before write")
	}
	result, err := n.multiNomad.RestoreOwnedDrain(ctx, id, op)
	if n.afterRestore != nil {
		f := n.afterRestore
		n.afterRestore = nil
		f()
	}
	if n.loseAck {
		return nomadclient.MutationResult{WriteAttempted: true, Unknown: true}, errors.New("lost restore acknowledgement")
	}
	return result, err
}

func TestPreDeleteAbortLostRestoreACKReconcilesAfterRestart(t *testing.T) {
	cfg, c, n, e := groupContractFixture(t, 2, 0, 2, true)
	e.Close()
	probe := &abortNomad{multiNomad: n, dir: cfg.Dir, loseAck: true}
	e = openMulti(t, cfg, c, probe)
	if _, err := e.Scale(1); err != nil {
		t.Fatal(err)
	}
	id := e.state.Pending.InstanceID
	c.snap.Group.Min = 2
	step(t, e)
	if e.state.Pending == nil || e.state.Pending.Phase != "aborting" || n.nodes[id].Eligibility != "eligible" || len(n.writes) != 2 {
		t.Fatal("lost acknowledgement dropped restoration obligation")
	}
	e.Close()
	e = openMulti(t, cfg, c, probe)
	step(t, e)
	if e.state.Pending != nil || e.state.ManualReview || e.state.LastOperation.DrainOutcome != "restored" || len(n.writes) != 2 || c.writeCount() != 0 {
		t.Fatal("readback duplicated restore or cloud write")
	}
	e.Close()
	e = openMulti(t, cfg, c, probe)
	if e.state.LastBatch.Phase != "failed" {
		t.Fatal("abort result was not durable")
	}
}

func TestPreDeleteAbortRecoversDespiteChangedGuardOrMembership(t *testing.T) {
	for _, mode := range []string{"disabled", "launch-config", "member-left", "other-unmapped"} {
		t.Run(mode, func(t *testing.T) {
			cfg, c, n, e := groupContractFixture(t, 2, 0, 3, false)
			if _, err := e.Scale(1); err != nil {
				t.Fatal(err)
			}
			id := e.state.Pending.InstanceID
			switch mode {
			case "disabled":
				c.snap.Group.Enabled = "DISABLED"
			case "launch-config":
				c.snap.LaunchConfiguration.VersionNumber++
			case "member-left":
				c.snap.Members = c.snap.Members[1:]
				c.snap.Group.Count = 1
				c.snap.Group.Desired = 1
			case "other-unmapped":
				delete(n.nodes, "ins-multi1")
			}
			e.Close()
			e = openMulti(t, cfg, c, n)
			_ = e.reconcileOnce()
			if c.writeCount() != 0 || n.nodes[id].Eligibility != "eligible" || e.state.Pending != nil || e.state.ManualReview || e.state.LastOperation.AbortReason == "" {
				t.Fatal("unrelated group guard prevented owned restoration")
			}
			if e.state.LastCompletedAt != 0 {
				t.Fatal("abort published completion time")
			}
		})
	}
}

func TestPreDeleteAbortNeverRestoresUnconfirmedOrForeignNode(t *testing.T) {
	for _, mode := range []string{"stopped-vm", "foreign-owner", "changed-identity", "cloud-read-error"} {
		t.Run(mode, func(t *testing.T) {
			_, c, n, e := groupContractFixture(t, 2, 0, 2, false)
			if _, err := e.Scale(1); err != nil {
				t.Fatal(err)
			}
			id := e.state.Pending.InstanceID
			if err := e.recordPreDeleteAbort("pre-delete-group-limits-changed"); err != nil {
				t.Fatal(err)
			}
			node := copyJSON(n.nodes[id])
			vm := c.snap.VMs[id]
			switch mode {
			case "stopped-vm":
				v := vm
				v.State = "STOPPED"
				c.snap.VMs[id] = v
			case "foreign-owner":
				v := copyJSON(node)
				v.LastDrain.Owner = "operator"
				n.nodes[id] = v
			case "changed-identity":
				v := copyJSON(node)
				v.InstanceID = "ins-other"
				n.nodes[id] = v
			case "cloud-read-error":
				c.observeFailure = true
			}
			_ = e.reconcileOnce()
			if len(n.writes) != 1 || c.writeCount() != 0 || e.state.Pending == nil {
				t.Fatal("unsafe abort restored or lost obligation")
			}
			n.nodes[id] = node
			c.snap.VMs[id] = vm
			c.observeFailure = false
			step(t, e)
			if e.state.Pending != nil || n.nodes[id].Eligibility != "eligible" {
				t.Fatal("observation recovery required manual unlock")
			}
		})
	}
}

func TestPreDeleteAbortDurableIntentRequiredBeforeRestore(t *testing.T) {
	cfg, c, n, e := groupContractFixture(t, 2, 0, 2, false)
	e.Close()
	probe := &abortNomad{multiNomad: n, dir: cfg.Dir}
	e = openMulti(t, cfg, c, probe)
	if _, err := e.Scale(1); err != nil {
		t.Fatal(err)
	}
	c.snap.Group.Min = 2
	probe.beforeRead = func() {
		path := filepath.Join(cfg.Dir, "live.json")
		if err := os.Rename(path, path+".saved"); err != nil {
			t.Fatal(err)
		}
		if err := os.Mkdir(path, 0700); err != nil {
			t.Fatal(err)
		}
	}
	if err := e.reconcileOnce(); err == nil {
		t.Fatal("persistence failure ignored")
	}
	if len(n.writes) != 1 || c.writeCount() != 0 {
		t.Fatal("restore wrote before failed durable abort")
	}
}

func TestPreDeleteAbortDrainTimeoutRestoresWithoutDeleting(t *testing.T) {
	_, c, n, e := groupContractFixture(t, 2, 0, 2, false)
	if _, err := e.Scale(1); err != nil {
		t.Fatal(err)
	}
	id := e.state.Pending.InstanceID
	e.state.Pending.StartedAt = time.Now().Add(-3 * time.Minute)
	n.nodes[id].LastDrain.Status = "draining"
	step(t, e)
	if e.state.Pending != nil || e.state.LastOperation.AbortReason != "drain-completion-timeout" || n.nodes[id].Eligibility != "eligible" || c.writeCount() != 0 {
		t.Fatal("timed out drain was left stuck or deleted")
	}
}

func TestPreDeleteAbortFinalSaveFailureRecoversFromDurableDecision(t *testing.T) {
	cfg, c, n, e := groupContractFixture(t, 2, 0, 2, false)
	e.Close()
	probe := &abortNomad{multiNomad: n, dir: cfg.Dir}
	e = openMulti(t, cfg, c, probe)
	if _, err := e.Scale(1); err != nil {
		t.Fatal(err)
	}
	c.snap.Group.Min = 2
	path := filepath.Join(cfg.Dir, "live.json")
	probe.afterRestore = func() {
		if err := os.Rename(path, path+".saved"); err != nil {
			t.Fatal(err)
		}
		if err := os.Mkdir(path, 0700); err != nil {
			t.Fatal(err)
		}
	}
	if e.reconcileOnce() == nil {
		t.Fatal("final save unexpectedly succeeded")
	}
	if e.state.LastCompletedAt != 0 || len(n.writes) != 2 || c.writeCount() != 0 {
		t.Fatal("failed save reported success or extra mutation")
	}
	e.Close()
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(path+".saved", path); err != nil {
		t.Fatal(err)
	}
	e = openMulti(t, cfg, c, probe)
	step(t, e)
	if e.state.Pending != nil || e.state.ManualReview || len(n.writes) != 2 || e.state.LastOperation.DrainOutcome != "restored" {
		t.Fatal("restart failed to reconcile already restored node")
	}
}

type abortSDK struct {
	*abortNomad
	rejectPre bool
}

func (n *abortSDK) PolicyDigest() string { return strings.Repeat("b", 64) }
func (n *abortSDK) SelectScaleIn(_ context.Context, ids []nomadclient.Identity, count int) ([]nomadclient.Identity, error) {
	return ids[:count], nil
}
func (n *abortSDK) PreScaleIn(ctx context.Context, id nomadclient.Identity, op string, d time.Duration, before func() error) (nomadclient.MutationResult, error) {
	if err := before(); err != nil {
		return nomadclient.MutationResult{}, err
	}
	if n.rejectPre {
		return nomadclient.MutationResult{}, errors.New("pre rejected before drain")
	}
	return n.StartOwnedDrain(ctx, id, op, d)
}
func (n *abortSDK) PostScaleIn(_ context.Context, _ nomadclient.Identity, _ string, before func() error) error {
	return before()
}
func (n *abortSDK) RequiredTasksStopped(context.Context, string) (bool, error) { return true, nil }

func TestPreDeleteAbortSDKPreRejectionClosesFailedBatch(t *testing.T) {
	cfg, c, n, e := groupContractFixture(t, 2, 0, 2, false)
	e.Close()
	probe := &abortSDK{abortNomad: &abortNomad{multiNomad: n, dir: cfg.Dir}, rejectPre: true}
	e = openMulti(t, cfg, c, probe)
	if changed, err := e.ScaleAndWait(1, 2); changed || err == nil || err.Error() != "SDKDrainNotSubmitted" {
		t.Fatal(changed, err)
	}
	if e.state.Pending != nil || e.state.Batch != nil || e.state.LastBatch.Phase != "failed" || e.state.LastOperation.DrainOutcome != "not-submitted" || len(n.writes) != 0 || c.writeCount() != 0 {
		t.Fatal("unsubmitted pre did not finish without writes")
	}
	e.Close()
	e = openMulti(t, cfg, c, probe)
	probe.rejectPre = false
	if changed, err := e.Scale(1); !changed || err != nil {
		t.Fatal("pre failure poisoned later evaluation", changed, err)
	}
}

func TestPreDeleteAbortJournalRejectsForgedCloudOrDrainOutcomes(t *testing.T) {
	_, _, _, e := groupContractFixture(t, 2, 0, 2, false)
	if _, err := e.Scale(1); err != nil {
		t.Fatal(err)
	}
	if err := e.recordPreDeleteAbort("pre-delete-group-limits-changed"); err != nil {
		t.Fatal(err)
	}
	saved := copyJSON(e.state)
	for _, mode := range []string{"cloud-request", "cloud-activity", "unknown", "missing-reason", "pending-outcome", "zero-drain-index", "legacy-version"} {
		t.Run(mode, func(t *testing.T) {
			e.state = copyJSON(saved)
			switch mode {
			case "cloud-request":
				e.state.Pending.RequestID = testRequest
			case "cloud-activity":
				e.state.Pending.ActivityID = testActivity
			case "unknown":
				e.state.Pending.Unknown = true
			case "missing-reason":
				e.state.Pending.AbortReason = ""
			case "pending-outcome":
				e.state.Pending.DrainOutcome = "restored"
			case "zero-drain-index":
				e.state.Pending.NodeModifyIndex = 0
			case "legacy-version":
				e.state.Version = 2
				e.state.MaxCapacity = 2
			}
			if e.validateState() == nil {
				t.Fatal("invalid abort evidence accepted")
			}
		})
	}
	e.state = saved
}
