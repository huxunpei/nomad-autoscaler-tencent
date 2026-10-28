package live

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadclient"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/tencentcloud"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type settlementCloud struct {
	*multiCloud
	history    []tencentcloud.Activity
	historyErr bool
}

func (c *settlementCloud) ActivitiesSince(ctx context.Context, g string, since time.Time) ([]tencentcloud.Activity, error) {
	if ctx.Err() != nil || c.historyErr {
		return nil, errors.New("history unavailable")
	}
	return copyJSON(c.history), nil
}

type settlementNomad struct {
	afterPost func()
	purged    map[string]bool
	*multiNomad
	dir               string
	postCalls, purges int
	lostPost          bool
}

func (n *settlementNomad) PolicyDigest() string {
	return "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
}
func (n *settlementNomad) SelectScaleIn(_ context.Context, ids []nomadclient.Identity, num int) ([]nomadclient.Identity, error) {
	return ids[:num], nil
}
func (n *settlementNomad) PreScaleIn(ctx context.Context, id nomadclient.Identity, op string, d time.Duration, before func() error) (nomadclient.MutationResult, error) {
	if err := before(); err != nil {
		return nomadclient.MutationResult{}, err
	}
	return n.StartOwnedDrain(ctx, id, op, d)
}
func (n *settlementNomad) RequiredTasksStopped(context.Context, string) (bool, error) {
	return true, nil
}
func (n *settlementNomad) PostScaleIn(_ context.Context, id nomadclient.Identity, _ string, before func() error) error {
	n.postCalls++
	if err := before(); err != nil {
		return err
	}
	raw, err := os.ReadFile(filepath.Join(n.dir, "live.json"))
	var s State
	if err != nil || json.Unmarshal(raw, &s) != nil || s.Pending == nil || (s.Pending.Phase != "failed-delete-post" && s.Pending.Phase != "post-scale") || (!failureTerminal(s.Pending.ActivityStatus) && s.Pending.ActivityStatus != "SUCCESSFUL") || s.Pending.AbsentObservations != 2 {
		return errors.New("post before durable settlement")
	}
	if n.purged == nil {
		n.purged = map[string]bool{}
	}
	if !n.purged[id.InstanceID] {
		n.purged[id.InstanceID] = true
		n.purges++
	}
	if n.afterPost != nil {
		f := n.afterPost
		n.afterPost = nil
		f()
	}
	if n.lostPost {
		n.lostPost = false
		return errors.New("lost post response")
	}
	return nil
}
func settlementFixture(t *testing.T, count int64) (Config, *settlementCloud, *settlementNomad, *Engine) {
	cfg, c, n, e := groupContractFixture(t, count, 0, count+1, false)
	e.Close()
	cloud := &settlementCloud{multiCloud: c}
	nomad := &settlementNomad{multiNomad: n, dir: cfg.Dir}
	e = openMulti(t, cfg, cloud, nomad)
	return cfg, cloud, nomad, e
}
func laterDeletion(t *testing.T, e *Engine, c *settlementCloud) {
	t.Helper()
	op := e.state.Pending
	stamp := op.StartedAt.Truncate(time.Second).Format(time.RFC3339)
	original := c.activities[op.ActivityID]
	original.Type = "REMOVE_INSTANCES"
	original.StartedAt = stamp
	original.EndedAt = stamp
	original.RequestIDs = []tencentcloud.RequestTrace{{Action: "DescribeAutoScalingActivities", RequestID: testRequest}}
	c.activities[op.ActivityID] = original
	later := original
	later.ID = "asa-later"
	later.Status = "SUCCESSFUL"
	later.RelatedInstances = []tencentcloud.RelatedInstance{{ID: op.InstanceID, Status: "SUCCESSFUL"}}
	c.history = []tencentcloud.Activity{original, later}
	kept := []tencentcloud.Member{}
	for _, m := range c.snap.Members {
		if m.ID != op.InstanceID {
			kept = append(kept, m)
		}
	}
	c.snap.Members = kept
	delete(c.snap.VMs, op.InstanceID)
	c.snap.Group.Count = int64(len(kept))
	c.snap.Group.Desired = int64(len(kept))
}
func TestFailedDeleteLaterRemovalClosesFailedBatchAndAllowsNextAction(t *testing.T) {
	for _, restoreFirst := range []bool{false, true} {
		t.Run(map[bool]string{false: "gone-before-restore", true: "restored-then-gone"}[restoreFirst], func(t *testing.T) {
			cfg, c, n, e := settlementFixture(t, 3)
			id := submittedFailure(t, e, c.multiCloud)
			opID := e.state.Pending.ID
			originalID := e.state.Pending.ActivityID
			batchID := e.state.Batch.ID
			if restoreFirst {
				step(t, e)
				if n.nodes[id].Eligibility != "eligible" {
					t.Fatal("survivor not restored")
				}
			}
			laterDeletion(t, e, c)
			step(t, e)
			if e.state.Pending == nil || n.postCalls != 0 {
				t.Fatal("one observation completed deletion")
			}
			e.Close()
			e = openMulti(t, cfg, c, n)
			step(t, e)
			if e.state.Pending != nil || e.state.Batch != nil || e.state.LastBatch == nil || e.state.LastBatch.Phase != "failed" || len(e.state.LastBatch.Results) != 1 || e.state.LastCompletedAt != 0 || e.state.LastBatch.CompletedAt != 0 || e.state.ManualReview || n.purges != 1 {
				t.Fatal("failed batch not automatically closed", e.state.LastEvent)
			}
			last := e.state.LastOperation
			if last.ID != opID || last.ActivityID != originalID || last.ActivityStatus != "FAILED" || last.InstanceResult != "FAILED" {
				t.Fatal("original failure overwritten")
			}
			if _, err := e.completionState(batchID); err == nil || err.Error() != "ScaleActionFailed" {
				t.Fatal("failed caller got success", err)
			}
			e.Close()
			e = openMulti(t, cfg, c, n)
			if ok, err := e.Scale(3); !ok || err != nil {
				t.Fatal("next action blocked", ok, err)
			}
			if len(c.writes) != 2 || c.writes[0] != "remove:"+id || c.writes[1] != "desired3" {
				t.Fatal("repeated deletion or unexpected compensation", c.writes)
			}
		})
	}
}
func TestFailedDeleteSettlementRejectsInsufficientEvidence(t *testing.T) {
	for _, mode := range []string{"history-error", "no-later", "same-id", "other-group", "other-instance", "other-type", "failed-later", "running", "missing-start", "before-operation", "future-end", "bad-trace", "vm-present", "desired-mismatch", "activity-running", "replacement-member", "retained-member"} {
		t.Run(mode, func(t *testing.T) {
			_, c, n, e := settlementFixture(t, 2)
			id := submittedFailure(t, e, c.multiCloud)
			step(t, e)
			laterDeletion(t, e, c)
			a := &c.history[1]
			switch mode {
			case "history-error":
				c.historyErr = true
			case "no-later":
				c.history = c.history[:1]
			case "same-id":
				a.ID = e.state.Pending.ActivityID
			case "other-group":
				a.GroupID = "asg-other"
			case "other-instance":
				a.RelatedInstances[0].ID = "ins-other"
			case "other-type":
				a.Type = "DETACH_INSTANCES"
			case "failed-later":
				a.Status = "FAILED"
				a.RelatedInstances[0].Status = "FAILED"
			case "running":
				a.Status = "RUNNING"
			case "missing-start":
				a.StartedAt = ""
			case "before-operation":
				a.StartedAt = "2020-01-01T00:00:00Z"
			case "future-end":
				a.EndedAt = "2099-01-01T00:00:00Z"
			case "bad-trace":
				a.RequestIDs = nil
			case "vm-present":
				c.snap.VMs[id] = tencentcloud.VM{ID: id, Returned: true, State: "RUNNING", ChargeType: "POSTPAID_BY_HOUR"}
			case "desired-mismatch":
				c.snap.Group.Desired++
			case "activity-running":
				c.snap.Group.Activity = "IN_ACTIVITY"
			case "replacement-member":
				c.snap.Members[0].ID = "ins-replacement"
			case "retained-member":
				c.snap.Members = append(c.snap.Members, tencentcloud.Member{ID: id, CreationType: "AUTO_CREATION", Lifecycle: "TERMINATION_FAILED"})
				c.snap.Group.Count++
			}
			for i := 0; i < 3; i++ {
				_ = e.reconcileOnce()
			}
			if e.state.Pending == nil || e.state.Batch == nil || e.state.LastCompletedAt != 0 || n.postCalls != 0 || len(c.writes) != 1 {
				t.Fatal("unsafe settlement", e.state.LastEvent, n.postCalls, c.writes)
			}
		})
	}
}
func TestFailedDeletePostLostResponseResumesWithoutAnotherCloudWrite(t *testing.T) {
	cfg, c, n, e := settlementFixture(t, 2)
	submittedFailure(t, e, c.multiCloud)
	step(t, e)
	laterDeletion(t, e, c)
	step(t, e)
	n.lostPost = true
	if err := e.reconcileOnce(); err == nil {
		t.Fatal("post error swallowed")
	}
	if e.state.Pending == nil || e.state.Pending.Phase != "failed-delete-post" || e.state.LastCompletedAt != 0 {
		t.Fatal("post obligation lost")
	}
	e.Close()
	e = openMulti(t, cfg, c, n)
	step(t, e)
	if e.state.Pending != nil || n.purges != 1 || n.postCalls != 2 || len(c.writes) != 1 || e.state.LastBatch.Phase != "failed" {
		t.Fatal("post restart duplicated cloud effect")
	}
}

func obstructSettlementJournal(t *testing.T, dir string) func() {
	t.Helper()
	p := filepath.Join(dir, "live.json")
	if err := os.Rename(p, p+".saved"); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(p, 0700); err != nil {
		t.Fatal(err)
	}
	return func() {
		if err := os.Remove(p); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(p+".saved", p); err != nil {
			t.Fatal(err)
		}
	}
}
func TestFailedDeleteSettlementPersistenceBarrierAndResume(t *testing.T) {
	for _, when := range []string{"before-post", "after-post"} {
		t.Run(when, func(t *testing.T) {
			cfg, c, n, e := settlementFixture(t, 2)
			submittedFailure(t, e, c.multiCloud)
			step(t, e)
			laterDeletion(t, e, c)
			step(t, e)
			var restore func()
			if when == "before-post" {
				restore = obstructSettlementJournal(t, cfg.Dir)
			} else {
				n.afterPost = func() { restore = obstructSettlementJournal(t, cfg.Dir) }
			}
			if err := e.reconcileOnce(); err == nil {
				t.Fatal("storage failure swallowed")
			}
			if when == "before-post" && n.purges != 0 {
				t.Fatal("purged before durable barrier")
			}
			if e.state.LastCompletedAt != 0 || len(c.writes) != 1 {
				t.Fatal("failure changed completion/cloud")
			}
			e.Close()
			restore()
			e = openMulti(t, cfg, c, n)
			step(t, e)
			if e.state.Pending != nil || e.state.LastBatch.Phase != "failed" || n.purges != 1 || len(c.writes) != 1 {
				t.Fatal("storage recovery failed or duplicated effects")
			}
		})
	}
}
func TestFailedDeleteSettlementResetsInterruptedObservations(t *testing.T) {
	_, c, n, e := settlementFixture(t, 2)
	submittedFailure(t, e, c.multiCloud)
	step(t, e)
	laterDeletion(t, e, c)
	step(t, e)
	if e.state.Pending.AbsentObservations != 1 {
		t.Fatal("missing first observation")
	}
	c.historyErr = true
	step(t, e)
	if e.state.Pending.AbsentObservations != 0 || e.state.Pending.DeleteSettlement != nil {
		t.Fatal("retained stale proof")
	}
	c.historyErr = false
	step(t, e)
	if e.state.Pending == nil || n.postCalls != 0 {
		t.Fatal("one fresh observation reused stale proof")
	}
	step(t, e)
	if e.state.Pending != nil {
		t.Fatal("two fresh observations did not settle")
	}
}
func TestFailedDeleteSettlementRetainsEarlierSuccessAndStopsRemainingChildren(t *testing.T) {
	cfg, c, n, e := settlementFixture(t, 4)
	// A successful earlier child must remain recorded when the next child fails.
	if _, err := e.Scale(1); err != nil {
		t.Fatal(err)
	}
	step(t, e)
	c.terminal(true)
	step(t, e)
	// The probe below accepts the ordinary post barrier as well.
	step(t, e)
	if e.state.Pending != nil || len(e.state.Batch.Results) != 1 {
		t.Fatal("first child incomplete")
	}
	first := e.state.LastOperation.ID
	step(t, e)
	step(t, e)
	c.terminal(false)
	step(t, e)
	failedID := e.state.Pending.InstanceID
	laterDeletion(t, e, c)
	step(t, e)
	step(t, e)
	if e.state.Pending != nil || e.state.Batch != nil || len(e.state.LastBatch.Results) != 2 || e.state.LastBatch.Results[0].ID != first || e.state.LastBatch.Results[1].InstanceID != failedID || e.state.LastBatch.Phase != "failed" || e.state.LastCompletedAt != 0 {
		t.Fatal("lost first result or completed original target")
	}
	drains := 0
	for _, w := range n.writes {
		if len(w) > 6 && w[:6] == "drain:" {
			drains++
		}
	}
	if drains != 2 || len(c.writes) != 2 {
		t.Fatal("advanced unprocessed child", drains, c.writes)
	}
	e.Close()
	e = openMulti(t, cfg, c, n)
	if changed, err := e.Scale(3); !changed || err != nil {
		t.Fatal("settled failed batch blocked next evaluation", err)
	}
}

func TestFailedDeleteSettlementMalformedJournalRejected(t *testing.T) {
	for _, mode := range []string{"wrong-original", "same-activity", "wrong-instance", "missing-trace", "missing-first-observation", "two-identical-observations", "wrong-phase", "post-without-sdk", "evidence-without-observation"} {
		t.Run(mode, func(t *testing.T) {
			cfg, c, n, e := settlementFixture(t, 2)
			submittedFailure(t, e, c.multiCloud)
			step(t, e)
			laterDeletion(t, e, c)
			step(t, e)
			e.Close()
			file := filepath.Join(cfg.Dir, "live.json")
			raw, err := os.ReadFile(file)
			if err != nil {
				t.Fatal(err)
			}
			var s State
			if json.Unmarshal(raw, &s) != nil {
				t.Fatal("invalid source journal")
			}
			op := s.Pending
			switch mode {
			case "wrong-original":
				op.DeleteSettlement.FailedActivity.ID = "asa-wrong"
			case "same-activity":
				op.DeleteSettlement.RemovalActivity.ID = op.ActivityID
			case "wrong-instance":
				op.DeleteSettlement.RemovalActivity.RelatedInstances[0].ID = "ins-other"
			case "missing-trace":
				op.DeleteSettlement.RemovalActivity.RequestIDs = nil
			case "missing-first-observation":
				op.DeleteSettlement.FirstObservedAt = time.Time{}
			case "two-identical-observations":
				op.AbsentObservations = 2
			case "wrong-phase":
				op.Phase = "draining"
			case "post-without-sdk":
				op.SDKLifecycle = false
				op.Phase = "failed-delete-post"
			case "evidence-without-observation":
				op.AbsentObservations = 0
			}
			raw, _ = json.Marshal(s)
			if err = os.WriteFile(file, raw, 0600); err != nil {
				t.Fatal(err)
			}
			reopened, err := Open(cfg, c, n)
			if err == nil {
				reopened.Close()
				t.Fatal("malformed settlement journal accepted")
			}
		})
	}
}
