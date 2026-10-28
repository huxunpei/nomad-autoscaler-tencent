package live

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/huxunpei/nomad-autoscaler-tencent/internal/tencentcloud"
)

type failedGrowthCloud struct {
	*multiCloud
	history   []tencentcloud.Activity
	complete  tencentcloud.Snapshot
	calls     int
	onHistory func(int)
	onDesired func()
}

func (c *failedGrowthCloud) SetDesired(ctx context.Context, g string, target int64) (tencentcloud.WriteResult, error) {
	r, err := c.multiCloud.SetDesired(ctx, g, target)
	if err == nil && c.onDesired != nil {
		c.onDesired()
	}
	return r, err
}
func (c *failedGrowthCloud) ActivitiesSince(_ context.Context, _ string, _ time.Time) ([]tencentcloud.Activity, error) {
	c.calls++
	if c.onHistory != nil {
		c.onHistory(c.calls)
	}
	return copyJSON(c.history), nil
}

func failedGrowthFixture(t *testing.T, unknown bool) (Config, *failedGrowthCloud, *multiNomad, *Engine) {
	t.Helper()
	cfg, base, n, e := groupContractFixture(t, 1, 0, 5, false)
	e.Close()
	c := &failedGrowthCloud{multiCloud: base}
	e = openMulti(t, cfg, c, n)
	if _, err := e.Scale(5); err != nil {
		t.Fatal(err)
	}
	op := e.state.Pending
	// Model an asynchronous activity starting after the acknowledged request.
	// Keep the real clock boundary instead of backdating durable write intents.
	time.Sleep(1100 * time.Millisecond)
	stamp := time.Now().UTC().Format(time.RFC3339)
	for i := 1; i < len(c.snap.Members); i++ {
		c.snap.Members[i].AddedAt = stamp
	}
	a := tencentcloud.Activity{ID: "asa-partialgrow", GroupID: testGroup, Type: "SCALE_OUT", Status: "PARTIALLY_SUCCESSFUL", StartedAt: stamp, EndedAt: stamp, RelatedInstances: []tencentcloud.RelatedInstance{{ID: "ins-multi1", Status: "SUCCESSFUL"}, {ID: "ins-multi2", Status: "SUCCESSFUL"}}, RequestIDs: []tencentcloud.RequestTrace{{Action: "DescribeAutoScalingActivities", RequestID: testRequest}}}
	c.history = []tencentcloud.Activity{a}
	c.complete = copyJSON(c.snap)
	c.snap.Members = c.snap.Members[:3]
	delete(c.snap.VMs, "ins-multi3")
	delete(c.snap.VMs, "ins-multi4")
	c.snap.Group.Count = 3
	c.snap.Group.Desired = 5
	c.snap.Group.RetryPolicy = "INCREMENTAL_INTERVALS"
	if unknown {
		op.RequestID = ""
		if err := e.unknown("growth-response-lost"); err != ErrUnknown {
			t.Fatal(err)
		}
	}
	return cfg, c, n, e
}

func TestFailedGrowthCloudRetrySettlesFailureWithoutSuccessCooldown(t *testing.T) {
	cfg, c, n, e := failedGrowthFixture(t, false)
	oldID := e.state.Batch.ID
	step(t, e)
	first := copyJSON(*e.state.Pending)
	// This changes the synthetic provider's observation only. The plugin must
	// issue no compensating Desired update and must retain its original failure.
	c.snap = copyJSON(c.complete)
	c.snap.Group.RetryPolicy = "INCREMENTAL_INTERVALS"
	step(t, e)
	s := multiState(t, e)
	if s.Pending != nil || s.Batch != nil || s.LastBatch == nil || s.LastBatch.ID != oldID || s.LastBatch.Phase != "failed" || s.LastCompletedAt != 0 || s.LastBatch.CompletedAt != 0 || s.LastOperation == nil || !reflect.DeepEqual(s.LastOperation.GrowthFailure, first.GrowthFailure) || c.writeCount() != 1 || len(n.writes) != 0 || e.checkRecoveryAudit() != nil {
		t.Fatal("retry lost failed outcome or replayed cloud write")
	}
	if _, err := e.completionState(oldID); err == nil || err.Error() != "ScaleActionFailed" {
		t.Fatal("retry converted failed foreground action to success", err)
	}
	e.Close()
	e, err := Open(cfg, c, n)
	if err != nil {
		t.Fatal("settled failure cannot restart", err)
	}
	defer e.Close()
	_, ready, err := e.Snapshot()
	if err != nil || !ready {
		t.Fatal("settled cloud remains blocked", ready, err)
	}
	if changed, err := e.Scale(4); !changed || err != nil {
		t.Fatal("next normal action blocked", changed, err)
	}
	if c.writeCount() != 1 {
		t.Fatal("next shrink bypassed drain")
	}
}

func TestFailedGrowthLostResponsePreservesOriginalRequestIdentity(t *testing.T) {
	_, c, _, e := failedGrowthFixture(t, true)
	defer e.Close()
	step(t, e)
	if e.state.Pending == nil || e.state.Pending.Unknown || e.state.Pending.RequestID != "" || e.state.Pending.Phase != "failed-grow" || e.state.Pending.GrowthFailure == nil || c.writeCount() != 1 || e.checkRecoveryAudit() != nil {
		t.Fatal("unknown grow failure not classified, or query RequestId adopted")
	}
}

func TestFailedGrowthUnknownWriteNeedsObservedDesiredEffect(t *testing.T) {
	_, c, _, e := failedGrowthFixture(t, true)
	defer e.Close()
	// No observation proves that this request changed the group's Desired.
	// A later activity alone cannot resolve its unknown acceptance outcome.
	c.snap.Group.Desired = 1
	c.snap.Group.Count = 1
	c.snap.Members = c.snap.Members[:1]
	delete(c.snap.VMs, "ins-multi1")
	delete(c.snap.VMs, "ins-multi2")
	_ = e.reconcileOnce()
	if e.state.Pending == nil || !e.state.Pending.Unknown || e.state.Pending.GrowthFailure != nil || e.state.RecoveryAudit != nil || c.writeCount() != 1 {
		t.Fatal("unsubmitted/unknown write borrowed a later failure")
	}
}

func TestFailedGrowthKeepsActualCountVisibleWhenNewVMHasNotRegistered(t *testing.T) {
	_, _, n, e := failedGrowthFixture(t, false)
	defer e.Close()
	delete(n.nodes, "ins-multi1")
	step(t, e)
	s, ready, err := e.Snapshot()
	if err != nil || ready || len(s.Members) != 3 || s.Pending == nil || s.Pending.Phase != "failed-grow" || s.ErrorCode != "NomadMemberMappingInvalid" {
		t.Fatal("failure classification hid known cloud capacity", ready, err, s.ErrorCode)
	}
}

func TestFailedGrowthRejectsActivityFromEarlierWithinSameSecond(t *testing.T) {
	_, c, _, e := failedGrowthFixture(t, false)
	defer e.Close()
	c.history[0].StartedAt = e.state.Pending.CloudIntent.At.Truncate(time.Second).UTC().Format(time.RFC3339)
	step(t, e)
	if e.state.Pending == nil || e.state.Pending.Phase != "growing" || e.state.Pending.GrowthFailure != nil || e.state.RecoveryAudit != nil {
		t.Fatal("second-precision old activity adopted as evidence of this write")
	}
}

func TestFailedGrowthDoesNotClassifyAmbiguousOrIncompleteEvidence(t *testing.T) {
	for _, mode := range []string{"empty", "two-activities", "wrong-group", "wrong-type", "running", "missing-end", "old-start", "future-end", "missing-trace", "running-child", "changed-on-second-read", "no-cloud-intent"} {
		t.Run(mode, func(t *testing.T) {
			_, c, _, e := failedGrowthFixture(t, false)
			defer e.Close()
			switch mode {
			case "empty":
				c.history = nil
			case "two-activities":
				c.history = append(c.history, copyJSON(c.history[0]))
				c.history[1].ID = "asa-other"
			case "wrong-group":
				c.history[0].GroupID = "asg-other"
			case "wrong-type":
				c.history[0].Type = "ATTACH_INSTANCES"
			case "running":
				c.history[0].Status = "RUNNING"
			case "missing-end":
				c.history[0].EndedAt = ""
			case "old-start":
				c.history[0].StartedAt = e.state.Pending.StartedAt.Add(-time.Minute).Format(time.RFC3339)
			case "future-end":
				c.history[0].EndedAt = time.Now().Add(time.Hour).Format(time.RFC3339)
			case "missing-trace":
				c.history[0].RequestIDs = nil
			case "running-child":
				c.history[0].RelatedInstances[0].Status = "RUNNING"
			case "changed-on-second-read":
				c.onHistory = func(i int) {
					if i == 2 {
						c.history[0].ID = "asa-swapped"
					}
				}
			case "no-cloud-intent":
				e.state.Pending.CloudIntent = nil
			}
			step(t, e)
			if e.state.Pending == nil || e.state.Pending.Phase != "growing" || e.state.Pending.GrowthFailure != nil || e.state.RecoveryAudit != nil || e.state.LastCompletedAt != 0 || c.writeCount() != 1 {
				t.Fatal("unproven activity claimed as original failure")
			}
		})
	}
}

func TestFailedGrowthSettlementRetainsObligationOnDrift(t *testing.T) {
	for _, mode := range []string{"desired", "count", "active", "vm-pending", "baseline-replaced", "old-new-member", "limits"} {
		t.Run(mode, func(t *testing.T) {
			_, c, _, e := failedGrowthFixture(t, false)
			defer e.Close()
			step(t, e)
			c.snap = copyJSON(c.complete)
			switch mode {
			case "desired":
				c.snap.Group.Desired = 4
			case "count":
				c.snap.Group.Count = 4
			case "active":
				c.snap.Group.Activity = "IN_ACTIVITY"
			case "vm-pending":
				v := c.snap.VMs["ins-multi1"]
				v.State = "PENDING"
				c.snap.VMs[v.ID] = v
			case "baseline-replaced":
				c.snap.Members = c.snap.Members[1:]
				c.snap.Group.Count = 4
				c.snap.Group.Desired = 4
			case "old-new-member":
				c.snap.Members[1].AddedAt = e.state.Pending.StartedAt.Add(-time.Hour).Format(time.RFC3339)
			case "limits":
				c.snap.Group.Max = 4
			}
			_ = e.reconcileOnce()
			if e.state.Pending == nil || e.state.Pending.Phase != "failed-grow" || e.state.Batch == nil || e.state.LastCompletedAt != 0 || c.writeCount() != 1 {
				t.Fatal("drifting cloud released original operation")
			}
		})
	}
}

func TestFailedGrowthForegroundReturnsKnownFailureBeforePollExhaustion(t *testing.T) {
	cfg, base, n, old := groupContractFixture(t, 1, 0, 5, false)
	old.Close()
	cfg.StabilizationInterval = time.Millisecond
	c := &failedGrowthCloud{multiCloud: base}
	e := openMulti(t, cfg, c, n)
	defer e.Close()
	c.onDesired = func() {
		time.Sleep(1100 * time.Millisecond)
		stamp := time.Now().UTC().Format(time.RFC3339)
		c.history = []tencentcloud.Activity{{ID: "asa-quota", GroupID: testGroup, Type: "SCALE_OUT", Status: "PARTIALLY_SUCCESSFUL", StartedAt: stamp, EndedAt: stamp, RelatedInstances: []tencentcloud.RelatedInstance{{ID: "ins-multi1", Status: "SUCCESSFUL"}, {ID: "ins-multi2", Status: "SUCCESSFUL"}}, RequestIDs: []tencentcloud.RequestTrace{{Action: "DescribeAutoScalingActivities", RequestID: testRequest}}}}
		c.snap.Members = c.snap.Members[:3]
		delete(c.snap.VMs, "ins-multi3")
		delete(c.snap.VMs, "ins-multi4")
		c.snap.Group.Count = 3
	}
	changed, err := e.ScaleAndWait(5, 10)
	if !changed || err == nil || err.Error() != "ScaleActionFailed" || e.state.Pending == nil || e.state.Pending.Phase != "failed-grow" || e.waiting || e.actionCtx != nil || e.state.LastCompletedAt != 0 || c.writeCount() != 1 {
		t.Fatal("foreground did not return known failure and retain worker obligation", changed, err)
	}
}

func TestFailedGrowthAuditFactsRemainValidAfterAging(t *testing.T) {
	_, c, _, e := failedGrowthFixture(t, false)
	defer e.Close()
	step(t, e)
	classify, err := readRecord(e.cfg.Dir, *e.state.RecoveryAudit)
	if err != nil {
		t.Fatal(err)
	}
	c.snap = copyJSON(c.complete)
	step(t, e)
	settle, err := readRecord(e.cfg.Dir, *e.state.RecoveryAudit)
	if err != nil {
		t.Fatal(err)
	}
	for _, record := range []recoveryRecord{classify, settle} {
		var data any
		if json.Unmarshal(jsonBytes(record), &data) != nil {
			t.Fatal("record encoding")
		}
		var age func(any) any
		age = func(v any) any {
			switch x := v.(type) {
			case string:
				if stamp, err := time.Parse(time.RFC3339Nano, x); err == nil && !stamp.IsZero() {
					return stamp.Add(-time.Hour).Format(time.RFC3339Nano)
				}
				return x
			case map[string]any:
				for k, value := range x {
					x[k] = age(value)
				}
				return x
			case []any:
				for i, value := range x {
					x[i] = age(value)
				}
				return x
			default:
				return v
			}
		}
		raw, err := json.Marshal(age(data))
		var aged recoveryRecord
		if err != nil || json.Unmarshal(raw, &aged) != nil {
			t.Fatal("aged encoding", err)
		}
		aged.BeforeSHA256 = sha(jsonBytes(aged.Before))
		aged.Plan.JournalSHA256 = aged.BeforeSHA256
		aged.Plan.Digest = planDigest(aged.Plan)
		if !validRecoveryRecordOrigin(aged) {
			t.Fatal("durable historical proof incorrectly depends on runtime freshness", record.Plan.Decision.Action)
		}
	}
}

func TestFailedGrowthMalformedAuditDoesNotBecomeAnAutomaticTransaction(t *testing.T) {
	_, _, _, e := failedGrowthFixture(t, false)
	defer e.Close()
	step(t, e)
	record, err := readRecord(e.cfg.Dir, *e.state.RecoveryAudit)
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"no-after", "no-evidence", "missing-discovery", "manual", "request-identity", "scope", "evidence-after-plan"} {
		t.Run(mode, func(t *testing.T) {
			r := copyJSON(record)
			switch mode {
			case "no-after":
				r.After.Pending = nil
			case "no-evidence":
				r.Evidence = nil
			case "missing-discovery":
				r.Evidence[1].Discovery = nil
				r.Plan.Evidence = r.Evidence
			case "manual":
				r.Automatic = false
			case "request-identity":
				r.After.Pending.RequestID = "23456789-1234-1234-1234-123456789abc"
			case "scope":
				r.Plan.Scope.GroupID = "asg-other"
			case "evidence-after-plan":
				r.Evidence[1].Discovery.ObservedAt = r.Plan.CreatedAt.Add(time.Hour)
				r.Plan.Evidence = r.Evidence
				r.After.Pending.GrowthFailure = r.Evidence[1].Discovery
			}
			if validRecoveryRecordOrigin(r) {
				t.Fatal("malformed evidence became automatically repairable")
			}
		})
	}
}

func TestFailedGrowthReturnsFailureAndRetainsCloudObligation(t *testing.T) {
	cfg, c, n, e := failedGrowthFixture(t, false)
	writes := c.writeCount()
	id := e.state.Batch.ID
	if err := e.reconcileOnce(); err != nil {
		t.Fatal(err)
	}
	if _, err := e.completionState(id); err == nil || err.Error() != "ScaleActionFailed" {
		t.Fatal("known provider failure still waits or reports success", err)
	}
	s, ready, err := e.Snapshot()
	if err != nil || ready || len(s.Members) != 3 || s.Desired != 5 || s.Pending == nil || s.Pending.Phase != "failed-grow" || s.Pending.Unknown || s.Pending.ActivityID != "asa-partialgrow" || s.Pending.ActivityStatus != "PARTIALLY_SUCCESSFUL" || s.LastCompletedAt != 0 || s.ManualReview || s.RecoveryAudit == nil {
		t.Fatalf("failure lost cloud obligation: ready=%v err=%v state=%+v", ready, err, s)
	}
	e.Close()
	e, err = Open(cfg, c, n)
	if err != nil {
		t.Fatal("failure cannot restart", err)
	}
	defer e.Close()
	if err = e.reconcileOnce(); err != nil {
		t.Fatal(err)
	}
	if _, err = e.Scale(5); err != ErrBusy {
		t.Fatal("overlapping write admitted", err)
	}
	if c.writeCount() != writes || len(n.writes) != 0 || e.state.LastCompletedAt != 0 || e.checkRecoveryAudit() != nil {
		t.Fatal("failure replayed writes or advanced cooldown")
	}
}
