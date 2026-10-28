package live

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/huxunpei/nomad-autoscaler-tencent/internal/tencentcloud"
)

type activityGrowthCloud struct {
	*multiCloud
	dir          string
	increments   []int64
	successes    int64
	status       string
	lost         bool
	activityOnly bool
}

func (c *activityGrowthCloud) ScaleOut(ctx context.Context, group string, number int64) (tencentcloud.WriteResult, error) {
	var s State
	raw, err := os.ReadFile(filepath.Join(c.dir, "live.json"))
	if err != nil || json.Unmarshal(raw, &s) != nil || s.Pending == nil || s.Pending.CloudIntent == nil || s.Pending.CloudIntent.Action != "ScaleOutInstances" {
		return tencentcloud.WriteResult{}, fmt.Errorf("increment preceded durable intent")
	}
	c.increments = append(c.increments, number)
	base := c.snap.Group.Count
	count := c.successes
	if c.status == "SUCCESSFUL" {
		count = number
	}
	_, _ = c.multiCloud.SetDesired(ctx, group, base+count)
	stamp := time.Now().UTC().Format(time.RFC3339)
	a := tencentcloud.Activity{ID: fmt.Sprintf("asa-grow%d", len(c.increments)), GroupID: group, Type: "SCALE_OUT", Status: c.status, StartedAt: stamp, EndedAt: stamp,
		RequestIDs: []tencentcloud.RequestTrace{{Action: "DescribeAutoScalingActivities", RequestID: testRequest}}}
	for i := base; i < base+count; i++ {
		a.RelatedInstances = append(a.RelatedInstances, tencentcloud.RelatedInstance{ID: fmt.Sprintf("ins-multi%d", i), Status: "SUCCESSFUL"})
	}
	c.activities[a.ID] = a
	if c.activityOnly {
		return tencentcloud.WriteResult{Action: "ScaleOutInstances", Attempted: true, Unknown: true, ActivityID: a.ID}, fmt.Errorf("synthetic-request-id-lost")
	}
	if c.lost {
		return tencentcloud.WriteResult{Action: "ScaleOutInstances", Attempted: true, Unknown: true}, fmt.Errorf("synthetic-response-lost")
	}
	return tencentcloud.WriteResult{Action: "ScaleOutInstances", Attempted: true, RequestID: testRequest, ActivityID: a.ID}, nil
}

func activityGrowthFixture(t *testing.T, status string, successes int64) (Config, *activityGrowthCloud, *multiNomad, *Engine) {
	t.Helper()
	cfg, base, n, e := groupContractFixture(t, 1, 0, 6, false)
	e.Close()
	// JSON keeps the regression compilable before the new observation field exists.
	if err := json.Unmarshal([]byte(`{"instance_allocation_policy":"LAUNCH_CONFIGURATION"}`), &base.snap.Group); err != nil {
		t.Fatal(err)
	}
	c := &activityGrowthCloud{multiCloud: base, dir: cfg.Dir, status: status, successes: successes}
	cfg.StabilizationInterval = time.Millisecond
	return cfg, c, n, openMulti(t, cfg, c, n)
}

func TestActivityScaleOutPartialFailureAllowsNextEvaluation(t *testing.T) {
	cfg, c, n, e := activityGrowthFixture(t, "PARTIALLY_SUCCESSFUL", 2)
	_, err := legacyActivityScale(t, e, 5, true)
	if err == nil || len(c.increments) != 1 || c.increments[0] != 4 {
		t.Fatal("partial growth was not issued through the activity API", err, c.increments)
	}
	for i := 0; i < 3 && e.state.Pending != nil; i++ {
		step(t, e)
	}
	if e.state.Pending != nil || e.state.Batch != nil || e.state.LastBatch.Phase != "failed" || e.state.LastCompletedAt != 0 || len(e.state.Members) != 3 || e.state.Desired != 3 || e.checkRecoveryAudit() != nil {
		t.Fatal("partial growth did not settle failed at actual capacity")
	}
	e.Close()
	c.status = "SUCCESSFUL"
	e = openMulti(t, cfg, c, n)
	if _, err = legacyActivityScale(t, e, 5, true); err != nil {
		t.Fatal("next evaluation failed", err)
	}
	if len(c.increments) != 2 || c.increments[1] != 2 || e.state.Pending != nil || len(e.state.Members) != 5 || e.state.LastCompletedAt == 0 {
		t.Fatal("next evaluation did not grow only the missing capacity")
	}
}

func TestActivityScaleOutAllFailedReturnsWithoutSuccessfulCooldown(t *testing.T) {
	_, c, _, e := activityGrowthFixture(t, "FAILED", 0)
	_, err := legacyActivityScale(t, e, 5, true)
	if err == nil || len(c.increments) != 1 {
		t.Fatal("missing failed activity operation", err)
	}
	for i := 0; i < 3 && e.state.Pending != nil; i++ {
		step(t, e)
	}
	if e.state.Pending != nil || e.state.LastBatch.Phase != "failed" || e.state.LastCompletedAt != 0 || len(e.state.Members) != 1 {
		t.Fatal("zero-success activity blocked or reported success")
	}
}

func TestActivityScaleOutLostResponseDoesNotReplay(t *testing.T) {
	cfg, c, n, e := activityGrowthFixture(t, "PARTIALLY_SUCCESSFUL", 0)
	c.lost = true
	if _, err := legacyActivityScale(t, e, 5, false); err == nil || len(c.increments) != 1 {
		t.Fatal("unknown delta operation missing", err)
	}
	e.Close()
	e = openMulti(t, cfg, c, n)
	step(t, e)
	if len(c.increments) != 1 || e.state.Pending == nil || !e.state.Pending.Unknown {
		t.Fatal("unknown delta operation was released or replayed")
	}
}

func TestActivityScaleOutKnownIDRecoversMissingRequestID(t *testing.T) {
	cfg, c, n, e := activityGrowthFixture(t, "PARTIALLY_SUCCESSFUL", 2)
	c.activityOnly = true
	if _, err := legacyActivityScale(t, e, 5, false); err == nil {
		t.Fatal("incomplete response should be unknown")
	}
	e.Close()
	e = openMulti(t, cfg, c, n)
	for i := 0; i < 4 && e.state.Pending != nil; i++ {
		step(t, e)
	}
	if e.state.Pending != nil || e.state.LastBatch.Phase != "failed" || e.state.LastOperation.RequestID != "" ||
		e.state.LastOperation.ActivityID != "asa-grow1" || len(e.state.Members) != 3 || len(c.increments) != 1 || e.state.LastCompletedAt != 0 {
		t.Fatal("known activity did not recover partial failure without inventing request identity")
	}
}

func TestActivityScaleOutSuccessfulKnownIDRecoversMissingRequestID(t *testing.T) {
	cfg, c, n, e := activityGrowthFixture(t, "SUCCESSFUL", 0)
	c.activityOnly = true
	if _, err := legacyActivityScale(t, e, 5, false); err == nil {
		t.Fatal("incomplete acknowledgement should remain unknown")
	}
	e.Close()
	e = openMulti(t, cfg, c, n)
	step(t, e)
	if e.state.Pending != nil || e.state.LastBatch.Phase != "completed" ||
		e.state.LastOperation.RequestID != "" || len(c.increments) != 1 ||
		e.state.LastCompletedAt == 0 || e.checkRecoveryAudit() != nil {
		t.Fatal("exact successful activity did not recover without replay")
	}
}

func TestActivityScaleOutInvalidIntentCannotSettle(t *testing.T) {
	_, c, _, e := activityGrowthFixture(t, "SUCCESSFUL", 0)
	if _, err := legacyActivityScale(t, e, 5, false); err != nil {
		t.Fatal(err)
	}
	e.state.Pending.CloudIntent.ScaleOut = nil
	if e.scaleOutTargetSettled(c.snap, c.activities["asa-grow1"]) {
		t.Fatal("invalid persisted increment settled")
	}
}

func TestActivityScaleOutRejectsMixedOrMissingModeBeforeWriting(t *testing.T) {
	for _, mode := range []string{"", "SPOT_MIXED", "UNSUPPORTED"} {
		t.Run(mode, func(t *testing.T) {
			_, c, _, e := activityGrowthFixture(t, "SUCCESSFUL", 0)
			c.snap.Group.AllocationPolicy = mode
			if changed, err := legacyActivityScale(t, e, 5, false); changed || err == nil || e.state.Pending != nil || e.state.Batch != nil || len(c.increments) != 0 || c.writeCount() != 0 {
				t.Fatal("ambiguous multi-activity group admitted")
			}
		})
	}
}

func TestActivityScaleOutSettlementRequiresExactSuccessMembers(t *testing.T) {
	for _, mode := range []string{"unrelated-member", "desired", "active", "mixed", "running-vm", "old-member"} {
		t.Run(mode, func(t *testing.T) {
			_, c, _, e := activityGrowthFixture(t, "PARTIALLY_SUCCESSFUL", 2)
			if _, err := legacyActivityScale(t, e, 5, false); err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "unrelated-member":
				a := c.activities["asa-grow1"]
				a.RelatedInstances[0].ID = "ins-unrelated"
				c.activities[a.ID] = a
			case "desired":
				c.snap.Group.Desired = 5
			case "active":
				c.snap.Group.Activity = "IN_ACTIVITY"
			case "mixed":
				c.snap.Group.AllocationPolicy = "SPOT_MIXED"
			case "running-vm":
				v := c.snap.VMs["ins-multi1"]
				v.State = "PENDING"
				c.snap.VMs[v.ID] = v
			case "old-member":
				c.snap.Members[1].AddedAt = time.Now().Add(-time.Hour).UTC().Format(time.RFC3339)
			}
			for i := 0; i < 3; i++ {
				_ = e.reconcileOnce()
			}
			if e.state.Pending == nil || e.state.LastCompletedAt != 0 || len(c.increments) != 1 {
				t.Fatal("incomplete terminal state released")
			}
		})
	}
}

func TestActivityScaleOutPreservesLegacyInFlightAction(t *testing.T) {
	cfg, base, n, e := groupContractFixture(t, 1, 0, 6, false)
	if _, err := e.Scale(5); err != nil {
		t.Fatal(err)
	}
	e.Close()
	base.snap.Group.AllocationPolicy = "LAUNCH_CONFIGURATION"
	c := &activityGrowthCloud{multiCloud: base, dir: cfg.Dir, status: "SUCCESSFUL"}
	e = openMulti(t, cfg, c, n)
	step(t, e)
	if e.state.Pending != nil || len(c.increments) != 0 || e.state.LastOperation.CloudIntent.Action != "ModifyDesiredCapacity" || c.writeCount() != 1 {
		t.Fatal("legacy acknowledged operation was rewritten or replayed")
	}
	if _, err := e.Scale(6); err != nil {
		t.Fatal(err)
	}
	step(t, e)
	if len(c.increments) != 0 || c.writeCount() != 2 || e.state.Pending != nil {
		t.Fatal("new operation incorrectly used the legacy increment API")
	}
}

func TestActivityScaleOutAuditsResumeAfterInterruption(t *testing.T) {
	for _, phase := range []string{"classification", "settlement"} {
		for _, point := range []string{"prepared", "journal", "receipt"} {
			t.Run(phase+"/"+point, func(t *testing.T) {
				cfg, c, n, e := activityGrowthFixture(t, "PARTIALLY_SUCCESSFUL", 2)
				if _, err := legacyActivityScale(t, e, 5, false); err != nil {
					t.Fatal(err)
				}
				if phase == "settlement" {
					step(t, e)
				}
				hit := false
				e.recoveryCheckpoint = func(at string) error {
					if at == point {
						hit = true
						return fmt.Errorf("synthetic-interruption")
					}
					return nil
				}
				_ = e.reconcileOnce()
				if !hit {
					t.Fatal("checkpoint not reached")
				}
				e.Close()
				e = openMulti(t, cfg, c, n)
				for i := 0; i < 4 && e.state.Pending != nil; i++ {
					step(t, e)
				}
				if e.state.Pending != nil || e.state.LastBatch.Phase != "failed" || len(c.increments) != 1 || e.state.LastCompletedAt != 0 || e.checkRecoveryAudit() != nil {
					t.Fatal("interrupted activity failure did not resume safely")
				}
			})
		}
	}
}

// legacyActivityScale creates an operation as the previous release did. These
// tests exercise recovery of already issued increments, never the new writer.
func legacyActivityScale(t *testing.T, e *Engine, target int64, wait bool) (bool, error) {
	t.Helper()
	e.mu.Lock()
	e.EnableReconciliation()
	if err := e.refresh(); err != nil {
		e.mu.Unlock()
		return false, err
	}
	if !e.ready || e.state.Cloud.Group.AllocationPolicy != "LAUNCH_CONFIGURATION" {
		e.mu.Unlock()
		return false, ErrNotReady
	}
	count := int64(len(e.state.Members))
	batchID, _ := newOperationID()
	opID, _ := newOperationID()
	at := time.Now().UTC()
	e.state.Batch = &Batch{ID: batchID, Kind: "grow", Phase: "running", Target: target, InitialCount: count, InitialMembers: memberIDs(e.state.Members), Results: []Operation{}, StartedAt: at}
	op := &Operation{ID: opID, Kind: "grow", Phase: "grow-intent", Target: target, StartedAt: at, CloudIntent: &CloudWriteIntent{Action: "ScaleOutInstances", At: at, ScaleOut: &ScaleOutIntent{Base: count, Number: target - count, AllocationPolicy: "LAUNCH_CONFIGURATION"}}}
	changed, err := e.accept(op)
	e.publishSnapshot(false)
	e.mu.Unlock()
	if err != nil || !wait {
		return changed, err
	}
	for i := 0; i < 5; i++ {
		if err = e.reconcileOnce(); err != nil {
			return changed, err
		}
		if e.state.Pending != nil && e.state.Pending.Phase == "failed-grow" {
			return changed, code("ScaleActionFailed")
		}
		if e.state.LastBatch != nil && e.state.LastBatch.ID == batchID {
			if e.state.LastBatch.Phase == "completed" {
				return changed, nil
			}
			return changed, code("ScaleActionFailed")
		}
	}
	return changed, code("ScaleCompletionTimeout")
}
