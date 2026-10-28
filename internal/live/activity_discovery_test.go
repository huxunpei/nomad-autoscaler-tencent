package live

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/huxunpei/nomad-autoscaler-tencent/internal/tencentcloud"
)

type discoveryCloud struct {
	*multiCloud
	dir          string
	history      []tencentcloud.Activity
	historyCalls int
	historyErr   bool
	beforeWrite  func(string)
	onHistory    func(int)
}

func (c *discoveryCloud) SetDesired(ctx context.Context, g string, target int64) (tencentcloud.WriteResult, error) {
	if c.beforeWrite != nil {
		c.beforeWrite("ModifyDesiredCapacity")
	}
	_, err := c.multiCloud.SetDesired(ctx, g, target)
	return tencentcloud.WriteResult{Attempted: true, Unknown: true}, err
}
func (c *discoveryCloud) RemoveExact(ctx context.Context, g, id string) (tencentcloud.WriteResult, error) {
	if c.beforeWrite != nil {
		c.beforeWrite("RemoveInstances")
	}
	_, err := c.multiCloud.RemoveExact(ctx, g, id)
	return tencentcloud.WriteResult{Attempted: true, Unknown: true}, err
}
func (c *discoveryCloud) ActivitiesSince(ctx context.Context, g string, since time.Time) ([]tencentcloud.Activity, error) {
	c.historyCalls++
	if c.onHistory != nil {
		c.onHistory(c.historyCalls)
	}
	if c.historyErr || ctx.Err() != nil {
		return nil, errors.New("unavailable")
	}
	if g != testGroup || since.IsZero() {
		return nil, errors.New("invalid history scope")
	}
	return copyJSON(c.history), nil
}
func discoveryFixture(t *testing.T, shrink bool) (Config, *discoveryCloud, *settlementNomad, *Engine) {
	t.Helper()
	count := int64(1)
	if shrink {
		count = 3
	}
	cfg, base, n, e := groupContractFixture(t, count, 0, 5, false)
	e.Close()
	c := &discoveryCloud{multiCloud: base, dir: cfg.Dir}
	nomad := &settlementNomad{multiNomad: n, dir: cfg.Dir}
	e = openMulti(t, cfg, c, nomad)
	return cfg, c, nomad, e
}
func startUnidentified(t *testing.T, e *Engine, c *discoveryCloud, shrink bool) {
	t.Helper()
	target := int64(3)
	if shrink {
		target = 2
	}
	changed, err := e.Scale(target)
	if !changed || (shrink && err != nil) || (!shrink && err != ErrUnknown) {
		t.Fatal("start", changed, err)
	}
	if shrink {
		if err = e.reconcileOnce(); err != ErrUnknown {
			t.Fatal("delete submit", err)
		}
	}
	if e.state.Pending == nil || !e.state.Pending.Unknown || e.state.Pending.ActivityID != "" || e.state.Pending.RequestID != "" {
		t.Fatal("not a missing-response fixture")
	}
}
func completeUnidentified(e *Engine, c *discoveryCloud, shrink bool) {
	op := e.state.Pending
	stamp := time.Now().UTC().Format(time.RFC3339)
	a := tencentcloud.Activity{ID: "asa-discovered", GroupID: testGroup, Status: "SUCCESSFUL", Type: "SCALE_OUT", StartedAt: stamp, EndedAt: stamp, RequestIDs: []tencentcloud.RequestTrace{{Action: "DescribeAutoScalingActivities", RequestID: testRequest}}}
	if shrink {
		c.terminal(true)
		a.Type = "REMOVE_INSTANCES"
		a.RelatedInstances = []tencentcloud.RelatedInstance{{ID: op.InstanceID, Status: "SUCCESSFUL"}}
	} else {
		initial := map[string]bool{}
		for _, id := range e.state.Batch.InitialMembers {
			initial[id] = true
		}
		for _, m := range c.snap.Members {
			if !initial[m.ID] {
				a.RelatedInstances = append(a.RelatedInstances, tencentcloud.RelatedInstance{ID: m.ID, Status: "SUCCESSFUL"})
			}
		}
	}
	c.activities[a.ID] = a
	c.history = []tencentcloud.Activity{a}
}
func TestUnidentifiedWritesPersistCloudIntentBeforeSending(t *testing.T) {
	for _, shrink := range []bool{false, true} {
		t.Run(map[bool]string{false: "grow", true: "remove"}[shrink], func(t *testing.T) {
			_, c, _, e := discoveryFixture(t, shrink)
			c.beforeWrite = func(action string) {
				raw, err := os.ReadFile(filepath.Join(c.dir, "live.json"))
				if err != nil {
					t.Fatal(err)
				}
				var s struct {
					Pending struct {
						CloudIntent struct {
							Action string    `json:"action"`
							At     time.Time `json:"at"`
						} `json:"cloud_intent"`
						StartedAt time.Time `json:"started_at"`
					} `json:"pending"`
				}
				if json.Unmarshal(raw, &s) != nil || s.Pending.CloudIntent.Action != action || s.Pending.CloudIntent.At.Before(s.Pending.StartedAt) || s.Pending.CloudIntent.At.After(time.Now().UTC()) {
					t.Error("cloud mutation lacks durable, exact-stage intent")
				}
			}
			startUnidentified(t, e, c, shrink)
		})
	}
}
func TestUnidentifiedSuccessfulWritesReconcileWithAuditAndNoReplay(t *testing.T) {
	for _, shrink := range []bool{false, true} {
		t.Run(map[bool]string{false: "grow", true: "remove"}[shrink], func(t *testing.T) {
			cfg, c, n, e := discoveryFixture(t, shrink)
			startUnidentified(t, e, c, shrink)
			completeUnidentified(e, c, shrink)
			writes := len(c.writes)
			id := e.state.Pending.ID
			e.Close()
			e = openMulti(t, cfg, c, n)
			step(t, e)
			if shrink {
				if e.state.Pending == nil || e.state.Pending.Phase != "post-scale" || n.purges != 0 {
					t.Fatal("discovered removal did not retain durable SDK post obligation")
				}
				step(t, e)
			}
			if e.state.Pending != nil || e.state.Batch != nil || e.state.LastOperation == nil || e.state.LastOperation.ID != id || e.state.RecoveryAudit == nil || e.state.LastCompletedAt == 0 {
				t.Fatal("missing ActivityId still blocks confirmed completion")
			}
			if len(c.writes) != writes || e.state.LastOperation.RequestID != "" || e.state.LastOperation.ActivityID != "asa-discovered" {
				t.Fatal("cloud write replayed or original request ID invented")
			}
			if shrink && n.purges != 1 {
				t.Fatal("post count", n.purges)
			}
			ref := *e.state.RecoveryAudit
			if shrink {
				// SDK post appends its own linked start/confirmation. Check
				// both exact records before the original discovery proof.
				for _, action := range []string{"confirm-sdk-post", "begin-sdk-post"} {
					post, err := readRecord(cfg.Dir, ref)
					if err != nil || post.Version != 3 || post.Plan.Decision.Action != action || post.Plan.Decision.OperationID != id || post.Before.RecoveryAudit == nil {
						t.Fatal("post audit did not retain original discovery", action, err)
					}
					ref = *post.Before.RecoveryAudit
				}
			}
			record, err := readRecord(cfg.Dir, ref)
			if err != nil || record.Plan.Operator != "automatic-reconciler" {
				t.Fatal("audit", err)
			}
			if record.Plan.Reference != "discovered-activity/"+id || len(record.Plan.Evidence) != 2 || len(record.Evidence) != 2 {
				t.Fatal("discovery evidence not persisted")
			}
			for _, ev := range append(record.Plan.Evidence, record.Evidence...) {
				if ev.Discovery == nil || len(ev.Discovery.Activity.RequestIDs) == 0 || ev.Discovery.ObservedAt.IsZero() || ev.Discovery.Activity.ID != "asa-discovered" {
					t.Fatal("missing read traces or discovery observation time")
				}
			}
			if c.historyCalls < 5 {
				t.Fatal("history not revalidated at preview and apply", c.historyCalls)
			}
			e.Close()
			e = openMulti(t, cfg, c, n)
			step(t, e)
			_, ready, err := e.Snapshot()
			if err != nil || !ready {
				t.Fatal("restart readiness", ready, err)
			}
			changed, nextErr := e.Scale(e.state.Desired + 1)
			if !changed || nextErr != ErrUnknown || len(c.writes) != writes+1 {
				t.Fatal("next engine action did not enter normal cloud path", changed, nextErr)
			}
		})
	}
}
func TestUnidentifiedDiscoveryRejectsAmbiguousOrInvalidHistory(t *testing.T) {
	modes := []string{"empty", "error", "duplicate", "two-successes", "failed-before-success", "running", "wrong-type", "wrong-instance", "wrong-group", "missing-start", "missing-end", "old-start", "future-end", "missing-trace", "bad-trace", "partial", "read-disagrees", "member-still-present", "capacity-mismatch"}
	for _, mode := range modes {
		t.Run(mode, func(t *testing.T) {
			_, c, n, e := discoveryFixture(t, true)
			startUnidentified(t, e, c, true)
			completeUnidentified(e, c, true)
			a := &c.history[0]
			switch mode {
			case "empty":
				c.history = nil
			case "error":
				c.historyErr = true
			case "duplicate":
				c.history = append(c.history, *a)
			case "two-successes", "failed-before-success", "running":
				b := copyJSON(*a)
				b.ID = "asa-other"
				if mode == "failed-before-success" {
					b.Status = "FAILED"
					b.RelatedInstances[0].Status = "FAILED"
				}
				if mode == "running" {
					b.Status = "RUNNING"
					b.EndedAt = ""
					b.RelatedInstances[0].Status = "RUNNING"
				}
				c.history = append(c.history, b)
			case "wrong-type":
				a.Type = "DETACH_INSTANCES"
			case "wrong-instance":
				a.RelatedInstances[0].ID = "ins-other"
			case "wrong-group":
				a.GroupID = "asg-other"
			case "missing-start":
				a.StartedAt = ""
			case "missing-end":
				a.EndedAt = ""
			case "old-start":
				a.StartedAt = e.state.Pending.StartedAt.Add(-time.Hour).Format(time.RFC3339)
			case "future-end":
				a.EndedAt = time.Now().Add(time.Hour).Format(time.RFC3339)
			case "missing-trace":
				a.RequestIDs = nil
			case "bad-trace":
				a.RequestIDs[0].RequestID = "bad"
			case "partial":
				a.Status = "PARTIALLY_SUCCESSFUL"
			case "read-disagrees":
				b := c.activities[a.ID]
				b.Status = "FAILED"
				b.RelatedInstances[0].Status = "FAILED"
				c.activities[a.ID] = b
			case "member-still-present":
				id := e.state.Pending.InstanceID
				c.snap.VMs[id] = tencentcloud.VM{ID: id, Returned: true, State: "RUNNING", ChargeType: "POSTPAID_BY_HOUR"}
			case "capacity-mismatch":
				c.snap.Group.Desired++
			}
			writes := len(c.writes)
			step(t, e)
			if e.state.Pending == nil || !e.state.Pending.Unknown || e.state.Pending.ActivityID != "" || e.state.RecoveryAudit != nil || n.purges != 0 || len(c.writes) != writes {
				t.Fatal("ambiguous history permitted recovery")
			}
		})
	}
}

func TestUnidentifiedDiscoveryRechecksHistoryUntilCommit(t *testing.T) {
	for _, at := range []int{2, 3, 4, 5} {
		t.Run(string(rune('0'+at)), func(t *testing.T) {
			_, c, n, e := discoveryFixture(t, true)
			startUnidentified(t, e, c, true)
			completeUnidentified(e, c, true)
			writes := len(c.writes)
			c.onHistory = func(call int) {
				if call == at {
					other := copyJSON(c.history[0])
					other.ID = "asa-competing"
					c.history = append(c.history, other)
				}
			}
			err := e.reconcileOnce()
			if at < 4 && err != nil {
				t.Fatal("preview change should remain retryable", err)
			}
			if at >= 4 && (err == nil || err.Error() != "RecoveryActivityDiscoveryUnconfirmed") {
				t.Fatal("apply change was not reported", err)
			}
			if e.state.Pending == nil || !e.state.Pending.Unknown || e.state.RecoveryAudit != nil || n.purges != 0 || len(c.writes) != writes {
				t.Fatal("changed history crossed audit commit")
			}
			c.onHistory = nil
			c.history = c.history[:1]
			step(t, e)
			step(t, e)
			if e.state.Pending != nil || e.state.RecoveryAudit == nil || n.purges != 1 || len(c.writes) != writes {
				t.Fatal("consistent later evidence did not resume")
			}
		})
	}
}
func TestUnidentifiedLegacyOrNomadOnlyIntentIsNotReinterpreted(t *testing.T) {
	for _, mode := range []string{"legacy-cloud-unknown", "nomad-drain-unknown"} {
		t.Run(mode, func(t *testing.T) {
			cfg, c, n, e := discoveryFixture(t, true)
			if mode == "legacy-cloud-unknown" {
				startUnidentified(t, e, c, true)
				completeUnidentified(e, c, true)
				e.state.Pending.CloudIntent = nil
			} else {
				if _, err := e.Scale(2); err != nil {
					t.Fatal(err)
				}
				e.state.Pending.Phase = "unknown"
				e.state.Pending.Unknown = true
				// Even an externally observed removal must not turn an uncertain drain
				// into a locally submitted cloud deletion.
				completeUnidentified(e, c, true)
			}
			e.state.LastEvent = "remove-write-unknown" // A display message is not durable authority.
			if err := e.save(); err != nil {
				t.Fatal(err)
			}
			writes := len(c.writes)
			e.Close()
			e = openMulti(t, cfg, c, n)
			step(t, e)
			if e.state.Pending == nil || !e.state.Pending.Unknown || c.historyCalls != 0 || len(c.writes) != writes || n.purges != 0 {
				t.Fatal("legacy/drain reinterpreted as submitted cloud write")
			}
		})
	}
}
func TestUnidentifiedIntentRestartDoesNotReplayCloudWrite(t *testing.T) {
	for _, shrink := range []bool{false, true} {
		t.Run(map[bool]string{false: "grow", true: "remove"}[shrink], func(t *testing.T) {
			cfg, c, n, e := discoveryFixture(t, shrink)
			startUnidentified(t, e, c, shrink)
			completeUnidentified(e, c, shrink)
			op := e.state.Pending
			op.Unknown = false
			op.Phase = "grow-intent"
			if shrink {
				op.Phase = "remove-intent"
			}
			if err := e.save(); err != nil {
				t.Fatal(err)
			}
			writes := len(c.writes)
			e.Close()
			e = openMulti(t, cfg, c, n)
			step(t, e)
			if shrink {
				step(t, e)
			}
			if e.state.Pending != nil || e.state.RecoveryAudit == nil || len(c.writes) != writes {
				t.Fatal("restart did not resolve intent without replay")
			}
		})
	}
}
func TestUnidentifiedAuditCrashResumesBeforeSDKPost(t *testing.T) {
	for _, at := range []string{"record", "prepared", "journal", "receipt", "cleared"} {
		t.Run(at, func(t *testing.T) {
			cfg, c, n, e := discoveryFixture(t, true)
			startUnidentified(t, e, c, true)
			completeUnidentified(e, c, true)
			writes := len(c.writes)
			e.recoveryCheckpoint = func(stage string) error {
				if stage == at {
					return code("SimulatedCrash")
				}
				return nil
			}
			if err := e.reconcileOnce(); err == nil {
				t.Fatal("did not interrupt audit")
			}
			if n.purges != 0 {
				t.Fatal("post preceded completed audit")
			}
			e.Close()
			e = openMulti(t, cfg, c, n)
			step(t, e)
			step(t, e)
			if e.state.Pending != nil || e.state.Batch != nil || e.state.RecoveryAudit == nil || n.purges != 1 || len(c.writes) != writes {
				t.Fatal("audit/post recovery failed")
			}
			if err := e.checkRecoveryAudit(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestUnidentifiedReceiptCorruptionRejected(t *testing.T) {
	cfg, c, n, e := discoveryFixture(t, true)
	startUnidentified(t, e, c, true)
	completeUnidentified(e, c, true)
	step(t, e)
	if e.state.Pending == nil || e.state.Pending.ActivityDiscovery == nil {
		t.Fatal("missing discovery receipt")
	}
	raw := jsonBytes(e.state)
	e.Close()
	for _, mode := range []string{"no-intent", "wrong-action", "intent-before-operation", "intent-in-future", "no-trace", "wrong-instance", "wrong-receipt-id", "receipt-before-intent", "failed-receipt", "unknown-with-receipt", "removed-receipt"} {
		t.Run(mode, func(t *testing.T) {
			var s State
			if json.Unmarshal(raw, &s) != nil {
				t.Fatal("state")
			}
			op := s.Pending
			switch mode {
			case "no-intent":
				op.CloudIntent = nil
			case "wrong-action":
				op.CloudIntent.Action = "ModifyDesiredCapacity"
			case "intent-before-operation":
				op.CloudIntent.At = op.StartedAt.Add(-time.Hour)
			case "intent-in-future":
				op.CloudIntent.At = time.Now().Add(time.Hour)
			case "no-trace":
				op.ActivityDiscovery.Activity.RequestIDs = nil
			case "wrong-instance":
				op.ActivityDiscovery.Activity.RelatedInstances[0].ID = "ins-other"
			case "wrong-receipt-id":
				op.ActivityDiscovery.Activity.ID = "asa-other"
			case "receipt-before-intent":
				op.ActivityDiscovery.ObservedAt = op.StartedAt.Add(-time.Hour)
			case "failed-receipt":
				op.ActivityDiscovery.Activity.Status = "FAILED"
			case "unknown-with-receipt":
				op.Unknown = true
				op.Phase = "unknown"
			case "removed-receipt":
				op.ActivityDiscovery = nil
			}
			if err := os.WriteFile(filepath.Join(cfg.Dir, "live.json"), jsonBytes(s), 0600); err != nil {
				t.Fatal(err)
			}
			opened, err := Open(cfg, c, n)
			if err == nil {
				opened.Close()
				t.Fatal("corrupt journal accepted")
			}
		})
	}
}
func TestUnidentifiedAuditOriginCannotBeSpoofed(t *testing.T) {
	cfg, c, _, e := discoveryFixture(t, false)
	startUnidentified(t, e, c, false)
	completeUnidentified(e, c, false)
	step(t, e)
	if e.state.RecoveryAudit == nil {
		t.Fatal("missing audit")
	}
	record, err := readRecord(cfg.Dir, *e.state.RecoveryAudit)
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"legacy-origin", "manual-origin", "no-cloud-intent", "no-plan-samples", "no-applied-samples", "missing-discovery", "wrong-activity", "wrong-after-intent", "invented-write-request"} {
		t.Run(mode, func(t *testing.T) {
			changed := copyJSON(record)
			switch mode {
			case "legacy-origin":
				changed.Plan.Reference = "recorded-activity/" + changed.Before.Pending.ID
			case "manual-origin":
				changed.Automatic = false
			case "no-cloud-intent":
				changed.Before.Pending.CloudIntent = nil
			case "no-plan-samples":
				changed.Plan.Evidence = nil
			case "no-applied-samples":
				changed.Evidence = nil
			case "missing-discovery":
				changed.Evidence[0].Discovery = nil
			case "wrong-activity":
				changed.Plan.Decision.ActivityID = "asa-other"
			case "wrong-after-intent":
				changed.After.LastOperation.CloudIntent.At = changed.After.LastOperation.CloudIntent.At.Add(-time.Second)
			case "invented-write-request":
				changed.After.LastOperation.RequestID = testRequest
			}
			if validRecoveryRecordOrigin(changed) {
				t.Fatal("invalid automatic origin accepted")
			}
		})
	}
}

func TestUnidentifiedDiscoveryToleratesRelatedInstanceOrdering(t *testing.T) {
	_, c, _, e := discoveryFixture(t, false)
	startUnidentified(t, e, c, false)
	completeUnidentified(e, c, false)
	if len(c.history[0].RelatedInstances) != 2 {
		t.Fatal("two new members required")
	}
	c.onHistory = func(int) {
		items := c.history[0].RelatedInstances
		items[0], items[1] = items[1], items[0]
	}
	step(t, e)
	if e.state.Pending != nil || e.state.RecoveryAudit == nil {
		t.Fatal("equivalent related-instance order blocked recovery")
	}
}
