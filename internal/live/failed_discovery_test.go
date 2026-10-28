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

func failUnidentified(e *Engine, c *discoveryCloud, status string) {
	c.terminal(false)
	op := e.state.Pending
	stamp := time.Now().UTC().Format(time.RFC3339)
	a := tencentcloud.Activity{ID: "asa-discovered", GroupID: testGroup, Type: "REMOVE_INSTANCES", Status: status, StartedAt: stamp, EndedAt: stamp,
		RelatedInstances: []tencentcloud.RelatedInstance{{ID: op.InstanceID, Status: "FAILED"}}, RequestIDs: []tencentcloud.RequestTrace{{Action: "DescribeAutoScalingActivities", RequestID: testRequest}}}
	c.activities[a.ID] = a
	c.history = []tencentcloud.Activity{a}
	for i := range c.snap.Members {
		if c.snap.Members[i].ID == op.InstanceID {
			c.snap.Members[i].Lifecycle = "TERMINATION_FAILED"
		}
	}
}
func laterUnidentifiedRemoval(e *Engine, c *discoveryCloud) {
	op := e.state.Pending
	original := copyJSON(c.activities[op.ActivityID])
	later := copyJSON(original)
	later.ID = "asa-later"
	later.Status = "SUCCESSFUL"
	later.RelatedInstances[0].Status = "SUCCESSFUL"
	later.StartedAt = time.Now().UTC().Format(time.RFC3339)
	later.EndedAt = later.StartedAt
	c.history = []tencentcloud.Activity{original, later}
	c.activities[later.ID] = later
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
func TestDiscoveredDeletionFailureRestoresAndRetainsOriginalFailure(t *testing.T) {
	for _, status := range []string{"FAILED", "CANCELLED", "PARTIALLY_SUCCESSFUL"} {
		t.Run(status, func(t *testing.T) {
			cfg, c, n, e := discoveryFixture(t, true)
			startUnidentified(t, e, c, true)
			failUnidentified(e, c, status)
			id, opID := e.state.Pending.InstanceID, e.state.Pending.ID
			writes := len(c.writes)
			step(t, e)
			op := e.state.Pending
			if op == nil || op.Unknown || op.Phase != "failed-delete" || op.DrainOutcome != "restored" || op.ActivityID != "asa-discovered" || op.ActivityStatus != status || op.InstanceResult != "FAILED" || op.RequestID != "" || n.nodes[id].Eligibility != "eligible" || e.state.RecoveryAudit == nil {
				t.Fatal("missing-response failure not restored/audited")
			}
			if e.state.LastCompletedAt != 0 || e.state.Batch == nil || e.state.Batch.Phase != "running" || len(c.writes) != writes || n.purges != 0 {
				t.Fatal("failure converted into completion or replay")
			}
			if _, err := e.completionState(e.state.Batch.ID); err == nil || err.Error() != "ScaleActionFailed" {
				t.Fatal("failure result lost", err)
			}
			record, err := findDiscoveredFailureRecord(cfg.Dir, *e.state.RecoveryAudit)
			if err != nil || record.Plan.Decision.Action != "confirm-remove-failure" || len(record.Plan.Decision.Remaining) != 0 || record.Plan.Decision.Count != 3 {
				t.Fatal("failure audit semantics", err)
			}
			if record.After.Pending == nil || record.After.Pending.DrainOutcome != "" || record.After.LastCompletedAt != record.Before.LastCompletedAt {
				t.Fatal("audit pre-approved restore or cooldown")
			}
			e.Close()
			e = openMulti(t, cfg, c, n)
			step(t, e)
			if len(n.writes) != 2 || e.state.Pending.ID != opID || e.state.Pending.DrainOutcome != "restored" {
				t.Fatal("restoration replayed after restart")
			}
			laterUnidentifiedRemoval(e, c)
			step(t, e)
			step(t, e)
			if e.state.Pending != nil || e.state.Batch != nil || e.state.LastBatch.Phase != "failed" || e.state.LastOperation.ID != opID || e.state.LastOperation.ActivityStatus != status || e.state.LastOperation.RequestID != "" || e.state.LastCompletedAt != 0 || n.purges != 1 || len(c.writes) != writes {
				t.Fatal("later removal lost original failed outcome")
			}
			e.Close()
			e = openMulti(t, cfg, c, n)
			if changed, err := e.Scale(3); !changed || err != ErrUnknown {
				t.Fatal("next engine action blocked", changed, err)
			}
		})
	}
}
func TestDiscoveredDeletionFailureRestorationSurvivesUnrelatedGuards(t *testing.T) {
	for _, mode := range []string{"unmapped-peer", "disabled", "changed-launch-config", "active-group", "protected"} {
		t.Run(mode, func(t *testing.T) {
			cfg, c, n, e := discoveryFixture(t, true)
			startUnidentified(t, e, c, true)
			failUnidentified(e, c, "FAILED")
			id := e.state.Pending.InstanceID
			switch mode {
			case "unmapped-peer":
				delete(n.nodes, "ins-multi2")
			case "disabled":
				c.snap.Group.Enabled = "DISABLED"
			case "changed-launch-config":
				c.snap.LaunchConfiguration.VersionNumber++
			case "active-group":
				c.snap.Group.Activity = "IN_ACTIVITY"
			case "protected":
				v := c.snap.VMs[id]
				v.Protected = true
				c.snap.VMs[id] = v
			}
			e.Close()
			e = openMulti(t, cfg, c, n)
			_ = e.reconcileOnce()
			if e.state.Pending == nil || e.state.Pending.Phase != "failed-delete" || e.state.Pending.DrainOutcome != "restored" || n.nodes[id].Eligibility != "eligible" || e.state.RecoveryAudit == nil || c.writeCount() != 1 {
				t.Fatal("unrelated guard prevented exact owned restore")
			}
		})
	}
}
func TestDiscoveredDeletionFailureAuditInterruptionRestarts(t *testing.T) {
	for _, at := range []string{"before-record", "record", "prepared", "journal", "receipt", "cleared"} {
		t.Run(at, func(t *testing.T) {
			cfg, c, n, e := discoveryFixture(t, true)
			startUnidentified(t, e, c, true)
			failUnidentified(e, c, "FAILED")
			e.recoveryCheckpoint = func(stage string) error {
				if stage == at {
					return code("SimulatedCrash")
				}
				return nil
			}
			if err := e.reconcileOnce(); err == nil {
				t.Fatal("missing audit interruption")
			}
			if len(n.writes) != 1 {
				t.Fatal("restore occurred before committed audit")
			}
			e.Close()
			e = openMulti(t, cfg, c, n)
			step(t, e)
			if e.state.Pending == nil || e.state.Pending.Phase != "failed-delete" || e.state.Pending.DrainOutcome != "restored" || e.state.RecoveryAudit == nil || len(n.writes) != 2 || c.writeCount() != 1 {
				t.Fatal("interrupted failure did not resume")
			}
			if err := e.checkRecoveryAudit(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

type discoveredFailureNomad struct {
	*settlementNomad
	lostAck      bool
	afterRestore func()
}

func (n *discoveredFailureNomad) RestoreOwnedDrain(ctx context.Context, id nomadclient.Identity, op string) (nomadclient.MutationResult, error) {
	raw, err := os.ReadFile(filepath.Join(n.dir, "live.json"))
	var s State
	if err != nil || json.Unmarshal(raw, &s) != nil || s.RecoveryAudit == nil || s.Pending == nil || s.Pending.Phase != "failed-delete" || s.Pending.ActivityDiscovery == nil {
		return nomadclient.MutationResult{}, errors.New("restore preceded discovery audit")
	}
	record, err := findDiscoveredFailureRecord(n.dir, *s.RecoveryAudit)
	if err != nil || record.Plan.Decision.Action != "confirm-remove-failure" {
		return nomadclient.MutationResult{}, errors.New("invalid failure audit")
	}
	result, err := n.multiNomad.RestoreOwnedDrain(ctx, id, op)
	if n.afterRestore != nil {
		f := n.afterRestore
		n.afterRestore = nil
		f()
	}
	if n.lostAck {
		return nomadclient.MutationResult{WriteAttempted: true, Unknown: true}, errors.New("lost restore response")
	}
	return result, err
}
func TestDiscoveredDeletionFailureRestoreLostAckAndPersistenceResume(t *testing.T) {
	for _, mode := range []string{"lost-ack", "after-restore-save"} {
		t.Run(mode, func(t *testing.T) {
			cfg, c, n, e := discoveryFixture(t, true)
			e.Close()
			probe := &discoveredFailureNomad{settlementNomad: n}
			e = openMulti(t, cfg, c, probe)
			startUnidentified(t, e, c, true)
			failUnidentified(e, c, "FAILED")
			var undo func()
			if mode == "lost-ack" {
				probe.lostAck = true
			} else {
				probe.afterRestore = func() { undo = obstructSettlementJournal(t, cfg.Dir) }
			}
			err := e.reconcileOnce()
			if mode == "after-restore-save" {
				if err == nil || undo == nil {
					t.Fatal("no persistence fault")
				}
				undo()
			} else if err != nil {
				t.Fatal(err)
			}
			e.Close()
			e = openMulti(t, cfg, c, probe)
			step(t, e)
			if e.state.Pending == nil || e.state.Pending.DrainOutcome != "restored" || e.state.RecoveryAudit == nil || len(n.writes) != 2 || c.writeCount() != 1 {
				t.Fatal("restoration repeated or forgotten")
			}
		})
	}
}

func failureWithHistory(t *testing.T, mode string) (Config, *discoveryCloud, *settlementNomad, *Engine) {
	t.Helper()
	cfg, c, n, e := discoveryFixture(t, true)
	startUnidentified(t, e, c, true)
	op := e.state.Pending
	op.StartedAt = time.Now().Add(-time.Minute).UTC()
	op.CloudIntent.At = op.StartedAt.Add(time.Second)
	if op.NomadDrainIntent == nil {
		t.Fatal("history fixture lacks new SDK drain intent")
	}
	op.NomadDrainIntent.At = op.StartedAt.Add(500 * time.Millisecond)
	e.state.Batch.StartedAt = op.StartedAt
	if err := e.validateState(); err != nil {
		t.Fatal("invalid backdated history fixture", err)
	}
	if err := e.save(); err != nil {
		t.Fatal(err)
	}
	failUnidentified(e, c, "FAILED")
	original := copyJSON(c.history[0])
	stamp := op.CloudIntent.At.Add(time.Second).Truncate(time.Second)
	original.StartedAt = stamp.Format(time.RFC3339)
	original.EndedAt = original.StartedAt
	c.activities[original.ID] = original
	later := copyJSON(original)
	later.ID = "asa-followup"
	later.StartedAt = stamp.Add(5 * time.Second).Format(time.RFC3339)
	later.EndedAt = later.StartedAt
	switch mode {
	case "retry-running":
		later.Status = "RUNNING"
		later.EndedAt = ""
		later.RelatedInstances[0].Status = "RUNNING"
		c.snap.Group.Activity = "IN_ACTIVITY"
	case "retry-failed":
	case "retry-succeeded":
		later.Status = "SUCCESSFUL"
		later.RelatedInstances[0].Status = "SUCCESSFUL"
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
	case "unrelated-terminal":
		later.Type = "SCALE_OUT"
		later.RelatedInstances[0].ID = "ins-other"
	}
	c.activities[later.ID] = later
	c.history = []tencentcloud.Activity{later, original}
	return cfg, c, n, e
}
func TestDiscoveredDeletionFailureWithLaterActivities(t *testing.T) {
	for _, mode := range []string{"retry-running", "retry-failed", "retry-succeeded", "unrelated-terminal"} {
		t.Run(mode, func(t *testing.T) {
			cfg, c, n, e := failureWithHistory(t, mode)
			id := e.state.Pending.InstanceID
			opID := e.state.Pending.ID
			c.onHistory = func(int) { c.history[0], c.history[1] = c.history[1], c.history[0] }
			step(t, e)
			if e.state.Pending == nil || e.state.Pending.Phase != "failed-delete" || e.state.Pending.ActivityID != "asa-discovered" || e.state.Pending.ActivityStatus != "FAILED" || e.state.RecoveryAudit == nil {
				t.Fatal("later activity hid original failure")
			}
			if mode == "retry-succeeded" {
				step(t, e)
				if e.state.Pending != nil || e.state.LastBatch.Phase != "failed" || e.state.LastOperation.ID != opID || e.state.LastCompletedAt != 0 || n.purges != 1 || len(n.writes) != 1 {
					t.Fatal("already absent VM was restored or failure became success")
				}
			} else if n.nodes[id].Eligibility != "eligible" || e.state.Pending.DrainOutcome != "restored" || n.purges != 0 || len(n.writes) != 2 {
				t.Fatal("surviving owned node not restored")
			}
			if c.writeCount() != 1 {
				t.Fatal("repeated cloud deletion")
			}
			record, err := findDiscoveredFailureRecord(cfg.Dir, *e.state.RecoveryAudit)
			if err != nil || record.Plan.Decision.Action != "confirm-remove-failure" {
				t.Fatal("failure audit", err)
			}
			if len(record.Plan.Evidence) != 2 || len(record.Evidence) != 2 {
				t.Fatal("missing repeated evidence")
			}
		})
	}
}

func TestDiscoveredDeletionFailureRejectsInvalidHistory(t *testing.T) {
	modes := []string{"same-second-attempts", "overlapping-attempt", "first-not-terminal", "first-successful", "first-other-instance", "other-operation-on-vm", "unknown-inflight-target", "before-intent", "duplicate-id", "wrong-group", "bad-read-trace", "exact-read-disagrees", "original-end-missing"}
	for _, mode := range modes {
		t.Run(mode, func(t *testing.T) {
			_, c, n, e := failureWithHistory(t, "retry-failed")
			later, original := &c.history[0], &c.history[1]
			switch mode {
			case "same-second-attempts":
				later.StartedAt = original.StartedAt
			case "overlapping-attempt":
				original.EndedAt = later.EndedAt
				start, _ := time.Parse(time.RFC3339, later.StartedAt)
				original.EndedAt = start.Add(time.Second).Format(time.RFC3339)
			case "first-not-terminal":
				original.Status = "RUNNING"
				original.EndedAt = ""
				original.RelatedInstances[0].Status = "RUNNING"
			case "first-successful":
				original.Status = "SUCCESSFUL"
				original.RelatedInstances[0].Status = "SUCCESSFUL"
			case "first-other-instance":
				original.RelatedInstances[0].ID = "ins-other"
				later.Status = "RUNNING"
				later.EndedAt = ""
				later.RelatedInstances[0].Status = "RUNNING"
			case "other-operation-on-vm":
				later.Type = "DETACH_INSTANCES"
			case "unknown-inflight-target":
				later.Status = "RUNNING"
				later.EndedAt = ""
				later.RelatedInstances = nil
			case "before-intent":
				original.StartedAt = e.state.Pending.CloudIntent.At.Add(-time.Minute).Format(time.RFC3339)
			case "duplicate-id":
				later.ID = original.ID
			case "wrong-group":
				original.GroupID = "asg-other"
			case "bad-read-trace":
				a := c.activities[original.ID]
				a.RequestIDs = nil
				c.activities[original.ID] = a
			case "exact-read-disagrees":
				a := c.activities[original.ID]
				a.Status = "SUCCESSFUL"
				a.RelatedInstances[0].Status = "SUCCESSFUL"
				c.activities[original.ID] = a
			case "original-end-missing":
				original.EndedAt = ""
			}
			_ = e.reconcileOnce()
			if e.state.Pending == nil || !e.state.Pending.Unknown || e.state.Pending.ActivityID != "" || e.state.RecoveryAudit != nil || len(n.writes) != 1 || c.writeCount() != 1 {
				t.Fatal("unproven failure led to restoration")
			}
		})
	}
}
func TestDiscoveredDeletionFailureStillChecksSurvivorOwnership(t *testing.T) {
	for _, mode := range []string{"foreign-owner", "foreign-operation", "changed-identity", "vm-terminating", "manual-member"} {
		t.Run(mode, func(t *testing.T) {
			_, c, n, e := discoveryFixture(t, true)
			startUnidentified(t, e, c, true)
			failUnidentified(e, c, "FAILED")
			id := e.state.Pending.InstanceID
			node := copyJSON(n.nodes[id])
			snap := copyJSON(c.snap)
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
			case "manual-member":
				for i := range c.snap.Members {
					if c.snap.Members[i].ID == id {
						c.snap.Members[i].CreationType = "MANUAL_ATTACH"
					}
				}
			}
			_ = e.reconcileOnce()
			if e.state.Pending == nil || e.state.Pending.Phase != "failed-delete" || e.state.Pending.DrainOutcome != "" || e.state.RecoveryAudit == nil || len(n.writes) != 1 || c.writeCount() != 1 {
				t.Fatal("failure discovery bypassed fresh ownership/survivor check")
			}
			n.nodes[id] = node
			c.snap = snap
			step(t, e)
			if e.state.Pending.DrainOutcome != "restored" || len(n.writes) != 2 || c.writeCount() != 1 {
				t.Fatal("restoration did not resume after authoritative read recovered")
			}
		})
	}
}
func TestDiscoveredDeletionFailureHistoryChangePreventsAuditCommit(t *testing.T) {
	for _, at := range []int{2, 3, 4, 5} {
		t.Run(string(rune('0'+at)), func(t *testing.T) {
			_, c, n, e := failureWithHistory(t, "retry-running")
			c.onHistory = func(call int) {
				if call == at {
					v := copyJSON(c.history[1])
					v.ID = "asa-ambiguous"
					c.history = append(c.history, v)
				}
			}
			_ = e.reconcileOnce()
			if e.state.Pending == nil || !e.state.Pending.Unknown || e.state.RecoveryAudit != nil || len(n.writes) != 1 || c.writeCount() != 1 {
				t.Fatal("changing history crossed audit commit")
			}
		})
	}
}
func TestDiscoveredDeletionFailureAuditRetainsFailureContract(t *testing.T) {
	cfg, _, _, e := failureWithHistory(t, "retry-running")
	step(t, e)
	if e.state.RecoveryAudit == nil {
		t.Fatal("missing audit")
	}
	record, err := findDiscoveredFailureRecord(cfg.Dir, *e.state.RecoveryAudit)
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"success-action", "completed-phase", "claimed-restored", "success-time", "remaining-deletions", "changed-origin", "missing-applied-sample"} {
		t.Run(mode, func(t *testing.T) {
			r := copyJSON(record)
			switch mode {
			case "success-action":
				r.Plan.Decision.Action = "confirm-remove-success"
			case "completed-phase":
				r.After.Pending.Phase = "completed"
			case "claimed-restored":
				r.After.Pending.DrainOutcome = "restored"
			case "success-time":
				r.After.LastCompletedAt = time.Now().UnixNano()
			case "remaining-deletions":
				r.Plan.Decision.Remaining = append(r.Plan.Decision.Remaining, r.Before.Batch.Selected[0])
			case "changed-origin":
				r.Before.Pending.CloudIntent = nil
			case "missing-applied-sample":
				r.Evidence = r.Evidence[:1]
			}
			if validRecoveryRecordOrigin(r) {
				t.Fatal("failure audit changed obligations")
			}
		})
	}
}

// A restoration's start/confirmation now follow the original cloud outcome.
// Keep testing that outcome and its repeated evidence through the linked chain.
func findDiscoveredFailureRecord(dir string, ref RecoveryAuditRef) (recoveryRecord, error) {
	var op string
	for i := 0; i < 3; i++ {
		r, err := readRecord(dir, ref)
		if err != nil {
			return r, err
		}
		if op == "" {
			op = r.Plan.Decision.OperationID
		}
		if r.Plan.Decision.OperationID != op {
			return r, errors.New("changed audit operation")
		}
		if r.Version == 2 && r.Plan.Decision.Action == "confirm-remove-failure" {
			return r, nil
		}
		if r.Version != 3 || r.Before.RecoveryAudit == nil {
			break
		}
		ref = *r.Before.RecoveryAudit
	}
	return recoveryRecord{}, errors.New("missing original discovery audit")
}
