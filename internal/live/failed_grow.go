package live

import (
	"context"
	"reflect"
	"time"

	"github.com/huxunpei/nomad-autoscaler-tencent/internal/tencentcloud"
)

// Failure is an immutable provider fact, not proof that queued retries stopped.
// Keep the cloud obligation while returning the known failure to the caller.
func (e *Engine) canObserveGrowthFailure() bool {
	op := e.state.Pending
	return e.cfg.GroupContract && e.state.Version == 3 && e.state.LegacyContract == nil &&
		op != nil && op.Kind == "grow" && op.CloudIntent != nil && op.CloudRejection == nil && !op.DesiredConflict &&
		(!isScaleOut(op) || op.ActivityID != "") &&
		(!op.Unknown || (isScaleOut(op) && op.ActivityID != "") || (e.state.Cloud != nil && e.state.Cloud.Group.Desired == op.Target && e.state.Desired == op.Target)) &&
		op.GrowthFailure == nil && (op.Phase == "growing" || (op.Phase == "unknown" && op.Unknown)) &&
		e.state.Batch != nil && e.state.Batch.Kind == "grow" && e.state.Batch.Phase == "running"
}

func validGrowthFailure(op *Operation, group string, d ActivityDiscovery) bool {
	if op == nil || op.Kind != "grow" || op.CloudIntent == nil || validateCloudIntent(op) != nil ||
		d.ObservedAt.Before(op.CloudIntent.At) || len(d.History) != 1 || !sameActivity(d.Activity, d.History[0]) {
		return false
	}
	a := d.Activity
	if !validHistoryActivity(a, group, d.ObservedAt) || a.Type != "SCALE_OUT" || !failureTerminal(a.Status) || a.EndedAt == "" ||
		(op.ActivityID != "" && op.ActivityID != a.ID) {
		return false
	}
	start, _ := time.Parse(time.RFC3339, a.StartedAt)
	// Provider timestamps may have only second precision. An activity from
	// earlier within the intent's second is ambiguous, not proof of this write.
	earliest := op.CloudIntent.At
	if isScaleOut(op) {
		if !validScaleOutActivity(op, group, a, d.ObservedAt) {
			return false
		}
		earliest = earliest.Truncate(time.Second)
	}
	if isDesiredCAS(op) {
		earliest = earliest.Truncate(time.Second)
		for _, id := range op.CloudIntent.Desired.PreviousActivities {
			if id == a.ID {
				return false
			}
		}
	}
	if start.Before(earliest) {
		return false
	}
	for _, item := range a.RelatedInstances {
		if item.Status != "SUCCESSFUL" && item.Status != "FAILED" {
			return false
		}
	}
	return true
}

func (e *Engine) observeGrowthFailure(ctx context.Context) (bool, error) {
	if !e.canObserveGrowthFailure() || e.persistError != nil {
		return false, nil
	}
	reader, ok := e.cloud.(activityHistoryReader)
	if !ok && !isScaleOut(e.state.Pending) {
		return false, nil
	}
	op := e.state.Pending
	evidence := make([]RecoveryEvidence, 0, 2)
	for i := 0; i < 2; i++ {
		var history []tencentcloud.Activity
		var err error
		if isScaleOut(op) {
			var a tencentcloud.Activity
			a, err = e.cloud.Activity(ctx, e.cfg.GroupID, op.ActivityID)
			history = []tencentcloud.Activity{a}
		} else {
			history, err = reader.ActivitiesSince(ctx, e.cfg.GroupID, op.CloudIntent.At.Truncate(time.Second))
		}
		if err == nil && isDesiredCAS(op) {
			g := e.state.Cloud.Group
			latest, found := latestDesiredGrowth(op, e.cfg.GroupID, history, time.Now().UTC())
			if !found || g.Activity != "NOT_IN_ACTIVITY" || g.Count >= g.Desired || g.Desired != op.Target {
				return false, nil
			}
			history = []tencentcloud.Activity{latest}
		}
		if err != nil || ctx.Err() != nil || len(history) != 1 {
			return false, nil
		}
		d := ActivityDiscovery{Activity: history[0], History: normalizedActivityHistory(history), ObservedAt: time.Now().UTC()}
		if !validGrowthFailure(op, e.cfg.GroupID, d) || (i > 0 && !sameActivity(evidence[0].Discovery.Activity, d.Activity)) {
			return false, nil
		}
		evidence = append(evidence, RecoveryEvidence{Discovery: &d})
	}
	after, err := failedGrowthAfter(e.cfg, e.state, evidence)
	if err != nil {
		return false, err
	}
	now := time.Now().UTC()
	p := RecoveryPlan{Version: 2, Scope: scope(e.cfg), JournalSHA256: sha(e.durableJournal), Operator: "automatic-reconciler", Reference: "growth-failure/" + op.ID, CreatedAt: now, ExpiresAt: now.Add(5 * time.Minute), Decision: failedGrowthDecision(op, after), Evidence: evidence}
	p.Digest = planDigest(p)
	r := &Recovery{e: e, automatic: true, workflowAudit: true, checkpoint: e.recoveryCheckpoint}
	e.ready = false
	_, err = r.commit(p, evidence, after, p.JournalSHA256)
	if err != nil && (e.automaticAudit != nil || e.checkRecoveryPending() != nil || e.persistError != nil) {
		e.persistError = code("AutomaticRecoveryPersistenceFailed")
	}
	return true, err
}

func failedGrowthDecision(op *Operation, after State) RecoveryDecision {
	return RecoveryDecision{Action: "confirm-grow-failure", OperationID: op.ID, ActivityID: after.Pending.ActivityID, Count: int64(len(after.Members)), RestartEffect: "retain-growth-cloud-reconciliation"}
}

func failedGrowthAfter(cfg Config, before State, evidence []RecoveryEvidence) (State, error) {
	e := &Engine{cfg: cfg, state: before}
	if !e.canObserveGrowthFailure() || e.validateState() != nil || len(evidence) != 2 {
		return State{}, code("InvalidGrowthFailureEvidence")
	}
	for i, ev := range evidence {
		if ev.Discovery == nil || !validGrowthFailure(before.Pending, before.GroupID, *ev.Discovery) ||
			!reflect.DeepEqual(ev, RecoveryEvidence{Discovery: ev.Discovery}) ||
			(i > 0 && (ev.Discovery.ObservedAt.Before(evidence[0].Discovery.ObservedAt) || !sameActivity(ev.Discovery.Activity, evidence[0].Discovery.Activity))) {
			return State{}, code("InvalidGrowthFailureEvidence")
		}
	}
	after := cloneRetryState(before)
	op := after.Pending
	op.GrowthFailure = evidence[1].Discovery
	op.ActivityID, op.ActivityStatus = op.GrowthFailure.Activity.ID, op.GrowthFailure.Activity.Status
	op.Unknown, op.Phase = false, "failed-grow"
	after.LastEvent = "growth-failure-confirmed"
	if after.ErrorCode == "" {
		after.ErrorCode = "GrowthActivityFailed"
	}
	e.state = after
	if e.validateState() != nil {
		return State{}, code("InvalidGrowthFailureResult")
	}
	return after, nil
}

func validGrowthFailureRecord(r recoveryRecord) bool {
	p := r.Plan
	op := r.Before.Pending
	if !r.Automatic || p.Version != 2 || op == nil || r.After.Pending == nil || len(r.Evidence) != 2 || r.Evidence[1].Discovery == nil || p.CreatedAt.Before(r.Evidence[1].Discovery.ObservedAt) || p.Operator != "automatic-reconciler" || p.Reference != "growth-failure/"+op.ID ||
		p.CreatedAt.IsZero() || !p.ExpiresAt.Equal(p.CreatedAt.Add(5*time.Minute)) || r.AppliedAt.Before(p.CreatedAt) ||
		r.BeforeSHA256 != sha(jsonBytes(r.Before)) || !reflect.DeepEqual(p.Evidence, r.Evidence) ||
		!reflect.DeepEqual(p.Decision, failedGrowthDecision(op, r.After)) {
		return false
	}
	s := p.Scope
	cfg := Config{Dir: s.Directory, GroupID: s.GroupID, Region: s.Region, MaxCapacity: s.MaxCapacity, DrainDeadline: s.DrainDeadline, ScaleInProtection: s.ScaleInProtection, GroupContract: s.GroupContract}
	after, err := failedGrowthAfter(cfg, r.Before, r.Evidence)
	return err == nil && reflect.DeepEqual(after, r.After)
}

func (e *Engine) validateFailedGrowth(op *Operation, pending bool) error {
	if op.GrowthFailure == nil {
		if op.Phase == "failed-grow" {
			return code("InvalidJournalGrowthFailure")
		}
		return nil
	}
	if !e.cfg.GroupContract || e.state.Version != 3 || (pending && op.Phase != "failed-grow") || (!pending && op.Phase != "completed") || op.Unknown ||
		op.CloudRejection != nil || op.ActivityDiscovery != nil || op.DeleteSettlement != nil || op.AbsentObservations != 0 ||
		op.AbortReason != "" || op.SDKLifecycle || op.InstanceID != "" || op.NodeID != "" || op.NodeModifyIndex != 0 || op.InstanceResult != "" ||
		!validGrowthFailure(op, e.cfg.GroupID, *op.GrowthFailure) || op.ActivityStatus != op.GrowthFailure.Activity.Status {
		return code("InvalidJournalGrowthFailure")
	}
	return nil
}

func (e *Engine) hasGrowthFailureObligation() bool {
	op := e.state.Pending
	return e.canObserveGrowthFailure() || (op != nil && op.GrowthFailure != nil && op.Phase == "failed-grow" && e.validateFailedGrowth(op, true) == nil)
}
