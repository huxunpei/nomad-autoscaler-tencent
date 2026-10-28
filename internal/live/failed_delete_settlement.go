package live

import (
	"context"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/tencentcloud"
	"time"
)

type activityHistoryReader interface {
	ActivitiesSince(context.Context, string, time.Time) ([]tencentcloud.Activity, error)
}

func settledDeleteFailure(op *Operation) bool {
	return op != nil && op.Kind == "shrink" && failureTerminal(op.ActivityStatus) && op.InstanceResult == "FAILED" && op.DeleteSettlement != nil && op.AbsentObservations == 2
}

// History is evidence about the exact VM, not a promise that a retry queue is
// empty. A surviving VM cannot be released using this absent-VM proof.
func validHistoryActivity(a tencentcloud.Activity, group string, observed time.Time) bool {
	if !a.ValidFailureCodes() || !resource(a.ID, "asa") || a.GroupID != group || !safeValue.MatchString(a.Type) ||
		(a.Status != "INIT" && a.Status != "RUNNING" && a.Status != "SUCCESSFUL" && !failureTerminal(a.Status)) || len(a.RequestIDs) == 0 {
		return false
	}
	start, err := time.Parse(time.RFC3339, a.StartedAt)
	if err != nil || start.After(observed) {
		return false
	}
	if a.EndedAt != "" {
		end, err := time.Parse(time.RFC3339, a.EndedAt)
		if err != nil || end.Before(start) || end.After(observed) {
			return false
		}
	}
	for _, tr := range a.RequestIDs {
		if tr.Action != "DescribeAutoScalingActivities" || !requestID.MatchString(tr.RequestID) {
			return false
		}
	}
	seen := map[string]bool{}
	for _, v := range a.RelatedInstances {
		if !resource(v.ID, "ins") || seen[v.ID] || (v.Status != "INIT" && v.Status != "RUNNING" && v.Status != "SUCCESSFUL" && v.Status != "FAILED") {
			return false
		}
		seen[v.ID] = true
	}
	return true
}
func validDeleteSettlement(op *Operation, group string) bool {
	r := op.DeleteSettlement
	if r == nil || (op.AbsentObservations != 1 && op.AbsentObservations != 2) || r.FirstObservedAt.IsZero() || r.ObservedAt.Before(r.FirstObservedAt) ||
		(op.AbsentObservations == 1 && !r.ObservedAt.Equal(r.FirstObservedAt)) || (op.AbsentObservations == 2 && !r.ObservedAt.After(r.FirstObservedAt)) {
		return false
	}
	a, b := r.FailedActivity, r.RemovalActivity
	if op.ActivityDiscovery != nil && !sameActivity(op.ActivityDiscovery.Activity, a) {
		return false
	}
	if !validHistoryActivity(a, group, r.FirstObservedAt) || !validHistoryActivity(b, group, r.FirstObservedAt) ||
		!validActivity(a, group, op.ActivityID, op.InstanceID) || a.Type != "REMOVE_INSTANCES" || a.Status != op.ActivityStatus || !failureTerminal(a.Status) || a.RelatedInstances[0].Status != "FAILED" || a.EndedAt == "" ||
		b.ID == a.ID || b.Type != "REMOVE_INSTANCES" || (b.Status != "SUCCESSFUL" && b.Status != "PARTIALLY_SUCCESSFUL") || b.EndedAt == "" {
		return false
	}
	originalStart, _ := time.Parse(time.RFC3339, a.StartedAt)
	originalEnd, _ := time.Parse(time.RFC3339, a.EndedAt)
	laterStart, _ := time.Parse(time.RFC3339, b.StartedAt)
	if originalStart.Before(op.StartedAt.Truncate(time.Second)) || laterStart.Before(originalEnd) {
		return false
	}
	for _, v := range b.RelatedInstances {
		if v.ID == op.InstanceID {
			return v.Status == "SUCCESSFUL"
		}
	}
	return false
}
func (e *Engine) resetFailedDeleteObservation(event string) error {
	e.state.Pending.AbsentObservations = 0
	e.state.Pending.DeleteSettlement = nil
	return e.blocked(event)
}
func (e *Engine) failedDeleteSnapshotSettled(s tencentcloud.Snapshot) bool {
	op := e.state.Pending
	if e.validateSnapshot(s) != nil || s.Group.Activity != "NOT_IN_ACTIVITY" || s.Group.Status != "NORMAL" || s.Group.Count != op.Target || s.Group.Desired != op.Target || int64(len(s.Members)) != op.Target || !e.batchMembershipMatches(s, true) {
		return false
	}
	vm, observed := s.VMs[op.InstanceID]
	if !observed || vm.Returned {
		return false
	}
	for _, m := range s.Members {
		v := s.VMs[m.ID]
		if m.ID == op.InstanceID || m.CreationType != "AUTO_CREATION" || m.Lifecycle != "IN_SERVICE" || !v.Returned || v.State != "RUNNING" || v.ChargeType != "POSTPAID_BY_HOUR" {
			return false
		}
	}
	return true
}
func (e *Engine) reconcileFailedDeleteAbsence(ctx context.Context, snap tencentcloud.Snapshot) error {
	op := e.state.Pending
	if !e.failedDeleteSnapshotSettled(snap) {
		return e.resetFailedDeleteObservation("failed-delete-cloud-not-settled")
	}
	reader, ok := e.cloud.(activityHistoryReader)
	if !ok {
		return e.resetFailedDeleteObservation("failed-delete-history-unavailable")
	}
	original, err := e.cloud.Activity(ctx, e.cfg.GroupID, op.ActivityID)
	if err != nil {
		return e.resetFailedDeleteObservation("failed-delete-original-unconfirmed")
	}
	history, err := reader.ActivitiesSince(ctx, e.cfg.GroupID, op.StartedAt.Truncate(time.Second))
	if err != nil {
		return e.resetFailedDeleteObservation("failed-delete-history-unavailable")
	}
	// Read membership/capacity again after the paginated history, so a stale
	// snapshot cannot bless a still-running activity or a replacement member.
	fresh, err := e.cloud.Observe(ctx, e.cfg.GroupID, op.InstanceID)
	if err != nil || !e.failedDeleteSnapshotSettled(fresh) {
		return e.resetFailedDeleteObservation("failed-delete-cloud-not-settled")
	}
	var chosen *DeleteSettlement
	seen := map[string]bool{}
	foundOriginal := false
	for _, a := range history {
		if !validHistoryActivity(a, e.cfg.GroupID, fresh.At) || seen[a.ID] || a.Status == "INIT" || a.Status == "RUNNING" {
			return e.resetFailedDeleteObservation("failed-delete-history-unsettled")
		}
		seen[a.ID] = true
		if a.ID == op.ActivityID {
			if a.Status != original.Status || a.Type != original.Type || a.StartedAt != original.StartedAt || a.EndedAt != original.EndedAt || !validActivity(a, e.cfg.GroupID, op.ActivityID, op.InstanceID) || a.RelatedInstances[0].Status != "FAILED" {
				return e.resetFailedDeleteObservation("failed-delete-original-unconfirmed")
			}
			foundOriginal = true
			continue
		}
		r := &DeleteSettlement{FailedActivity: original, RemovalActivity: a, FirstObservedAt: fresh.At, ObservedAt: fresh.At}
		trial := *op
		trial.DeleteSettlement = r
		trial.AbsentObservations = 1
		if validDeleteSettlement(&trial, e.cfg.GroupID) && (chosen == nil || a.EndedAt > chosen.RemovalActivity.EndedAt || (a.EndedAt == chosen.RemovalActivity.EndedAt && a.ID > chosen.RemovalActivity.ID)) {
			chosen = r
		}
	}
	if chosen == nil || !foundOriginal {
		return e.resetFailedDeleteObservation("failed-delete-removal-unconfirmed")
	}
	previous := op.DeleteSettlement
	op.AbsentObservations = 1
	if previous != nil && previous.RemovalActivity.ID == chosen.RemovalActivity.ID && fresh.At.After(previous.ObservedAt) {
		chosen.FirstObservedAt = previous.FirstObservedAt
		op.AbsentObservations = 2
	}
	op.DeleteSettlement = chosen
	if !validDeleteSettlement(op, e.cfg.GroupID) {
		return e.resetFailedDeleteObservation("failed-delete-evidence-inconsistent")
	}
	if op.AbsentObservations < 2 {
		return e.blocked("failed-delete-removal-observed")
	}
	if !requiresSDKPost(op) {
		return e.finish("failed-delete-settled-removed")
	}
	op.Phase = "failed-delete-post"
	e.state.LastEvent = "failed-delete-post"
	if err = e.save(); err != nil {
		return err
	}
	return e.finishFailedDeletePost()
}
func (e *Engine) finishFailedDeletePost() error {
	op := e.state.Pending
	if op == nil || op.Phase != "failed-delete-post" || !requiresSDKPost(op) || !settledDeleteFailure(op) || !validDeleteSettlement(op, e.cfg.GroupID) {
		return code("InvalidFailedDeleteSettlement")
	}
	ctx, cancel := e.callContext()
	defer cancel()
	fresh, err := e.cloud.Observe(ctx, e.cfg.GroupID, op.InstanceID)
	if err != nil || !e.failedDeleteSnapshotSettled(fresh) {
		return e.blocked("failed-delete-post-cloud-unsettled")
	}
	adapter, ok := e.nomad.(sdkLifecycle)
	if !ok {
		return e.blocked("sdk-post-adapter-unavailable")
	}
	return e.auditedSDKPost(ctx, adapter, fresh)
}
