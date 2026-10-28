package live

import "github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadclient"

// A terminal activity failure and restoring Nomad scheduling are separate from
// cloud settlement. Keep the failed child and its batch pending until the cloud
// outcome is reconciled; do not finish the batch or advance to another node.
func (e *Engine) canReconcileSubmittedFailure() bool {
	op := e.state.Pending
	if !e.cfg.GroupContract || op == nil || op.Kind != "shrink" ||
		op.InstanceID == "" || op.NodeID == "" || op.AbortReason != "" {
		return false
	}
	if op.ActivityID == "" {
		return op.Unknown && op.Phase == "unknown" && op.CloudIntent != nil && validateCloudIntent(op) == nil
	}
	return op.Phase == "removing" || op.Phase == "unknown" || (op.Phase == "failed-delete" || op.Phase == "failed-delete-post")
}

// A changed group guard or an unrelated unmapped member must not prevent a
// safe restoration of our surviving node. This path independently verifies the
// exact failed activity and then reads the exact VM and node. It never performs
// a cloud write, nor interprets a failed group refresh as deletion success.
func (e *Engine) reconcileSubmittedFailureAfterReadError() (bool, error) {
	if !e.canReconcileSubmittedFailure() || e.persistError != nil {
		return false, nil
	}
	op := e.state.Pending
	if op.Phase == "failed-delete" || op.Phase == "failed-delete-post" {
		return true, e.reconcileFailedDelete()
	}
	ctx, cancel := e.callContext()
	defer cancel()
	if op.ActivityID == "" {
		err := e.reconcileRecordedOutcome(ctx)
		handled := err != nil || (e.state.Pending != nil && e.state.Pending.Phase == "failed-delete")
		return handled, err
	}
	a, err := e.cloud.Activity(ctx, e.cfg.GroupID, op.ActivityID)
	if err != nil || !validActivity(a, e.cfg.GroupID, op.ActivityID, op.InstanceID) ||
		!failureTerminal(a.Status) || a.RelatedInstances[0].Status != "FAILED" {
		return false, nil
	}
	op.ActivityStatus, op.InstanceResult = a.Status, a.RelatedInstances[0].Status
	return true, e.recordFailedDelete()
}

func (e *Engine) recordFailedDelete() error {
	op := e.state.Pending
	if !e.canReconcileSubmittedFailure() || !failureTerminal(op.ActivityStatus) || op.InstanceResult != "FAILED" {
		return code("InvalidFailedDeleteRecovery")
	}
	// Unlike a one-shot restore-intent, this durable phase describes an
	// idempotent obligation. Restart reads back before deciding to mutate Nomad.
	op.Phase, op.Unknown, op.AbsentObservations = "failed-delete", false, 0
	e.state.LastEvent = "failed-delete-restoration-pending"
	e.ready = false
	if err := e.save(); err != nil {
		return err
	}
	return e.reconcileFailedDelete()
}

func (e *Engine) reconcileFailedDelete() error {
	op := e.state.Pending
	if !e.canReconcileSubmittedFailure() || (op.Phase != "failed-delete" && op.Phase != "failed-delete-post") ||
		!failureTerminal(op.ActivityStatus) || op.InstanceResult != "FAILED" || op.Unknown || e.persistError != nil {
		return code("InvalidFailedDeleteRecovery")
	}
	if op.Phase == "failed-delete-post" {
		return e.finishFailedDeletePost()
	}
	ctx, cancel := e.callContext()
	defer cancel()
	fresh, err := e.cloud.Observe(ctx, e.cfg.GroupID, op.InstanceID)
	if err != nil || e.validateSnapshot(fresh) != nil {
		return e.resetFailedDeleteObservation("failed-delete-cloud-observation-unavailable")
	}
	vm, observed := fresh.VMs[op.InstanceID]
	_, present := cloudMember(fresh, op.InstanceID)
	if !present && observed && !vm.Returned {
		return e.reconcileFailedDeleteAbsence(ctx, fresh)
	}
	op.AbsentObservations, op.DeleteSettlement = 0, nil
	if op.DrainOutcome == "restored" {
		// Restoring scheduling does not cancel AS retries or confer ownership of a
		// later operator drain. Read cloud state, but never reassert eligibility.
		return e.blocked("failed-delete-awaiting-cloud-reconciliation")
	}
	member, present := cloudMember(fresh, op.InstanceID)
	if !present || member.CreationType != "AUTO_CREATION" || !observed || !vm.Returned ||
		vm.State != "RUNNING" || vm.ChargeType != "POSTPAID_BY_HOUR" {
		return e.blocked("failed-delete-survivor-unconfirmed")
	}
	n, err := e.nomad.ReadNode(ctx, op.NodeID)
	if err != nil || nomadclient.CheckIdentity(n, e.identity(op)) != nil || !owned(n, op) || n.Status != "ready" {
		return e.blocked("failed-delete-restore-ownership-rejected")
	}
	if restoredNode(n, e.identity(op), op) {
		if err = e.beginNodeRecovery(fresh, n); err != nil {
			return err
		}
		return e.confirmNodeRecovery(fresh, n)
	}
	if n.Eligibility != "ineligible" || n.LastDrain.Status != "complete" {
		return e.blocked("failed-delete-restore-state-rejected")
	}
	if err = e.beginNodeRecovery(fresh, n); err != nil {
		return err
	}
	op = e.state.Pending
	result, err := e.nomad.RestoreOwnedDrain(ctx, e.identity(op), op.ID)
	if result.NodeModifyIndex > 0 {
		op.NodeModifyIndex = result.NodeModifyIndex
	}
	if err != nil || result.Unknown || result.After == nil || !restoredNode(*result.After, e.identity(op), op) {
		// A lost ACK must not discard the obligation or convert a known cloud
		// failure into an unknown cloud write. The next iteration reads first.
		return e.blocked("failed-delete-restore-unconfirmed")
	}
	return e.confirmNodeRecovery(fresh, *result.After)
}
