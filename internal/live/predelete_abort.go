package live

import (
	"time"

	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadclient"
)

func validAbortReason(reason string) bool {
	switch reason {
	case "pre-delete-cloud-guard-rejected", "pre-delete-group-limits-changed",
		"pre-delete-member-guard-rejected", "pre-delete-observation-rejected",
		"sdk-drain-not-submitted", "sdk-drain-response-lost", "legacy-drain-response-lost", "drain-completion-timeout",
		"batch-membership-drift", "batch-group-limits-changed",
		"batch-next-member-not-selectable", "batch-observation-rejected", "batch-drain-wait-timeout":
		return true
	}
	return false
}

// A recorded draining phase precedes every cloud delete. Unknown writes and
// remove-intent are deliberately excluded: they must use outcome reconciliation.
func (e *Engine) canAbortPreDelete() bool {
	op := e.state.Pending
	return e.cfg.GroupContract && op != nil && op.Kind == "shrink" &&
		op.NodeModifyIndex > 0 &&
		!op.Unknown && (op.Phase == "draining" || op.Phase == "aborting") &&
		op.RequestID == "" && op.ActivityID == "" && op.ActivityStatus == "" && op.InstanceResult == ""
}

func (e *Engine) canAbortNextChild() bool {
	b := e.state.Batch
	return e.cfg.GroupContract && e.state.Pending == nil && b != nil && b.Kind == "shrink" &&
		b.Phase == "running" && !e.state.ManualReview && len(b.Results) > 0 && len(b.Results) < len(b.Selected)
}

func abortableObservation(err error) bool {
	if err == nil {
		return false
	}
	switch err.Error() {
	case "ExclusiveGroupGuardRejected", "LaunchConfigurationChanged",
		"NomadMemberMappingUnavailable", "NomadMemberMappingInvalid":
		return true
	}
	return false
}

func (e *Engine) drainTimedOut() bool {
	op := e.state.Pending
	return e.canAbortPreDelete() && op.Phase == "draining" &&
		!time.Now().Before(e.operationDrainUntil(op))
}

// Persist the terminal decision before any restoration. The phase intentionally
// is not a generic write intent: restart reconciles this idempotent desired
// state by reading the original node, never by resuming the abandoned deletion.
func (e *Engine) recordPreDeleteAbort(reason string) error {
	if !e.canAbortPreDelete() || !validAbortReason(reason) {
		return code("InvalidPreDeleteAbort")
	}
	op := e.state.Pending
	if op.Phase != "aborting" {
		op.Phase, op.AbortReason = "aborting", reason
	}
	e.ready = false
	e.state.LastEvent = "pre-delete-abort-pending"
	return e.save()
}

func (e *Engine) abortPreDelete(reason string) error {
	if !e.cfg.GroupContract {
		return e.blocked(reason)
	}
	if err := e.recordPreDeleteAbort(reason); err != nil {
		return err
	}
	return e.reconcilePreDeleteAbort()
}

func restoredNode(n nomadclient.Node, id nomadclient.Identity, op *Operation) bool {
	return nomadclient.CheckIdentity(n, id) == nil && owned(n, op) &&
		n.Status == "ready" && n.Eligibility == "eligible" && !n.Drain && !n.HasDrainStrategy
}

func (e *Engine) reconcilePreDeleteAbort() error {
	op := e.state.Pending
	if !e.canAbortPreDelete() || op.Phase != "aborting" || !validAbortReason(op.AbortReason) || e.persistError != nil {
		return code("InvalidPreDeleteAbort")
	}
	ctx, cancel := e.callContext()
	defer cancel()
	// A failed group guard or another unmapped member must not block undoing
	// our drain. Still require a fresh, valid observation of this exact VM.
	fresh, err := e.cloud.Observe(ctx, e.cfg.GroupID, op.InstanceID)
	if err != nil || e.validateSnapshot(fresh) != nil {
		return e.blocked("abort-cloud-observation-unavailable")
	}
	vm, ok := fresh.VMs[op.InstanceID]
	if !ok || !vm.Returned || vm.State != "RUNNING" || vm.ChargeType != "POSTPAID_BY_HOUR" {
		return e.blocked("abort-survivor-unconfirmed")
	}
	n, err := e.nomad.ReadNode(ctx, op.NodeID)
	if err != nil || nomadclient.CheckIdentity(n, e.identity(op)) != nil || !owned(n, op) || n.Status != "ready" {
		return e.blocked("abort-ownership-or-state-rejected")
	}
	if restoredNode(n, e.identity(op), op) {
		if err = e.beginNodeRecovery(fresh, n); err != nil {
			return err
		}
		return e.confirmNodeRecovery(fresh, n)
	}
	if n.Eligibility != "ineligible" {
		return e.blocked("abort-ownership-or-state-rejected")
	}
	// RestoreOwnedDrain rechecks identity and ownership before mutation. A lost
	// response remains in this phase; the next fresh read can prove completion
	// without repeating a completed restore. No cloud write occurs here.
	if err = e.beginNodeRecovery(fresh, n); err != nil {
		return err
	}
	op = e.state.Pending
	result, err := e.nomad.RestoreOwnedDrain(ctx, e.identity(op), op.ID)
	if result.NodeModifyIndex > 0 {
		op.NodeModifyIndex = result.NodeModifyIndex
	}
	if err != nil || result.Unknown || result.After == nil || !restoredNode(*result.After, e.identity(op), op) {
		return e.blocked("abort-restore-unconfirmed")
	}
	return e.confirmNodeRecovery(fresh, *result.After)
}

func (e *Engine) abortUnsubmittedDrain(reason string) error {
	op := e.state.Pending
	if !e.cfg.GroupContract || op == nil || op.Kind != "shrink" || op.Phase != "drain-intent" ||
		op.NodeModifyIndex != 0 || op.RequestID != "" || op.ActivityID != "" || !validAbortReason(reason) {
		return code("InvalidPreDeleteAbort")
	}
	op.AbortReason, op.DrainOutcome = reason, "not-submitted"
	return e.finish("pre-delete-aborted-drain-not-submitted")
}

// Serial batches have not drained the next selected node yet. Recording its
// unsubmitted outcome closes the batch without inventing a cloud failure or
// discarding the preceding successful child results.
func (e *Engine) abortNextChild(reason string) error {
	if !e.cfg.GroupContract {
		return e.blocked(reason)
	}
	if !e.canAbortNextChild() || !validAbortReason(reason) {
		return code("InvalidPreDeleteAbort")
	}
	b := e.state.Batch
	next := b.Selected[len(b.Results)]
	id, err := newOperationID()
	if err != nil {
		return err
	}
	e.state.Sequence++
	e.state.Pending = &Operation{ID: id, Kind: "shrink", Phase: "drain-intent", Target: b.InitialCount - int64(len(b.Results)) - 1,
		InstanceID: next.InstanceID, NodeID: next.NodeID, StartedAt: time.Now().UTC()}
	return e.abortUnsubmittedDrain(reason)
}
