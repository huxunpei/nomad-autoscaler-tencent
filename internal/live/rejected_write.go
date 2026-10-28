package live

import (
	"errors"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadclient"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/tencentcloud"
)

func (e *Engine) validateRejectedWrite(op *Operation, pending bool) error {
	r := op.CloudRejection
	if r == nil {
		if op.Phase == "rejected" {
			return code("InvalidJournalRejection")
		}
		return nil
	}
	proof := tencentcloud.WriteResult{Action: r.Action, RequestID: op.RequestID, Attempted: true}
	localCode := r.Code == "LocalCredentialGateNotSubmitted" || r.Code == "LocalClientGateNotSubmitted" || r.Code == "LocalTransportNotSubmitted"
	if !e.cfg.GroupContract || e.state.Version != 3 || !(tencentcloud.IsRejectedWrite(proof, r) && !op.CloudNotSubmitted || op.CloudNotSubmitted && localCode && r.RequestID == "" && op.RequestID == "" && !r.Unknown && (r.Action == "ModifyDesiredCapacity" || r.Action == "RemoveInstances" || r.Action == "ScaleOutInstances")) ||
		op.CloudIntent == nil || op.CloudIntent.Action != r.Action || op.Unknown || op.ActivityID != "" ||
		op.ActivityStatus != "" || op.InstanceResult != "" || op.ActivityDiscovery != nil || op.DeleteSettlement != nil ||
		op.AbortReason != "" || op.AbsentObservations != 0 ||
		(pending && (op.Phase != "rejected" || op.DrainOutcome != "")) || (!pending && op.Phase != "completed") {
		return code("InvalidJournalRejection")
	}
	if op.Kind == "grow" {
		if op.InstanceID != "" || op.NodeID != "" || op.NodeModifyIndex != 0 || op.DrainOutcome != "" || op.SDKLifecycle {
			return code("InvalidJournalRejection")
		}
	} else if op.Kind != "shrink" || op.InstanceID == "" || op.NodeID == "" || op.NodeModifyIndex == 0 || (!pending && op.DrainOutcome != "restored") {
		return code("InvalidJournalRejection")
	}
	return nil
}

func (e *Engine) recordRejectedWrite(result tencentcloud.WriteResult, err error) error {
	op := e.state.Pending
	if op == nil || op.CloudIntent == nil || op.CloudIntent.Action != result.Action ||
		(op.Phase != "grow-intent" && op.Phase != "remove-intent") || !(tencentcloud.IsRejectedWrite(result, err) || tencentcloud.IsNotSubmittedWrite(result, err)) {
		return code("InvalidCloudRejection")
	}
	var provider *tencentcloud.Error
	if localCode := tencentcloud.NotSubmittedCode(result, err); localCode != "" {
		op.CloudNotSubmitted = true
		provider = &tencentcloud.Error{Action: result.Action, Code: localCode}
	} else {
		_ = errors.As(err, &provider)
	}
	copy := *provider
	op.CloudRejection = &copy
	op.RequestID = result.RequestID
	op.Phase = "rejected"
	op.Unknown = false
	e.state.LastEvent = "cloud-write-rejected"
	if op.CloudNotSubmitted {
		e.state.LastEvent = "cloud-write-not-submitted"
	}
	e.ready = false
	// No restoration before the response evidence is durable. If this save fails,
	// restart sees only the old write intent and must reconcile it as unknown.
	if err = e.save(); err != nil {
		return err
	}
	return e.reconcileRejectedWrite()
}

func (e *Engine) canReconcileRejectedWrite() bool {
	op := e.state.Pending
	return op != nil && op.Phase == "rejected" && op.CloudRejection != nil && e.validateOperation(op, true) == nil
}

func (e *Engine) reconcileRejectedWrite() error {
	op := e.state.Pending
	if op == nil || op.Phase != "rejected" || op.CloudRejection == nil || e.persistError != nil || e.validateOperation(op, true) != nil {
		return code("InvalidCloudRejection")
	}
	if op.Kind == "grow" {
		return e.finish("cloud-grow-rejected")
	}
	ctx, cancel := e.callContext()
	defer cancel()
	fresh, err := e.cloud.Observe(ctx, e.cfg.GroupID, op.InstanceID)
	if err != nil || e.validateSnapshot(fresh) != nil {
		return e.blocked("rejected-delete-observation-unavailable")
	}
	vm, ok := fresh.VMs[op.InstanceID]
	if !ok || !vm.Returned || vm.State != "RUNNING" || vm.ChargeType != "POSTPAID_BY_HOUR" {
		return e.blocked("rejected-delete-survivor-unconfirmed")
	}
	n, err := e.nomad.ReadNode(ctx, op.NodeID)
	if err != nil || nomadclient.CheckIdentity(n, e.identity(op)) != nil || !owned(n, op) || n.Status != "ready" {
		return e.blocked("rejected-delete-ownership-unconfirmed")
	}
	if restoredNode(n, e.identity(op), op) {
		if err = e.beginNodeRecovery(fresh, n); err != nil {
			return err
		}
		return e.confirmNodeRecovery(fresh, n)
	}
	if n.Eligibility != "ineligible" {
		return e.blocked("rejected-delete-ownership-unconfirmed")
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
		return e.blocked("rejected-delete-restore-unconfirmed")
	}
	return e.confirmNodeRecovery(fresh, *result.After)
}
