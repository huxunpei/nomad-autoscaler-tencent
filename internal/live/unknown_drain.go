package live

import (
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadclient"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/tencentcloud"
	"path/filepath"
	"reflect"
	"time"
)

// This explicit source exists only on new SDK pre intents. Its absence on an
// older unknown operation is not evidence that no cloud deletion was submitted.
type NomadDrainIntent struct {
	At time.Time `json:"at"`
}

func (e *Engine) canRecoverUnknownSDKDrain() bool {
	op := e.state.Pending
	return e.cfg.GroupContract && e.state.Version == 3 && e.state.Batch != nil && !e.state.ManualReview && op != nil && op.SDKLifecycle && op.Kind == "shrink" && op.Phase == "unknown" && op.Unknown && op.NomadDrainIntent != nil && op.CloudIntent == nil && op.CloudRejection == nil && op.RequestID == "" && op.ActivityID == "" && op.ActivityStatus == "" && op.InstanceResult == "" && op.AbsentObservations == 0 && op.AbortReason == "" && op.DrainOutcome == "" && op.DeleteSettlement == nil && op.ActivityDiscovery == nil
}
func (e *Engine) reconcileUnknownSDKDrain() error {
	if !e.canRecoverUnknownSDKDrain() || e.persistError != nil {
		return code("InvalidUnknownSDKDrain")
	}
	ctx, cancel := e.callContext()
	defer cancel()
	op := e.state.Pending
	s, err := e.cloud.Observe(ctx, e.cfg.GroupID, op.InstanceID)
	if err != nil || e.validateSnapshot(s) != nil {
		return e.blocked("sdk-drain-cloud-readback-unavailable")
	}
	n, err := e.nomad.ReadNode(ctx, op.NodeID)
	if err != nil {
		return e.blocked("sdk-drain-node-readback-unavailable")
	}
	ev := RecoveryEvidence{Cloud: s, Nodes: []nomadclient.Node{n}}
	if _, err = unknownSDKDrainAfter(e.cfg, e.state, ev); err != nil {
		return e.blocked("sdk-drain-readback-unconfirmed")
	}
	// Persist the observation and classification before any eligibility write.
	if err = e.save(); err != nil {
		return err
	}
	raw, err := readPrivate(filepath.Join(e.cfg.Dir, "live.json"), stateFileLimit)
	if err != nil || sha(raw) != sha(jsonBytes(e.state)) {
		return code("RecoveryJournalConflict")
	}
	after, err := unknownSDKDrainAfter(e.cfg, e.state, ev)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	p := RecoveryPlan{Version: 2, Scope: scope(e.cfg), JournalSHA256: sha(raw), Operator: "automatic-reconciler", Reference: "sdk-drain-readback/" + op.ID, CreatedAt: now, ExpiresAt: now.Add(5 * time.Minute), Decision: unknownSDKDrainDecision(op, ev), Evidence: []RecoveryEvidence{ev}}
	p.Digest = planDigest(p)
	r := &Recovery{e: e, automatic: true, workflowAudit: true, checkpoint: e.recoveryCheckpoint}
	e.ready = false
	if _, err = r.commit(p, p.Evidence, after, p.JournalSHA256); err != nil {
		if e.automaticAudit != nil || e.checkRecoveryPending() != nil || e.persistError != nil {
			e.persistError = code("AutomaticRecoveryPersistenceFailed")
		}
		return err
	}
	return e.reconcilePreDeleteAbort()
}
func unknownSDKDrainDecision(op *Operation, ev RecoveryEvidence) RecoveryDecision {
	return RecoveryDecision{Action: "confirm-sdk-drain-observed", OperationID: op.ID, Count: int64(len(ev.Cloud.Members)), RestartEffect: "restore-recorded-sdk-drain"}
}
func unknownSDKDrainAfter(cfg Config, before State, ev RecoveryEvidence) (State, error) {
	after := cloneRetryState(before)
	e := &Engine{cfg: cfg, state: after}
	if !e.canRecoverUnknownSDKDrain() || e.validateState() != nil || e.validateSnapshot(ev.Cloud) != nil || len(ev.Nodes) != 1 || ev.Journal != nil || ev.Post != nil || ev.Discovery != nil || !reflect.DeepEqual(ev.Activity, tencentcloud.Activity{}) {
		return State{}, code("InvalidUnknownSDKDrainEvidence")
	}
	op := after.Pending
	n := ev.Nodes[0]
	m, present := cloudMember(ev.Cloud, op.InstanceID)
	vm, found := ev.Cloud.VMs[op.InstanceID]
	if !present || m.CreationType != "AUTO_CREATION" || !found || !vm.Returned || vm.State != "RUNNING" || vm.ChargeType != "POSTPAID_BY_HOUR" || nomadclient.CheckIdentity(n, e.identity(op)) != nil || !owned(n, op) || n.Status != "ready" || n.ModifyIndex == 0 ||
		(n.LastDrain.Status != "draining" && n.LastDrain.Status != "complete" && n.LastDrain.Status != "canceled") ||
		(n.LastDrain.Status == "canceled" && !restoredNode(n, e.identity(op), op)) ||
		(n.Eligibility != "ineligible" && !restoredNode(n, e.identity(op), op)) {
		return State{}, code("InvalidUnknownSDKDrainEvidence")
	}
	op.Unknown = false
	op.Phase = "aborting"
	op.AbortReason = "sdk-drain-response-lost"
	op.NodeModifyIndex = n.ModifyIndex
	after.LastEvent = "sdk-drain-observed-restoration-pending"
	e.state = after
	if e.validateState() != nil {
		return State{}, code("InvalidUnknownSDKDrainResult")
	}
	return after, nil
}
func validUnknownSDKDrainRecord(r recoveryRecord) bool {
	p := r.Plan
	op := r.Before.Pending
	if !r.Automatic || p.Version != 2 || op == nil || p.Operator != "automatic-reconciler" || p.Reference != "sdk-drain-readback/"+op.ID || len(r.Evidence) != 1 || !reflect.DeepEqual(p.Evidence, r.Evidence) || p.CreatedAt.IsZero() || !p.ExpiresAt.Equal(p.CreatedAt.Add(5*time.Minute)) || r.AppliedAt.Before(p.CreatedAt) || r.BeforeSHA256 != sha(jsonBytes(r.Before)) || !reflect.DeepEqual(p.Decision, unknownSDKDrainDecision(op, r.Evidence[0])) {
		return false
	}
	s := p.Scope
	cfg := Config{Dir: s.Directory, GroupID: s.GroupID, Region: s.Region, MaxCapacity: s.MaxCapacity, DrainDeadline: s.DrainDeadline, ScaleInProtection: s.ScaleInProtection, GroupContract: s.GroupContract}
	after, err := unknownSDKDrainAfter(cfg, r.Before, r.Evidence[0])
	return err == nil && reflect.DeepEqual(after, r.After)
}
