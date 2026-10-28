package live

import (
	"path/filepath"
	"reflect"
	"time"

	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadclient"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/tencentcloud"
)

// The original custom drain intent could only submit a Nomad write. Do not
// manufacture the SDK intent introduced by a later implementation, or use a
// healthy eligible node as proof that an uncertain request was not accepted.
func (e *Engine) canRecoverLegacyCustomDrain() bool {
	op := e.state.Pending
	if !e.cfg.GroupContract || e.state.Version != 3 || e.state.Batch == nil || e.state.ManualReview || op == nil || op.LegacyOrigin == nil || op.SDKLifecycle || op.Kind != "shrink" || op.Phase != "unknown" || !op.Unknown {
		return false
	}
	o := op.LegacyOrigin
	return (o.Phase == "drain-intent" || (o.Phase == "unknown" && o.Event == "drain-write-unknown")) && op.NomadDrainIntent == nil && op.CloudIntent == nil && op.CloudRejection == nil && op.RequestID == "" && op.ActivityID == "" && op.ActivityStatus == "" && op.InstanceResult == "" && op.AbsentObservations == 0 && op.AbortReason == "" && op.DrainOutcome == "" && op.DeleteSettlement == nil && op.ActivityDiscovery == nil
}

func legacyCustomDrainDecision(op *Operation, ev RecoveryEvidence) RecoveryDecision {
	return RecoveryDecision{Action: "confirm-legacy-custom-drain-observed", OperationID: op.ID, Count: int64(len(ev.Cloud.Members)), RestartEffect: "restore-observed-original-custom-drain"}
}

func legacyCustomDrainAfter(cfg Config, before State, ev RecoveryEvidence) (State, error) {
	after := cloneRetryState(before)
	e := &Engine{cfg: cfg, state: after}
	bad := code("InvalidLegacyCustomDrainEvidence")
	if !e.canRecoverLegacyCustomDrain() || e.validateState() != nil || e.validateSnapshot(ev.Cloud) != nil || len(ev.Nodes) != 1 || ev.Journal != nil || ev.Post != nil || ev.Discovery != nil || !reflect.DeepEqual(ev.Activity, tencentcloud.Activity{}) {
		return State{}, bad
	}
	op := after.Pending
	n := ev.Nodes[0]
	m, present := cloudMember(ev.Cloud, op.InstanceID)
	vm, found := ev.Cloud.VMs[op.InstanceID]
	if !present || m.CreationType != "AUTO_CREATION" || !found || !vm.Returned || vm.State != "RUNNING" || vm.ChargeType != "POSTPAID_BY_HOUR" || nomadclient.CheckIdentity(n, e.identity(op)) != nil || !owned(n, op) || n.Status != "ready" || n.ModifyIndex == 0 || (n.LastDrain.Status != "draining" && n.LastDrain.Status != "complete" && n.LastDrain.Status != "canceled") || (n.LastDrain.Status == "canceled" && !restoredNode(n, e.identity(op), op)) || (n.Eligibility != "ineligible" && !restoredNode(n, e.identity(op), op)) {
		return State{}, bad
	}
	op.Unknown = false
	op.Phase = "aborting"
	op.AbortReason = "legacy-drain-response-lost"
	// This is the fresh read's index, not an invented historical ACK. The source
	// bytes and previous index remain in the immutable migration/audit records.
	op.NodeModifyIndex = n.ModifyIndex
	after.LastEvent = "legacy-custom-drain-observed-restoration-pending"
	e.state = after
	if e.validateState() != nil {
		return State{}, bad
	}
	return after, nil
}

func (e *Engine) reconcileLegacyCustomDrain() error {
	if !e.canRecoverLegacyCustomDrain() || e.persistError != nil {
		return code("InvalidLegacyCustomDrain")
	}
	ctx, cancel := e.callContext()
	defer cancel()
	op := e.state.Pending
	s, err := e.cloud.Observe(ctx, e.cfg.GroupID, op.InstanceID)
	if err != nil || e.validateSnapshot(s) != nil {
		return e.blocked("legacy-custom-drain-cloud-readback-unavailable")
	}
	n, err := e.nomad.ReadNode(ctx, op.NodeID)
	if err != nil {
		return e.blocked("legacy-custom-drain-node-readback-unavailable")
	}
	ev := RecoveryEvidence{Cloud: s, Nodes: []nomadclient.Node{n}}
	if _, err = legacyCustomDrainAfter(e.cfg, e.state, ev); err != nil {
		return e.blocked("legacy-custom-drain-readback-unconfirmed")
	}
	if err = e.save(); err != nil {
		return err
	}
	raw, err := readPrivate(filepath.Join(e.cfg.Dir, "live.json"), stateFileLimit)
	if err != nil || sha(raw) != sha(jsonBytes(e.state)) {
		return code("RecoveryJournalConflict")
	}
	after, err := legacyCustomDrainAfter(e.cfg, e.state, ev)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	p := RecoveryPlan{Version: 2, Scope: scope(e.cfg), JournalSHA256: sha(raw), Operator: "automatic-reconciler", Reference: "legacy-custom-drain-readback/" + op.ID, CreatedAt: now, ExpiresAt: now.Add(5 * time.Minute), Decision: legacyCustomDrainDecision(op, ev), Evidence: []RecoveryEvidence{ev}}
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

func validLegacyCustomDrainRecord(r recoveryRecord) bool {
	p := r.Plan
	op := r.Before.Pending
	if r.Version != 3 || !r.Automatic || p.Version != 2 || op == nil || p.Operator != "automatic-reconciler" || p.Reference != "legacy-custom-drain-readback/"+op.ID || len(r.Evidence) != 1 || !reflect.DeepEqual(p.Evidence, r.Evidence) || p.CreatedAt.IsZero() || !p.ExpiresAt.Equal(p.CreatedAt.Add(5*time.Minute)) || r.AppliedAt.Before(p.CreatedAt) || r.BeforeSHA256 != sha(jsonBytes(r.Before)) || !reflect.DeepEqual(p.Decision, legacyCustomDrainDecision(op, r.Evidence[0])) {
		return false
	}
	s := p.Scope
	cfg := Config{Dir: s.Directory, GroupID: s.GroupID, Region: s.Region, MaxCapacity: s.MaxCapacity, DrainDeadline: s.DrainDeadline, ScaleInProtection: s.ScaleInProtection, GroupContract: s.GroupContract}
	after, err := legacyCustomDrainAfter(cfg, r.Before, r.Evidence[0])
	return err == nil && reflect.DeepEqual(after, r.After)
}
