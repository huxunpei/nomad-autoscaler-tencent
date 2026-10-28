package live

import (
	"encoding/json"
	"path/filepath"
	"reflect"
	"time"

	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadclient"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/tencentcloud"
)

// The same journal/record/marker/receipt transaction used for unknown outcomes
// also brackets known-failure node restoration. Version 3 is explicit origin;
// a manual plan or a display name cannot opt into automatic bookkeeping.
// No new journal field is needed: this single writer's audit head records the
// start, while DrainOutcome records completion. Fresh node checks still decide
// whether a Nomad write is allowed; a prior start is never permission to replay.
func (e *Engine) nodeRecoveryStarted() (bool, error) {
	if e.state.RecoveryAudit == nil {
		return false, nil
	}
	r, err := readRecord(e.cfg.Dir, *e.state.RecoveryAudit)
	if err != nil {
		return false, err
	}
	op := e.state.Pending
	return r.Version == 3 && r.Plan.Decision.Action == "begin-node-restore" &&
		op != nil && r.Before.Pending.ID == op.ID && r.Before.Pending.Phase == op.Phase, nil
}

func (e *Engine) beginNodeRecovery(s tencentcloud.Snapshot, n nomadclient.Node) error {
	started, err := e.nodeRecoveryStarted()
	if err != nil || started {
		return err
	}
	return e.commitNodeRecovery("begin-node-restore", s, n)
}
func (e *Engine) confirmNodeRecovery(s tencentcloud.Snapshot, n nomadclient.Node) error {
	started, err := e.nodeRecoveryStarted()
	if err != nil {
		return err
	}
	if !started {
		return code("NodeRecoveryStartMissing")
	}
	return e.commitNodeRecovery("confirm-node-restored", s, n)
}

func (e *Engine) commitNodeRecovery(action string, s tencentcloud.Snapshot, n nomadclient.Node) error {
	if e.persistError != nil {
		return e.persistError
	}
	// Persist current observations/modify index before hashing. In particular,
	// the known cloud refusal is already durable before this transaction starts.
	if err := e.save(); err != nil {
		return err
	}
	ev := RecoveryEvidence{Cloud: s, Nodes: []nomadclient.Node{n}}
	after, err := nodeRecoveryAfter(e.cfg, e.state, action, ev)
	if err != nil {
		return err
	}
	raw, err := readPrivate(filepath.Join(e.cfg.Dir, "live.json"), stateFileLimit)
	if err != nil || sha(raw) != sha(jsonBytes(e.state)) {
		return code("RecoveryJournalConflict")
	}
	now := time.Now().UTC()
	p := RecoveryPlan{Version: 2, Scope: scope(e.cfg), JournalSHA256: sha(raw), Operator: "automatic-reconciler",
		Reference: "node-restoration/" + e.state.Pending.ID, CreatedAt: now, ExpiresAt: now.Add(5 * time.Minute),
		Decision: nodeRecoveryDecision(e.state.Pending, action, ev), Evidence: []RecoveryEvidence{ev}}
	p.Digest = planDigest(p)
	r := &Recovery{e: e, automatic: true, workflowAudit: true, checkpoint: e.recoveryCheckpoint}
	e.ready = false
	if _, err = r.commit(p, p.Evidence, after, p.JournalSHA256); err != nil {
		if e.automaticAudit != nil || e.checkRecoveryPending() != nil || e.persistError != nil {
			e.persistError = code("AutomaticRecoveryPersistenceFailed")
		}
		return err
	}
	return nil
}

func nodeRecoveryDecision(op *Operation, action string, ev RecoveryEvidence) RecoveryDecision {
	return RecoveryDecision{Action: action, OperationID: op.ID, ActivityID: op.ActivityID,
		Count: int64(len(ev.Cloud.Members)), RestartEffect: "resume-recorded-node-restoration"}
}

// Reconstruct the only allowed delta. Audit validation cannot turn a restoration
// receipt into capacity changes, a new selection, a successful cooldown, or a
// rewrite of unrelated operation history.
func nodeRecoveryAfter(cfg Config, before State, action string, ev RecoveryEvidence) (State, error) {
	var after State
	if err := json.Unmarshal(jsonBytes(before), &after); err != nil {
		return after, code("InvalidNodeRecovery")
	}
	e := &Engine{cfg: cfg, state: after}
	op := after.Pending
	if !cfg.GroupContract || after.Version != 3 || after.ManualReview || after.Batch == nil ||
		op == nil || op.Kind != "shrink" || op.Unknown || op.DrainOutcome != "" || op.DeleteSettlement != nil ||
		(op.Phase != "aborting" && op.Phase != "rejected" && op.Phase != "failed-delete") || e.validateState() != nil ||
		e.validateSnapshot(ev.Cloud) != nil || len(ev.Nodes) != 1 || ev.Discovery != nil || ev.Post != nil || ev.Journal != nil || !reflect.DeepEqual(ev.Activity, tencentcloud.Activity{}) {
		return State{}, code("InvalidNodeRecovery")
	}
	n := ev.Nodes[0]
	vm, found := ev.Cloud.VMs[op.InstanceID]
	if !found || !vm.Returned || vm.State != "RUNNING" || vm.ChargeType != "POSTPAID_BY_HOUR" ||
		nomadclient.CheckIdentity(n, e.identity(op)) != nil || !owned(n, op) || n.Status != "ready" {
		return State{}, code("InvalidNodeRecoveryEvidence")
	}
	restored := restoredNode(n, e.identity(op), op)
	if op.Phase == "failed-delete" {
		member, present := cloudMember(ev.Cloud, op.InstanceID)
		if !present || member.CreationType != "AUTO_CREATION" || (!restored && n.LastDrain.Status != "complete") {
			return State{}, code("InvalidNodeRecoveryEvidence")
		}
	}
	switch action {
	case "begin-node-restore":
		if !restored && n.Eligibility != "ineligible" {
			return State{}, code("InvalidNodeRecoveryEvidence")
		}
		after.LastEvent = "node-restoration-audit-started"
	case "confirm-node-restored":
		if !restored || after.RecoveryAudit == nil {
			return State{}, code("InvalidNodeRecoveryEvidence")
		}
		op.DrainOutcome = "restored"
		switch op.Phase {
		case "aborting":
			finishOperation(&after, true, "pre-delete-aborted-node-restored")
		case "rejected":
			finishOperation(&after, true, "cloud-delete-rejected-node-restored")
		case "failed-delete":
			after.LastEvent = "failed-delete-node-restored-cloud-pending"
		}
	default:
		return State{}, code("InvalidNodeRecovery")
	}
	e.state = after
	if e.validateState() != nil {
		return State{}, code("InvalidNodeRecoveryResult")
	}
	return after, nil
}

func validNodeRecoveryRecord(r recoveryRecord) bool {
	p := r.Plan
	op := r.Before.Pending
	if !r.Automatic || p.Version != 2 || op == nil || p.Operator != "automatic-reconciler" ||
		p.Reference != "node-restoration/"+op.ID || len(r.Evidence) != 1 || !reflect.DeepEqual(p.Evidence, r.Evidence) ||
		p.CreatedAt.IsZero() || !p.ExpiresAt.Equal(p.CreatedAt.Add(5*time.Minute)) || r.AppliedAt.Before(p.CreatedAt) ||
		r.BeforeSHA256 != sha(jsonBytes(r.Before)) || !reflect.DeepEqual(p.Decision, nodeRecoveryDecision(op, p.Decision.Action, r.Evidence[0])) {
		return false
	}
	s := p.Scope
	cfg := Config{Dir: s.Directory, GroupID: s.GroupID, Region: s.Region, MaxCapacity: s.MaxCapacity,
		DrainDeadline: s.DrainDeadline, ScaleInProtection: s.ScaleInProtection, GroupContract: s.GroupContract}
	after, err := nodeRecoveryAfter(cfg, r.Before, p.Decision.Action, r.Evidence[0])
	return err == nil && reflect.DeepEqual(after, r.After)
}
