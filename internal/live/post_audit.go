package live

import (
	"context"
	"encoding/json"
	"path/filepath"
	"reflect"
	"time"

	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadclient"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/tencentcloud"
)

type sdkPostReporter interface {
	PostScaleInWithEvidence(context.Context, nomadclient.Identity, string, func() error) (nomadclient.PostScaleInResult, error)
}

func (e *Engine) postAuditStarted() (bool, error) {
	if e.state.RecoveryAudit == nil {
		return false, nil
	}
	r, err := readRecord(e.cfg.Dir, *e.state.RecoveryAudit)
	if err != nil {
		return false, err
	}
	op := e.state.Pending
	return r.Version == 3 && r.Plan.Decision.Action == "begin-sdk-post" && op != nil && r.Before.Pending.ID == op.ID && r.Before.Pending.Phase == op.Phase, nil
}

func (e *Engine) auditedSDKPost(ctx context.Context, adapter sdkLifecycle, cloud tencentcloud.Snapshot) error {
	reporter, ok := adapter.(sdkPostReporter)
	if !ok {
		return code("SDKPostEvidenceUnavailable")
	}
	started, err := e.postAuditStarted()
	if err != nil {
		return err
	}
	if !started {
		if err = e.commitPostAudit("begin-sdk-post", cloud, nil); err != nil {
			return err
		}
	}
	// begin commit replaces the state with a deep copy. The barrier must retain
	// the new pending pointer, original identity and phase; never a stale intent.
	op := e.state.Pending
	result, err := reporter.PostScaleInWithEvidence(ctx, e.identity(op), op.ID, e.durableSDKBarrier(op, op.Phase))
	if err != nil {
		if e.persistError != nil {
			return e.persistError
		}
		event := "sdk-post-incomplete"
		if op.Phase == "failed-delete-post" {
			event = "failed-delete-post-incomplete"
		}
		if saveErr := e.blocked(event); saveErr != nil {
			return saveErr
		}
		return code("SDKPostIncomplete")
	}
	return e.commitPostAudit("confirm-sdk-post", cloud, &result)
}

func (e *Engine) commitPostAudit(action string, cloud tencentcloud.Snapshot, result *nomadclient.PostScaleInResult) error {
	if e.persistError != nil {
		return e.persistError
	}
	if err := e.save(); err != nil {
		return err
	}
	raw, err := readPrivate(filepath.Join(e.cfg.Dir, "live.json"), stateFileLimit)
	if err != nil || sha(raw) != sha(jsonBytes(e.state)) {
		return code("RecoveryJournalConflict")
	}
	now := time.Now().UTC()
	ev := RecoveryEvidence{Cloud: cloud, Post: result}
	p := RecoveryPlan{Version: 2, Scope: scope(e.cfg), JournalSHA256: sha(raw), Operator: "automatic-reconciler",
		Reference: "sdk-post/" + e.state.Pending.ID, CreatedAt: now, ExpiresAt: now.Add(5 * time.Minute),
		Decision: postAuditDecision(e.state.Pending, action, ev), Evidence: []RecoveryEvidence{ev}}
	p.Digest = planDigest(p)
	after, err := postAuditAfter(e.cfg, e.state, p)
	if err != nil {
		return err
	}
	e.ready = false
	r := &Recovery{e: e, automatic: true, workflowAudit: true, checkpoint: e.recoveryCheckpoint}
	if _, err = r.commit(p, p.Evidence, after, p.JournalSHA256); err != nil {
		if e.automaticAudit != nil || e.checkRecoveryPending() != nil || e.persistError != nil {
			e.persistError = code("AutomaticRecoveryPersistenceFailed")
		}
		return err
	}
	return nil
}

func postAuditDecision(op *Operation, action string, ev RecoveryEvidence) RecoveryDecision {
	return RecoveryDecision{Action: action, OperationID: op.ID, ActivityID: op.ActivityID, Count: int64(len(ev.Cloud.Members)), RestartEffect: "resume-recorded-sdk-post"}
}

func postAuditAfter(cfg Config, before State, p RecoveryPlan) (State, error) {
	var after State
	if err := json.Unmarshal(jsonBytes(before), &after); err != nil {
		return after, code("InvalidSDKPostAudit")
	}
	e := &Engine{cfg: cfg, state: after}
	op := after.Pending
	if !cfg.GroupContract || after.Version != 3 || after.ManualReview || after.Batch == nil || op == nil || !requiresSDKPost(op) || op.Kind != "shrink" ||
		(op.Phase != "post-scale" && op.Phase != "failed-delete-post") || op.Unknown || op.AbsentObservations != 2 || e.validateState() != nil || len(p.Evidence) != 1 {
		return State{}, code("InvalidSDKPostAudit")
	}
	ev := p.Evidence[0]
	s := ev.Cloud
	if e.validateSnapshot(s) != nil || len(ev.Nodes) != 0 || ev.Discovery != nil || ev.Journal != nil || !reflect.DeepEqual(ev.Activity, tencentcloud.Activity{}) {
		return State{}, code("InvalidSDKPostEvidence")
	}
	vm, observed := s.VMs[op.InstanceID]
	_, present := cloudMember(s, op.InstanceID)
	if present || !observed || vm.Returned || s.Group.Count != op.Target || s.Group.Desired != op.Target || int64(len(s.Members)) != op.Target ||
		s.Group.Activity != "NOT_IN_ACTIVITY" || !e.batchMembershipMatches(s, true) ||
		(op.Phase == "failed-delete-post" && !e.failedDeleteSnapshotSettled(s)) {
		return State{}, code("InvalidSDKPostCloudEvidence")
	}
	switch p.Decision.Action {
	case "begin-sdk-post":
		if ev.Post != nil {
			return State{}, code("InvalidSDKPostEvidence")
		}
		after.LastEvent = "sdk-post-audit-started"
	case "confirm-sdk-post":
		result := ev.Post
		if result == nil || after.RecoveryAudit == nil || result.Identity != e.identity(op) || result.OperationID != op.ID || result.PolicySHA256 != after.NomadPolicySHA256 ||
			result.ObservedAt.IsZero() || result.ObservedAt.After(p.CreatedAt) || result.ObservedAt.Before(op.StartedAt) ||
			(result.Purge && result.Outcome != "purged" && result.Outcome != "already-absent") || (!result.Purge && result.Outcome != "purge-disabled") {
			return State{}, code("InvalidSDKPostResult")
		}
		event := "deletion-complete"
		if op.Phase == "failed-delete-post" {
			event = "failed-delete-settled-removed"
		}
		// The recorded decision owns the one completion time. Repair never obtains
		// another wall-clock timestamp for an already committed successful batch.
		finishOperationAt(&after, true, event, p.CreatedAt.UnixNano())
	default:
		return State{}, code("InvalidSDKPostAudit")
	}
	e.state = after
	if e.validateState() != nil {
		return State{}, code("InvalidSDKPostAuditResult")
	}
	return after, nil
}

func validPostAuditRecord(r recoveryRecord) bool {
	p := r.Plan
	op := r.Before.Pending
	if !r.Automatic || p.Version != 2 || op == nil || p.Operator != "automatic-reconciler" || p.Reference != "sdk-post/"+op.ID ||
		len(r.Evidence) != 1 || !reflect.DeepEqual(p.Evidence, r.Evidence) || p.CreatedAt.IsZero() || !p.ExpiresAt.Equal(p.CreatedAt.Add(5*time.Minute)) ||
		r.AppliedAt.Before(p.CreatedAt) || r.BeforeSHA256 != sha(jsonBytes(r.Before)) || !reflect.DeepEqual(p.Decision, postAuditDecision(op, p.Decision.Action, r.Evidence[0])) {
		return false
	}
	s := p.Scope
	cfg := Config{Dir: s.Directory, GroupID: s.GroupID, Region: s.Region, MaxCapacity: s.MaxCapacity, DrainDeadline: s.DrainDeadline, ScaleInProtection: s.ScaleInProtection, GroupContract: s.GroupContract}
	after, err := postAuditAfter(cfg, r.Before, p)
	return err == nil && reflect.DeepEqual(after, r.After)
}
