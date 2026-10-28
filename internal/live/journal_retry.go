package live

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"github.com/huxunpei/nomad-autoscaler-tencent/internal/tencentcloud"
)

const journalRetryQueueName = "journal-save-pending.json"

// These are snapshots, never references to the engine's mutable state. A
// response received before a failed save must survive later read failures.
type JournalRetryEvidence struct {
	Durable           State     `json:"durable"`
	Attempted         State     `json:"attempted"`
	DurableSHA256     string    `json:"durable_sha256"`
	AttemptedSHA256   string    `json:"attempted_sha256"`
	FailedAt          time.Time `json:"failed_at"`
	UnsubmittedSource string    `json:"unsubmitted_source,omitempty"`
	Rollback          *State    `json:"rollback,omitempty"`
}
type journalRetryQueue struct {
	Version   int                  `json:"version"`
	Automatic bool                 `json:"automatic"`
	Origin    string               `json:"origin"`
	Scope     RecoveryScope        `json:"scope"`
	Failure   JournalRetryEvidence `json:"failure"`
}

func (e *Engine) captureFailedSave(attempted []byte) {
	if !e.cfg.GroupContract || e.automaticAudit != nil || e.pendingSave != nil || len(e.durableJournal) == 0 || len(attempted) == 0 {
		return
	}
	var durable, next State
	if decodeStrict(e.durableJournal, &durable) != nil || decodeStrict(attempted, &next) != nil {
		return
	}
	e.pendingSave = &JournalRetryEvidence{Durable: durable, Attempted: next, DurableSHA256: sha(e.durableJournal), AttemptedSHA256: sha(attempted), FailedAt: time.Now().UTC()}
}

// Called only on a branch that returned before its provider write, or after the
// actual SDK reports that its failed durable barrier attempted no write.
func (e *Engine) markUnsubmittedSave(source string, base []byte) {
	f := e.pendingSave
	if f == nil {
		return
	}
	var previous State
	if len(base) == 0 {
		previous = f.Durable
	} else if decodeStrict(base, &previous) != nil {
		return
	}
	f.UnsubmittedSource = source
	f.Rollback = &previous
}

func cloneRetryState(s State) State {
	var out State
	_ = json.Unmarshal(jsonBytes(s), &out)
	return out
}
func validAcceptRollback(next, base State) bool {
	op := next.Pending
	if op == nil || (op.Phase != "grow-intent" && op.Phase != "drain-intent") || op.Unknown || op.NodeModifyIndex != 0 || op.RequestID != "" || op.ActivityID != "" ||
		op.ActivityStatus != "" || op.InstanceResult != "" || next.Sequence != base.Sequence+1 || base.Pending != nil {
		return false
	}
	if base.Batch == nil {
		if next.Batch == nil || len(next.Batch.Results) != 0 || next.Batch.Phase != "running" {
			return false
		}
	} else if !reflect.DeepEqual(next.Batch, base.Batch) {
		return false
	}
	check := cloneRetryState(next)
	check.Pending = nil
	check.Sequence = base.Sequence
	check.Batch = base.Batch
	check.LastEvent = base.LastEvent
	return reflect.DeepEqual(check, base)
}

func journalRetryTarget(cfg Config, f JournalRetryEvidence) (State, error) {
	invalid := code("InvalidJournalRetryEvidence")
	if !cfg.GroupContract || f.FailedAt.IsZero() || f.DurableSHA256 != sha(jsonBytes(f.Durable)) || f.AttemptedSHA256 != sha(jsonBytes(f.Attempted)) {
		return State{}, invalid
	}
	for _, s := range []State{f.Durable, f.Attempted} {
		e := &Engine{cfg: cfg, state: s}
		if s.Version != 3 || e.validateState() != nil {
			return State{}, invalid
		}
	}
	if f.Durable.NomadPolicySHA256 != f.Attempted.NomadPolicySHA256 || f.Durable.NomadInstanceIDMetaKey != f.Attempted.NomadInstanceIDMetaKey ||
		!reflect.DeepEqual(f.Durable.RecoveryAudit, f.Attempted.RecoveryAudit) {
		return State{}, invalid
	}
	if f.UnsubmittedSource == "" {
		if f.Rollback != nil {
			return State{}, invalid
		}
		if op := f.Attempted.Pending; op != nil && strings.HasSuffix(op.Phase, "-intent") {
			return State{}, code("JournalIntentOutcomeUnclassified")
		}
		return cloneRetryState(f.Attempted), nil
	}
	if f.Rollback == nil {
		return State{}, invalid
	}
	base := *f.Rollback
	if e := (&Engine{cfg: cfg, state: base}); e.validateState() != nil || base.Version != 3 {
		return State{}, invalid
	}
	switch f.UnsubmittedSource {
	case "accept-before-provider":
		if !reflect.DeepEqual(base, f.Durable) || !validAcceptRollback(f.Attempted, base) {
			return State{}, invalid
		}
	case "sdk-drain-barrier":
		op := f.Attempted.Pending
		if !reflect.DeepEqual(f.Durable, f.Attempted) || op == nil || !op.SDKLifecycle || op.Phase != "drain-intent" || !validAcceptRollback(f.Attempted, base) {
			return State{}, invalid
		}
	case "remove-before-provider":
		a, d := f.Attempted.Pending, f.Durable.Pending
		if !reflect.DeepEqual(base, f.Durable) || a == nil || d == nil || a.Phase != "remove-intent" || d.Phase != "draining" || a.ID != d.ID || a.RequestID != "" || a.ActivityID != "" || a.CloudIntent == nil || a.CloudIntent.Action != "RemoveInstances" {
			return State{}, invalid
		}
		check := cloneRetryState(f.Attempted)
		check.Pending = d
		check.LastEvent = f.Durable.LastEvent
		if !reflect.DeepEqual(check, f.Durable) {
			return State{}, invalid
		}
	default:
		return State{}, invalid
	}
	return cloneRetryState(base), nil
}

func retryQueue(cfg Config, f JournalRetryEvidence) journalRetryQueue {
	return journalRetryQueue{Version: 1, Automatic: true, Origin: "live-journal-writer", Scope: scope(cfg), Failure: f}
}
func (e *Engine) loadJournalRetryQueue() error {
	path := filepath.Join(e.cfg.Dir, journalRetryQueueName)
	if _, err := os.Lstat(path); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return code("JournalRetryQueueUnavailable")
	}
	raw, err := readPrivate(path, 3*stateFileLimit)
	var q journalRetryQueue
	if err != nil || decodeStrict(raw, &q) != nil || q.Version != 1 || !q.Automatic || q.Origin != "live-journal-writer" || q.Scope != scope(e.cfg) || sha(raw) != sha(jsonBytes(q)) {
		return code("JournalRetryQueueInvalid")
	}
	if _, err = journalRetryTarget(e.cfg, q.Failure); err != nil {
		return err
	}
	e.pendingSave = &q.Failure
	return nil
}
func (e *Engine) ensureJournalRetryQueue(f JournalRetryEvidence) error {
	path := filepath.Join(e.cfg.Dir, journalRetryQueueName)
	want := jsonBytes(retryQueue(e.cfg, f))
	if _, err := os.Lstat(path); errors.Is(err, os.ErrNotExist) {
		return writeOnce(path, want)
	} else if err != nil {
		return code("JournalRetryQueueUnavailable")
	}
	raw, err := readPrivate(path, 3*stateFileLimit)
	if err != nil || sha(raw) != sha(want) {
		return code("JournalRetryQueueConflict")
	}
	return nil
}
func (e *Engine) clearJournalRetryQueue(f JournalRetryEvidence) error {
	path := filepath.Join(e.cfg.Dir, journalRetryQueueName)
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		if err != nil {
			return code("JournalRetryQueueUnavailable")
		}
		raw, err := readPrivate(path, 3*stateFileLimit)
		if err != nil || sha(raw) != sha(jsonBytes(retryQueue(e.cfg, f))) {
			return code("JournalRetryQueueConflict")
		}
		if err = os.Remove(path); err != nil {
			return code("JournalRetryQueueCleanupFailed")
		}
	}
	if syncDir(e.cfg.Dir) != nil {
		return code("JournalRetryQueueCleanupFailed")
	}
	return nil
}

func journalRetryDecision(f JournalRetryEvidence, target State) RecoveryDecision {
	id, activity := "", ""
	if op := f.Attempted.Pending; op != nil {
		id, activity = op.ID, op.ActivityID
	} else if op := f.Attempted.LastOperation; op != nil {
		id, activity = op.ID, op.ActivityID
	}
	return RecoveryDecision{Action: "retry-journal-save", OperationID: id, ActivityID: activity, Count: int64(len(target.Members)), RestartEffect: "resume-local-journal-without-provider-replay"}
}

func (e *Engine) resumeJournalSave() (err error) {
	f := e.pendingSave
	if f == nil {
		return nil
	}
	// Remain frozen until both the audit and its queue cleanup are durable.
	defer func() {
		if err != nil {
			e.persistError = err
			e.ready = false
		}
	}()
	if e.checkRecoveryPending() != nil && e.automaticAudit == nil {
		return code("RecoveryAuditRepairRequired")
	}
	target, err := journalRetryTarget(e.cfg, *f)
	if err != nil {
		return err
	}
	if adapter, ok := e.nomad.(sdkLifecycle); ok && adapter.PolicyDigest() != target.NomadPolicySHA256 {
		return code("SDKPolicyChangeRequiresMigration")
	}
	raw, err := readPrivate(filepath.Join(e.cfg.Dir, "live.json"), stateFileLimit)
	if err != nil {
		return err
	}
	loaded := &Engine{cfg: e.cfg}
	exists, err := loaded.load()
	if err != nil || !exists {
		return code("JournalRetryJournalInvalid")
	}
	if err = loaded.checkRecoveryAudit(); err != nil {
		return err
	}
	// A process can die after audit commit but before deleting the retry queue.
	// Recognize that exact result; do not append another audit or timestamp.
	if ref := loaded.state.RecoveryAudit; ref != nil {
		record, readErr := readRecord(e.cfg.Dir, *ref)
		if readErr != nil {
			return readErr
		}
		if record.Plan.Decision.Action == "retry-journal-save" && len(record.Evidence) == 1 && reflect.DeepEqual(record.Evidence[0].Journal, f) &&
			record.Plan.Scope == scope(e.cfg) && sha(raw) == sha(jsonBytes(resultState(record, *ref))) {
			if err = e.clearJournalRetryQueue(*f); err != nil {
				return err
			}
			e.state = loaded.state
			e.durableJournal = loaded.durableJournal
			e.pendingSave = nil
			e.persistError = nil
			e.ready = false
			return nil
		}
	}
	if sha(raw) != f.DurableSHA256 && sha(raw) != f.AttemptedSHA256 {
		return code("JournalRetryJournalConflict")
	}
	if !reflect.DeepEqual(loaded.state.RecoveryAudit, target.RecoveryAudit) {
		return code("JournalRetryAuditHeadConflict")
	}
	if err = e.ensureJournalRetryQueue(*f); err != nil {
		return err
	}
	now := time.Now().UTC()
	ev := RecoveryEvidence{Journal: f}
	p := RecoveryPlan{Version: 2, Scope: scope(e.cfg), JournalSHA256: sha(raw), Operator: "automatic-reconciler", Reference: "journal-write/" + f.AttemptedSHA256, CreatedAt: now, ExpiresAt: now.Add(5 * time.Minute), Decision: journalRetryDecision(*f, target), Evidence: []RecoveryEvidence{ev}}
	p.Digest = planDigest(p)
	e.state = loaded.state
	e.durableJournal = loaded.durableJournal
	e.persistError = nil
	e.ready = false
	r := &Recovery{e: e, automatic: true, workflowAudit: true, checkpoint: e.recoveryCheckpoint}
	if _, err = r.commit(p, p.Evidence, target, p.JournalSHA256); err != nil {
		return err
	}
	if err = e.clearJournalRetryQueue(*f); err != nil {
		return err
	}
	e.pendingSave = nil
	e.persistError = nil
	return nil
}

func validJournalRetryRecord(record recoveryRecord) bool {
	p := record.Plan
	if !record.Automatic || p.Version != 2 || p.Operator != "automatic-reconciler" || len(record.Evidence) != 1 || !reflect.DeepEqual(p.Evidence, record.Evidence) || p.CreatedAt.IsZero() ||
		!p.ExpiresAt.Equal(p.CreatedAt.Add(5*time.Minute)) || record.AppliedAt.Before(p.CreatedAt) || record.BeforeSHA256 != sha(jsonBytes(record.Before)) {
		return false
	}
	ev := record.Evidence[0]
	f := ev.Journal
	if f == nil || ev.Post != nil || ev.Discovery != nil || len(ev.Nodes) != 0 || !reflect.DeepEqual(ev.Cloud, tencentcloud.Snapshot{}) || !reflect.DeepEqual(ev.Activity, tencentcloud.Activity{}) ||
		p.Reference != "journal-write/"+f.AttemptedSHA256 || (record.BeforeSHA256 != f.DurableSHA256 && record.BeforeSHA256 != f.AttemptedSHA256) {
		return false
	}
	s := p.Scope
	cfg := Config{Dir: s.Directory, GroupID: s.GroupID, Region: s.Region, MaxCapacity: s.MaxCapacity, DrainDeadline: s.DrainDeadline, ScaleInProtection: s.ScaleInProtection, GroupContract: s.GroupContract}
	target, err := journalRetryTarget(cfg, *f)
	return err == nil && reflect.DeepEqual(p.Decision, journalRetryDecision(*f, target)) && reflect.DeepEqual(target, record.After)
}
