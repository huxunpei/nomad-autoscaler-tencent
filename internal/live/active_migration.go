package live

import (
	"errors"
	"fmt"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadclient"
	"os"
	"path/filepath"
	"reflect"
	"time"
)

// LegacyContract binds an unfinished SDK batch to its original fixed-capacity
// contract. Version 3 supplies recovery/audit semantics without relaxing the
// original cloud bounds or AS protection while that batch remains active.
type LegacyContract struct {
	Custom        *LegacyCustomBinding `json:"custom,omitempty"`
	SourceVersion int                  `json:"source_version"`
	MaxCapacity   int64                `json:"max_capacity"`
	BatchID       string               `json:"batch_id"`
	BackupSHA256  string               `json:"backup_sha256"`
}

func (e *Engine) usesGroupCapacityContract() bool {
	return e.cfg.GroupContract && e.state.LegacyContract == nil
}
func (e *Engine) validateLegacyContract() error {
	m := e.state.LegacyContract
	if m == nil {
		return nil
	}
	b := e.state.Batch
	if c := m.Custom; c != nil {
		if (c.InstanceMetaKey == "cloud_instance_id" && c.MappingProof != nil) || (c.InstanceMetaKey != "cloud_instance_id" && !validLegacyMappingProof(c.MappingProof, c.InstanceMetaKey, e.cfg.GroupID, e.cfg.Region, nil)) || !digestPattern.MatchString(c.PolicySHA256) || c.PolicySHA256 != e.state.NomadPolicySHA256 || c.InstanceMetaKey != e.state.NomadInstanceIDMetaKey || c.DrainDeadline != e.cfg.DrainDeadline || !validControllerDrainDeadline(e.cfg) || !legacyCustomWaitBindingMatches(e.cfg, c, b) {
			return code("InvalidLegacyCustomBinding")
		}
	}
	if !e.cfg.GroupContract || e.state.Version != 3 || !((m.SourceVersion == 1 && m.MaxCapacity == 1) || (m.SourceVersion == 2 && m.MaxCapacity >= 2 && m.MaxCapacity <= MaxSupportedCapacity)) || !operationID.MatchString(m.BatchID) || !digestPattern.MatchString(m.BackupSHA256) || b == nil || b.ID != m.BatchID || b.Phase != "running" || b.InitialCount > m.MaxCapacity || b.Target > m.MaxCapacity || e.state.ManualReview || e.state.NomadPolicySHA256 == "" {
		return code("InvalidLegacyContract")
	}
	return nil
}

// v1 permits exactly one instance and has no batch. These original limits
// prove the initial capacity (grow 0->1, shrink 1->0); the operation itself is
// retained unchanged. The deterministic batch is new bookkeeping, not a new
// selection, request identity or observation of historical allocation counts.
func legacyMigrationBatch(before State) *Batch {
	if before.Version == 2 {
		return before.Batch
	}
	op := before.Pending
	if before.Version != 1 || op == nil || before.Batch != nil || before.LastBatch != nil || before.MaxCapacity != 0 {
		return nil
	}
	id := "live-" + sha([]byte("legacy-v1-sdk-operation/" + op.ID))[:32]
	if id == op.ID {
		return nil
	}
	b := &Batch{ID: id, Kind: op.Kind, Phase: "running", Target: op.Target, StartedAt: op.StartedAt, InitialMembers: []string{}, Results: []Operation{}}
	switch {
	case op.Kind == "grow" && op.Target == 1:
		b.InitialCount = 0
	case op.Kind == "shrink" && op.Target == 0:
		b.InitialCount = 1
		b.InitialMembers = []string{op.InstanceID}
		b.Selected = []Candidate{{Identity: nomadclient.Identity{NodeID: op.NodeID, InstanceID: op.InstanceID, GroupID: before.GroupID, Region: before.Region}}}
	default:
		return nil
	}
	return b
}

func legacyMigrationAfter(cfg Config, before State, sourceHash string) (State, error) {
	e := &Engine{cfg: cfg, state: before}
	if !cfg.GroupContract || (before.Version != 1 && before.Version != 2) || before.LegacyContract != nil || before.ManualReview || legacyMigrationBatch(before) == nil || before.NomadPolicySHA256 == "" || !digestPattern.MatchString(sourceHash) || e.validateState() != nil {
		return State{}, code("LegacyOperationRequiresCompletion")
	}
	if op := before.Pending; op != nil && op.Kind == "shrink" && !op.SDKLifecycle {
		return State{}, code("LegacyOperationRequiresCompletion")
	}
	for _, r := range legacyMigrationBatch(before).Results {
		if r.Kind == "shrink" && !r.SDKLifecycle {
			return State{}, code("LegacyOperationRequiresCompletion")
		}
	}
	after := cloneRetryState(before)
	after.Version = 3
	after.MaxCapacity = 0
	limit := before.MaxCapacity
	if before.Version == 1 {
		limit = 1
		after.Batch = legacyMigrationBatch(before)
	}
	after.LegacyContract = &LegacyContract{SourceVersion: before.Version, MaxCapacity: limit, BatchID: after.Batch.ID, BackupSHA256: sourceHash}
	e.state = after
	if e.validateState() != nil {
		return State{}, code("InvalidLegacyMigration")
	}
	return after, nil
}

func (e *Engine) migrateActiveSDKState() error {
	adapter, ok := e.nomad.(sdkLifecycle)
	if !ok {
		return code("LegacyOperationRequiresCompletion")
	}
	key := ""
	if mapping, ok := e.nomad.(interface{ InstanceMetaKey() string }); ok {
		key = mapping.InstanceMetaKey()
	}
	if e.state.NomadPolicySHA256 == "" {
		return e.migrateLegacyCustom(adapter, key)
	}
	if adapter.PolicyDigest() != e.state.NomadPolicySHA256 || key != e.state.NomadInstanceIDMetaKey {
		return code("SDKPolicyChangeRequiresMigration")
	}
	sourceHash := sha(e.durableJournal)
	after, err := legacyMigrationAfter(e.cfg, e.state, sourceHash)
	if err != nil {
		return err
	}
	if err = e.backupLegacyState(fmt.Sprintf("live-v%d-before-group-contract.backup.json", e.state.Version)); err != nil {
		return err
	}
	now := time.Now().UTC()
	p := RecoveryPlan{Version: 2, Scope: scope(e.cfg), JournalSHA256: sourceHash, Operator: "automatic-reconciler", Reference: "legacy-sdk-contract/" + legacyMigrationReferenceID(e.state), CreatedAt: now, ExpiresAt: now.Add(5 * time.Minute), Decision: legacyMigrationDecision(e.state)}
	p.Digest = planDigest(p)
	r := &Recovery{e: e, automatic: true, workflowAudit: true, checkpoint: e.recoveryCheckpoint}
	_, err = r.commit(p, nil, after, sourceHash)
	return err
}
func legacyMigrationReferenceID(before State) string {
	if before.Batch != nil {
		return before.Batch.ID
	}
	if before.Version == 1 && before.Pending != nil {
		return before.Pending.ID
	}
	return ""
}
func legacyMigrationDecision(before State) RecoveryDecision {
	action, effect := "migrate-v2-sdk-contract", "resume-original-sdk-batch"
	if before.Version == 1 {
		action, effect = "migrate-v1-sdk-operation", "resume-original-sdk-operation"
	}
	return RecoveryDecision{Action: action, OperationID: legacyMigrationReferenceID(before), Count: int64(len(before.Members)), RestartEffect: effect}
}
func validActiveMigrationRecord(r recoveryRecord) bool {
	p := r.Plan
	if !r.Automatic || p.Version != 2 || p.Operator != "automatic-reconciler" || legacyMigrationReferenceID(r.Before) == "" || p.Reference != "legacy-sdk-contract/"+legacyMigrationReferenceID(r.Before) || len(p.Evidence) != 0 || len(r.Evidence) != 0 || p.CreatedAt.IsZero() || !p.ExpiresAt.Equal(p.CreatedAt.Add(5*time.Minute)) || r.AppliedAt.Before(p.CreatedAt) || !reflect.DeepEqual(p.Decision, legacyMigrationDecision(r.Before)) {
		return false
	}
	s := p.Scope
	cfg := Config{Dir: s.Directory, GroupID: s.GroupID, Region: s.Region, MaxCapacity: s.MaxCapacity, DrainDeadline: s.DrainDeadline, ScaleInProtection: s.ScaleInProtection, GroupContract: s.GroupContract}
	after, err := legacyMigrationAfter(cfg, r.Before, r.BeforeSHA256)
	return err == nil && reflect.DeepEqual(after, r.After)
}

// The original bytes remain immutable evidence. Reopening a migrated batch
// verifies both their hash and the batch's immutable selection/target contract.
func (e *Engine) checkLegacyContractBackup() error {
	m := e.state.LegacyContract
	if m == nil {
		return nil
	}
	path := filepath.Join(e.cfg.Dir, fmt.Sprintf("live-v%d-before-group-contract.backup.json", m.SourceVersion))
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return code("LegacyMigrationEvidenceInvalid")
	}
	if err != nil {
		return code("LegacyMigrationEvidenceUnavailable")
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Size() > stateFileLimit {
		return code("LegacyMigrationEvidenceInvalid")
	}
	raw, err := readPrivate(path, stateFileLimit)
	if err != nil {
		return code("LegacyMigrationEvidenceUnavailable")
	}
	var original State
	if sha(raw) != m.BackupSHA256 || decodeStrict(raw, &original) != nil {
		return code("LegacyMigrationEvidenceInvalid")
	}
	expected, err := legacyMigrationAfter(e.cfg, original, m.BackupSHA256)
	if m.Custom != nil {
		expected, err = legacyCustomAfter(e.cfg, original, m.BackupSHA256, *m.Custom)
	}
	if err != nil || !reflect.DeepEqual(expected.LegacyContract, m) || expected.NomadPolicySHA256 != e.state.NomadPolicySHA256 || expected.NomadInstanceIDMetaKey != e.state.NomadInstanceIDMetaKey || original.Sequence > e.state.Sequence || !reflect.DeepEqual(original.LaunchConfiguration, e.state.LaunchConfiguration) || !reflect.DeepEqual(expected.LastBatch, e.state.LastBatch) {
		return code("LegacyMigrationEvidenceInvalid")
	}
	if m.Custom != nil && !legacyCustomOperationsMatch(expected, e.state) {
		return code("LegacyMigrationEvidenceInvalid")
	}
	a, b := *expected.Batch, *e.state.Batch
	if len(b.Results) < len(a.Results) {
		return code("LegacyMigrationEvidenceInvalid")
	}
	for i := range a.Results {
		if !reflect.DeepEqual(a.Results[i], b.Results[i]) {
			return code("LegacyMigrationEvidenceInvalid")
		}
	}
	a.Results = nil
	b.Results = nil
	if !reflect.DeepEqual(a, b) {
		return code("LegacyMigrationEvidenceInvalid")
	}
	return nil
}

// Complete only the recorded original batch; later actions use the new bounds.
func finishLegacyContract(state *State) {
	if state.LegacyContract != nil && state.Pending == nil && state.Batch == nil && state.LastBatch != nil && state.LastBatch.ID == state.LegacyContract.BatchID {
		state.LegacyContract = nil
	}
}
