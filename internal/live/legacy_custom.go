package live

import (
	"context"
	"fmt"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadclient"
	"reflect"
	"time"
)

// LegacyOperationOrigin identifies the old custom workflow. It does not assert
// that an SDK pre-call or an uncertain provider write ever completed.
type LegacyOperationOrigin struct {
	SourceVersion int           `json:"source_version"`
	JournalSHA256 string        `json:"journal_sha256"`
	DrainDeadline time.Duration `json:"drain_deadline_nanoseconds"`
	Phase         string        `json:"phase"`
	Event         string        `json:"event,omitempty"`
}

type LegacyCustomBinding struct {
	MappingProof    *nomadclient.InstanceMappingProof `json:"mapping_proof,omitempty"`
	DrainWait       *DrainWaitWindow                  `json:"drain_wait,omitempty"`
	PolicySHA256    string                            `json:"policy_sha256"`
	InstanceMetaKey string                            `json:"instance_id_meta_key"`
	DrainDeadline   time.Duration                     `json:"drain_deadline_nanoseconds"`
}

func requiresSDKPost(op *Operation) bool {
	return op != nil && op.Kind == "shrink" && (op.SDKLifecycle || op.LegacyOrigin != nil)
}

func (e *Engine) validateLegacyOrigin(op *Operation) error {
	o := op.LegacyOrigin
	if o == nil {
		return nil
	}
	if !e.cfg.GroupContract || e.state.Version != 3 || op.SDKLifecycle || !digestPattern.MatchString(e.state.NomadPolicySHA256) ||
		(o.SourceVersion != 1 && o.SourceVersion != 2) || !digestPattern.MatchString(o.JournalSHA256) || o.DrainDeadline <= 0 || o.DrainDeadline > 24*time.Hour ||
		!safeValue.MatchString(o.Phase) || (o.Event != "" && !safeValue.MatchString(o.Event)) {
		return code("InvalidLegacyOperationOrigin")
	}
	if m := e.state.LegacyContract; m != nil && (m.Custom == nil || m.SourceVersion != o.SourceVersion || m.BackupSHA256 != o.JournalSHA256) {
		return code("InvalidLegacyOperationOrigin")
	}
	return nil
}

func legacyCustomSource(cfg Config, s State) bool {
	if !cfg.GroupContract || (s.Version != 1 && s.Version != 2) || s.LegacyContract != nil || s.NomadPolicySHA256 != "" || s.NomadInstanceIDMetaKey != "" || s.LastCompletedAt != 0 || s.DrainDeadlineNanoseconds <= 0 || time.Duration(s.DrainDeadlineNanoseconds) > 24*time.Hour {
		return false
	}
	plain := func(op *Operation) bool {
		return op == nil || (op.LegacyOrigin == nil && !op.SDKLifecycle && op.DrainWait == nil && op.GrowthFailure == nil && op.NomadDrainIntent == nil && op.CloudRejection == nil && op.CloudIntent == nil && op.ActivityDiscovery == nil && op.DeleteSettlement == nil && op.AbortReason == "" && op.DrainOutcome == "")
	}
	if !plain(s.Pending) || !plain(s.LastOperation) {
		return false
	}
	for _, b := range []*Batch{s.Batch, s.LastBatch} {
		if b == nil {
			continue
		}
		if b.Source != nil || b.DrainWait != nil || b.LegacyDrainOrigin != nil || b.CompletedAt != 0 {
			return false
		}
		for i := range b.Results {
			if !plain(&b.Results[i]) {
				return false
			}
		}
	}
	old := cfg
	old.GroupContract = false
	old.DrainDeadline = time.Duration(s.DrainDeadlineNanoseconds)
	old.MaxCapacity = s.MaxCapacity
	if s.Version == 1 {
		old.MaxCapacity = 1
	}
	return (&Engine{cfg: old, state: s}).validateState() == nil
}

func legacyCustomReference(s State) string {
	if s.Batch != nil {
		return s.Batch.ID
	}
	if s.Pending != nil {
		return s.Pending.ID
	}
	if s.ManualReview && s.LastBatch != nil {
		return s.LastBatch.ID
	}
	if s.ManualReview && s.LastOperation != nil {
		return s.LastOperation.ID
	}
	return ""
}

func legacyCustomAfter(cfg Config, before State, sourceHash string, binding LegacyCustomBinding) (State, error) {
	bad := code("LegacyOperationRequiresCompletion")
	if !legacyCustomSource(cfg, before) || legacyCustomReference(before) == "" || !digestPattern.MatchString(sourceHash) {
		return State{}, bad
	}
	// A destination mapping must freshly prove continuity with journaled IDs.
	// No old pre-call or historical request is rewritten by that proof.
	if !digestPattern.MatchString(binding.PolicySHA256) || !legacyMappingBindingMatches(before, binding) || binding.DrainDeadline != cfg.DrainDeadline || !validControllerDrainDeadline(cfg) {
		return State{}, code("SDKPolicyChangeRequiresMigration")
	}
	after := cloneRetryState(before)
	if before.ManualReview {
		op := before.LastOperation
		if before.Pending != nil || before.Batch != nil || op == nil || op.Kind != "shrink" || op.Phase != "completed" || !failureTerminal(op.ActivityStatus) || op.InstanceResult != "FAILED" || before.LastEvent != "deletion-failed-node-restored-cloud-retry-still-possible" {
			return State{}, bad
		}
		copy := *op
		copy.Phase = "failed-delete"
		copy.Unknown = false
		copy.AbsentObservations = 0
		after.Pending = &copy
		after.ManualReview = false
		if before.Version == 2 {
			if before.LastBatch == nil || before.LastBatch.Phase != "failed" || len(before.LastBatch.Results) == 0 || !reflect.DeepEqual(before.LastBatch.Results[len(before.LastBatch.Results)-1], *op) {
				return State{}, bad
			}
			after.Batch = cloneRetryState(before).LastBatch
			after.Batch.Phase = "running"
			after.Batch.Results = after.Batch.Results[:len(after.Batch.Results)-1]
		}
		after.LastBatch = nil
		after.LastOperation = nil
		if after.Batch != nil && len(after.Batch.Results) > 0 {
			v := after.Batch.Results[len(after.Batch.Results)-1]
			after.LastOperation = &v
		}
	}
	if after.Batch == nil && after.Version == 1 && after.Pending != nil {
		after.Batch = legacyMigrationBatch(after)
	}
	if after.Batch == nil {
		return State{}, bad
	}
	if op := after.Pending; op != nil {
		op.LegacyOrigin = &LegacyOperationOrigin{SourceVersion: before.Version, JournalSHA256: sourceHash, DrainDeadline: time.Duration(before.DrainDeadlineNanoseconds), Phase: func() string {
			if before.Pending != nil {
				return before.Pending.Phase
			}
			return before.LastOperation.Phase
		}(), Event: before.LastEvent}
		if op.Phase == "restore-intent" && failureTerminal(op.ActivityStatus) && op.InstanceResult == "FAILED" {
			op.Phase = "failed-delete"
			op.Unknown = false
		}
	}
	limit := before.MaxCapacity
	if before.Version == 1 {
		limit = 1
	}
	after.Version = 3
	after.MaxCapacity = 0
	after.DrainDeadlineNanoseconds = int64(cfg.DrainDeadline)
	after.NomadPolicySHA256 = binding.PolicySHA256
	after.NomadInstanceIDMetaKey = binding.InstanceMetaKey
	if err := adoptLegacyCustomDrainWait(cfg, &after, sourceHash, time.Duration(before.DrainDeadlineNanoseconds), before.Version, &binding); err != nil {
		return State{}, err
	}
	after.LegacyContract = &LegacyContract{SourceVersion: before.Version, MaxCapacity: limit, BatchID: after.Batch.ID, BackupSHA256: sourceHash, Custom: &binding}
	if (&Engine{cfg: cfg, state: after}).validateState() != nil {
		return State{}, code("InvalidLegacyCustomMigration")
	}
	return after, nil
}

func legacyCustomDecision(s State) RecoveryDecision {
	return RecoveryDecision{Action: "migrate-legacy-custom-contract", OperationID: legacyCustomReference(s), Count: int64(len(s.Members)), RestartEffect: "resume-original-custom-operation-with-sdk-post"}
}

func (e *Engine) migrateLegacyCustom(adapter sdkLifecycle, key string) error {
	binding := LegacyCustomBinding{PolicySHA256: adapter.PolicyDigest(), InstanceMetaKey: key, DrainDeadline: e.cfg.DrainDeadline}
	if key != "cloud_instance_id" {
		ids, err := legacyMappingOperands(e.state)
		prover, ok := adapter.(interface {
			ProveInstanceMapping(context.Context, string, []nomadclient.Identity) (nomadclient.InstanceMappingProof, error)
		})
		if err != nil || !ok {
			return code("LegacyMappingUnavailable")
		}
		ctx, cancel := e.callContext()
		defer cancel()
		proof, err := prover.ProveInstanceMapping(ctx, "cloud_instance_id", ids)
		if err != nil || !validLegacyMappingProof(&proof, key, e.cfg.GroupID, e.cfg.Region, ids) || !mappingProofFresh(&proof, time.Now().UTC()) {
			return code("LegacyMappingUnavailable")
		}
		binding.MappingProof = &proof
	}
	sourceHash := sha(e.durableJournal)
	after, err := legacyCustomAfter(e.cfg, e.state, sourceHash, binding)
	if err != nil {
		return err
	}
	if err = e.backupLegacyState(fmt.Sprintf("live-v%d-before-group-contract.backup.json", e.state.Version)); err != nil {
		return err
	}
	now := time.Now().UTC()
	if !mappingProofFresh(binding.MappingProof, now) {
		return code("LegacyMappingUnavailable")
	}
	p := RecoveryPlan{Version: 2, Scope: scope(e.cfg), JournalSHA256: sourceHash, Operator: "automatic-reconciler", Reference: "legacy-custom-contract/" + legacyCustomReference(e.state), CreatedAt: now, ExpiresAt: now.Add(5 * time.Minute), Decision: legacyCustomDecision(e.state)}
	p.Digest = planDigest(p)
	r := &Recovery{e: e, automatic: true, workflowAudit: true, checkpoint: e.recoveryCheckpoint}
	_, err = r.commit(p, nil, after, sourceHash)
	return err
}

func validLegacyCustomRecord(r recoveryRecord) bool {
	p := r.Plan
	if r.Version != 3 || !r.Automatic || p.Version != 2 || p.Operator != "automatic-reconciler" || r.After.LegacyContract == nil || r.After.LegacyContract.Custom == nil || p.Reference != "legacy-custom-contract/"+legacyCustomReference(r.Before) || legacyCustomReference(r.Before) == "" || len(p.Evidence) != 0 || len(r.Evidence) != 0 || p.CreatedAt.IsZero() || !p.ExpiresAt.Equal(p.CreatedAt.Add(5*time.Minute)) || r.AppliedAt.Before(p.CreatedAt) || !reflect.DeepEqual(p.Decision, legacyCustomDecision(r.Before)) {
		return false
	}
	if !mappingProofFresh(r.After.LegacyContract.Custom.MappingProof, p.CreatedAt) {
		return false
	}
	s := p.Scope
	cfg := Config{Dir: s.Directory, GroupID: s.GroupID, Region: s.Region, MaxCapacity: s.MaxCapacity, DrainDeadline: s.DrainDeadline, ScaleInProtection: s.ScaleInProtection, GroupContract: s.GroupContract}
	after, err := legacyCustomAfter(cfg, r.Before, r.BeforeSHA256, *r.After.LegacyContract.Custom)
	return err == nil && reflect.DeepEqual(after, r.After)
}

// Mutable observations may advance, but the origin and exact original intent
// remain tied to the immutable backup while this migration is active.
func legacyCustomOperationsMatch(expected, current State) bool {
	original := expected.Pending
	seen := false
	check := func(op *Operation) bool {
		if op == nil {
			return true
		}
		if original == nil {
			return op.LegacyOrigin == nil
		}
		if op.ID != original.ID {
			return op.LegacyOrigin == nil
		}
		seen = true
		if op.SDKLifecycle || !reflect.DeepEqual(op.LegacyOrigin, original.LegacyOrigin) || op.Kind != original.Kind || op.Target != original.Target || !op.StartedAt.Equal(original.StartedAt) {
			return false
		}
		if op.Kind == "shrink" && (op.InstanceID != original.InstanceID || op.NodeID != original.NodeID) {
			return false
		}
		return (original.RequestID == "" || op.RequestID == original.RequestID) && (original.ActivityID == "" || op.ActivityID == original.ActivityID)
	}
	if !check(current.Pending) || !check(current.LastOperation) {
		return false
	}
	for _, batch := range []*Batch{current.Batch, current.LastBatch} {
		if batch != nil {
			for i := range batch.Results {
				if !check(&batch.Results[i]) {
					return false
				}
			}
		}
	}
	return original == nil || seen
}
