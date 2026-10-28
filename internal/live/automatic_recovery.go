package live

import (
	"context"
	"time"
)

// reconcileRecordedOutcome runs under the engine's single-writer lock. It uses
// the same read-only, exact-activity proof as the manual recovery entrypoint;
// a count match alone is never sufficient. No provider mutation is replayed.
func (e *Engine) reconcileRecordedOutcome(ctx context.Context) error {
	op := e.state.Pending
	if op == nil || !op.Unknown {
		return nil
	}
	activity := op.ActivityID
	reference := "recorded-activity/" + op.ID
	if activity == "" {
		found, err := discoverActivity(ctx, e.cloud, e.cfg.GroupID, op)
		if err != nil {
			return nil
		}
		activity = found.Activity.ID
		reference = "discovered-activity/" + op.ID
	}
	r := &Recovery{e: e, cloud: e.cloud, nomad: e.nomad, delay: time.Second, automatic: true, checkpoint: e.recoveryCheckpoint}
	plan, err := r.Preview(ctx, activity, "automatic-reconciler", reference)
	if err != nil {
		// Keep the original unknown intent; a later observation may establish
		// completion. Neither a transient error nor ambiguous evidence is success.
		return nil
	}
	if _, err = r.Apply(ctx, plan, plan.Digest); err != nil {
		// An interrupted audit transaction must not permit subsequent writes in
		// this process, even when the journal replacement has already completed.
		// Pre-commit evidence changes remain retryable without freezing the engine.
		if e.automaticAudit != nil || e.checkRecoveryPending() != nil || e.persistError != nil {
			e.persistError = code("AutomaticRecoveryPersistenceFailed")
		}
		return err
	}
	// Publish readiness only after another fresh observation. Status continues
	// to use its immutable snapshot while these bounded reads are in progress.
	e.ready = false
	if pending := e.state.Pending; pending != nil && pending.Phase == "failed-delete" {
		// The local audit must be committed before any Nomad restoration. Its
		// eligibility and ownership checks are freshly performed by this stage.
		return e.reconcileFailedDelete()
	}
	e.observeError = nil
	return nil
}
