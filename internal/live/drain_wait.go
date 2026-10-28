package live

import (
	"strings"
	"time"
)

const defaultDrainWaitAttempts = 15

func validControllerDrainDeadline(cfg Config) bool {
	return validPositiveDrainDeadline(cfg.DrainDeadline) || (cfg.GroupContract && cfg.DrainDeadline <= 0)
}

func (e *Engine) nonpositiveDrainMode() bool {
	return e.cfg.GroupContract && e.cfg.DrainDeadline <= 0
}

func earlierDrainTime(a, b time.Time) time.Time {
	if b.Before(a) {
		return b.UTC()
	}
	return a.UTC()
}

// Bind the whole batch before its first drain. New background children inherit
// this original bound even if the foreground returned or the process restarted.
func (e *Engine) prepareBatchDrainWait(b *Batch) error {
	if !e.nonpositiveDrainMode() || b.Kind != "shrink" {
		return nil
	}
	attempts := e.actionAttempts
	if attempts == 0 {
		attempts = defaultDrainWaitAttempts // accepted-operation API has no waiter argument
	}
	budget, err := e.completionBudget(b.InitialCount, b.Target, attempts)
	if err != nil {
		return err
	}
	until := b.StartedAt.Add(budget).UTC()
	if deadline, ok := e.workContext().Deadline(); ok {
		until = earlierDrainTime(until, deadline)
	}
	b.DrainWait = &DrainWaitWindow{Until: until, Attempts: attempts, Interval: e.cfg.StabilizationInterval}
	return nil
}

func (e *Engine) prepareOperationDrainWait(op *Operation) error {
	if !e.nonpositiveDrainMode() || op.Kind != "shrink" {
		return nil
	}
	w := DrainWaitWindow{Attempts: defaultDrainWaitAttempts, Interval: e.cfg.StabilizationInterval}
	var parent *DrainWaitWindow
	if b := e.state.Batch; b != nil && b.Kind == "shrink" {
		parent = b.DrainWait
		if parent == nil {
			return code("InvalidJournalDrainWait")
		}
		w = *parent
	}
	budget, err := completionBudgetFor(e.cfg.DrainDeadline, w.Interval, 1, 0, w.Attempts)
	if err != nil {
		return err
	}
	if e.cfg.DrainDeadline < 0 {
		// Nomad forces the allocations; still allow bounded observation of
		// actual completion/poststop. The raw negative value is not changed.
		budget = time.Minute
	}
	w.Until = op.StartedAt.Add(budget).UTC()
	if parent != nil {
		w.Until = earlierDrainTime(w.Until, parent.Until)
	}
	if deadline, ok := e.workContext().Deadline(); ok {
		w.Until = earlierDrainTime(w.Until, deadline)
	}
	op.DrainWait = &w
	return nil
}

func (e *Engine) operationDrainUntil(op *Operation) time.Time {
	if op.DrainWait != nil {
		return op.DrainWait.Until
	}
	if op.LegacyOrigin != nil {
		return op.StartedAt.Add(op.LegacyOrigin.DrainDeadline + time.Minute)
	}
	return op.StartedAt.Add(e.cfg.DrainDeadline + time.Minute)
}

// Past deadlines remain valid historical evidence. Reopening never substitutes
// time.Now here; an expired wait must be reconciled as expired.
func (e *Engine) validDrainWait(w *DrainWaitWindow, started time.Time, count, target int64, operation bool) bool {
	if w == nil || w.Until.IsZero() || started.IsZero() {
		return false
	}
	budget, err := completionBudgetFor(e.cfg.DrainDeadline, w.Interval, count, target, w.Attempts)
	if err != nil {
		return false
	}
	if operation && e.cfg.DrainDeadline < 0 {
		budget = time.Minute
	}
	return !w.Until.After(started.Add(budget))
}

func undrainedBatchOutcome(op *Operation) bool {
	return strings.HasPrefix(op.AbortReason, "batch-") && op.DrainOutcome == "not-submitted" &&
		!op.SDKLifecycle && op.NodeModifyIndex == 0 && op.NomadDrainIntent == nil
}

func (e *Engine) validateOperationDrainWait(op *Operation) error {
	bad := code("InvalidJournalDrainWait")
	// Historical custom pre keeps its original positive bound and has no
	// fabricated modern wait. Immutable old result prefixes are proven separately.
	if op.LegacyOrigin != nil || e.legacyCustomPrefixResult(op) {
		if op.DrainWait != nil {
			return bad
		}
		return nil
	}
	if !e.nonpositiveDrainMode() || op.Kind != "shrink" {
		if op.DrainWait != nil {
			return bad
		}
		return nil
	}
	if op.DrainWait == nil && undrainedBatchOutcome(op) {
		return nil
	}
	if e.state.Version != 3 || !e.validDrainWait(op.DrainWait, op.StartedAt, 1, 0, true) {
		return bad
	}
	return nil
}

func (e *Engine) validateBatchDrainWait(b *Batch, active bool) error {
	bad := code("InvalidJournalDrainWait")
	if !e.nonpositiveDrainMode() || b.Kind != "shrink" {
		if b.DrainWait != nil || b.LegacyDrainOrigin != nil {
			return bad
		}
		return nil
	}
	if b.LegacyDrainOrigin != nil && !e.validLegacyBatchDrainOrigin(b) {
		return bad
	}
	if e.state.Version != 3 || !e.validDrainWait(b.DrainWait, b.StartedAt, b.InitialCount, b.Target, false) {
		return bad
	}
	within := func(op *Operation) bool {
		if b.LegacyDrainOrigin != nil && op.LegacyOrigin != nil && op.ID == b.LegacyDrainOrigin.OperationID {
			return op.DrainWait == nil
		}
		if op.DrainWait == nil {
			return undrainedBatchOutcome(op)
		}
		w := op.DrainWait
		return w.Attempts == b.DrainWait.Attempts && w.Interval == b.DrainWait.Interval && !w.Until.After(b.DrainWait.Until)
	}
	for i := range b.Results {
		if b.LegacyDrainOrigin != nil && i < b.LegacyDrainOrigin.PrefixCount {
			continue
		}
		if !within(&b.Results[i]) {
			return bad
		}
	}
	if active && e.state.Pending != nil && !within(e.state.Pending) {
		return bad
	}
	return nil
}
