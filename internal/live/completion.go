package live

import (
	"context"
	"math"
	"time"
)

// ScaleAndWait is the public target contract. Scale retains the experimental
// accepted-operation API for existing callers. Only this waiter reconciles the
// batch while it owns the foreground action; Status reads immutable snapshots.
func (e *Engine) ScaleAndWait(target int64, attempts int) (bool, error) {
	return e.ScaleAndWaitWithPolicy(target, attempts, "")
}

func (e *Engine) ScaleAndWaitWithPolicy(target int64, attempts int, policyID string) (bool, error) {
	if _, err := e.completionBudget(0, 1, attempts); err != nil {
		return false, err
	}
	e.mu.Lock()
	if !e.cfg.GroupContract {
		e.mu.Unlock()
		return false, code("GroupContractRequired")
	}
	if e.waiting {
		e.mu.Unlock()
		return false, ErrBusy
	}
	e.waiting = true
	e.actionAttempts = attempts
	e.publishSnapshot(true)
	if !requestID.MatchString(policyID) {
		policyID = ""
	}
	e.actionSource = &ActionSource{PolicyID: policyID, RequestedCount: target}
	changed, err := e.scaleLocked(target, attempts)
	e.actionSource = nil
	batchID := ""
	if b := e.state.Batch; b != nil {
		batchID = b.ID
	}
	ctx := e.workContext()
	e.publishSnapshot(false)
	e.mu.Unlock()
	defer func() {
		e.mu.Lock()
		defer e.mu.Unlock()
		if e.actionCancel != nil {
			e.actionCancel()
		}
		e.actionCtx, e.actionCancel, e.waiting = nil, nil, false
		e.actionAttempts = 0
		e.publishSnapshot(false)
	}()
	if err != nil || !changed {
		return changed, err
	}
	if batchID == "" {
		return changed, code("CompletionIdentityMissing")
	}

	operationID, polls := "", 0
	for {
		e.mu.Lock()
		done, checkErr := e.completionState(batchID)
		poll := false
		if !done && checkErr == nil {
			if op := e.state.Pending; op != nil {
				if op.ID != operationID {
					operationID, polls = op.ID, 0
				}
				poll = op.Phase == "growing" || op.Phase == "removing"
				if poll && polls >= attempts {
					checkErr = code("GroupStabilizationAttemptsExhausted")
				}
				if op.Phase == "draining" && e.drainTimedOut() {
					checkErr = code("DrainCompletionTimeout")
					if abortErr := e.recordPreDeleteAbort("drain-completion-timeout"); abortErr != nil {
						checkErr = abortErr
					}
				}
			}
		}
		e.mu.Unlock()
		if done || checkErr != nil {
			return changed, checkErr
		}

		timer := time.NewTimer(e.cfg.StabilizationInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			if e.ctx.Err() != nil {
				return changed, ErrClosed
			}
			return changed, code("ScaleCompletionTimeout")
		case <-timer.C:
		}
		if poll {
			polls++
		}
		if err := e.reconcileStep(false); err != nil {
			return changed, err
		}
	}
}

// completionState requires mu. Count equality is never completion evidence.
func (e *Engine) completionState(batchID string) (bool, error) {
	if e.closed || e.ctx.Err() != nil {
		return false, ErrClosed
	}
	if e.persistError != nil {
		return false, e.persistError
	}
	if e.actionCtx != nil && e.actionCtx.Err() != nil {
		return false, code("ScaleCompletionTimeout")
	}
	if b := e.state.LastBatch; b != nil && b.ID == batchID {
		if b.Phase != "completed" || e.state.ManualReview {
			return false, code("ScaleActionFailed")
		}
		if e.state.Pending != nil || e.state.Batch != nil || b.CompletedAt <= 0 || b.CompletedAt != e.state.LastCompletedAt {
			return false, code("CompletionRecordInvalid")
		}
		return true, nil
	}
	if e.state.Batch == nil || e.state.Batch.ID != batchID {
		return false, code("CompletionIdentityChanged")
	}
	if e.state.ManualReview {
		return false, code("ScaleActionFailed")
	}
	// Return the known failed action while the worker retains restoration and
	// cloud reconciliation obligations. A failed child must not run the rest of
	// its batch, publish a success timestamp, or require a manual unlock.
	if op := e.state.Pending; op != nil && (op.Phase == "failed-delete" || op.Phase == "failed-delete-post" || op.Phase == "failed-grow" || op.Phase == "rejected") {
		return false, code("ScaleActionFailed")
	}
	if op := e.state.Pending; op != nil && op.Unknown {
		return false, ErrUnknown
	}
	return false, nil
}

// The poll budget applies per acknowledged cloud write. Each shrink child is
// settled before the next child writes. Drain/post phases do not consume it.
// The separate foreground deadline bounds all waits, including phases that do
// not consume polls. Arithmetic is checked before the first mutation.
func (e *Engine) completionBudget(count, target int64, attempts int) (time.Duration, error) {
	return completionBudgetFor(e.cfg.DrainDeadline, e.cfg.StabilizationInterval, count, target, attempts)
}
func completionBudgetFor(deadline, interval time.Duration, count, target int64, attempts int) (time.Duration, error) {
	invalid := code("InvalidCompletionBudget")
	if attempts < 1 || count < 0 || target < 0 || interval <= 0 || interval > time.Hour {
		return 0, invalid
	}
	cost := 2*time.Minute + interval
	fixed := 3 * time.Minute
	children := int64(1)
	if target < count {
		children = count - target
		if deadline > 0 && !validPositiveDrainDeadline(deadline) {
			return 0, invalid
		}
		drainWindow := time.Minute
		if deadline > 0 {
			drainWindow += deadline
		}
		if drainWindow > time.Duration(math.MaxInt64)-fixed {
			return 0, invalid
		}
		fixed += drainWindow
	}
	if int64(attempts) > (math.MaxInt64-int64(fixed))/int64(cost) {
		return 0, invalid
	}
	unit := int64(fixed) + int64(attempts)*int64(cost)
	if children > math.MaxInt64/unit {
		return 0, invalid
	}
	return time.Duration(children * unit), nil
}

// workContext requires mu, except in Open before publication.
func (e *Engine) workContext() context.Context {
	if e.actionCtx != nil {
		return e.actionCtx
	}
	return e.ctx
}

func runningCloudMember(m Member) bool {
	return m.CreationType == "AUTO_CREATION" && m.Lifecycle == "IN_SERVICE" && m.Running && m.ChargeType == "POSTPAID_BY_HOUR"
}

// Preserve positive SDK deadlines without a product-specific duration ceiling.
// The controller also needs one minute of representable observation allowance.
// Nonpositive deadline modes use separately persisted controller windows.
func validPositiveDrainDeadline(deadline time.Duration) bool {
	return deadline > 0 && int64(deadline) <= math.MaxInt64-int64(time.Minute)
}
