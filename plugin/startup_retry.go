package plugin

import (
	"errors"
	"time"

	"github.com/huxunpei/nomad-autoscaler-tencent/internal/tencentcloud"
)

// Keep the retry classification stable while retaining the structured cloud
// cause for diagnostics. Never include the raw observation error in Error().
type routeObservationError struct{ cause error }

func (e *routeObservationError) Error() string { return "GroupInitializationObservationFailed" }
func (e *routeObservationError) Unwrap() error { return e.cause }

// Only fixed, known availability/IO errors opt into automatic initialization
// retry. Scope conflicts, corrupt journals, unsafe records, migrations and
// manual recovery markers keep their original terminal error.
func retryableRouteInitialization(err error) bool {
	if err == nil {
		return false
	}
	switch err.Error() {
	case "GroupInitializationObservationFailed", "RouteRecordPersistenceFailed", "RouteRecordUnreadable",
		"RouteLockUnavailable", "RouteLockSyncFailed",
		"StateDirectoryFailed", "StateDirectorySyncFailed", "StateReadFailed", "StateLockFailed", "StatePersistenceFailed",
		"RecoveryAuditDirectoryFailed", "RecoveryAuditWriteFailed", "RecoveryFileUnavailable",
		"JournalRetryQueueUnavailable", "JournalRetryQueueCleanupFailed", "LegacyBackupFailed", "LegacyMigrationEvidenceUnavailable", "LegacyMappingUnavailable":
		return true
	}
	return false
}

func routeInitializationDelay(base time.Duration, attempt uint64) time.Duration {
	const maximum = time.Minute
	if base <= 0 {
		base = 5 * time.Second
	}
	if base >= maximum {
		return maximum
	}
	delay := base
	for i := uint64(0); i < attempt; i++ {
		if delay >= maximum/2 {
			return maximum
		}
		delay *= 2
	}
	return delay
}

// One route epoch owns one sequential initializer. openRoute releases every
// failed attempt's engine and lock; successful ownership lasts until Close.
// Keeping ready open across retries lets Status wait within its own budget.
func (r *policyRuntime) initializeRoute(route *groupRoute) {
	for attempt := uint64(0); ; attempt++ {
		if r.ctx.Err() != nil {
			route.err = errRuntimeClosed
			return
		}
		engine, lock, err := r.openRoute(route.policy, route.resume)
		if err == nil {
			route.engine, route.lock = engine, lock
			return
		}
		if r.ctx.Err() != nil {
			route.err = errRuntimeClosed
			return
		}
		if !retryableRouteInitialization(err) {
			route.err = err
			return
		}
		delay := routeInitializationDelay(r.deps.interval, attempt)
		if r.logger != nil {
			fields := []interface{}{"group", route.policy.GroupID, "code", err.Error(), "attempt", attempt + 1, "retry_delay", delay}
			var cloudErr *tencentcloud.Error
			if errors.As(err, &cloudErr) {
				// The adapter exposes sanitized codes, not provider messages or credentials.
				fields = append(fields, "cause_code", cloudErr.Code)
			}
			r.logger.Warn("group controller initialization will retry", fields...)
		}
		timer := time.NewTimer(delay)
		select {
		case <-r.ctx.Done():
			timer.Stop()
			route.err = errRuntimeClosed
			return
		case <-timer.C:
		}
	}
}
