package live

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadclient"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/tencentcloud"
)

const drainOwner = "nomad-autoscaler-tencent"

var resourceID = regexp.MustCompile(`^[a-z]+-[A-Za-z0-9]{1,64}$`)
var regionID = regexp.MustCompile(`^[a-z][a-z0-9-]{1,63}$`)
var requestID = regexp.MustCompile(`^[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}$`)

func resource(id, prefix string) bool {
	return strings.HasPrefix(id, prefix+"-") && resourceID.MatchString(id)
}
func code(s string) error { return errors.New(s) }

// publishedSnapshot is immutable after publication. Serialized state avoids
// sharing maps, slices or operation pointers with the workflow or callers.
type publishedSnapshot struct {
	state []byte
	ready bool
	err   error
}

type Engine struct {
	lastGroupWarning string
	registrationLog  map[string]string
	actionAttempts   int // foreground only; durable nonpositive windows retain it
	actionSource     *ActionSource
	lastLifecycleLog [32]byte
	lastTerminalLog  string
	waiting          bool            // protected by mu; background reconciliation yields to ScaleAndWait
	actionCtx        context.Context // used only while holding mu
	actionCancel     context.CancelFunc
	reconcileEnabled atomic.Bool
	published        atomic.Pointer[publishedSnapshot]
	mu               sync.Mutex
	cfg              Config
	cloud            Cloud
	nomad            Nomad
	state            State
	lock             *os.File
	ctx              context.Context
	cancel           context.CancelFunc
	done             chan struct{}
	closeOnce        sync.Once
	closed           bool
	persistError     error
	durableJournal   []byte
	pendingSave      *JournalRetryEvidence
	automaticAudit   *recoveryMarker
	// Test-only interruption hook; never set by product configuration.
	recoveryCheckpoint func(string) error
	journalCheckpoint  func(string) error // test-only; no product configuration
	observeError       error
	ready              bool
}

func Open(cfg Config, cloud Cloud, nomad Nomad) (*Engine, error) {
	if cfg.RegistrationGracePeriod == 0 {
		cfg.RegistrationGracePeriod = 30 * time.Minute
	}
	if cfg.RegistrationGracePeriod < 0 {
		return nil, code("InvalidRegistrationGracePeriod")
	}
	if cfg.StabilizationInterval == 0 {
		cfg.StabilizationInterval = 5 * time.Second
	}
	if cfg.StabilizationInterval <= 0 || cfg.StabilizationInterval > time.Hour {
		return nil, code("InvalidStabilizationInterval")
	}
	if cfg.GroupContract {
		cfg.MaxCapacity = 0
	} else if cfg.MaxCapacity == 0 {
		cfg.MaxCapacity = 1
	}
	if !cfg.GroupContract && (cfg.MaxCapacity < 1 || cfg.MaxCapacity > MaxSupportedCapacity) {
		return nil, code("InvalidCapacityLimit")
	}
	cfg.Dir = filepath.Clean(cfg.Dir)
	if !filepath.IsAbs(cfg.Dir) || cfg.Dir == "/" || !resource(cfg.GroupID, "asg") || !regionID.MatchString(cfg.Region) || cfg.ReconcileInterval <= 0 || cfg.ReconcileInterval > time.Hour || !validControllerDrainDeadline(cfg) || cloud == nil || nomad == nil {
		return nil, code("InvalidLiveConfiguration")
	}
	parent := cfg.Context
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	e := &Engine{cfg: cfg, cloud: cloud, nomad: nomad, ctx: ctx, cancel: cancel, done: make(chan struct{})}
	e.reconcileEnabled.Store(!cfg.DisableReconciliation)
	if ctx.Err() != nil {
		cancel()
		return nil, ErrClosed
	}
	if err := e.lockDir(); err != nil {
		cancel()
		return nil, err
	}
	if err := e.checkPolicyPending(); err != nil {
		cancel()
		e.release()
		return nil, err
	}
	if err := e.loadJournalRetryQueue(); err != nil {
		cancel()
		e.release()
		return nil, err
	}
	if err := e.resumeAutomaticRecoveryAudit(); err != nil {
		cancel()
		e.release()
		return nil, err
	}
	exists, err := e.load()
	if err != nil {
		cancel()
		e.release()
		return nil, err
	}
	e.registrationLog = map[string]string{}
	for id, m := range e.state.Members {
		e.registrationLog[id] = m.RegistrationState
	}
	if e.state.LastBatch != nil {
		e.lastTerminalLog = e.state.LastBatch.ID
	}
	if err = e.checkRecoveryAudit(); err != nil {
		cancel()
		e.release()
		return nil, err
	}
	if !exists {
		e.state = State{Version: 1, GroupID: cfg.GroupID, Region: cfg.Region, DrainDeadlineNanoseconds: int64(cfg.DrainDeadline), Members: map[string]Member{}}
	} else if !cfg.GroupContract && e.state.Version == 1 && cfg.MaxCapacity > 1 {
		if err = e.upgradeLegacy(); err != nil {
			cancel()
			e.release()
			return nil, err
		}
	}
	if cfg.GroupContract {
		if exists && e.state.Version != 3 {
			if err = e.upgradeGroupContract(); err != nil {
				cancel()
				e.release()
				return nil, err
			}
		} else if !exists {
			e.state.Version = 3
			e.state.MaxCapacity = 0
		}
	}
	if !exists && !cfg.GroupContract && cfg.MaxCapacity > 1 {
		e.state.Version = 2
		e.state.MaxCapacity = cfg.MaxCapacity
	}
	if err = e.bindSDKPolicy(); err != nil {
		cancel()
		e.release()
		return nil, err
	}
	if e.pendingSave != nil {
		if err = e.resumeJournalSave(); err != nil {
			cancel()
			e.release()
			return nil, err
		}
	}
	if op := e.state.Pending; op != nil && strings.HasSuffix(op.Phase, "-intent") {
		// A crash may occur before the network call, during it, or after the
		// response. The journal cannot distinguish these cases: never replay.
		op.Phase, op.Unknown = "unknown", true
		e.state.LastEvent = "write-outcome-unknown-after-restart"
	}
	if err = e.save(); err != nil {
		cancel()
		e.release()
		return nil, err
	}
	// Read failures make Status unready, but do not prevent a later read-only
	// reconciliation when the API becomes available again.
	err = e.refresh()
	if ctx.Err() != nil {
		cancel()
		e.release()
		return nil, ErrClosed
	}
	if e.state.Cloud != nil && e.guard(*e.state.Cloud) != nil && !e.canAbortPreDelete() && !e.canAbortNextChild() && !e.canReconcileSubmittedFailure() && !e.canReconcileRejectedWrite() && !e.canRecoverUnknownSDKDrain() && !e.canRecoverLegacyCustomDrain() && !e.hasGrowthFailureObligation() {
		cancel()
		e.release()
		return nil, err
	}
	e.publishSnapshot(false)
	go e.worker()
	return e, nil
}

func (e *Engine) Close() {
	e.closeOnce.Do(func() {
		e.cancel()
		<-e.done
		e.mu.Lock()
		e.closed = true
		e.cfg.Metrics.ForgetRegistration(e.cfg.Dir)
		e.ready = false
		e.release()
		e.publishSnapshot(false)
		e.mu.Unlock()
	})
}

// A newly discovered Status route starts with observation only. Explicit Scale
// or startup recovery of an already recorded route enables the writer.
func (e *Engine) EnableReconciliation() { e.reconcileEnabled.Store(true) }

func (e *Engine) worker() {
	defer close(e.done)
	timer := time.NewTimer(e.cfg.ReconcileInterval)
	defer timer.Stop()
	for {
		select {
		case <-e.ctx.Done():
			return
		case <-timer.C:
			_ = e.reconcileStep(true)
			// Slow observations must still leave an interval before the next
			// poll; a ticker can queue an immediate follow-up indefinitely.
			timer.Reset(e.cfg.ReconcileInterval)
		}
	}
}

func (e *Engine) callContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(e.workContext(), 60*time.Second)
}

// reconcileOnce and Scale keep one workflow writer, including journal and
// external I/O. Status uses a separate immutable publication, never this lock.
func (e *Engine) reconcileOnce() error { return e.reconcileStep(false) }
func (e *Engine) reconcileStep(background bool) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if background && e.waiting {
		return nil
	}
	e.publishSnapshot(true)
	defer e.publishSnapshot(false)
	if e.closed || e.ctx.Err() != nil {
		return ErrClosed
	}
	if e.automaticAudit != nil && (e.reconcileEnabled.Load() || e.pendingSave != nil) {
		if err := e.resumeAutomaticRecoveryAudit(); err != nil {
			return err
		}
	}
	if e.pendingSave != nil && e.automaticAudit == nil {
		if err := e.resumeJournalSave(); err != nil {
			return err
		}
	}
	if err := e.refresh(); err != nil {
		if e.persistError == nil && e.reconcileEnabled.Load() {
			if e.canObserveGrowthFailure() {
				ctx, cancel := e.callContext()
				handled, growthErr := e.observeGrowthFailure(ctx)
				cancel()
				if handled || growthErr != nil {
					return growthErr
				}
			}
			if e.canRecoverLegacyCustomDrain() {
				return e.reconcileLegacyCustomDrain()
			}
			if e.canRecoverUnknownSDKDrain() {
				return e.reconcileUnknownSDKDrain()
			}
			if op := e.state.Pending; op != nil && op.Phase == "rejected" {
				return e.reconcileRejectedWrite()
			}
			if handled, recoveryErr := e.reconcileSubmittedFailureAfterReadError(); handled {
				return recoveryErr
			}
			if e.canAbortPreDelete() {
				if e.state.Pending.Phase == "aborting" {
					return e.reconcilePreDeleteAbort()
				}
				if e.drainTimedOut() {
					return e.abortPreDelete("drain-completion-timeout")
				}
				if abortableObservation(err) {
					return e.abortPreDelete("pre-delete-observation-rejected")
				}
			}
			if e.canAbortNextChild() && abortableObservation(err) {
				return e.abortNextChild("batch-observation-rejected")
			}
		}
		// Cloud completion of an expansion does not depend on new clients
		// already having registered with Nomad. Status remains unready while
		// mapping is unavailable, and shrink never bypasses mapping checks.
		growing := e.usesGroupCapacityContract() && e.state.Pending != nil && e.state.Pending.Kind == "grow"
		mapping := e.state.ErrorCode == "NomadMemberMappingUnavailable" || e.state.ErrorCode == "NomadMemberMappingInvalid"
		if !growing || !mapping || e.persistError != nil {
			return err
		}
	}
	if e.persistError == nil && e.reconcileEnabled.Load() {
		// Disabled is an idle readiness condition, but an owned drain must be
		// undone immediately rather than left waiting or followed by a delete.
		if e.state.Cloud != nil && e.state.Cloud.Group.Enabled != "ENABLED" {
			if e.canAbortPreDelete() {
				return e.abortPreDelete("pre-delete-cloud-guard-rejected")
			}
			if e.canAbortNextChild() {
				return e.abortNextChild("batch-observation-rejected")
			}
		}
		if e.state.Pending != nil {
			return e.reconcile()
		}
		if e.state.Batch != nil {
			return e.advanceBatch()
		}
	}
	return nil
}

// publishSnapshot requires e.mu, except during Open before the engine escapes.
// Mark the last completed observation unready before starting any slow I/O;
// publish the new state and sanitized error on every exit, including failures.
func (e *Engine) publishSnapshot(busy bool) {
	b, err := json.Marshal(e.state)
	out := &publishedSnapshot{state: b}
	switch {
	case err != nil:
		out.err = code("SnapshotFailed")
	case e.closed || e.ctx.Err() != nil:
		out.err = ErrClosed
	case e.persistError != nil:
		out.err = e.persistError
	case e.state.Pending != nil && e.state.Pending.Unknown:
		out.err = ErrUnknown
	case e.observeError != nil:
		// Complete AS/CVM membership with missing Nomad mapping remains visible
		// as unready. The writer still keeps the observation error and cannot act.
		if (e.state.ErrorCode != "NomadMemberMappingUnavailable" && e.state.ErrorCode != "NomadMemberMappingInvalid") || e.observeError.Error() != e.state.ErrorCode {
			out.err = e.observeError
		}
	default:
		out.ready = !busy && e.ready && e.state.Pending == nil
	}
	e.published.Store(out)
}

// Snapshot reads the last published observation without waiting for cloud,
// Nomad, journal I/O or the workflow lock. Each caller gets its own deep copy.
func (e *Engine) Snapshot() (State, bool, error) {
	cached := e.published.Load()
	if cached == nil {
		return State{}, false, code("SnapshotUnavailable")
	}
	var out State
	if json.Unmarshal(cached.state, &out) != nil {
		return State{}, false, code("SnapshotFailed")
	}
	// Cancellation is visible immediately even while Close awaits worker exit.
	if e.ctx.Err() != nil {
		return out, false, ErrClosed
	}
	return out, cached.ready, cached.err
}

// Scale uses the explicitly configured capacity bound. Dry-run bypasses it.
func (e *Engine) Scale(target int64) (bool, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.waiting {
		return false, ErrBusy
	}
	e.publishSnapshot(true)
	defer e.publishSnapshot(false)
	return e.scaleLocked(target, 0)
}
func (e *Engine) scaleLocked(target int64, attempts int) (bool, error) {
	if e.closed || e.ctx.Err() != nil {
		return false, ErrClosed
	}
	if e.persistError != nil {
		return false, e.persistError
	}
	if target < 0 || (!e.cfg.GroupContract && target > e.cfg.MaxCapacity) {
		return false, code("UnsupportedLiveCapacity")
	}
	e.EnableReconciliation()
	if e.state.Pending != nil {
		if e.state.Pending.Unknown {
			return false, ErrUnknown
		}
		return false, ErrBusy
	}
	if err := e.refresh(); err != nil {
		return false, err
	}
	if !e.ready || e.state.ManualReview || e.state.Batch != nil {
		return false, ErrNotReady
	}
	if e.cfg.GroupContract {
		target = e.clampToGroup(target)
	}
	if err := e.checkGrowthBackoff(target); err != nil {
		return false, err
	}
	count := int64(len(e.state.Members))
	if count == target {
		return false, nil
	}
	if target < count && e.cfg.ScaleInProtection && !e.cfg.GroupContract {
		return false, code("ScaleInProtectionEnabled")
	}
	if attempts > 0 {
		budget, err := e.completionBudget(count, target, attempts)
		if err != nil {
			return false, err
		}
		e.actionCtx, e.actionCancel = context.WithTimeout(e.ctx, budget)
	}
	if e.cfg.GroupContract || e.cfg.MaxCapacity > 1 {
		return e.startBatch(target)
	}
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return false, code("OperationIdentityFailed")
	}
	op := &Operation{ID: "live-" + hex.EncodeToString(b), Target: target, StartedAt: time.Now().UTC()}
	if target == 1 {
		if count != 0 || e.state.Desired != 0 {
			return false, ErrNotReady
		}
		op.Kind, op.Phase = "grow", "grow-intent"
	} else {
		if count != 1 || e.state.Desired != 1 {
			return false, ErrNotReady
		}
		selected, err := e.selectForRemoval(1)
		if err != nil {
			return false, err
		}
		op.InstanceID, op.NodeID = selected[0].InstanceID, selected[0].NodeID
		op.Kind, op.Phase = "shrink", "drain-intent"
	}
	return e.accept(op)
}

func (e *Engine) accept(op *Operation) (bool, error) {
	beforeAccept := append([]byte(nil), e.durableJournal...)
	if err := e.prepareOperationDrainWait(op); err != nil {
		return false, err
	}
	if op.Kind == "grow" && op.CloudIntent == nil {
		op.CloudIntent = &CloudWriteIntent{Action: "ModifyDesiredCapacity", At: time.Now().UTC()}
	}
	if _, ok := e.nomad.(sdkLifecycle); ok && op.Kind == "shrink" {
		op.SDKLifecycle = true
		if e.cfg.GroupContract {
			op.NomadDrainIntent = &NomadDrainIntent{At: time.Now().UTC()}
		}
	}
	e.state.Sequence++
	e.state.Pending = op
	e.ready = false
	e.state.LastEvent = op.Phase
	if err := e.save(); err != nil {
		e.markUnsubmittedSave("accept-before-provider", beforeAccept)
		return false, err
	}
	ctx, cancel := e.callContext()
	defer cancel()
	if op.Kind == "grow" {
		if isDesiredCAS(op) {
			err := e.submitDesiredCAS(ctx)
			return true, err
		}
		var result tencentcloud.WriteResult
		var err error
		if isScaleOut(op) {
			result, err = e.cloud.(scaleOutCloud).ScaleOut(ctx, e.cfg.GroupID, op.CloudIntent.ScaleOut.Number)
			if resource(result.ActivityID, "asa") {
				op.ActivityID = result.ActivityID
			}
		} else {
			result, err = e.cloud.SetDesired(ctx, e.cfg.GroupID, op.Target)
		}
		if requestID.MatchString(result.RequestID) {
			op.RequestID = result.RequestID
		}
		if e.cfg.GroupContract && (tencentcloud.IsRejectedWrite(result, err) || tencentcloud.IsNotSubmittedWrite(result, err)) {
			if recoveryErr := e.recordRejectedWrite(result, err); recoveryErr != nil {
				return true, recoveryErr
			}
			return true, err
		}
		if err != nil || result.Unknown || !result.Attempted || !requestID.MatchString(result.RequestID) ||
			(isScaleOut(op) && (result.Action != "ScaleOutInstances" || op.ActivityID == "")) {
			return true, e.unknown("expand-write-unknown")
		}
		op.RequestID, op.Phase = result.RequestID, "growing"
	} else {
		var result nomadclient.MutationResult
		var err error
		if adapter, ok := e.nomad.(sdkLifecycle); ok {
			// The action wait budget must accommodate the configured drain.
			// API calls retain their shorter per-request transport timeout.
			var drainCtx context.Context
			var drainCancel context.CancelFunc
			if op.DrainWait != nil {
				drainCtx, drainCancel = context.WithDeadline(e.workContext(), op.DrainWait.Until)
			} else {
				drainCtx, drainCancel = context.WithTimeout(e.workContext(), e.cfg.DrainDeadline+60*time.Second)
			}
			result, err = adapter.PreScaleIn(drainCtx, e.identity(op), op.ID, e.cfg.DrainDeadline, e.durableSDKBarrier(op, "drain-intent"))
			drainCancel()
			if !result.WriteAttempted && result.Unknown {
				return true, e.unknown("drain-write-unknown")
			}
			if !result.WriteAttempted {
				if e.persistError != nil {
					if !result.Unknown {
						e.markUnsubmittedSave("sdk-drain-barrier", beforeAccept)
					}
					return false, e.persistError
				}
				if e.cfg.GroupContract {
					if abortErr := e.abortUnsubmittedDrain("sdk-drain-not-submitted"); abortErr != nil {
						return false, abortErr
					}
					return false, code("SDKDrainNotSubmitted")
				}
				// No provider request crossed the durable write barrier.
				// Keep prior successful batch results if this was a later child.
				e.state.Pending = nil
				if b := e.state.Batch; b != nil && len(b.Results) == 0 {
					e.state.Batch = nil
				}
				e.state.LastEvent = "sdk-drain-not-submitted"
				if saveErr := e.save(); saveErr != nil {
					return false, saveErr
				}
				return false, code("SDKDrainNotSubmitted")
			}
			if result.MonitoringIncomplete && !result.Unknown && result.NodeModifyIndex > 0 {
				op.NodeModifyIndex, op.Phase = result.NodeModifyIndex, "draining"
				e.state.LastEvent = "sdk-drain-monitoring-incomplete"
				if saveErr := e.save(); saveErr != nil {
					return true, saveErr
				}
				return true, code("SDKDrainMonitoringIncomplete")
			}
		} else {
			result, err = e.nomad.StartOwnedDrain(ctx, e.identity(op), op.ID, e.cfg.DrainDeadline)
		}
		op.NodeModifyIndex = result.NodeModifyIndex
		if err != nil || result.Unknown || !result.WriteAttempted || result.NodeModifyIndex == 0 {
			return true, e.unknown("drain-write-unknown")
		}
		op.NodeModifyIndex, op.Phase = result.NodeModifyIndex, "draining"
	}
	e.state.LastEvent = op.Phase
	if err := e.save(); err != nil {
		return true, err
	}
	return true, nil
}

func stableMember(m Member) bool {
	m.Protected = false
	return selectable(m)
}

func selectable(m Member) bool {
	return m.CreationType == "AUTO_CREATION" && m.Lifecycle == "IN_SERVICE" && !m.Protected && m.Running && m.ChargeType == "POSTPAID_BY_HOUR" && m.Joined && m.Eligible && m.DrainedBy == ""
}

func (e *Engine) identity(op *Operation) nomadclient.Identity {
	return nomadclient.Identity{NodeID: op.NodeID, InstanceID: op.InstanceID, GroupID: e.cfg.GroupID, Region: e.cfg.Region}
}

func owned(n nomadclient.Node, op *Operation) bool {
	return n.LastDrain != nil && n.LastDrain.Owner == drainOwner && n.LastDrain.OperationID == op.ID
}

func (e *Engine) unknown(event string) error {
	e.state.Pending.Phase = "unknown"
	e.state.Pending.Unknown = true
	e.state.LastEvent = event
	e.ready = false
	if err := e.save(); err != nil {
		return err
	}
	return ErrUnknown
}

// finishOperation changes only memory; callers choose ordinary persistence or an audit transaction.
func finishOperation(state *State, groupContract bool, event string) {
	finishOperationAt(state, groupContract, event, time.Now().UTC().UnixNano())
}

func finishOperationAt(state *State, groupContract bool, event string, completedAt int64) {
	state.LastOperation = state.Pending
	state.LastOperation.Phase = "completed"
	state.Pending = nil
	state.LastEvent = event
	if batch := state.Batch; batch != nil {
		batch.Results = append(batch.Results, *state.LastOperation)
		if state.ManualReview || state.LastOperation.AbortReason != "" || state.LastOperation.CloudRejection != nil || state.LastOperation.GrowthFailure != nil || settledDeleteFailure(state.LastOperation) {
			batch.Phase = "failed"
		} else if batch.Kind == "grow" || state.LastOperation.Target == batch.Target {
			batch.Phase = "completed"
		}
		if batch.Phase != "running" {
			if groupContract && batch.Phase == "completed" {
				batch.CompletedAt = completedAt
				state.LastCompletedAt = batch.CompletedAt
				state.GrowthBackoff = nil
			}
			state.LastBatch = batch
			state.Batch = nil
		}
	}
	if groupContract {
		finishLegacyContract(state)
	}
}

func (e *Engine) finish(event string) error {
	previousCompletion := e.state.LastCompletedAt
	finishOperation(&e.state, e.cfg.GroupContract, event)
	if isDesiredCAS(e.state.LastOperation) && e.state.LastBatch != nil && e.state.LastBatch.Phase == "failed" {
		recordGrowthBackoff(&e.state, e.state.LastOperation.Target, time.Now().UTC())
	}
	if err := e.save(); err != nil {
		// Do not publish an uncommitted cooldown time. Persistence failure
		// freezes further writes; restart re-reads the durable operation.
		e.state.LastCompletedAt = previousCompletion
		if e.state.LastBatch != nil && e.state.LastBatch.CompletedAt != 0 && e.state.LastBatch.CompletedAt != previousCompletion {
			e.state.LastBatch.CompletedAt = 0
		}
		return err
	}
	return nil
}

func (e *Engine) blocked(event string) error {
	e.state.LastEvent = event
	e.ready = false
	return e.save()
}

func (e *Engine) refresh() error {
	defer e.publishRegistrationObservation()
	ctx, cancel := e.callContext()
	defer cancel()
	tracked := ""
	if op := e.state.Pending; op != nil {
		tracked = op.InstanceID
	}
	snap, err := e.cloud.Observe(ctx, e.cfg.GroupID, tracked)
	if err != nil {
		e.ready = false
		e.state.ErrorCode = "CloudObservationFailed"
		e.observeError = code("CloudObservationFailed")
		return e.observeError
	}
	if err = e.validateSnapshot(snap); err != nil {
		e.ready = false
		e.state.ErrorCode = err.Error()
		e.observeError = err
		return err
	}
	lcChanged := e.state.LaunchConfiguration != nil && !reflect.DeepEqual(*e.state.LaunchConfiguration, snap.LaunchConfiguration)
	// Publish full AS membership before consulting Nomad. Missing registration
	// must never turn a real VM into a zero-capacity Status result.
	e.state.Desired = snap.Group.Desired
	e.state.ObservedAt = snap.At
	e.state.Cloud = &snap
	previousMembers := e.state.Members
	members := make(map[string]Member, len(snap.Members))
	ids := make([]string, 0, len(snap.Members))
	for _, m := range snap.Members {
		vm := snap.VMs[m.ID]
		members[m.ID] = Member{ID: m.ID, CreationType: m.CreationType, Lifecycle: m.Lifecycle, Protected: m.Protected || vm.Protected, Running: vm.Returned && vm.State == "RUNNING", ChargeType: vm.ChargeType}
		if e.registrationEnabled() {
			member := members[m.ID]
			member.EverJoined = previousMembers[m.ID].EverJoined || previousMembers[m.ID].Joined
			member.RegistrationState = "unknown"
			members[m.ID] = member
		}
		ids = append(ids, m.ID)
	}
	e.state.Members = members
	e.observeError = nil
	if len(ids) > 0 {
		nodes, partial, resolveErr := e.observeRegistrationMembers(ctx, ids)
		if resolveErr != nil || (!partial && len(nodes) != len(ids)) {
			e.observeError = code("NomadMemberMappingUnavailable")
		} else {
			seen := map[string]bool{}
			for _, n := range nodes {
				m, ok := members[n.InstanceID]
				if !ok || seen[n.InstanceID] || n.Identity.Validate() != nil || n.GroupID != e.cfg.GroupID || n.Region != e.cfg.Region {
					e.observeError = code("NomadMemberMappingInvalid")
					break
				}
				seen[n.InstanceID] = true
				m.NodeID = n.NodeID
				m.Joined = true
				if partial {
					m.EverJoined = true
					m.RegistrationState = "registered"
				}
				m.Eligible = n.Status == "ready" && n.Eligibility == "eligible" && !n.Drain && !n.HasDrainStrategy
				if n.LastDrain != nil && n.LastDrain.Owner == drainOwner && (n.Drain || n.HasDrainStrategy || n.Eligibility != "eligible") {
					m.DrainedBy = n.LastDrain.OperationID
				}
				members[n.InstanceID] = m
			}
		}
	}
	if e.registrationEnabled() && e.observeError == nil {
		e.classifyMissingRegistrations(snap)
	}
	guardErr := e.guard(snap)
	e.warnGroup(snap)
	if guardErr != nil {
		e.observeError = guardErr
	}
	if lcChanged {
		e.observeError = code("LaunchConfigurationChanged")
	}
	e.state.ErrorCode = ""
	if e.observeError != nil {
		e.state.ErrorCode = e.observeError.Error()
	}
	if isDesiredCAS(e.state.Pending) && e.state.Pending.DesiredConflict && e.observeError == nil {
		e.state.ErrorCode = "DesiredCapacityConflict"
	}
	if e.state.LaunchConfiguration == nil && guardErr == nil {
		lc := snap.LaunchConfiguration
		e.state.LaunchConfiguration = &lc
	}
	e.ready = e.observeError == nil && e.state.Pending == nil && e.state.Batch == nil && !e.state.ManualReview && snap.Group.Activity == "NOT_IN_ACTIVITY" && snap.Group.Count == int64(len(members)) && (snap.Group.Desired == int64(len(members)) || e.failedDesiredReady(snap))
	if snap.Group.Enabled != "ENABLED" {
		e.ready = false
	}
	for _, m := range members {
		if !(stableMember(m) || e.expiredRegistrationMember(m)) || (!e.cfg.GroupContract && e.cfg.MaxCapacity == 1 && m.Protected) {
			e.ready = false
		}
	}
	if e.persistError != nil {
		e.ready = false
		return e.persistError
	}
	if err = e.save(); err != nil {
		return err
	}
	return e.observeError
}

func (e *Engine) guard(s tencentcloud.Snapshot) error {
	if e.usesGroupCapacityContract() {
		return e.groupGuard(s)
	}
	g := s.Group
	limit := e.cfg.MaxCapacity
	if e.state.LegacyContract != nil {
		limit = e.state.LegacyContract.MaxCapacity
	}
	if g.Min != 0 || g.Max != limit || g.Desired < 0 || g.Desired > limit || g.Count > limit || g.Enabled != "ENABLED" || g.Status != "NORMAL" || g.RetryPolicy != "NO_RETRY" || int64(len(s.Members)) > limit || s.PolicyCount != 0 || len(s.Policies) != 0 || s.LaunchConfiguration.Status != "NORMAL" || s.LaunchConfiguration.ChargeType != "POSTPAID_BY_HOUR" {
		return code("ExclusiveGroupGuardRejected")
	}
	return nil
}

func (e *Engine) reconcile() error {
	op := e.state.Pending
	if op == nil || e.persistError != nil {
		return nil
	}
	ctx, cancel := e.callContext()
	defer cancel()
	if isDesiredCAS(op) && (op.Phase == "growing" || op.Unknown) {
		return e.reconcileDesiredCAS(ctx)
	}
	if isScaleOut(op) && (op.Phase == "growing" || op.Unknown) {
		return e.reconcileScaleOut(ctx)
	}
	if op.Unknown {
		if handled, err := e.observeGrowthFailure(ctx); handled || err != nil {
			return err
		}
		if e.canRecoverLegacyCustomDrain() {
			return e.reconcileLegacyCustomDrain()
		}
		if e.canRecoverUnknownSDKDrain() {
			return e.reconcileUnknownSDKDrain()
		}
		// A partial response may identify the exact activity. Retain its
		// diagnostics, then use authoritative evidence to resolve success
		// automatically. This path never repeats the uncertain mutation.
		if op.ActivityID != "" {
			a, err := e.cloud.Activity(ctx, e.cfg.GroupID, op.ActivityID)
			if err == nil && validActivity(a, e.cfg.GroupID, op.ActivityID, op.InstanceID) {
				op.ActivityStatus, op.InstanceResult = a.Status, a.RelatedInstances[0].Status
				if e.cfg.GroupContract && op.Kind == "shrink" && failureTerminal(a.Status) && op.InstanceResult == "FAILED" {
					return e.recordFailedDelete()
				}
				if err = e.save(); err != nil {
					return err
				}
			}
		}
		return e.reconcileRecordedOutcome(ctx)
	}
	switch op.Phase {
	case "failed-grow":
		return e.retainFailedGrowth()
	case "rejected":
		return e.reconcileRejectedWrite()
	case "failed-delete", "failed-delete-post":
		return e.reconcileFailedDelete()
	case "aborting":
		return e.reconcilePreDeleteAbort()
	case "post-scale":
		return e.postScale(ctx, op)
	case "growing":
		if e.state.Batch != nil {
			return e.reconcileBatchGrow(op)
		}
		if e.state.Desired != 1 || len(e.state.Members) != 1 {
			return nil
		}
		for _, m := range e.state.Members {
			if !selectable(m) {
				return nil
			}
			// A pre-existing/manually joined instance is not proof of our grow.
			added := ""
			for _, cm := range e.state.Cloud.Members {
				if cm.ID == m.ID {
					added = cm.AddedAt
				}
			}
			at, err := time.Parse(time.RFC3339, added)
			if err != nil || at.Before(op.StartedAt.Truncate(time.Second)) {
				return e.blocked("expanded-member-provenance-unconfirmed")
			}
			op.InstanceID, op.NodeID = m.ID, m.NodeID
		}
		if e.state.Cloud.Group.Count != 1 || e.state.Cloud.Group.Activity != "NOT_IN_ACTIVITY" {
			return nil
		}
		return e.finish("expansion-complete")
	case "draining":
		if e.drainTimedOut() {
			return e.abortPreDelete("drain-completion-timeout")
		}
		if e.cfg.ScaleInProtection && !e.cfg.GroupContract {
			return e.blocked("scale-in-protection-enabled")
		}
		n, err := e.nomad.ReadNode(ctx, op.NodeID)
		if err != nil {
			return e.blocked("drain-observation-unavailable")
		}
		if nomadclient.CheckIdentity(n, e.identity(op)) != nil || !owned(n, op) || n.Status != "ready" {
			return e.blocked("drain-identity-or-ownership-lost")
		}
		if n.LastDrain.Status != "complete" || n.Eligibility != "ineligible" {
			return nil
		}
		// Recheck cloud membership, CVM and protection after the drain. The
		// completed drain may report Drain=false; LastDrain is authoritative.
		fresh, err := e.cloud.Observe(ctx, e.cfg.GroupID, op.InstanceID)
		if err != nil || e.validateSnapshot(fresh) != nil {
			return e.blocked("pre-delete-cloud-guard-rejected")
		}
		if fresh.Group.Enabled != "ENABLED" || e.guard(fresh) != nil || !reflect.DeepEqual(*e.state.LaunchConfiguration, fresh.LaunchConfiguration) {
			return e.abortPreDelete("pre-delete-cloud-guard-rejected")
		}
		if e.cfg.GroupContract && (op.Target < fresh.Group.Min || op.Target > fresh.Group.Max || serviceMembers(fresh)-1 < fresh.Group.Min) {
			return e.abortPreDelete("pre-delete-group-limits-changed")
		}
		m, ok := cloudMember(fresh, op.InstanceID)
		vm := fresh.VMs[op.InstanceID]
		if !ok || int64(len(fresh.Members)) != op.Target+1 || fresh.Group.Count != op.Target+1 || fresh.Group.Desired != op.Target+1 || !e.batchMembershipMatches(fresh, false) || fresh.Group.Activity != "NOT_IN_ACTIVITY" || m.CreationType != "AUTO_CREATION" || m.Lifecycle != "IN_SERVICE" || e.deleteProtected(m, vm) || !vm.Returned || vm.State != "RUNNING" || vm.ChargeType != "POSTPAID_BY_HOUR" {
			return e.abortPreDelete("pre-delete-member-guard-rejected")
		}
		// Re-read identity/owner immediately before the destructive intent.
		n, err = e.nomad.ReadNode(ctx, op.NodeID)
		if err != nil || nomadclient.CheckIdentity(n, e.identity(op)) != nil || !owned(n, op) || n.Status != "ready" || n.Eligibility != "ineligible" || n.LastDrain.Status != "complete" {
			return e.blocked("pre-delete-nomad-guard-rejected")
		}
		// LastDrain and allocation status can become terminal before poststop
		// tasks finish. Re-read all required task states on every attempt,
		// including after restart, before publishing the destructive intent.
		var stopped bool
		if adapter, ok := e.nomad.(sdkLifecycle); ok && op.LegacyOrigin == nil {
			stopped, err = adapter.RequiredTasksStopped(ctx, op.NodeID)
		} else {
			stopped, err = e.nomad.TasksStopped(ctx, op.NodeID, false)
		}
		if err != nil {
			return e.blocked("pre-delete-task-observation-unavailable")
		}
		if !stopped {
			return e.blocked("waiting-for-node-tasks")
		}
		op.Phase = "remove-intent"
		op.CloudIntent = &CloudWriteIntent{Action: "RemoveInstances", At: time.Now().UTC()}
		e.state.LastEvent = op.Phase
		if err = e.save(); err != nil {
			e.markUnsubmittedSave("remove-before-provider", nil)
			return err
		}
		result, err := e.cloud.RemoveExact(ctx, e.cfg.GroupID, op.InstanceID)
		if requestID.MatchString(result.RequestID) {
			op.RequestID = result.RequestID
		}
		if resource(result.ActivityID, "asa") {
			op.ActivityID = result.ActivityID
		}
		if e.cfg.GroupContract && (tencentcloud.IsRejectedWrite(result, err) || tencentcloud.IsNotSubmittedWrite(result, err)) {
			return e.recordRejectedWrite(result, err)
		}
		if err != nil || result.Unknown || !result.Attempted || !requestID.MatchString(result.RequestID) || !resource(result.ActivityID, "asa") {
			return e.unknown("remove-write-unknown")
		}
		op.RequestID, op.ActivityID, op.Phase = result.RequestID, result.ActivityID, "removing"
		e.state.LastEvent = op.Phase
		return e.save()
	case "removing":
		activity, err := e.cloud.Activity(ctx, e.cfg.GroupID, op.ActivityID)
		if err != nil || !validActivity(activity, e.cfg.GroupID, op.ActivityID, op.InstanceID) {
			op.AbsentObservations = 0
			return e.blocked("delete-activity-unconfirmed")
		}
		op.ActivityStatus, op.InstanceResult = activity.Status, activity.RelatedInstances[0].Status
		vm, vmObserved := e.state.Cloud.VMs[op.InstanceID]
		_, memberExists := e.state.Members[op.InstanceID]
		if activity.Status == "SUCCESSFUL" && op.InstanceResult == "SUCCESSFUL" {
			if !memberExists && vmObserved && !vm.Returned && e.state.Cloud.Group.Count == op.Target && e.state.Desired == op.Target && int64(len(e.state.Members)) == op.Target && e.batchMembershipMatches(*e.state.Cloud, true) && e.state.Cloud.Group.Activity == "NOT_IN_ACTIVITY" {
				op.AbsentObservations++
				if op.AbsentObservations >= 2 {
					return e.postScale(ctx, op)
				}
			} else {
				op.AbsentObservations = 0
			}
			return e.save()
		}
		op.AbsentObservations = 0
		if !failureTerminal(activity.Status) || op.InstanceResult != "FAILED" {
			return e.save()
		}
		if e.cfg.GroupContract {
			return e.recordFailedDelete()
		}
		// An idle group flag is insufficient. Require the exact terminal
		// activity/instance failure and a fresh RUNNING surviving CVM.
		// AS/CVM deletion protection still forbids future deletes, but must not
		// prevent returning our already-drained survivor to the scheduler.
		// Restoring scheduling never clears either cloud protection flag.
		if !memberExists || !vmObserved || !vm.Returned || vm.State != "RUNNING" || vm.ChargeType != "POSTPAID_BY_HOUR" {
			return e.blocked("failed-delete-survivor-unconfirmed")
		}
		survivor, ok := cloudMember(*e.state.Cloud, op.InstanceID)
		if !ok || survivor.CreationType != "AUTO_CREATION" {
			return e.blocked("failed-delete-member-unconfirmed")
		}
		n, err := e.nomad.ReadNode(ctx, op.NodeID)
		if err != nil || nomadclient.CheckIdentity(n, e.identity(op)) != nil || !owned(n, op) || n.Status != "ready" || n.Eligibility != "ineligible" || n.LastDrain.Status != "complete" {
			return e.blocked("restore-ownership-or-state-rejected")
		}
		// This records a one-attempt restoration; it does not change Desired
		// or claim that a future cloud async retry has become impossible.
		op.Phase = "restore-intent"
		e.state.LastEvent = op.Phase
		if err = e.save(); err != nil {
			return err
		}
		result, err := e.nomad.RestoreOwnedDrain(ctx, e.identity(op), op.ID)
		op.NodeModifyIndex = result.NodeModifyIndex
		if err != nil || result.Unknown || !result.WriteAttempted || result.NodeModifyIndex == 0 {
			return e.unknown("restore-write-unknown")
		}
		if result.After == nil || nomadclient.CheckIdentity(*result.After, e.identity(op)) != nil || !owned(*result.After, op) || result.After.Status != "ready" || result.After.Drain || result.After.HasDrainStrategy || result.After.Eligibility != "eligible" {
			return e.unknown("restore-readback-unconfirmed")
		}
		op.NodeModifyIndex = result.NodeModifyIndex
		e.state.ManualReview = true
		return e.finish("deletion-failed-node-restored-cloud-retry-still-possible")
	default:
		return e.unknown("invalid-operation-phase")
	}
}

func cloudMember(s tencentcloud.Snapshot, id string) (tencentcloud.Member, bool) {
	for _, m := range s.Members {
		if m.ID == id {
			return m, true
		}
	}
	return tencentcloud.Member{}, false
}
func failureTerminal(s string) bool {
	return s == "FAILED" || s == "PARTIALLY_SUCCESSFUL" || s == "CANCELLED"
}
func validActivity(a tencentcloud.Activity, group, activity, instance string) bool {
	if !a.ValidFailureCodes() || a.GroupID != group || a.ID != activity || !resource(a.ID, "asa") || len(a.RelatedInstances) != 1 || a.RelatedInstances[0].ID != instance {
		return false
	}
	switch a.Status {
	case "INIT", "RUNNING", "SUCCESSFUL", "PARTIALLY_SUCCESSFUL", "FAILED", "CANCELLED":
	default:
		return false
	}
	switch a.RelatedInstances[0].Status {
	case "INIT", "RUNNING", "SUCCESSFUL", "FAILED":
	default:
		return false
	}
	return true
}
