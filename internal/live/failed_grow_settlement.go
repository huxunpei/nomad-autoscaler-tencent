package live

import (
	"reflect"
	"sort"
	"time"

	"github.com/huxunpei/nomad-autoscaler-tencent/internal/tencentcloud"
)

func normalizedGrowthSnapshot(s tencentcloud.Snapshot) tencentcloud.Snapshot {
	s.At, s.RequestIDs = time.Time{}, nil
	s.Members = append([]tencentcloud.Member(nil), s.Members...)
	s.Policies = append([]tencentcloud.Policy(nil), s.Policies...)
	sort.Slice(s.Members, func(i, j int) bool { return s.Members[i].ID < s.Members[j].ID })
	sort.Slice(s.Policies, func(i, j int) bool { return s.Policies[i].ID < s.Policies[j].ID })
	return s
}

// A fulfilled absolute target is safe to close without changing Desired.
// This does not establish how an unfulfilled provider retry queue can be stopped.
func (e *Engine) failedGrowthTargetSettled(s tencentcloud.Snapshot) bool {
	op, b := e.state.Pending, e.state.Batch
	if isScaleOut(op) {
		return op.GrowthFailure != nil && op.Phase == "failed-grow" && !s.At.Before(e.state.ObservedAt) &&
			e.scaleOutTargetSettled(s, op.GrowthFailure.Activity)
	}
	if op == nil || op.GrowthFailure == nil || op.Phase != "failed-grow" || b == nil || b.Kind != "grow" ||
		e.validateSnapshot(s) != nil || e.guard(s) != nil || e.state.LaunchConfiguration == nil || !reflect.DeepEqual(*e.state.LaunchConfiguration, s.LaunchConfiguration) ||
		s.At.Before(e.state.ObservedAt) ||
		s.Group.Activity != "NOT_IN_ACTIVITY" || s.Group.Desired != op.Target || int64(len(s.Members)) != s.Group.Count ||
		(s.Group.Count != op.Target && !(isDesiredCAS(op) && s.Group.Count < op.Target)) || op.Target < s.Group.Min || op.Target > s.Group.Max {
		return false
	}
	initial := map[string]bool{}
	for _, id := range b.InitialMembers {
		initial[id] = true
	}
	for _, m := range s.Members {
		vm := s.VMs[m.ID]
		if m.CreationType != "AUTO_CREATION" || m.Lifecycle != "IN_SERVICE" || !vm.Returned || vm.State != "RUNNING" || vm.ChargeType != "POSTPAID_BY_HOUR" {
			return false
		}
		if initial[m.ID] {
			delete(initial, m.ID)
			continue
		}
		at, err := time.Parse(time.RFC3339, m.AddedAt)
		if err != nil || at.Before(op.StartedAt.Truncate(time.Second)) {
			return false
		}
	}
	return len(initial) == 0
}

func (e *Engine) retainFailedGrowth() error {
	if e.state.Cloud == nil || e.state.Cloud.At.After(time.Now()) || time.Since(e.state.Cloud.At) > 30*time.Second || !e.failedGrowthTargetSettled(*e.state.Cloud) {
		if e.observeError == nil {
			e.state.ErrorCode = "GrowthActivityFailed"
		}
		return e.blocked("failed-grow-awaiting-cloud-reconciliation")
	}
	ctx, cancel := e.callContext()
	defer cancel()
	fresh, err := e.cloud.Observe(ctx, e.cfg.GroupID, "")
	if err != nil || fresh.At.After(time.Now()) || time.Since(fresh.At) > 30*time.Second || !e.failedGrowthTargetSettled(fresh) || !reflect.DeepEqual(normalizedGrowthSnapshot(*e.state.Cloud), normalizedGrowthSnapshot(fresh)) {
		if e.observeError == nil {
			e.state.ErrorCode = "GrowthActivityFailed"
		}
		return e.blocked("failed-grow-settlement-unconfirmed")
	}
	ev := RecoveryEvidence{Cloud: fresh}
	after, err := failedGrowthSettlementAfter(e.cfg, e.state, ev)
	if err != nil {
		return err
	}
	op := e.state.Pending
	now := time.Now().UTC()
	p := RecoveryPlan{Version: 2, Scope: scope(e.cfg), JournalSHA256: sha(e.durableJournal), Operator: "automatic-reconciler", Reference: "growth-settlement/" + op.ID, CreatedAt: now, ExpiresAt: now.Add(5 * time.Minute), Decision: failedGrowthSettlementDecision(op, after), Evidence: []RecoveryEvidence{ev}}
	p.Digest = planDigest(p)
	r := &Recovery{e: e, automatic: true, workflowAudit: true, checkpoint: e.recoveryCheckpoint}
	e.ready = false
	_, err = r.commit(p, p.Evidence, after, p.JournalSHA256)
	if err != nil && (e.automaticAudit != nil || e.checkRecoveryPending() != nil || e.persistError != nil) {
		e.persistError = code("AutomaticRecoveryPersistenceFailed")
	}
	return err
}

func failedGrowthSettlementDecision(op *Operation, after State) RecoveryDecision {
	return RecoveryDecision{Action: "finish-grow-failure", OperationID: op.ID, ActivityID: op.ActivityID, Count: int64(len(after.Members)), RestartEffect: "failed-growth-finished-without-success-cooldown"}
}

func failedGrowthSettlementAfter(cfg Config, before State, ev RecoveryEvidence) (State, error) {
	e := &Engine{cfg: cfg, state: before}
	if before.Cloud == nil || e.validateState() != nil || !e.failedGrowthTargetSettled(*before.Cloud) || !e.failedGrowthTargetSettled(ev.Cloud) ||
		!reflect.DeepEqual(ev, RecoveryEvidence{Cloud: ev.Cloud}) || !reflect.DeepEqual(normalizedGrowthSnapshot(*before.Cloud), normalizedGrowthSnapshot(ev.Cloud)) {
		return State{}, code("InvalidGrowthSettlementEvidence")
	}
	after := cloneRetryState(before)
	after.Cloud = &ev.Cloud
	after.ObservedAt = ev.Cloud.At
	finishOperationAt(&after, cfg.GroupContract, "failed-grow-cloud-settled", 0)
	if isDesiredCAS(before.Pending) {
		recordGrowthBackoff(&after, before.Pending.Target, ev.Cloud.At)
	}
	if after.ErrorCode == "" {
		after.ErrorCode = "GrowthActivityFailed"
	}
	e.state = after
	if e.validateState() != nil {
		return State{}, code("InvalidGrowthSettlementResult")
	}
	return after, nil
}

func validGrowthSettlementRecord(r recoveryRecord) bool {
	p := r.Plan
	op := r.Before.Pending
	if !r.Automatic || p.Version != 2 || op == nil || p.Operator != "automatic-reconciler" || p.Reference != "growth-settlement/"+op.ID ||
		len(r.Evidence) != 1 || p.CreatedAt.IsZero() || !p.ExpiresAt.Equal(p.CreatedAt.Add(5*time.Minute)) || r.AppliedAt.Before(p.CreatedAt) || p.CreatedAt.Before(r.Evidence[0].Cloud.At) ||
		r.BeforeSHA256 != sha(jsonBytes(r.Before)) || !reflect.DeepEqual(p.Evidence, r.Evidence) || !reflect.DeepEqual(p.Decision, failedGrowthSettlementDecision(op, r.After)) {
		return false
	}
	s := p.Scope
	cfg := Config{Dir: s.Directory, GroupID: s.GroupID, Region: s.Region, MaxCapacity: s.MaxCapacity, DrainDeadline: s.DrainDeadline, ScaleInProtection: s.ScaleInProtection, GroupContract: s.GroupContract}
	after, err := failedGrowthSettlementAfter(cfg, r.Before, r.Evidence[0])
	return err == nil && reflect.DeepEqual(after, r.After)
}
