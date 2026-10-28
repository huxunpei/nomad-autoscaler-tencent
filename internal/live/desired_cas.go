package live

import (
	"context"
	"errors"
	"sort"
	"time"

	"github.com/huxunpei/nomad-autoscaler-tencent/internal/tencentcloud"
)

type desiredCASCloud interface {
	SetDesiredCAS(context.Context, string, int64, int64) (tencentcloud.WriteResult, error)
}

func isDesiredCAS(op *Operation) bool {
	return op != nil && op.Kind == "grow" && op.CloudIntent != nil && op.CloudIntent.Action == "ModifyDesiredCapacity" && op.CloudIntent.Desired != nil
}

func (e *Engine) prepareDesiredIntent(op *Operation) error {
	reader, ok := e.cloud.(activityHistoryReader)
	if !ok || e.state.Cloud == nil {
		return code("DesiredCapacityHistoryUnavailable")
	}
	// Exclude pre-existing activities within the API's second-resolution
	// timestamp. They must not turn a fresh retry into an immediate failure.
	at := time.Now().UTC()
	ctx, cancel := e.callContext()
	defer cancel()
	history, err := reader.ActivitiesSince(ctx, e.cfg.GroupID, at.Truncate(time.Second))
	if err != nil {
		return err
	}
	ids := make([]string, 0, len(history))
	for _, a := range history {
		if !validHistoryActivity(a, e.cfg.GroupID, time.Now().UTC()) {
			return code("InvalidDesiredCapacityHistory")
		}
		ids = append(ids, a.ID)
	}
	sort.Strings(ids)
	op.CloudIntent = &CloudWriteIntent{Action: "ModifyDesiredCapacity", At: at,
		Desired: &DesiredIntent{Origin: e.state.Desired, PreviousActivities: ids}}
	return nil
}

func (e *Engine) desiredConflict() error {
	e.state.Pending.DesiredConflict = true
	e.state.ErrorCode = "DesiredCapacityConflict"
	if e.cfg.Logger != nil {
		e.cfg.Logger.Warn("desired capacity changed concurrently; automatic replay stopped", "group", e.cfg.GroupID,
			"expected_desired", e.state.Pending.CloudIntent.Desired.Origin, "observed_desired", e.state.Desired,
			"requested_count", e.state.Pending.Target)
	}
	return e.unknown("desired-capacity-conflict")
}

func (e *Engine) submitDesiredCAS(ctx context.Context) error {
	op := e.state.Pending
	api, ok := e.cloud.(desiredCASCloud)
	if !ok || !isDesiredCAS(op) || op.DesiredConflict {
		return code("DesiredCapacityCASUnavailable")
	}
	result, err := api.SetDesiredCAS(ctx, e.cfg.GroupID, op.Target, op.CloudIntent.Desired.Origin)
	if requestID.MatchString(result.RequestID) {
		op.RequestID = result.RequestID
	}
	var provider *tencentcloud.Error
	if errors.As(err, &provider) && provider.Code == "InvalidParameterValue.AssertDesiredCapacityFailed" {
		return e.desiredConflict()
	}
	if tencentcloud.IsRejectedWrite(result, err) || tencentcloud.IsNotSubmittedWrite(result, err) {
		if recoveryErr := e.recordRejectedWrite(result, err); recoveryErr != nil {
			return recoveryErr
		}
		return err
	}
	if err != nil || result.Unknown || !result.Attempted || !requestID.MatchString(result.RequestID) || result.Action != "ModifyDesiredCapacity" {
		return e.unknown("expand-write-unknown")
	}
	op.DesiredAccepted, op.Unknown, op.Phase = true, false, "growing"
	e.state.LastEvent = "desired-capacity-accepted"
	return e.save()
}

func (e *Engine) reconcileDesiredCAS(ctx context.Context) error {
	op := e.state.Pending
	if !isDesiredCAS(op) || validateCloudIntent(op) != nil || e.state.Cloud == nil {
		return code("InvalidDesiredCapacityIntent")
	}
	if op.DesiredConflict {
		return ErrUnknown
	}
	g := e.state.Cloud.Group
	origin := op.CloudIntent.Desired.Origin
	if (op.DesiredAccepted && g.Desired != op.Target) || (g.Desired != origin && g.Desired != op.Target) {
		return e.desiredConflict()
	}
	if !op.DesiredAccepted {
		// A resumed absolute target can equal its original Desired. A lost
		// reply does not prove admission, but a new terminal failure still
		// must settle and back off before another same-value resume.
		if op.Unknown && origin == op.Target && g.Desired == op.Target {
			if handled, err := e.observeGrowthFailure(ctx); handled || err != nil {
				return err
			}
		}
		if g.Desired == op.Target && origin != op.Target {
			// The target is already installed. Do not fabricate a RequestId or
			// resend the write; success still requires running, stable members.
			op.DesiredAccepted, op.Unknown, op.Phase = true, false, "growing"
			if err := e.save(); err != nil {
				return err
			}
		} else {
			if g.Activity != "NOT_IN_ACTIVITY" || g.Enabled != "ENABLED" {
				return nil
			}
			// Same target + same assertion is safe to repeat. The original
			// baseline is never replaced by a fresh value after a conflict.
			op.Phase, op.Unknown = "grow-intent", false
			if err := e.save(); err != nil {
				return err
			}
			return e.submitDesiredCAS(ctx)
		}
	}
	return e.reconcileBatchGrow(op)
}

func latestDesiredGrowth(op *Operation, group string, history []tencentcloud.Activity, observed time.Time) (tencentcloud.Activity, bool) {
	prior := map[string]bool{}
	for _, id := range op.CloudIntent.Desired.PreviousActivities {
		prior[id] = true
	}
	var latest tencentcloud.Activity
	var stamp time.Time
	ambiguous := false
	for _, a := range history {
		if prior[a.ID] {
			continue
		}
		if !validHistoryActivity(a, group, observed) {
			return latest, false
		}
		at, _ := time.Parse(time.RFC3339, a.StartedAt)
		if at.Before(op.CloudIntent.At.Truncate(time.Second)) {
			continue
		}
		if at.After(stamp) {
			latest, stamp, ambiguous = a, at, false
		} else if at.Equal(stamp) && a.ID != latest.ID {
			ambiguous = true
		}
	}
	return latest, latest.ID != "" && !ambiguous && latest.Type == "SCALE_OUT"
}

func (e *Engine) failedDesiredReady(s tencentcloud.Snapshot) bool {
	op := e.state.LastOperation
	return isDesiredCAS(op) && op.GrowthFailure != nil && !op.DesiredConflict &&
		s.Group.Desired == op.Target && s.Group.Count < s.Group.Desired
}

func recordGrowthBackoff(s *State, target int64, at time.Time) {
	n := 1
	if b := s.GrowthBackoff; b != nil && b.Target == target {
		n = b.Failures + 1
	}
	if n > 31 {
		n = 31
	}
	minutes := 30
	if n < 5 {
		minutes = 1 << n
	}
	s.GrowthBackoff = &GrowthBackoff{Target: target, Failures: n, RetryAfter: at.Add(time.Duration(minutes) * time.Minute)}
}

func (e *Engine) checkGrowthBackoff(target int64) error {
	b := e.state.GrowthBackoff
	if b == nil {
		return nil
	}
	if b.Target != target || int64(len(e.state.Members)) == target {
		e.state.GrowthBackoff = nil
		return e.save()
	}
	if time.Now().Before(b.RetryAfter) {
		return code("GrowthRetryBackoffActive")
	}
	return nil
}
