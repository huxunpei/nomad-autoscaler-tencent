package live

import (
	"context"
	"reflect"
	"time"

	"github.com/huxunpei/nomad-autoscaler-tencent/internal/tencentcloud"
)

type scaleOutCloud interface {
	ScaleOut(context.Context, string, int64) (tencentcloud.WriteResult, error)
}

func isScaleOut(op *Operation) bool {
	return op != nil && op.Kind == "grow" && op.CloudIntent != nil && op.CloudIntent.Action == "ScaleOutInstances"
}

func validScaleOutActivity(op *Operation, group string, a tencentcloud.Activity, observed time.Time) bool {
	if !isScaleOut(op) || validateCloudIntent(op) != nil || op.ActivityID == "" || a.ID != op.ActivityID ||
		a.Type != "SCALE_OUT" || !validHistoryActivity(a, group, observed) {
		return false
	}
	start, _ := time.Parse(time.RFC3339, a.StartedAt)
	return !start.Before(op.CloudIntent.At.Truncate(time.Second))
}

// In LAUNCH_CONFIGURATION mode the acknowledged activity describes the entire
// increment. Only its successful new members may change the stable baseline.
// Neither a matching count alone nor the first activity of SPOT_MIXED suffices.
func (e *Engine) scaleOutTargetSettled(s tencentcloud.Snapshot, a tencentcloud.Activity) bool {
	op, b := e.state.Pending, e.state.Batch
	if !isScaleOut(op) || validateCloudIntent(op) != nil || b == nil || b.Kind != "grow" || b.InitialCount != op.CloudIntent.ScaleOut.Base ||
		!validScaleOutActivity(op, e.cfg.GroupID, a, s.At) || a.EndedAt == "" ||
		(a.Status != "SUCCESSFUL" && !failureTerminal(a.Status)) ||
		e.validateSnapshot(s) != nil || e.guard(s) != nil || e.state.LaunchConfiguration == nil ||
		!reflect.DeepEqual(*e.state.LaunchConfiguration, s.LaunchConfiguration) ||
		s.Group.AllocationPolicy != "LAUNCH_CONFIGURATION" || s.Group.Activity != "NOT_IN_ACTIVITY" ||
		s.Group.Count != s.Group.Desired || s.Group.Count != int64(len(s.Members)) ||
		s.Group.Count < s.Group.Min || s.Group.Count > s.Group.Max || s.At.Before(op.CloudIntent.At) {
		return false
	}
	initial := map[string]bool{}
	for _, id := range b.InitialMembers {
		initial[id] = true
	}
	added := map[string]bool{}
	for _, item := range a.RelatedInstances {
		if initial[item.ID] || (item.Status != "SUCCESSFUL" && item.Status != "FAILED") {
			return false
		}
		if item.Status == "SUCCESSFUL" {
			added[item.ID] = true
		}
	}
	number := op.CloudIntent.ScaleOut.Number
	if int64(len(added)) > number || s.Group.Count != b.InitialCount+int64(len(added)) ||
		(a.Status == "SUCCESSFUL" && (int64(len(added)) != number || len(added) != len(a.RelatedInstances))) {
		return false
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
		if !added[m.ID] || err != nil || at.Before(op.CloudIntent.At.Truncate(time.Second)) {
			return false
		}
		delete(added, m.ID)
	}
	return len(initial) == 0 && len(added) == 0
}

func (e *Engine) reconcileScaleOut(ctx context.Context) error {
	op := e.state.Pending
	if !isScaleOut(op) || validateCloudIntent(op) != nil {
		return code("InvalidScaleOutIntent")
	}
	if op.ActivityID == "" {
		// A missing acknowledgement is never permission to resend an increment.
		return e.reconcileRecordedOutcome(ctx)
	}
	if handled, err := e.observeGrowthFailure(ctx); handled || err != nil {
		return err
	}
	if op.Unknown {
		return e.reconcileRecordedOutcome(ctx)
	}
	a, err := e.cloud.Activity(ctx, e.cfg.GroupID, op.ActivityID)
	if err != nil || a.Status != "SUCCESSFUL" || e.state.Cloud == nil ||
		time.Since(e.state.Cloud.At) > 30*time.Second || e.state.Cloud.At.After(time.Now()) ||
		!e.scaleOutTargetSettled(*e.state.Cloud, a) {
		return nil
	}
	op.ActivityStatus = "SUCCESSFUL"
	return e.finish("expansion-complete")
}
