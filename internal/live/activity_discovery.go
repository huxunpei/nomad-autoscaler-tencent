package live

import (
	"context"
	"reflect"
	"sort"
	"time"

	"github.com/huxunpei/nomad-autoscaler-tencent/internal/tencentcloud"
)

func validateCloudIntent(op *Operation) error {
	i := op.CloudIntent
	if i == nil {
		return nil
	} // Old records are not silently upgraded to cloud writes.
	if i.At.IsZero() || i.At.Before(op.StartedAt) || i.At.After(time.Now().UTC().Add(time.Second)) || op.AbortReason != "" {
		return code("InvalidJournalCloudIntent")
	}
	if op.Kind == "grow" {
		if i.Desired != nil && (i.Action != "ModifyDesiredCapacity" || i.ScaleOut != nil || i.Desired.Origin < 0) {
			return code("InvalidJournalCloudIntent")
		}
		if i.Desired != nil {
			seen := map[string]bool{}
			for _, id := range i.Desired.PreviousActivities {
				if !resource(id, "asa") || seen[id] {
					return code("InvalidJournalCloudIntent")
				}
				seen[id] = true
			}
		}
		if i.Action == "ScaleOutInstances" {
			s := i.ScaleOut
			if s == nil || s.Base < 0 || s.Number < 1 || s.Number > 2000 ||
				op.Target < s.Base || op.Target-s.Base != s.Number || s.AllocationPolicy != "LAUNCH_CONFIGURATION" {
				return code("InvalidJournalCloudIntent")
			}
		} else if i.Action != "ModifyDesiredCapacity" || i.ScaleOut != nil {
			return code("InvalidJournalCloudIntent")
		}
	} else if op.Kind == "shrink" {
		if i.Action != "RemoveInstances" || i.ScaleOut != nil || i.Desired != nil || op.Phase == "drain-intent" || op.Phase == "draining" || op.Phase == "aborting" {
			return code("InvalidJournalCloudIntent")
		}
	} else {
		return code("InvalidJournalCloudIntent")
	}
	return nil
}

func sameActivity(a, b tencentcloud.Activity) bool {
	if !a.ValidFailureCodes() || !b.ValidFailureCodes() {
		return false
	}
	a.RequestIDs, b.RequestIDs = nil, nil
	// Late or reordered diagnostic detail must not change operation identity or
	// settlement proof. Callers separately validate the bounded diagnostic data.
	a.FailureCodes, b.FailureCodes = nil, nil
	a.RelatedInstances = append([]tencentcloud.RelatedInstance(nil), a.RelatedInstances...)
	b.RelatedInstances = append([]tencentcloud.RelatedInstance(nil), b.RelatedInstances...)
	sort.Slice(a.RelatedInstances, func(i, j int) bool { return a.RelatedInstances[i].ID < a.RelatedInstances[j].ID })
	sort.Slice(b.RelatedInstances, func(i, j int) bool { return b.RelatedInstances[i].ID < b.RelatedInstances[j].ID })
	return reflect.DeepEqual(a, b)
}

func validActivityDiscovery(op *Operation, group string, d ActivityDiscovery) bool {
	if op.CloudIntent == nil || validateCloudIntent(op) != nil || d.ObservedAt.Before(op.CloudIntent.At) || !validHistoryActivity(d.Activity, group, d.ObservedAt) {
		return false
	}
	a := d.Activity
	if len(d.History) > 0 {
		original, ok := earliestExactFailure(d.History, op, group, d.ObservedAt)
		if !ok || !sameActivity(original, a) {
			return false
		}
	}
	start, err := time.Parse(time.RFC3339, a.StartedAt)
	if err != nil || start.Before(op.CloudIntent.At.Truncate(time.Second)) || a.EndedAt == "" || len(a.RelatedInstances) == 0 {
		return false
	}
	if op.Kind == "shrink" {
		if a.Type != "REMOVE_INSTANCES" || len(a.RelatedInstances) != 1 || a.RelatedInstances[0].ID != op.InstanceID {
			return false
		}
		return (a.Status == "SUCCESSFUL" && a.RelatedInstances[0].Status == "SUCCESSFUL") ||
			(failureTerminal(a.Status) && a.RelatedInstances[0].Status == "FAILED")
	}
	if op.Kind != "grow" || a.Type != "SCALE_OUT" || a.Status != "SUCCESSFUL" {
		return false
	}
	for _, item := range a.RelatedInstances {
		if item.Status != "SUCCESSFUL" {
			return false
		}
	}
	return true
}

// A durable write intent plus a complete, unique terminal activity is only
// candidate evidence. Recovery checks success against actual capacity and
// members; deletion failure only records an obligation to restore/reconcile.
// Both outcomes are checked twice at preview and again before audited commit.
// This relies on the existing single-writer ASG contract. A uniquely earliest
// exact deletion failure can coexist with later attempts; it stays a failure.
// Empty/ambiguous history never proves that a write was not submitted.
func discoverActivity(ctx context.Context, cloud RecoveryCloud, group string, op *Operation) (ActivityDiscovery, error) {
	var out ActivityDiscovery
	if op == nil || !op.Unknown || op.ActivityID != "" || op.CloudIntent == nil || validateCloudIntent(op) != nil {
		return out, code("RecoveryDiscoveryRequiresCloudIntent")
	}
	reader, ok := cloud.(activityHistoryReader)
	if !ok {
		return out, code("RecoveryHistoryUnavailable")
	}
	history, err := reader.ActivitiesSince(ctx, group, op.CloudIntent.At.Truncate(time.Second))
	if err != nil || ctx.Err() != nil {
		return out, code("RecoveryHistoryUnavailable")
	}
	now := time.Now().UTC()
	if len(history) == 1 {
		out = ActivityDiscovery{Activity: history[0], ObservedAt: now}
	} else {
		original, valid := earliestExactFailure(history, op, group, now)
		if !valid {
			return out, code("RecoveryHistoryAmbiguous")
		}
		out = ActivityDiscovery{Activity: original, History: normalizedActivityHistory(history), ObservedAt: now}
	}
	if !validActivityDiscovery(op, group, out) {
		return ActivityDiscovery{}, code("RecoveryHistoryUnconfirmed")
	}
	// RelatedInstances is a set. Normalize a private copy before storing and
	// comparing evidence; provider response ordering does not change its meaning.
	out.Activity.RelatedInstances = append([]tencentcloud.RelatedInstance(nil), out.Activity.RelatedInstances...)
	sort.Slice(out.Activity.RelatedInstances, func(i, j int) bool { return out.Activity.RelatedInstances[i].ID < out.Activity.RelatedInstances[j].ID })
	return out, nil
}

// Discovery audit records opt in explicitly; a manual record or a legacy drain
// cannot become an automatically resumed cloud write through a display name.
func validDiscoveredRecoveryRecord(record recoveryRecord) bool {
	op := record.Before.Pending
	if op == nil || op.CloudIntent == nil || validateCloudIntent(op) != nil || record.Plan.Reference != "discovered-activity/"+op.ID ||
		len(record.Plan.Evidence) != 2 || len(record.Evidence) != 2 {
		return false
	}
	for _, samples := range [][]RecoveryEvidence{record.Plan.Evidence, record.Evidence} {
		for _, ev := range samples {
			if ev.Discovery == nil || !validActivityDiscovery(op, record.Before.GroupID, *ev.Discovery) ||
				ev.Discovery.Activity.ID != record.Plan.Decision.ActivityID || !sameActivity(ev.Activity, ev.Discovery.Activity) {
				return false
			}
		}
	}
	resolved := record.After.LastOperation
	if record.After.Pending != nil {
		resolved = record.After.Pending
	}
	if resolved == nil || resolved.ActivityDiscovery == nil {
		return false
	}
	failed := failureTerminal(resolved.ActivityDiscovery.Activity.Status)
	if failed {
		if record.Plan.Decision.Action != "confirm-remove-failure" || record.After.Pending != resolved || resolved.Phase != "failed-delete" ||
			resolved.DrainOutcome != "" || resolved.DeleteSettlement != nil || resolved.AbsentObservations != 0 || resolved.InstanceResult != "FAILED" ||
			!record.Plan.Scope.GroupContract || record.Before.Version != 3 ||
			record.After.LastCompletedAt != record.Before.LastCompletedAt || !reflect.DeepEqual(record.After.Batch, record.Before.Batch) ||
			!reflect.DeepEqual(record.After.LastOperation, record.Before.LastOperation) || len(record.Plan.Decision.Remaining) != 0 {
			return false
		}
	} else if record.Plan.Decision.Action != "confirm-grow-success" && record.Plan.Decision.Action != "confirm-remove-success" {
		return false
	}
	return resolved.ID == op.ID && !resolved.Unknown && resolved.ActivityStatus == resolved.ActivityDiscovery.Activity.Status &&
		resolved.ActivityID == record.Plan.Decision.ActivityID && resolved.RequestID == op.RequestID &&
		reflect.DeepEqual(resolved.CloudIntent, op.CloudIntent) &&
		reflect.DeepEqual(resolved.ActivityDiscovery, record.Evidence[1].Discovery)
}
