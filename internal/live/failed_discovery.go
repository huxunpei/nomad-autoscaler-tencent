package live

import (
	"encoding/json"
	"sort"
	"time"

	"github.com/huxunpei/nomad-autoscaler-tencent/internal/tencentcloud"
)

// Failure discovery changes only local classification. It does not claim
// capacity convergence, cancel AS retries, mark Nomad eligible, or complete the
// batch. The existing failed-delete worker performs those independent checks.
func (r *Recovery) observeDiscoveredDeletionFailure(s tencentcloud.Snapshot, a tencentcloud.Activity, d ActivityDiscovery) (RecoveryEvidence, State, error) {
	var ev RecoveryEvidence
	var next State
	e := r.e
	op := e.state.Pending
	fail := func() (RecoveryEvidence, State, error) { return ev, next, code("RecoveryDeletionFailureUnconfirmed") }
	if !r.automatic || !e.cfg.GroupContract || e.state.Version != 3 || op.Kind != "shrink" || op.ActivityID != "" || !op.Unknown ||
		!validActivityDiscovery(op, e.cfg.GroupID, d) || !failureTerminal(a.Status) || !sameActivity(d.Activity, a) ||
		!validHistoryActivity(a, e.cfg.GroupID, time.Now().UTC()) ||
		e.validateSnapshot(s) != nil || s.At.After(time.Now().Add(time.Second)) || time.Since(s.At) > 30*time.Second {
		return fail()
	}
	if _, observed := s.VMs[op.InstanceID]; !observed {
		return fail()
	}
	// These are set-valued fields; ordering is not a change in the evidence.
	sort.Slice(s.Members, func(i, j int) bool { return s.Members[i].ID < s.Members[j].ID })
	sort.Slice(s.Policies, func(i, j int) bool { return s.Policies[i].ID < s.Policies[j].ID })
	ev = RecoveryEvidence{Cloud: s, Activity: a, Discovery: &d}
	if json.Unmarshal(jsonBytes(e.state), &next) != nil {
		return fail()
	}
	pending := next.Pending
	pending.Unknown = false
	pending.Phase = "failed-delete"
	pending.ActivityID = a.ID
	pending.ActivityStatus = a.Status
	pending.InstanceResult = "FAILED"
	pending.ActivityDiscovery = &d
	pending.AbsentObservations = 0
	next.LastEvent = "discovered-deletion-failure-restoration-pending"
	// Preserve the previous observed state, batch, successful children and
	// completion timestamp. A later refresh owns publication of changed members.
	candidate := &Engine{cfg: e.cfg, state: next}
	if candidate.validateState() != nil {
		return fail()
	}
	return ev, next, nil
}

// Activity timestamps are second-granular. Equal earliest starts, overlapping
// attempts, or untyped in-flight records cannot establish the first failure.
// Later attempts are observations, not asserted children of the original API
// request. In particular, a retry still in progress does not cancel the duty
// to restore an owned node that is independently confirmed to be running.
func earliestExactFailure(history []tencentcloud.Activity, op *Operation, group string, observed time.Time) (tencentcloud.Activity, bool) {
	var first tencentcloud.Activity
	if op == nil || op.Kind != "shrink" || op.CloudIntent == nil || len(history) < 2 {
		return first, false
	}
	seen := map[string]bool{}
	var earliest time.Time
	tied := false
	relevant := []tencentcloud.Activity{}
	for _, a := range history {
		if !validHistoryActivity(a, group, observed) || seen[a.ID] {
			return first, false
		}
		seen[a.ID] = true
		start, _ := time.Parse(time.RFC3339, a.StartedAt)
		if start.Before(op.CloudIntent.At.Truncate(time.Second)) {
			return first, false
		}
		mentions := false
		for _, v := range a.RelatedInstances {
			if v.ID == op.InstanceID {
				mentions = true
			}
		}
		if !mentions {
			// An unfinished activity with no identities could still concern our VM.
			if (a.Status == "INIT" || a.Status == "RUNNING") && len(a.RelatedInstances) == 0 {
				return first, false
			}
			continue
		}
		if a.Type != "REMOVE_INSTANCES" {
			return first, false
		}
		relevant = append(relevant, a)
		if first.ID == "" || start.Before(earliest) {
			first = a
			earliest = start
			tied = false
		} else if start.Equal(earliest) {
			tied = true
		}
	}
	if first.ID == "" || tied || !failureTerminal(first.Status) || len(first.RelatedInstances) != 1 ||
		first.RelatedInstances[0].ID != op.InstanceID || first.RelatedInstances[0].Status != "FAILED" || first.EndedAt == "" {
		return first, false
	}
	end, _ := time.Parse(time.RFC3339, first.EndedAt)
	for _, a := range relevant {
		if a.ID == first.ID {
			continue
		}
		start, _ := time.Parse(time.RFC3339, a.StartedAt)
		if !start.After(earliest) || start.Before(end) {
			return first, false
		}
	}
	return first, true
}

func normalizedActivityHistory(history []tencentcloud.Activity) []tencentcloud.Activity {
	out := append([]tencentcloud.Activity(nil), history...)
	for i := range out {
		out[i].RelatedInstances = append([]tencentcloud.RelatedInstance(nil), out[i].RelatedInstances...)
		sort.Slice(out[i].RelatedInstances, func(a, b int) bool { return out[i].RelatedInstances[a].ID < out[i].RelatedInstances[b].ID })
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}
