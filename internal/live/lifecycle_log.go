package live

import (
	"crypto/sha256"
	"encoding/json"
	"sort"
	"time"
)

// ActionSource is persisted with the batch before its first mutation. A nil
// source means an older/direct caller: never reconstruct a policy ID or an
// unclamped request from group configuration or the effective target.
type ActionSource struct {
	PolicyID       string `json:"policy_id,omitempty"`
	RequestedCount int64  `json:"requested_count"`
}

func validActionSource(s *ActionSource) bool {
	return s == nil || (s.RequestedCount >= 0 && (s.PolicyID == "" || requestID.MatchString(s.PolicyID)))
}

// Called under mu only after a successful journal save. This is an observation
// of durable workflow facts, not a second cloud action or success decision.
func (e *Engine) logLifecycle() {
	if e.cfg.Logger == nil || !e.cfg.Logger.IsInfo() {
		return
	}
	b := e.state.Batch
	if b == nil {
		b = e.state.LastBatch
	}
	if b == nil {
		return
	}
	if e.state.Batch == nil && b.ID == e.lastTerminalLog {
		return
	}
	result := b.Phase
	op := e.state.Pending
	if op == nil && len(b.Results) > 0 {
		op = &b.Results[len(b.Results)-1]
	}
	if e.state.Batch != nil && op != nil {
		switch op.Phase {
		case "failed-delete", "failed-delete-post", "failed-grow", "rejected", "aborting":
			result = "failed-pending-reconciliation"
		case "unknown":
			result = "unknown"
		}
	}
	selected, removed, instances, restored := []string{}, []string{}, []string{}, []string{}
	for _, candidate := range b.Selected {
		selected = append(selected, candidate.NodeID)
	}
	for _, child := range b.Results {
		if child.Kind == "shrink" && ((child.ActivityStatus == "SUCCESSFUL" && child.InstanceResult == "SUCCESSFUL") || settledDeleteFailure(&child)) {
			removed = append(removed, child.NodeID)
			instances = append(instances, child.InstanceID)
		}
		if child.DrainOutcome == "restored" {
			restored = append(restored, child.NodeID)
		}
	}
	if e.state.Pending != nil && e.state.Pending.DrainOutcome == "restored" {
		restored = append(restored, e.state.Pending.NodeID)
	}
	sort.Strings(selected)
	sort.Strings(removed)
	sort.Strings(instances)
	sort.Strings(restored)
	fields := map[string]any{"group": e.cfg.GroupID, "operation_id": b.ID, "kind": b.Kind, "result": result, "event": e.state.LastEvent, "old_count": b.InitialCount, "effective_count": b.Target, "selected_nodes": selected, "removed_nodes": removed, "removed_instances": instances, "restored_nodes": restored, "policy_id_available": false, "requested_count_available": b.Source != nil, "observed_count_available": e.observeError == nil && !e.state.ObservedAt.IsZero()}
	if b.Source != nil {
		fields["requested_count"] = b.Source.RequestedCount
		if b.Source.PolicyID != "" {
			fields["policy_id"] = b.Source.PolicyID
			fields["policy_id_available"] = true
		}
	}
	if op != nil {
		fields["child_operation_id"] = op.ID
		fields["phase"] = op.Phase
		if op.GrowthFailure != nil {
			fields["provider_activity_id"] = op.ActivityID
			fields["provider_failure_status"] = op.ActivityStatus
			a := op.GrowthFailure.Activity
			fields["provider_failure_codes_available"] = a.ValidFailureCodes() && len(a.FailureCodes) > 0
			if fields["provider_failure_codes_available"] == true {
				fields["provider_failure_codes"] = a.FailureCodes
			}
			fields["cloud_request_id_available"] = op.RequestID != ""
			if op.RequestID != "" {
				fields["cloud_request_id"] = op.RequestID
			}
		}
	}
	if fields["observed_count_available"] == true {
		fields["observed_new_count"] = len(e.state.Members)
	}
	encoded, err := json.Marshal(fields)
	if err != nil {
		return
	}
	digest := sha256.Sum256(encoded)
	if digest == e.lastLifecycleLog {
		return
	}
	e.lastLifecycleLog = digest
	if fields["observed_count_available"] == true {
		fields["observed_at"] = e.state.ObservedAt.UTC().Format(time.RFC3339Nano)
	}
	keys := make([]string, 0, len(fields))
	for key := range fields {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	args := make([]any, 0, len(fields)*2)
	for _, key := range keys {
		args = append(args, key, fields[key])
	}
	e.cfg.Logger.Info("scaling lifecycle", args...)
	if e.state.Batch == nil {
		e.lastTerminalLog = b.ID
	}
}
