package live

import (
	"math"
	"reflect"
	"regexp"
	"strings"
	"time"

	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadclient"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/tencentcloud"
)

var safeValue = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,128}$`)
var operationID = regexp.MustCompile(`^live-[a-f0-9]{32}$`)

func validLC(lc tencentcloud.LaunchConfiguration) bool {
	if !resource(lc.ID, "asc") || !resource(lc.ImageID, "img") || lc.VersionNumber < 1 || !safeValue.MatchString(lc.Status) || !safeValue.MatchString(lc.ChargeType) {
		return false
	}
	if lc.InstanceType == "" && len(lc.InstanceTypes) == 0 {
		return false
	}
	if lc.InstanceType != "" && !safeValue.MatchString(lc.InstanceType) {
		return false
	}
	for _, t := range lc.InstanceTypes {
		if !safeValue.MatchString(t) {
			return false
		}
	}
	seen := map[string]bool{}
	for _, id := range lc.SecurityGroupIDs {
		if !resource(id, "sg") || seen[id] {
			return false
		}
		seen[id] = true
	}
	return true
}

func (e *Engine) validateSnapshot(s tencentcloud.Snapshot) error {
	if s.Region != e.cfg.Region || s.Group.ID != e.cfg.GroupID || s.At.IsZero() || s.Group.Min < 0 || s.Group.Max < s.Group.Min || s.Group.Desired < 0 || s.Group.Count < 0 || s.Group.LaunchConfiguration != s.LaunchConfiguration.ID || !validLC(s.LaunchConfiguration) || s.VMs == nil || s.PolicyCount != len(s.Policies) {
		return code("InvalidCloudSnapshot")
	}
	g := s.Group
	if g.AllocationPolicy != "" && g.AllocationPolicy != "LAUNCH_CONFIGURATION" && g.AllocationPolicy != "SPOT_MIXED" && g.AllocationPolicy != "UNSUPPORTED" {
		return code("InvalidCloudSnapshot")
	}
	if (g.Enabled != "ENABLED" && g.Enabled != "DISABLED") || (g.Activity != "IN_ACTIVITY" && g.Activity != "NOT_IN_ACTIVITY") || !safeValue.MatchString(g.Status) || !safeValue.MatchString(g.RetryPolicy) {
		return code("InvalidCloudSnapshot")
	}
	ids := map[string]bool{}
	for _, m := range s.Members {
		if !resource(m.ID, "ins") || ids[m.ID] || !safeValue.MatchString(m.CreationType) || !safeValue.MatchString(m.Lifecycle) {
			return code("InvalidCloudSnapshot")
		}
		if _, err := time.Parse(time.RFC3339, m.AddedAt); err != nil {
			return code("InvalidCloudSnapshot")
		}
		ids[m.ID] = true
		vm, ok := s.VMs[m.ID]
		if !ok || vm.ID != m.ID {
			return code("IncompleteCVMObservation")
		}
	}
	for id, vm := range s.VMs {
		if !resource(id, "ins") || vm.ID != id || (vm.Returned && (!safeValue.MatchString(vm.State) || !safeValue.MatchString(vm.ChargeType))) || (!vm.Returned && (vm.State != "" || vm.ChargeType != "" || vm.Protected)) {
			return code("InvalidCVMObservation")
		}
	}
	policies := map[string]bool{}
	for _, p := range s.Policies {
		if !resource(p.ID, "asp") || policies[p.ID] || !safeValue.MatchString(p.Type) {
			return code("InvalidCloudSnapshot")
		}
		policies[p.ID] = true
	}
	for _, tr := range s.RequestIDs {
		if !safeValue.MatchString(tr.Action) || !requestID.MatchString(tr.RequestID) {
			return code("InvalidRequestTrace")
		}
	}
	return nil
}

func (e *Engine) validateOperation(op *Operation, pending bool) error {
	if op == nil {
		return nil
	}
	if !operationID.MatchString(op.ID) || op.StartedAt.IsZero() || op.AbsentObservations < 0 || op.AbsentObservations > 2 {
		return code("InvalidJournalOperation")
	}
	if err := e.validateLegacyOrigin(op); err != nil {
		return err
	}
	if err := e.validateOperationDrainWait(op); err != nil {
		return err
	}
	if intent := op.NomadDrainIntent; intent != nil {
		if !e.cfg.GroupContract || e.state.Version != 3 || !op.SDKLifecycle || op.Kind != "shrink" || intent.At.IsZero() || intent.At.Before(op.StartedAt) || (op.CloudIntent != nil && op.CloudIntent.At.Before(intent.At)) {
			return code("InvalidNomadDrainIntent")
		}
	}
	if op.AbortReason == "sdk-drain-response-lost" && op.NomadDrainIntent == nil {
		return code("InvalidNomadDrainIntent")
	}
	if err := validateCloudIntent(op); err != nil {
		return err
	}
	if (op.DesiredAccepted || op.DesiredConflict) && !isDesiredCAS(op) {
		return code("InvalidDesiredCapacityEvidence")
	}
	if op.DesiredConflict && (!pending || !op.Unknown) {
		return code("InvalidDesiredCapacityConflict")
	}
	if d := op.ActivityDiscovery; d != nil {
		if !validActivityDiscovery(op, e.cfg.GroupID, *d) || op.Unknown || d.Activity.ID != op.ActivityID || d.Activity.Status != op.ActivityStatus || op.AbortReason != "" {
			return code("InvalidJournalActivityDiscovery")
		}
		if d.Activity.Status == "SUCCESSFUL" {
			if (op.Phase != "completed" && op.Phase != "post-scale") || op.DeleteSettlement != nil ||
				(op.Kind == "shrink" && (op.InstanceResult != "SUCCESSFUL" || op.AbsentObservations != 2)) {
				return code("InvalidJournalActivityDiscovery")
			}
		} else if !e.cfg.GroupContract || e.state.Version != 3 || op.Kind != "shrink" || op.InstanceResult != "FAILED" ||
			(op.Phase != "failed-delete" && op.Phase != "failed-delete-post" && op.Phase != "completed") {
			return code("InvalidJournalActivityDiscovery")
		}
	}
	if op.RequestID != "" && !requestID.MatchString(op.RequestID) {
		return code("InvalidJournalOperation")
	}
	if op.ActivityID != "" && !resource(op.ActivityID, "asa") {
		return code("InvalidJournalOperation")
	}
	if isScaleOut(op) && (!e.cfg.GroupContract || e.state.Version != 3 || e.state.LegacyContract != nil ||
		(op.Phase == "growing" && op.ActivityID == "")) {
		return code("InvalidJournalScaleOut")
	}
	if op.InstanceID != "" && !resource(op.InstanceID, "ins") {
		return code("InvalidJournalOperation")
	}
	if op.NodeID != "" && (nomadclient.Identity{NodeID: op.NodeID, InstanceID: op.InstanceID, GroupID: e.cfg.GroupID, Region: e.cfg.Region}).Validate() != nil {
		return code("InvalidJournalOperation")
	}
	if op.ActivityStatus != "" && op.ActivityStatus != "INIT" && op.ActivityStatus != "RUNNING" && op.ActivityStatus != "SUCCESSFUL" && !failureTerminal(op.ActivityStatus) {
		return code("InvalidJournalOperation")
	}
	if op.InstanceResult != "" && op.InstanceResult != "INIT" && op.InstanceResult != "RUNNING" && op.InstanceResult != "SUCCESSFUL" && op.InstanceResult != "FAILED" {
		return code("InvalidJournalOperation")
	}
	if err := e.validateFailedGrowth(op, pending); err != nil {
		return err
	}
	if op.DeleteSettlement != nil && op.AbortReason != "" {
		return code("InvalidJournalFailedDelete")
	}
	if op.CloudNotSubmitted && op.CloudRejection == nil {
		return code("InvalidUnsubmittedCloudWrite")
	}
	if op.CloudRejection != nil || op.Phase == "rejected" {
		if err := e.validateRejectedWrite(op, pending); err != nil {
			return err
		}
	} else if op.AbortReason != "" {
		unsubmitted := op.AbortReason == "sdk-drain-not-submitted" || strings.HasPrefix(op.AbortReason, "batch-")
		if e.state.Version != 3 || !e.cfg.GroupContract || !validAbortReason(op.AbortReason) || op.Kind != "shrink" ||
			op.InstanceID == "" || op.NodeID == "" || op.RequestID != "" || op.ActivityID != "" ||
			op.ActivityStatus != "" || op.InstanceResult != "" || op.Unknown || op.AbsentObservations != 0 {
			return code("InvalidJournalAbort")
		}
		if pending {
			if unsubmitted || op.Phase != "aborting" || op.DrainOutcome != "" || op.NodeModifyIndex == 0 {
				return code("InvalidJournalAbort")
			}
		} else if (unsubmitted != (op.DrainOutcome == "not-submitted")) || (op.DrainOutcome != "restored" && op.DrainOutcome != "not-submitted") ||
			(op.DrainOutcome == "restored" && op.NodeModifyIndex == 0) || (op.DrainOutcome == "not-submitted" && op.NodeModifyIndex != 0) {
			return code("InvalidJournalAbort")
		}
	} else if op.Phase == "failed-delete" || op.Phase == "failed-delete-post" || op.DeleteSettlement != nil {
		if !e.cfg.GroupContract || e.state.Version != 3 || op.Kind != "shrink" ||
			op.ActivityID == "" || !failureTerminal(op.ActivityStatus) || op.InstanceResult != "FAILED" ||
			op.Unknown || (op.DrainOutcome != "" && op.DrainOutcome != "restored") {
			return code("InvalidJournalFailedDelete")
		}
		if op.DeleteSettlement == nil {
			if !pending || op.Phase != "failed-delete" || op.AbsentObservations != 0 {
				return code("InvalidJournalFailedDelete")
			}
		} else if !validDeleteSettlement(op, e.cfg.GroupID) ||
			(pending && op.Phase != "failed-delete" && op.Phase != "failed-delete-post") ||
			(op.Phase == "failed-delete-post" && (!requiresSDKPost(op) || op.AbsentObservations != 2)) ||
			(!pending && (op.Phase != "completed" || op.AbsentObservations != 2)) {
			return code("InvalidJournalFailedDelete")
		}
	} else if op.Phase == "aborting" || op.DrainOutcome != "" {
		return code("InvalidJournalAbort")
	}
	if pending {
		if op.Unknown != (op.Phase == "unknown") {
			return code("InvalidJournalOperation")
		}
		if op.Kind == "grow" && op.Target >= 1 && op.Target <= e.journalLimit() {
			if op.Phase != "grow-intent" && op.Phase != "growing" && op.Phase != "unknown" && op.Phase != "rejected" && op.Phase != "failed-grow" {
				return code("InvalidJournalOperation")
			}
			if op.Phase == "growing" && op.RequestID == "" && !(isDesiredCAS(op) && op.DesiredAccepted) {
				return code("InvalidJournalOperation")
			}
		} else if op.Kind == "shrink" && op.Target >= 0 && op.Target < e.journalLimit() {
			if op.InstanceID == "" || op.NodeID == "" {
				return code("InvalidJournalOperation")
			}
			switch op.Phase {
			case "drain-intent", "draining", "remove-intent", "removing", "restore-intent", "post-scale", "unknown", "aborting", "failed-delete", "failed-delete-post", "rejected":
			default:
				return code("InvalidJournalOperation")
			}
			if op.Phase == "post-scale" && (!requiresSDKPost(op) || op.ActivityStatus != "SUCCESSFUL" || op.InstanceResult != "SUCCESSFUL" || op.AbsentObservations != 2) {
				return code("InvalidJournalOperation")
			}
			if (op.Phase == "removing" || op.Phase == "restore-intent" || op.Phase == "post-scale") && (op.ActivityID == "" || (op.RequestID == "" && !(op.Phase == "post-scale" && op.ActivityDiscovery != nil))) {
				return code("InvalidJournalOperation")
			}
			if op.Phase == "restore-intent" && (!failureTerminal(op.ActivityStatus) || op.InstanceResult != "FAILED") {
				return code("InvalidJournalOperation")
			}
		} else {
			return code("InvalidJournalOperation")
		}
	} else if op.Phase != "completed" || op.Unknown || (op.Kind != "grow" && op.Kind != "shrink") || (op.Kind == "grow" && (op.Target < 1 || op.Target > e.journalLimit())) || (op.Kind == "shrink" && (op.Target < 0 || op.Target >= e.journalLimit())) {
		return code("InvalidJournalOperation")
	}
	return nil
}

func (e *Engine) validateState() error {
	s := e.state
	if b := s.GrowthBackoff; b != nil && (s.Version != 3 || b.Target < 0 || b.Failures < 1 || b.Failures > 31 || b.RetryAfter.IsZero()) {
		return code("InvalidGrowthBackoff")
	}
	if err := e.validateLegacyContract(); err != nil {
		return err
	}
	if e.cfg.DrainDeadline <= 0 && s.Version != 3 {
		return code("InvalidJournalDrainWait")
	}
	if s.LastCompletedAt < 0 || (s.LastCompletedAt != 0 && s.Version != 3) {
		return code("InvalidCompletionTimestamp")
	}
	if b := s.LastBatch; b != nil && b.CompletedAt != 0 && b.CompletedAt != s.LastCompletedAt {
		return code("InvalidCompletionTimestamp")
	}
	if k := s.NomadInstanceIDMetaKey; k != "" && (s.NomadPolicySHA256 == "" || strings.TrimSpace(k) == "" || strings.ContainsAny(k, "\x00\r\n")) {
		return code("InvalidSDKMappingKey")
	}
	if s.NomadPolicySHA256 != "" && !digestPattern.MatchString(s.NomadPolicySHA256) {
		return code("InvalidSDKPolicyIdentity")
	}
	if s.Pending != nil && (s.Pending.SDKLifecycle || requiresSDKPost(s.Pending)) && s.NomadPolicySHA256 == "" {
		return code("MissingSDKPolicyIdentity")
	}
	if s.RecoveryAudit != nil && !validRef(s.RecoveryAudit) {
		return code("InvalidRecoveryAuditReference")
	}
	if (s.Version != 1 && s.Version != 2 && s.Version != 3) || s.GroupID != e.cfg.GroupID || s.Region != e.cfg.Region || s.DrainDeadlineNanoseconds != int64(e.cfg.DrainDeadline) || s.Members == nil || s.Desired < 0 || s.Desired > e.journalLimit() {
		return code("JournalConfigurationMismatch")
	}
	if (s.Version == 3 && (!e.cfg.GroupContract || s.MaxCapacity != 0)) || (s.Version == 1 && (s.MaxCapacity != 0 || s.Batch != nil || s.LastBatch != nil)) || (s.Version == 2 && (s.MaxCapacity < 2 || s.MaxCapacity > MaxSupportedCapacity || (!e.cfg.GroupContract && s.MaxCapacity != e.cfg.MaxCapacity))) {
		return code("JournalConfigurationMismatch")
	}
	if s.LastEvent != "" && !safeValue.MatchString(s.LastEvent) {
		return code("InvalidJournal")
	}
	if s.ErrorCode != "" && !safeValue.MatchString(s.ErrorCode) {
		return code("InvalidJournal")
	}
	for id, m := range s.Members {
		if id != m.ID || !resource(id, "ins") || !safeValue.MatchString(m.CreationType) || !safeValue.MatchString(m.Lifecycle) || (m.ChargeType != "" && !safeValue.MatchString(m.ChargeType)) {
			return code("InvalidJournalMember")
		}
		if m.DrainedBy != "" && nomadclient.ValidateOperationID(m.DrainedBy) != nil {
			return code("InvalidJournalMember")
		}
		if m.Joined {
			if (nomadclient.Identity{NodeID: m.NodeID, InstanceID: id, GroupID: s.GroupID, Region: s.Region}).Validate() != nil {
				return code("InvalidJournalMember")
			}
		} else if m.NodeID != "" || m.Eligible || m.DrainedBy != "" {
			return code("InvalidJournalMember")
		}
	}
	if s.LaunchConfiguration != nil && !validLC(*s.LaunchConfiguration) {
		return code("InvalidJournalLaunchConfiguration")
	}
	if s.Cloud != nil {
		if e.validateSnapshot(*s.Cloud) != nil || s.Cloud.Group.Desired != s.Desired || !s.Cloud.At.Equal(s.ObservedAt) {
			return code("InvalidJournalCloudSnapshot")
		}
		if len(s.Cloud.Members) != len(s.Members) {
			return code("InvalidJournalMember")
		}
		for _, m := range s.Cloud.Members {
			if _, ok := s.Members[m.ID]; !ok {
				return code("InvalidJournalMember")
			}
		}
	}
	if s.Pending != nil && (s.Sequence == 0 || s.LaunchConfiguration == nil) {
		return code("InvalidJournalOperation")
	}
	if err := e.validateOperation(s.Pending, true); err != nil {
		return err
	}
	lastOperationID := ""
	if s.LastOperation != nil {
		lastOperationID = s.LastOperation.ID
	}
	lastBatchID := ""
	if s.LastBatch != nil {
		lastBatchID = s.LastBatch.ID
	}
	if h := s.PolicyHistory; h != nil && (s.PolicyAuditSHA256 == "" || (h.OperationID != "" && !operationID.MatchString(h.OperationID)) || (h.BatchID != "" && !operationID.MatchString(h.BatchID)) || (h.OperationID == "" && h.BatchID == "") || !validControllerDrainDeadline(Config{GroupContract: true, DrainDeadline: h.DrainDeadline})) {
		return code("InvalidPolicyHistory")
	}
	if err := e.historicalPolicyEngine(lastOperationID, "").validateOperation(s.LastOperation, false); err != nil {
		return err
	}
	if err := e.validateBatch(s.Batch, true); err != nil {
		return err
	}
	if err := e.historicalPolicyEngine("", lastBatchID).validateBatch(s.LastBatch, false); err != nil {
		return err
	}
	if s.Version >= 2 && s.Pending != nil && s.Batch == nil {
		return code("InvalidJournalBatch")
	}
	var last *Operation
	if s.Batch != nil && len(s.Batch.Results) > 0 {
		last = &s.Batch.Results[len(s.Batch.Results)-1]
	} else if s.Batch == nil && s.LastBatch != nil {
		last = &s.LastBatch.Results[len(s.LastBatch.Results)-1]
	}
	if last != nil && !reflect.DeepEqual(last, s.LastOperation) {
		return code("InvalidJournalBatchResult")
	}
	if s.LastBatch != nil && s.LastBatch.Phase == "failed" && s.LastBatch.Results[len(s.LastBatch.Results)-1].AbortReason == "" && s.LastBatch.Results[len(s.LastBatch.Results)-1].CloudRejection == nil && s.LastBatch.Results[len(s.LastBatch.Results)-1].GrowthFailure == nil && !settledDeleteFailure(&s.LastBatch.Results[len(s.LastBatch.Results)-1]) && (!s.ManualReview || s.Batch != nil) {
		return code("InvalidJournalReviewGate")
	}
	restored := s.LastOperation != nil && s.LastOperation.Kind == "shrink" && failureTerminal(s.LastOperation.ActivityStatus) && s.LastOperation.InstanceResult == "FAILED" && !settledDeleteFailure(s.LastOperation)
	if restored != s.ManualReview || (s.ManualReview && s.Pending != nil) {
		return code("InvalidJournalReviewGate")
	}
	return nil
}

func (e *Engine) journalLimit() int64 {
	if e.state.Version == 3 {
		return math.MaxInt64
	}
	if e.state.Version == 1 {
		return 1
	}
	return e.state.MaxCapacity
}

func (e *Engine) validateBatch(b *Batch, active bool) error {
	if b == nil {
		return nil
	}
	bad := func() error { return code("InvalidJournalBatch") }
	if !validActionSource(b.Source) {
		return bad()
	}
	if b.CompletedAt < 0 || (b.CompletedAt != 0 && (active || b.Phase != "completed" || b.CompletedAt < b.StartedAt.UnixNano())) {
		return bad()
	}
	limit := e.journalLimit()
	if (e.state.Version != 2 && e.state.Version != 3) || !operationID.MatchString(b.ID) || b.StartedAt.IsZero() || b.Target < 0 || b.Target > limit || b.InitialCount < 0 || b.InitialCount > limit || int64(len(b.InitialMembers)) != b.InitialCount {
		return bad()
	}
	if active {
		if b.Phase != "running" || e.state.ManualReview {
			return bad()
		}
	} else if b.Phase != "completed" && b.Phase != "failed" {
		return bad()
	}
	initial := map[string]bool{}
	for _, id := range b.InitialMembers {
		if !resource(id, "ins") || initial[id] {
			return bad()
		}
		initial[id] = true
	}
	if b.Kind == "grow" {
		if b.Target <= b.InitialCount || len(b.Selected) != 0 || len(b.Results) > 1 || (b.Phase == "failed" && (len(b.Results) != 1 || (b.Results[0].CloudRejection == nil && b.Results[0].GrowthFailure == nil))) {
			return bad()
		}
	} else if b.Kind == "shrink" {
		if b.Target >= b.InitialCount || int64(len(b.Selected)) != b.InitialCount-b.Target || len(b.Results) > len(b.Selected) {
			return bad()
		}
		selected := map[string]bool{}
		for _, v := range b.Selected {
			if v.Identity.Validate() != nil || v.GroupID != e.cfg.GroupID || v.Region != e.cfg.Region || !initial[v.InstanceID] || selected[v.InstanceID] || v.Allocations < 0 {
				return bad()
			}
			selected[v.InstanceID] = true
		}
	} else {
		return bad()
	}
	if err := e.validateBatchDrainWait(b, active); err != nil {
		return err
	}
	ids := map[string]bool{b.ID: true}
	for i := range b.Results {
		r := &b.Results[i]
		if e.validateOperation(r, false) != nil || ids[r.ID] || r.Kind != b.Kind {
			return bad()
		}
		ids[r.ID] = true
		if b.Kind == "grow" {
			if isScaleOut(r) && (r.CloudIntent.ScaleOut.Base != b.InitialCount || r.CloudIntent.ScaleOut.Number != b.Target-b.InitialCount) {
				return bad()
			}
			if (r.CloudRejection != nil || r.GrowthFailure != nil) != (b.Phase == "failed") {
				return bad()
			}
			if r.Target != b.Target {
				return bad()
			}
		} else {
			v := b.Selected[i]
			if r.InstanceID != v.InstanceID || r.NodeID != v.NodeID || r.Target != b.InitialCount-int64(i)-1 {
				return bad()
			}
			failed := (failureTerminal(r.ActivityStatus) && r.InstanceResult == "FAILED") || r.AbortReason != "" || r.CloudRejection != nil
			if failed {
				if b.Phase != "failed" || i != len(b.Results)-1 {
					return bad()
				}
			} else if r.ActivityStatus != "SUCCESSFUL" || r.InstanceResult != "SUCCESSFUL" {
				return bad()
			}
		}
	}
	if active {
		expected := 1
		if b.Kind == "shrink" {
			expected = len(b.Selected)
		}
		if len(b.Results) >= expected {
			return bad()
		}
		if op := e.state.Pending; op != nil {
			if ids[op.ID] || op.Kind != b.Kind {
				return bad()
			}
			if b.Kind == "grow" {
				if isScaleOut(op) && (op.CloudIntent.ScaleOut.Base != b.InitialCount || op.CloudIntent.ScaleOut.Number != b.Target-b.InitialCount) {
					return bad()
				}
				if op.Target != b.Target {
					return bad()
				}
			} else {
				v := b.Selected[len(b.Results)]
				if op.InstanceID != v.InstanceID || op.NodeID != v.NodeID || op.Target != b.InitialCount-int64(len(b.Results))-1 {
					return bad()
				}
			}
		} else if b.Kind != "shrink" || len(b.Results) == 0 {
			return bad()
		}
	} else {
		if len(b.Results) == 0 {
			return bad()
		}
		last := b.Results[len(b.Results)-1]
		if b.Phase == "completed" && last.Target != b.Target {
			return bad()
		}
		if b.Phase == "failed" && last.AbortReason == "" && last.CloudRejection == nil && last.GrowthFailure == nil && (!failureTerminal(last.ActivityStatus) || last.InstanceResult != "FAILED") {
			return bad()
		}
	}
	return nil
}
