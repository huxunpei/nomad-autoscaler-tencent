package live

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"time"

	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadclient"
)

const acceptLaunchConfiguration = "accept-launch-configuration"

// Maintenance changes only the accepted launch configuration. Completion time,
// historical operations, registration clocks, membership and policy survive.
func launchConfigurationAfter(before State, evidence RecoveryEvidence) (State, error) {
	if before.Version != 3 || before.Pending != nil || before.Batch != nil || before.ManualReview || before.LegacyContract != nil || before.LaunchConfiguration == nil {
		return State{}, code("LaunchConfigurationRequiresIdleController")
	}
	s := evidence.Cloud
	e := &Engine{cfg: Config{GroupContract: true, GroupID: before.GroupID, Region: before.Region}}
	if e.validateSnapshot(s) != nil || e.groupGuard(s) != nil || s.Group.AllocationPolicy != "LAUNCH_CONFIGURATION" || s.Group.Activity != "NOT_IN_ACTIVITY" || s.Group.Count != int64(len(s.Members)) || s.Group.Desired != s.Group.Count || len(before.Members) != len(s.Members) || reflect.DeepEqual(*before.LaunchConfiguration, s.LaunchConfiguration) {
		return State{}, code("LaunchConfigurationObservationRejected")
	}
	if len(evidence.Nodes) != len(s.Members) {
		return State{}, code("LaunchConfigurationNodesNotReady")
	}
	nodes := map[string]nomadclient.Node{}
	nodeIDs := map[string]bool{}
	for _, n := range evidence.Nodes {
		if n.Identity.Validate() != nil || n.GroupID != before.GroupID || n.Region != before.Region || n.Status != "ready" || n.Eligibility != "eligible" || n.Drain || n.HasDrainStrategy || nodes[n.InstanceID].NodeID != "" || nodeIDs[n.NodeID] {
			return State{}, code("LaunchConfigurationNodesNotReady")
		}
		nodes[n.InstanceID] = n
		nodeIDs[n.NodeID] = true
	}
	for _, m := range s.Members {
		vm := s.VMs[m.ID]
		old, exists := before.Members[m.ID]
		n, mapped := nodes[m.ID]
		if !exists || !mapped || (old.NodeID != "" && old.NodeID != n.NodeID) || m.CreationType != "AUTO_CREATION" || m.Lifecycle != "IN_SERVICE" || !vm.Returned || vm.State != "RUNNING" || vm.ChargeType != "POSTPAID_BY_HOUR" {
			return State{}, code("LaunchConfigurationMembersNotStable")
		}
	}
	after := cloneRetryState(before)
	lc := s.LaunchConfiguration
	after.LaunchConfiguration = &lc
	return after, nil
}

func (r *Recovery) launchConfigurationEvidence(ctx context.Context) ([]RecoveryEvidence, State, error) {
	if r.closed {
		return nil, State{}, code("RecoveryClosed")
	}
	var evidence []RecoveryEvidence
	var after State
	for i := 0; i < 2; i++ {
		if i > 0 {
			timer := time.NewTimer(r.delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, State{}, code("RecoveryCanceled")
			case <-timer.C:
			}
		}
		s, err := r.cloud.Observe(ctx, r.e.cfg.GroupID, "")
		if err != nil {
			return nil, State{}, code("LaunchConfigurationCloudUnavailable")
		}
		now := time.Now().UTC()
		if s.At.After(now.Add(time.Second)) || now.Sub(s.At) > 2*time.Minute {
			return nil, State{}, code("LaunchConfigurationStaleObservation")
		}
		ids := make([]string, 0, len(s.Members))
		for _, m := range s.Members {
			ids = append(ids, m.ID)
		}
		sort.Strings(ids)
		var nodes []nomadclient.Node
		if len(ids) > 0 {
			nodes, err = r.nomad.ResolveMembers(ctx, r.e.cfg.GroupID, r.e.cfg.Region, ids)
			if err != nil {
				return nil, State{}, code("LaunchConfigurationNodesUnavailable")
			}
		}
		sort.Slice(s.Members, func(i, j int) bool { return s.Members[i].ID < s.Members[j].ID })
		sort.Slice(nodes, func(i, j int) bool { return nodes[i].InstanceID < nodes[j].InstanceID })
		ev := RecoveryEvidence{Cloud: s, Nodes: nodes}
		after, err = launchConfigurationAfter(r.e.state, ev)
		if err != nil {
			return nil, State{}, err
		}
		evidence = append(evidence, ev)
	}
	if evidenceKey(evidence[0]) != evidenceKey(evidence[1]) {
		return nil, State{}, code("LaunchConfigurationObservationChanged")
	}
	return evidence, after, nil
}

func (r *Recovery) PreviewLaunchConfiguration(ctx context.Context, operator, reference, routeHash string) (RecoveryPlan, error) {
	var p RecoveryPlan
	if !actorPattern.MatchString(operator) || !referencePattern.MatchString(reference) || !digestPattern.MatchString(routeHash) {
		return p, code("InvalidLaunchConfigurationInput")
	}
	if err := r.e.checkRecoveryPending(); err != nil {
		return p, err
	}
	if err := r.e.checkJournalRetryPending(); err != nil {
		return p, err
	}
	h, err := r.currentHash()
	if err != nil {
		return p, err
	}
	evidence, _, err := r.launchConfigurationEvidence(ctx)
	if err != nil {
		return p, err
	}
	now := time.Now().UTC()
	p = RecoveryPlan{Version: 2, Scope: scope(r.e.cfg), JournalSHA256: h, Operator: operator, Reference: reference, CreatedAt: now, ExpiresAt: now.Add(5 * time.Minute), Decision: RecoveryDecision{Action: acceptLaunchConfiguration, Count: evidence[1].Cloud.Group.Count, RestartEffect: "refresh accepted launch configuration; preserve history and cooldown"}, Evidence: evidence, MaintenanceRouteSHA256: routeHash, BeforeLaunchConfiguration: cloneRetryState(r.e.state).LaunchConfiguration}
	p.Digest = planDigest(p)
	return p, nil
}

func (r *Recovery) ApplyLaunchConfiguration(ctx context.Context, p RecoveryPlan, confirm, routeHash string) (RecoveryResult, error) {
	var out RecoveryResult
	if r.closed || !digestPattern.MatchString(confirm) || p.Digest != confirm || planDigest(p) != confirm || p.Version != 2 || p.Scope != scope(r.e.cfg) || p.MaintenanceRouteSHA256 != routeHash || !digestPattern.MatchString(routeHash) || !actorPattern.MatchString(p.Operator) || !referencePattern.MatchString(p.Reference) || p.Decision.Action != acceptLaunchConfiguration || !reflect.DeepEqual(p.BeforeLaunchConfiguration, r.e.state.LaunchConfiguration) || len(p.Evidence) != 2 {
		return out, code("LaunchConfigurationPlanMismatch")
	}
	validTime := func() bool {
		now := time.Now().UTC()
		return !p.CreatedAt.After(now) && p.ExpiresAt.Equal(p.CreatedAt.Add(5*time.Minute)) && now.Before(p.ExpiresAt)
	}
	if !validTime() {
		return out, code("RecoveryPlanExpired")
	}
	if err := r.e.checkRecoveryPending(); err != nil {
		return out, err
	}
	if err := r.e.checkJournalRetryPending(); err != nil {
		return out, err
	}
	h, err := r.currentHash()
	if err != nil {
		return out, err
	}
	if h != p.JournalSHA256 {
		return out, code("RecoveryJournalChanged")
	}
	evidence, after, err := r.launchConfigurationEvidence(ctx)
	if err != nil {
		return out, err
	}
	if evidenceKey(evidence[1]) != evidenceKey(p.Evidence[0]) || evidenceKey(evidence[1]) != evidenceKey(p.Evidence[1]) || p.Decision.Count != evidence[1].Cloud.Group.Count {
		return out, code("LaunchConfigurationEvidenceChanged")
	}
	if !validTime() {
		return out, code("RecoveryPlanExpired")
	}
	if ctx.Err() != nil {
		return out, code("RecoveryCanceled")
	}
	return r.commit(p, evidence, after, h)
}

func validLaunchConfigurationRecord(record recoveryRecord) bool {
	p := record.Plan
	if record.Version != 4 || record.Automatic || p.Version != 2 || p.Decision.Action != acceptLaunchConfiguration || !p.Scope.GroupContract || !digestPattern.MatchString(p.MaintenanceRouteSHA256) || !actorPattern.MatchString(p.Operator) || !referencePattern.MatchString(p.Reference) || len(p.Evidence) != 2 || len(record.Evidence) != 2 || p.CreatedAt.IsZero() || !p.ExpiresAt.Equal(p.CreatedAt.Add(5*time.Minute)) || !reflect.DeepEqual(p.BeforeLaunchConfiguration, record.Before.LaunchConfiguration) || record.AppliedAt.Before(p.CreatedAt) || !record.AppliedAt.Before(p.ExpiresAt) {
		return false
	}
	for _, ev := range append(append([]RecoveryEvidence{}, p.Evidence...), record.Evidence...) {
		after, err := launchConfigurationAfter(record.Before, ev)
		if err != nil || !reflect.DeepEqual(after, record.After) || evidenceKey(ev) != evidenceKey(p.Evidence[0]) || ev.Cloud.Group.Count != p.Decision.Count {
			return false
		}
	}
	return true
}

// The newest maintenance audit is authoritative; later operation audits do not
// change the accepted LC. Missing or modified history is rejected on restart.
func (e *Engine) checkLaunchConfigurationAudit(record recoveryRecord) error {
	if !reflect.DeepEqual(e.state.LaunchConfiguration, record.After.LaunchConfiguration) {
		return code("LaunchConfigurationAuditMismatch")
	}
	return nil
}

// A saved retry intent must settle before any maintenance transaction.
func (e *Engine) checkJournalRetryPending() error {
	if _, err := os.Lstat(filepath.Join(e.cfg.Dir, journalRetryQueueName)); !os.IsNotExist(err) {
		return code("PolicyChangeRecoveryPending")
	}
	return nil
}
