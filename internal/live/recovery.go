package live

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"time"

	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadclient"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/tencentcloud"
)

// Recovery deliberately has no cloud or Nomad mutation capability.
type RecoveryCloud interface {
	Observe(context.Context, string, string) (tencentcloud.Snapshot, error)
	Activity(context.Context, string, string) (tencentcloud.Activity, error)
}
type RecoveryNomad interface {
	ResolveMembers(context.Context, string, string, []string) ([]nomadclient.Node, error)
}
type RecoveryScope struct {
	GroupContract     bool          `json:"group_contract,omitempty"`
	Directory         string        `json:"directory"`
	GroupID           string        `json:"group_id"`
	Region            string        `json:"region"`
	MaxCapacity       int64         `json:"max_capacity"`
	DrainDeadline     time.Duration `json:"drain_deadline_nanoseconds"`
	ScaleInProtection bool          `json:"scale_in_protection"`
}
type RecoveryEvidence struct {
	Journal   *JournalRetryEvidence          `json:"journal_retry,omitempty"`
	Post      *nomadclient.PostScaleInResult `json:"sdk_post,omitempty"`
	Discovery *ActivityDiscovery             `json:"activity_discovery,omitempty"`
	Cloud     tencentcloud.Snapshot          `json:"cloud"`
	Activity  tencentcloud.Activity          `json:"activity"`
	// Empty for current-contract growth: its completion is proven by cloud
	// evidence, not Nomad registration. Readiness requires a fresh observation.
	Nodes []nomadclient.Node `json:"nodes"`
}
type RecoveryDecision struct {
	Action        string      `json:"action"`
	OperationID   string      `json:"operation_id"`
	ActivityID    string      `json:"activity_id"`
	Count         int64       `json:"count"`
	BatchTarget   *int64      `json:"batch_target,omitempty"`
	Remaining     []Candidate `json:"remaining_batch_candidates,omitempty"`
	RestartEffect string      `json:"agent_restart_effect"`
}
type RecoveryPlan struct {
	BeforeLaunchConfiguration *tencentcloud.LaunchConfiguration `json:"before_launch_configuration,omitempty"`
	MaintenanceRouteSHA256    string                            `json:"maintenance_route_sha256,omitempty"`
	Version                   int                               `json:"version"`
	Digest                    string                            `json:"digest"`
	Scope                     RecoveryScope                     `json:"scope"`
	JournalSHA256             string                            `json:"journal_sha256"`
	Operator                  string                            `json:"operator"`
	Reference                 string                            `json:"change_reference"`
	CreatedAt                 time.Time                         `json:"created_at"`
	ExpiresAt                 time.Time                         `json:"expires_at"`
	Decision                  RecoveryDecision                  `json:"decision"`
	Evidence                  []RecoveryEvidence                `json:"evidence"`
}
type Recovery struct {
	e             *Engine
	cloud         RecoveryCloud
	nomad         RecoveryNomad
	delay         time.Duration
	closed        bool
	automatic     bool
	workflowAudit bool
	// Test-only crash injection; production leaves nil.
	checkpoint func(string) error
}

var digestPattern = regexp.MustCompile(`^[a-f0-9]{64}$`)
var actorPattern = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_.@-]{0,63}$`)
var referencePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.:/#-]{0,127}$`)

func sha(b []byte) string              { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }
func jsonBytes(v any) []byte           { b, _ := json.MarshalIndent(v, "", "  "); return append(b, '\n') }
func planDigest(p RecoveryPlan) string { p.Digest = ""; return sha(jsonBytes(p)) }
func scope(cfg Config) RecoveryScope {
	return RecoveryScope{Directory: cfg.Dir, GroupID: cfg.GroupID, Region: cfg.Region, MaxCapacity: cfg.MaxCapacity, DrainDeadline: cfg.DrainDeadline, ScaleInProtection: cfg.ScaleInProtection, GroupContract: cfg.GroupContract}
}
func recoveryEngine(cfg Config) (*Engine, error) {
	if cfg.GroupContract {
		cfg.MaxCapacity = 0
	} else if cfg.MaxCapacity == 0 {
		cfg.MaxCapacity = 1
	}
	cfg.Dir = filepath.Clean(cfg.Dir)
	if !filepath.IsAbs(cfg.Dir) || cfg.Dir == "/" || !resource(cfg.GroupID, "asg") || !regionID.MatchString(cfg.Region) || (!cfg.GroupContract && (cfg.MaxCapacity < 1 || cfg.MaxCapacity > MaxSupportedCapacity)) || !validControllerDrainDeadline(cfg) {
		return nil, code("InvalidRecoveryConfiguration")
	}
	e := &Engine{cfg: cfg}
	// Recovery never initializes a missing journal directory.
	if _, err := readPrivate(filepath.Join(cfg.Dir, "live.json"), stateFileLimit); err != nil {
		return nil, err
	}
	if err := e.lockDir(); err != nil {
		return nil, err
	}
	if err := e.checkPolicyPending(); err != nil {
		e.release()
		return nil, err
	}
	return e, nil
}
func OpenRecovery(cfg Config, c RecoveryCloud, n RecoveryNomad) (*Recovery, error) {
	if c == nil || n == nil {
		return nil, code("InvalidRecoveryConfiguration")
	}
	e, err := recoveryEngine(cfg)
	if err != nil {
		return nil, err
	}
	fail := func(err error) (*Recovery, error) { e.release(); return nil, err }
	if err = e.checkRecoveryPending(); err != nil {
		return fail(err)
	}
	exists, err := e.load()
	if err != nil {
		return fail(err)
	}
	if !exists {
		return fail(code("RecoveryJournalMissing"))
	}
	if err = e.checkRecoveryAudit(); err != nil {
		return fail(err)
	}
	// Reuse the journaled mapping convention for the read-only recovery CLI.
	// Legacy journals retain their original group/region metadata contract.
	if raw, ok := n.(*nomadclient.Client); ok && e.state.NomadInstanceIDMetaKey != "" {
		scoped, scopeErr := raw.WithScope(cfg.GroupID, cfg.Region, e.state.NomadInstanceIDMetaKey)
		if scopeErr != nil {
			return fail(code("RecoveryMappingConfigurationFailed"))
		}
		n = scoped
	}
	return &Recovery{e: e, cloud: c, nomad: n, delay: time.Second}, nil
}
func (r *Recovery) Close() {
	if !r.closed {
		r.closed = true
		r.e.release()
	}
}
func (r *Recovery) currentHash() (string, error) {
	if r.closed {
		return "", code("RecoveryClosed")
	}
	b, err := readPrivate(filepath.Join(r.e.cfg.Dir, "live.json"), stateFileLimit)
	if err != nil {
		return "", err
	}
	return sha(b), nil
}
func (r *Recovery) Preview(ctx context.Context, activity, operator, reference string) (RecoveryPlan, error) {
	p := RecoveryPlan{}
	if !resource(activity, "asa") || !actorPattern.MatchString(operator) || !referencePattern.MatchString(reference) {
		return p, code("InvalidRecoveryInput")
	}
	h, err := r.currentHash()
	if err != nil {
		return p, err
	}
	evidence, decision, _, err := r.evaluate(ctx, activity)
	if err != nil {
		return p, err
	}
	now := time.Now().UTC()
	p = RecoveryPlan{Version: 1, Scope: scope(r.e.cfg), JournalSHA256: h, Operator: operator, Reference: reference, CreatedAt: now, ExpiresAt: now.Add(5 * time.Minute), Decision: decision, Evidence: evidence}
	p.Digest = planDigest(p)
	return p, nil
}
func (r *Recovery) Apply(ctx context.Context, p RecoveryPlan, confirmation string) (RecoveryResult, error) {
	var out RecoveryResult
	if !digestPattern.MatchString(confirmation) || p.Digest != confirmation || planDigest(p) != confirmation || p.Version != 1 || !reflect.DeepEqual(p.Scope, scope(r.e.cfg)) || !actorPattern.MatchString(p.Operator) || !referencePattern.MatchString(p.Reference) || !resource(p.Decision.ActivityID, "asa") {
		return out, code("RecoveryPlanMismatch")
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
	h, err := r.currentHash()
	if err != nil {
		return out, err
	}
	if h != p.JournalSHA256 {
		return out, code("RecoveryJournalChanged")
	}
	evidence, decision, after, err := r.evaluate(ctx, p.Decision.ActivityID)
	if err != nil {
		return out, err
	}
	if !reflect.DeepEqual(decision, p.Decision) || len(p.Evidence) != 2 || evidenceKey(evidence[1]) != evidenceKey(p.Evidence[0]) || evidenceKey(evidence[1]) != evidenceKey(p.Evidence[1]) {
		return out, code("RecoveryEvidenceChanged")
	}
	if !validTime() {
		return out, code("RecoveryPlanExpired")
	}
	if ctx.Err() != nil {
		return out, code("RecoveryCanceled")
	}
	// No API call after this point: a local, recoverable audit/journal transaction.
	return r.commit(p, evidence, after, h)
}
func evidenceKey(e RecoveryEvidence) string {
	e.Cloud.At = time.Time{}
	e.Cloud.RequestIDs = nil
	e.Activity.RequestIDs = nil
	if e.Discovery != nil {
		// Do not mutate evidence retained by the plan/audit through a shared pointer.
		discovery := *e.Discovery
		discovery.ObservedAt = time.Time{}
		discovery.Activity.RequestIDs = nil
		discovery.History = append([]tencentcloud.Activity(nil), discovery.History...)
		for i := range discovery.History {
			discovery.History[i].RequestIDs = nil
		}
		e.Discovery = &discovery
	}
	return sha(jsonBytes(e))
}
func (r *Recovery) evaluate(ctx context.Context, activity string) ([]RecoveryEvidence, RecoveryDecision, State, error) {
	var samples []RecoveryEvidence
	var decision RecoveryDecision
	var after State
	op := r.e.state.Pending
	if op == nil || !op.Unknown || op.Phase != "unknown" || r.e.state.ManualReview {
		return nil, decision, after, code("RecoveryRequiresUnknownOperation")
	}
	if op.ActivityID != "" && op.ActivityID != activity {
		return nil, decision, after, code("RecoveryActivityMismatch")
	}
	for i := 0; i < 2; i++ {
		if i > 0 {
			timer := time.NewTimer(r.delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, decision, after, code("RecoveryCanceled")
			case <-timer.C:
			}
		}
		sample, next, err := r.observe(ctx, activity)
		if err != nil {
			return nil, decision, after, err
		}
		samples = append(samples, sample)
		after = next
	}
	if evidenceKey(samples[0]) != evidenceKey(samples[1]) {
		return nil, decision, after, code("RecoveryEvidenceUnstable")
	}
	if pending := after.Pending; pending != nil && pending.Phase == "failed-delete" {
		decision = RecoveryDecision{Action: "confirm-remove-failure", OperationID: op.ID, ActivityID: activity, Count: samples[1].Cloud.Group.Count, RestartEffect: "restore-owned-node-and-reconcile-cloud"}
		if b := after.Batch; b != nil {
			target := b.Target
			decision.BatchTarget = &target
		}
		return samples, decision, after, nil
	}
	decision = RecoveryDecision{Action: "confirm-grow-success", OperationID: op.ID, ActivityID: activity, Count: op.Target, RestartEffect: "policy-evaluation-resumes-on-agent-restart"}
	if r.automatic {
		decision.RestartEffect = "policy-evaluation-resumes-after-reconciliation"
	}
	if op.Kind == "shrink" {
		decision.Action = "confirm-remove-success"
	}
	if b := after.Batch; b != nil {
		target := b.Target
		decision.BatchTarget = &target
		decision.Remaining = append([]Candidate(nil), b.Selected[len(b.Results):]...)
		decision.RestartEffect = "remaining-batch-deletions-resume-on-agent-restart"
		if r.e.cfg.ScaleInProtection && !r.e.cfg.GroupContract {
			decision.RestartEffect = "remaining-batch-held-by-scale-in-protection"
		}
	}
	if after.Pending != nil && after.Pending.Phase == "post-scale" {
		decision.RestartEffect = "sdk-post-scale-resumes-before-policy-evaluation"
		if b := after.Batch; b != nil {
			decision.Remaining = append([]Candidate(nil), b.Selected[len(b.Results)+1:]...)
			if len(decision.Remaining) > 0 {
				decision.RestartEffect = "sdk-post-scale-then-remaining-batch-resumes"
			}
		}
	}
	return samples, decision, after, nil
}
func (r *Recovery) observe(ctx context.Context, activity string) (RecoveryEvidence, State, error) {
	var ev RecoveryEvidence
	var next State
	e := r.e
	op := e.state.Pending
	fail := func(s string) (RecoveryEvidence, State, error) { return ev, next, code(s) }
	s, err := r.cloud.Observe(ctx, e.cfg.GroupID, op.InstanceID)
	if err != nil {
		return fail("RecoveryCloudObservationFailed")
	}
	var a tencentcloud.Activity
	if r.automatic && op.ActivityID == "" {
		a, err = r.cloud.Activity(ctx, e.cfg.GroupID, activity)
		if err != nil {
			return fail("RecoveryActivityUnavailable")
		}
		discovery, discoveryErr := discoverActivity(ctx, r.cloud, e.cfg.GroupID, op)
		if discoveryErr != nil || discovery.Activity.ID != activity || !sameActivity(discovery.Activity, a) {
			return fail("RecoveryActivityDiscoveryUnconfirmed")
		}
		ev.Discovery = &discovery
		if failureTerminal(a.Status) {
			return r.observeDiscoveredDeletionFailure(s, a, discovery)
		}
	}
	if e.validateSnapshot(s) != nil || e.guard(s) != nil || e.state.LaunchConfiguration == nil || !reflect.DeepEqual(*e.state.LaunchConfiguration, s.LaunchConfiguration) {
		return fail("RecoveryCloudGuardRejected")
	}
	if isScaleOut(op) && s.Group.AllocationPolicy != "LAUNCH_CONFIGURATION" {
		return fail("RecoveryScaleOutModeChanged")
	}
	if s.At.After(time.Now().Add(time.Second)) || time.Since(s.At) > 30*time.Second || s.Group.Count != op.Target || s.Group.Desired != op.Target || int64(len(s.Members)) != op.Target || s.Group.Activity != "NOT_IN_ACTIVITY" {
		return fail("RecoveryTargetNotSettled")
	}
	if ev.Discovery == nil {
		a, err = r.cloud.Activity(ctx, e.cfg.GroupID, activity)
		if err != nil {
			return fail("RecoveryActivityUnavailable")
		}
	}
	start, se := time.Parse(time.RFC3339, a.StartedAt)
	end, ee := time.Parse(time.RFC3339, a.EndedAt)
	if a.ID != activity || a.GroupID != e.cfg.GroupID || a.Status != "SUCCESSFUL" || se != nil || ee != nil || start.Before(op.StartedAt.Truncate(time.Second)) || end.Before(start) || end.After(time.Now().UTC()) || len(a.RelatedInstances) == 0 {
		return fail("RecoveryActivityUnconfirmed")
	}
	if isScaleOut(op) {
		// Discovery supplies a candidate ID, not permission to relax the
		// original increment's exact activity/member settlement contract.
		proof := &Engine{cfg: e.cfg, state: cloneRetryState(e.state)}
		proof.state.Pending.ActivityID = activity
		if !proof.scaleOutTargetSettled(s, a) {
			return fail("RecoveryScaleOutUnconfirmed")
		}
	}
	related := map[string]bool{}
	for _, item := range a.RelatedInstances {
		if !resource(item.ID, "ins") || related[item.ID] || item.Status != "SUCCESSFUL" {
			return fail("RecoveryActivityUnconfirmed")
		}
		related[item.ID] = true
	}
	for _, trace := range a.RequestIDs {
		if !safeValue.MatchString(trace.Action) || !requestID.MatchString(trace.RequestID) {
			return fail("RecoveryActivityUnconfirmed")
		}
	}
	if op.Kind == "shrink" {
		vm, observed := s.VMs[op.InstanceID]
		_, exists := cloudMember(s, op.InstanceID)
		if a.Type != "REMOVE_INSTANCES" || len(related) != 1 || !related[op.InstanceID] || exists || !observed || vm.Returned || !e.batchMembershipMatches(s, true) {
			return fail("RecoveryDeletionUnconfirmed")
		}
	} else {
		if a.Type != "SCALE_OUT" {
			return fail("RecoveryActivityTypeMismatch")
		}
		initial := map[string]bool{}
		if e.state.Batch != nil {
			for _, id := range e.state.Batch.InitialMembers {
				initial[id] = true
			}
		}
		added := map[string]bool{}
		for _, m := range s.Members {
			if initial[m.ID] {
				delete(initial, m.ID)
				continue
			}
			at, err := time.Parse(time.RFC3339, m.AddedAt)
			if err != nil || at.Before(op.StartedAt.Truncate(time.Second)) || !related[m.ID] || (!e.usesGroupCapacityContract() && (m.Protected || s.VMs[m.ID].Protected)) {
				return fail("RecoveryGrowthProvenanceUnconfirmed")
			}
			added[m.ID] = true
		}
		if len(initial) != 0 || len(added) != len(related) {
			return fail("RecoveryGrowthProvenanceUnconfirmed")
		}
	}
	members := map[string]Member{}
	ids := []string{}
	cloudGrowth := e.state.Version == 3 && e.usesGroupCapacityContract() && op.Kind == "grow"
	for _, m := range s.Members {
		vm := s.VMs[m.ID]
		member := Member{ID: m.ID, Running: vm.Returned && vm.State == "RUNNING", Protected: m.Protected || vm.Protected, CreationType: m.CreationType, Lifecycle: m.Lifecycle, ChargeType: vm.ChargeType}
		if cloudGrowth {
			if !runningCloudMember(member) {
				return fail("RecoveryGrowthCloudMemberNotReady")
			}
			// Do not turn a previously registered node's disappearance into
			// a first-registration timeout, or publish stale node readiness.
			before := e.state.Members[m.ID]
			member.EverJoined = before.EverJoined || before.Joined
			member.RegistrationState = "unknown"
		}
		members[m.ID] = member
		ids = append(ids, m.ID)
	}
	sort.Strings(ids)
	nodes := []nomadclient.Node{}
	if len(ids) > 0 && !cloudGrowth {
		nodes, err = r.nomad.ResolveMembers(ctx, e.cfg.GroupID, e.cfg.Region, ids)
		if err != nil || len(nodes) != len(ids) {
			return fail("RecoveryNomadMappingUnavailable")
		}
		seen := map[string]bool{}
		for _, node := range nodes {
			m, exists := members[node.InstanceID]
			if !exists || seen[node.InstanceID] || node.Identity.Validate() != nil || node.GroupID != e.cfg.GroupID || node.Region != e.cfg.Region {
				return fail("RecoveryNomadIdentityRejected")
			}
			seen[node.InstanceID] = true
			m.NodeID = node.NodeID
			m.Joined = true
			m.Eligible = node.Status == "ready" && node.Eligibility == "eligible" && !node.Drain && !node.HasDrainStrategy
			if !stableMember(m) {
				return fail("RecoveryMemberNotReady")
			}
			members[m.ID] = m
		}
	}
	sort.Slice(s.Members, func(i, j int) bool { return s.Members[i].ID < s.Members[j].ID })
	sort.Slice(a.RelatedInstances, func(i, j int) bool { return a.RelatedInstances[i].ID < a.RelatedInstances[j].ID })
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].InstanceID < nodes[j].InstanceID })
	ev.Cloud, ev.Activity, ev.Nodes = s, a, nodes
	// A deep copy is required: preview must not mutate the loaded state.
	if json.Unmarshal(jsonBytes(e.state), &next) != nil {
		return fail("RecoveryStateCopyFailed")
	}
	resolved := next.Pending
	resolved.Unknown = false
	resolved.Phase = "completed"
	resolved.ActivityID = activity
	resolved.ActivityDiscovery = ev.Discovery
	resolved.ActivityStatus = "SUCCESSFUL"
	if op.Kind == "shrink" {
		resolved.InstanceResult = "SUCCESSFUL"
		resolved.AbsentObservations = 2
	} else if next.Version == 1 {
		for _, m := range members {
			resolved.InstanceID = m.ID
			resolved.NodeID = m.NodeID
		}
	}
	next.LastOperation = resolved
	next.Pending = nil
	next.LastEvent = "manual-recovery-confirmed-success"
	if r.automatic {
		next.LastEvent = "automatic-recovery-confirmed-success"
	}
	next.ErrorCode = ""
	next.ManualReview = false
	next.Cloud = &s
	next.ObservedAt = s.At
	next.Members = members
	next.Desired = s.Group.Desired
	needsPost := requiresSDKPost(resolved)
	if needsPost {
		resolved.Phase = "post-scale"
		next.Pending = resolved
		next.LastOperation = e.state.LastOperation
		next.LastEvent = "recovery-confirmed-awaiting-sdk-post"
	}
	if b := next.Batch; b != nil && !needsPost {
		b.Results = append(b.Results, *resolved)
		if b.Kind == "grow" || resolved.Target == b.Target {
			b.Phase = "completed"
			if e.cfg.GroupContract {
				b.CompletedAt = time.Now().UTC().UnixNano()
				next.LastCompletedAt = b.CompletedAt
			}
			next.LastBatch = b
			next.Batch = nil
		}
	}
	finishLegacyContract(&next)
	candidate := &Engine{cfg: e.cfg, state: next}
	if err = candidate.validateState(); err != nil {
		return fail("RecoveryResultInvalid")
	}
	return ev, next, nil
}
