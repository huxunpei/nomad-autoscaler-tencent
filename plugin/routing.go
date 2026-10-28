package plugin

import (
	"context"
	"errors"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/telemetry"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/nomad-autoscaler/plugins/shared"
	"github.com/hashicorp/nomad-autoscaler/sdk"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/live"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadclient"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadsdk"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/policybounds"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/targetconfig"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/tencentcloud"
)

var (
	ErrRouteInitializing   = errors.New("GroupControllerInitializing")
	ErrRoutePolicyChanged  = errors.New("GroupPolicyChangeRequiresMigration")
	errRuntimeClosed       = errors.New("TargetRuntimeClosed")
	verifiedAccountPattern = regexp.MustCompile(`^[1-9][0-9]{0,19}$`)
)

type accountCloud interface {
	live.Cloud
	AccountID() string
	Region() string
	Close()
}
type groupController interface {
	Snapshot() (live.State, bool, error)
	ScaleAndWait(int64, int) (bool, error)
	Close()
}
type runtimeDependencies struct {
	metrics    *telemetry.Collector
	openCloud  func(context.Context, tencentcloud.AuthConfig) (accountCloud, error)
	openEngine func(live.Config, live.Cloud, live.Nomad) (groupController, error)
	interval   time.Duration
}
type groupRoute struct {
	actionMu sync.Mutex
	resume   bool
	policy   targetconfig.Policy
	ready    chan struct{}
	engine   groupController
	lock     *os.File
	err      error
}
type policyRuntime struct {
	policyBounds  *policybounds.Checker
	metrics       *telemetry.Collector
	metricsServer *telemetry.Server
	agent         targetconfig.Agent
	account, root string
	cloud         accountCloud
	nomad         *nomadclient.Client
	deps          runtimeDependencies
	logger        hclog.Logger
	ctx           context.Context
	cancel        context.CancelFunc
	mu            sync.Mutex
	routes        map[string]*groupRoute
	startedAt     time.Time
	closing       bool
	initializing  sync.WaitGroup
	closeOnce     sync.Once
}

func (t *Target) configureRuntime(config map[string]string) error {
	agent, err := targetconfig.ParseAgent(config)
	if err != nil {
		return err
	}
	ncConfig, err := nomadclient.ConfigFromMap(config, os.Getenv)
	if err != nil {
		return err
	}
	nc, err := nomadclient.New(ncConfig)
	if err != nil {
		return err
	}
	deps := runtimeDependencies{}
	if t.runtimeDeps != nil {
		deps = *t.runtimeDeps
	}
	deps.metrics = telemetry.New()
	if deps.openCloud == nil {
		deps.openCloud = func(ctx context.Context, cfg tencentcloud.AuthConfig) (accountCloud, error) {
			return tencentcloud.NewAuthenticated(ctx, cfg)
		}
	}
	if deps.openEngine == nil {
		deps.openEngine = func(cfg live.Config, c live.Cloud, n live.Nomad) (groupController, error) {
			return live.Open(cfg, c, n)
		}
	}
	if deps.interval == 0 {
		deps.interval = 5 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	cloud, err := deps.openCloud(ctx, tencentcloud.AuthConfig{Metrics: deps.metrics, Region: agent.Region, Method: agent.AuthMethod, CredentialsFile: agent.CredentialsFile, Timeout: 10 * time.Second})
	cancel()
	if err != nil {
		if cloud != nil {
			cloud.Close()
		}
		nc.Close()
		return err
	}
	if cloud == nil || !verifiedAccountPattern.MatchString(cloud.AccountID()) || cloud.Region() != agent.Region {
		if cloud != nil {
			cloud.Close()
		}
		nc.Close()
		return errors.New("InvalidVerifiedCloudIdentity")
	}
	r, err := newPolicyRuntime(agent, cloud, nc, deps, t.logger)
	if err != nil {
		cloud.Close()
		nc.Close()
		return err
	}
	t.runtime = r
	t.backend = "tencent-as"
	return nil
}

func newPolicyRuntime(agent targetconfig.Agent, cloud accountCloud, nc *nomadclient.Client, deps runtimeDependencies, logger hclog.Logger) (*policyRuntime, error) {
	root := filepath.Join(agent.StateDir, "accounts", cloud.AccountID(), agent.Region)
	if err := routePrivateDir(root); err != nil {
		return nil, err
	}
	if deps.metrics == nil {
		deps.metrics = telemetry.New()
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := &policyRuntime{metrics: deps.metrics, agent: agent, account: cloud.AccountID(), root: root, cloud: cloud, nomad: nc, deps: deps, logger: logger, ctx: ctx, cancel: cancel, routes: map[string]*groupRoute{}, startedAt: time.Now().UTC()}
	r.policyBounds = policybounds.New(agent.PolicyDir, logger)
	// Discover all saved policies before any recovery worker is started. A fresh
	// Status never becomes the trigger for replaying an old controller's writes.
	entries, err := os.ReadDir(root)
	if err != nil {
		cancel()
		return nil, errors.New("RouteDirectoryUnreadable")
	}
	var records []routeRecord
	for _, entry := range entries {
		if entry.Name() == "" || entry.Name()[0] == '.' {
			continue
		}
		if !entry.IsDir() || entry.Type()&os.ModeSymlink != 0 {
			cancel()
			return nil, errors.New("InvalidRouteDirectory")
		}
		if err := resumeIdlePolicyUpdate(filepath.Join(root, entry.Name())); err != nil {
			cancel()
			return nil, err
		}
		record, exists, err := readRouteRecord(filepath.Join(root, entry.Name()))
		if err != nil {
			cancel()
			return nil, err
		}
		if !exists {
			continue
		}
		if r.validateRecord(record) != nil || record.Policy.GroupID != entry.Name() {
			cancel()
			return nil, errors.New("InvalidRouteRecord")
		}
		records = append(records, record)
	}
	r.metricsServer, err = r.metrics.Start(agent.MetricsAddress)
	if err != nil {
		cancel()
		return nil, err
	}
	if r.metricsServer != nil && logger != nil {
		logger.Info("Tencent metrics endpoint started", "address", r.metricsServer.Address())
	}
	for _, record := range records {
		r.getRoute(record.Policy, true)
	}
	return r, nil
}

func (r *policyRuntime) validateRecord(record routeRecord) error {
	if record.Account != r.account || record.Region != r.agent.Region || !((record.Version == 1 && record.MaxCapacity >= 1 && record.MaxCapacity <= live.MaxSupportedCapacity) || (record.Version == 2 && record.MaxCapacity == 0)) {
		return errors.New("RouteIdentityMismatch")
	}
	values := record.Policy.SDKConfig()
	values["tencent_group_id"] = record.Policy.GroupID
	values["instance_id_meta_key"] = record.Policy.InstanceIDMetaKey
	values["scale_in_protection"] = strconv.FormatBool(record.Policy.ScaleInProtection)
	canonical, err := r.agent.Policy(values)
	if err != nil || canonical != record.Policy {
		return errors.New("InvalidRoutePolicy")
	}
	return nil
}

func (r *policyRuntime) getRoute(policy targetconfig.Policy, resume bool) (*groupRoute, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closing {
		return nil, errRuntimeClosed
	}
	if route := r.routes[policy.GroupID]; route != nil {
		if route.policy != policy {
			return nil, ErrRoutePolicyChanged
		}
		select {
		case <-route.ready:
			if route.err != nil {
				return r.startRouteLocked(policy, route.resume || resume), nil
			}
		default:
		}
		return route, nil
	}
	return r.startRouteLocked(policy, resume), nil
}

// Caller holds r.mu. Retrying a recorded startup route retains its recovery
// mode; retries of a newly observed route remain read-only until Scale.
func (r *policyRuntime) startRouteLocked(policy targetconfig.Policy, resume bool) *groupRoute {
	route := &groupRoute{policy: policy, resume: resume, ready: make(chan struct{})}
	r.routes[policy.GroupID] = route
	r.initializing.Add(1)
	go func() {
		defer r.initializing.Done()
		defer close(route.ready)
		r.initializeRoute(route)
	}()
	return route
}

func (r *policyRuntime) openRoute(policy targetconfig.Policy, resume bool) (groupController, *os.File, error) {
	dir := filepath.Join(r.root, policy.GroupID)
	lock, err := lockRouteDir(dir)
	if err != nil {
		return nil, nil, err
	}
	retained := false
	defer func() {
		if !retained {
			releaseRouteLock(lock)
		}
	}()
	record, exists, err := readRouteRecord(dir)
	if err != nil {
		return nil, nil, err
	}
	if exists {
		if err = r.validateRecord(record); err != nil {
			return nil, nil, err
		}
		if record.Policy != policy {
			return nil, nil, ErrRoutePolicyChanged
		}
	} else {
		if _, err = os.Lstat(filepath.Join(dir, "live.json")); !os.IsNotExist(err) {
			return nil, nil, errors.New("MissingRouteRecordForJournal")
		}
		ctx, cancel := context.WithTimeout(r.ctx, 60*time.Second)
		snap, readErr := r.cloud.Observe(ctx, policy.GroupID, "")
		cancel()
		if readErr != nil {
			return nil, nil, errors.New("GroupInitializationObservationFailed")
		}
		if snap.Region != r.agent.Region || snap.Group.ID != policy.GroupID || snap.Group.Min < 0 || snap.Group.Max < snap.Group.Min {
			return nil, nil, errors.New("UnsupportedGroupCapacity")
		}
		record = routeRecord{Version: 2, Account: r.account, Region: r.agent.Region, Policy: policy}
		if err = writeRouteRecord(dir, record); err != nil {
			return nil, nil, err
		}
	}
	scoped, err := r.nomad.WithScope(policy.GroupID, r.agent.Region, policy.InstanceIDMetaKey)
	if err != nil {
		return nil, nil, err
	}
	if r.ctx.Err() != nil {
		return nil, nil, errRuntimeClosed
	}
	engine, err := r.deps.openEngine(live.Config{RegistrationGracePeriod: r.agent.RegistrationGracePeriod, Metrics: r.metrics, StabilizationInterval: r.deps.interval, GroupContract: true, Logger: r.logger, Context: r.ctx, DisableReconciliation: !resume, Dir: dir, GroupID: policy.GroupID, Region: r.agent.Region, MaxCapacity: record.MaxCapacity, ScaleInProtection: policy.ScaleInProtection, ReconcileInterval: r.deps.interval, DrainDeadline: policy.DrainDeadline}, r.cloud, nomadsdk.New(scoped, policy))
	if err != nil {
		if engine != nil {
			engine.Close()
		}
		return nil, nil, err
	}
	if engine == nil {
		return nil, nil, errors.New("GroupControllerUnavailable")
	}
	if r.ctx.Err() != nil {
		engine.Close()
		return nil, nil, errRuntimeClosed
	}
	retained = true
	return engine, lock, nil
}

// restartCooldownHolding is the plugin-side hold for an official Agent bug:
// after a restart, NewPolicyHandler starts a timer but leaves the deadline
// Run actually checks at zero, so the first evaluation skips the remaining
// cooldown. Returning Ready=false makes that evaluation stop before Scale.
// Completions recorded in this process are left to the Agent's own cooldown.
func restartCooldownHolding(cooldown time.Duration, lastCompletedAt int64, started, now time.Time) bool {
	if cooldown <= 0 || lastCompletedAt <= 0 || started.IsZero() {
		return false
	}
	completed := time.Unix(0, lastCompletedAt).UTC()
	if !completed.Before(started) {
		return false
	}
	return !now.After(completed.Add(cooldown))
}

func (r *policyRuntime) Status(values map[string]string) (*sdk.TargetStatus, error) {
	p, err := r.agent.Policy(values)
	if err != nil {
		return nil, err
	}
	route, err := r.getRoute(p, false)
	if err != nil {
		return nil, err
	}
	// The standard Agent calls Status while constructing a policy handler and
	// does not retain that handler when the first observation fails. Wait for
	// initialization within the caller's Status budget instead of immediately
	// rejecting every newly discovered group. Never invent Count=0 meanwhile.
	budget := 60 * time.Second
	if raw := values[shared.PluginConfigKeyGRPCTimeout]; raw != "" {
		if parsed, parseErr := time.ParseDuration(raw); parseErr == nil && parsed < budget {
			budget = parsed
		}
	}
	timer := time.NewTimer(budget)
	defer timer.Stop()
	select {
	case <-route.ready:
	case <-r.ctx.Done():
		return nil, errRuntimeClosed
	case <-timer.C:
		return nil, ErrRouteInitializing
	}
	if route.err != nil {
		return nil, route.err
	}
	state, ready, err := route.engine.Snapshot()
	// An unresolved write is a workflow state, not a failed membership read.
	// Keep the official Agent's handler alive while reconciliation obtains
	// authoritative evidence. Never mask a failed observation or journal error.
	unknown := errors.Is(err, live.ErrUnknown) && state.Pending != nil && state.Pending.Unknown
	observed := state.Cloud != nil && !state.ObservedAt.IsZero() &&
		(state.ErrorCode == "" || state.ErrorCode == "NomadMemberMappingUnavailable" || state.ErrorCode == "NomadMemberMappingInvalid" || state.ErrorCode == "DesiredCapacityConflict")
	if err != nil && !(unknown && observed) {
		return nil, err
	}
	if state.Cloud != nil && r.policyBounds != nil {
		r.policyBounds.Check(p.GroupID, state.Cloud.Group.Min, state.Cloud.Group.Max, time.Now().UTC())
	}
	meta := map[string]string{"tencent.backend": "tencent-as", "tencent.group_id": p.GroupID}
	if unknown {
		ready = false
		meta["tencent.write_outcome"] = "unknown"
		meta["tencent.error"] = live.ErrUnknown.Error()
	}
	registration := live.CountRegistration(state.Members)
	meta["tencent.registration_waiting"] = strconv.FormatUint(registration.Waiting, 10)
	meta["tencent.registration_timed_out"] = strconv.FormatUint(registration.TimedOut, 10)
	meta["tencent.registration_unknown"] = strconv.FormatUint(registration.Unknown, 10)
	if state.ErrorCode != "" {
		meta["tencent.error"] = state.ErrorCode
	}
	if state.LastEvent != "" {
		meta["tencent.event"] = state.LastEvent
	}
	if state.LastCompletedAt > 0 {
		meta[sdk.TargetStatusMetaKeyLastEvent] = strconv.FormatInt(state.LastCompletedAt, 10)
	}
	cooldown, err := targetconfig.RestartCooldown(values)
	if err != nil {
		return nil, err
	}
	if restartCooldownHolding(cooldown, state.LastCompletedAt, r.startedAt, time.Now().UTC()) {
		ready = false
		meta["tencent.restart_cooldown"] = "active"
	}
	return &sdk.TargetStatus{Count: int64(len(state.Members)), Ready: ready, Meta: meta}, nil
}

func (r *policyRuntime) Scale(action sdk.ScalingAction, values map[string]string) error {
	result := "error"
	defer func() { r.metrics.Scale(result) }()
	p, err := r.agent.Policy(values)
	if err != nil {
		return err
	}
	logger := scaleActionLogger(r.logger, action, p.GroupID)
	logger.Debug("scale request started")
	defer func() {
		message := "scale request finished"
		if result == "success" {
			message = "scale request completed"
		}
		logger.Info(message, "result", result)
	}()
	if action.Count == sdk.StrategyActionMetaValueDryRunCount || values["dry-run"] == "true" {
		result = "dry-run"
		return nil
	}
	route, err := r.getRoute(p, false)
	if err != nil {
		return err
	}
	timer := time.NewTimer(60 * time.Second)
	defer timer.Stop()
	select {
	case <-route.ready:
	case <-r.ctx.Done():
		return errRuntimeClosed
	case <-timer.C:
		return ErrRouteInitializing
	}
	if route.err != nil {
		return route.err
	}
	if !route.actionMu.TryLock() {
		return live.ErrBusy
	}
	defer route.actionMu.Unlock()
	if r.ctx.Err() != nil {
		return errRuntimeClosed
	}
	var changed bool
	if engine, ok := route.engine.(interface {
		ScaleAndWaitWithPolicy(int64, int, string) (bool, error)
	}); ok {
		id, _ := action.Meta["nomad_policy_id"].(string)
		if !agentPolicyIDPattern.MatchString(id) {
			id = ""
		}
		changed, err = engine.ScaleAndWaitWithPolicy(action.Count, r.agent.RetryAttempts, id)
	} else {
		changed, err = route.engine.ScaleAndWait(action.Count, r.agent.RetryAttempts)
	}
	if err != nil {
		return err
	}
	if !changed {
		result = "no-op"
		return sdk.NewTargetScalingNoOpError("target already at requested capacity")
	}
	result = "success"
	return nil
}

func (r *policyRuntime) Close() {
	r.closeOnce.Do(func() {
		r.mu.Lock()
		r.closing = true
		r.cancel()
		r.mu.Unlock()
		r.initializing.Wait()
		for _, route := range r.routes {
			if route.engine != nil {
				route.engine.Close()
			}
			if route.lock != nil {
				releaseRouteLock(route.lock)
			}
		}
		r.nomad.Close()
		r.cloud.Close()
		r.metricsServer.Close()
	})
}
