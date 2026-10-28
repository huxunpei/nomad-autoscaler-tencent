package plugin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/nomad-autoscaler/plugins/shared"
	"github.com/hashicorp/nomad-autoscaler/sdk"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/live"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadclient"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadsdk"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/targetconfig"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/tencentcloud"
)

type routingCloud struct {
	account, region string
	reads           atomic.Int32
	writes          atomic.Int32
	closed          atomic.Bool
	blockGroup      string
	block           <-chan struct{}
}

func (c *routingCloud) AccountID() string { return c.account }
func (c *routingCloud) Region() string    { return c.region }
func (c *routingCloud) Close()            { c.closed.Store(true) }
func (c *routingCloud) Observe(ctx context.Context, g, i string) (tencentcloud.Snapshot, error) {
	c.reads.Add(1)
	if g == c.blockGroup {
		select {
		case <-ctx.Done():
			return tencentcloud.Snapshot{}, ctx.Err()
		case <-c.block:
		}
	}
	return tencentcloud.Snapshot{At: time.Now().UTC(), Region: c.region, Group: tencentcloud.Group{ID: g, Min: 0, Max: 2, Desired: 0, Count: 0, Enabled: "ENABLED", Status: "NORMAL", Activity: "NOT_IN_ACTIVITY", RetryPolicy: "NO_RETRY", LaunchConfiguration: "asc-test"}, Members: []tencentcloud.Member{}, VMs: map[string]tencentcloud.VM{}, Policies: []tencentcloud.Policy{}, LaunchConfiguration: tencentcloud.LaunchConfiguration{ID: "asc-test", Status: "NORMAL", ChargeType: "POSTPAID_BY_HOUR", InstanceType: "S5.SMALL2", ImageID: "img-test", VersionNumber: 1}}, nil
}
func (c *routingCloud) SetDesired(context.Context, string, int64) (tencentcloud.WriteResult, error) {
	c.writes.Add(1)
	return tencentcloud.WriteResult{}, errors.New("synthetic cloud write")
}
func (c *routingCloud) RemoveExact(context.Context, string, string) (tencentcloud.WriteResult, error) {
	c.writes.Add(1)
	return tencentcloud.WriteResult{}, errors.New("synthetic cloud write")
}
func (c *routingCloud) Activity(context.Context, string, string) (tencentcloud.Activity, error) {
	return tencentcloud.Activity{}, errors.New("synthetic missing activity")
}

type routingEngine struct {
	cfg     live.Config
	mu      sync.Mutex
	count   int64
	calls   atomic.Int32
	enabled atomic.Bool
	closed  atomic.Bool
	entered chan struct{}
	block   <-chan struct{}
}

func (e *routingEngine) Snapshot() (live.State, bool, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	s := live.State{GroupID: e.cfg.GroupID, Region: e.cfg.Region, Members: map[string]live.Member{}}
	for i := int64(0); i < e.count; i++ {
		s.Members[fmt.Sprint(i)] = live.Member{}
	}
	return s, true, nil
}
func (e *routingEngine) Scale(n int64) (bool, error) {
	e.calls.Add(1)
	if e.entered != nil {
		select {
		case e.entered <- struct{}{}:
		default:
		}
	}
	if e.block != nil {
		select {
		case <-e.cfg.Context.Done():
			return false, errors.New("closed")
		case <-e.block:
		}
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	changed := e.count != n
	e.count = n
	return changed, nil
}
func (e *routingEngine) ScaleAndWait(n int64, _ int) (bool, error) { return e.Scale(n) }
func (e *routingEngine) EnableReconciliation()                     { e.enabled.Store(true) }
func (e *routingEngine) Close()                                    { e.closed.Store(true) }

func routeTarget(t *testing.T, root string, c *routingCloud, open func(live.Config, live.Cloud, live.Nomad) (groupController, error)) *Target {
	t.Helper()
	p := &Target{runtimeDeps: &runtimeDependencies{openCloud: func(ctx context.Context, cfg tencentcloud.AuthConfig) (accountCloud, error) {
		if cfg.Region != c.region {
			t.Error("region changed before authentication")
		}
		return c, nil
	}, openEngine: open}}
	if err := p.SetConfig(map[string]string{"tencent_region": c.region, "tencent_state_dir": root}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	return p
}
func readyRoute(t *testing.T, p *Target, cfg map[string]string) *sdk.TargetStatus {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		s, err := p.Status(cfg)
		if err == nil {
			return s
		}
		if err != ErrRouteInitializing {
			t.Fatal(err)
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("route did not initialize")
	return nil
}

func TestConfigValidatesBeforeCreatingRoutes(t *testing.T) {
	calls := 0
	p := &Target{runtimeDeps: &runtimeDependencies{openCloud: func(context.Context, tencentcloud.AuthConfig) (accountCloud, error) {
		calls++
		return nil, errors.New("SyntheticCredentialRejected")
	}}}
	if err := p.SetConfig(map[string]string{"tencent_region": "ap-guangzhou", "tencent_state_dir": t.TempDir()}); err == nil || calls != 1 || p.runtime != nil {
		t.Fatal("failed credentials retained a configured target")
	}
	for _, extra := range []map[string]string{{"tencent_group_id": "asg-test"}, {"tencent_secret_key": "PRIVATE"}, {"tencent_state_dir": "relative"}, {"nomad_ca-cert": "/synthetic/missing.pem"}} {
		cfg := map[string]string{"tencent_region": "ap-guangzhou", "tencent_state_dir": t.TempDir()}
		for k, v := range extra {
			cfg[k] = v
		}
		if err := p.SetConfig(cfg); err == nil || strings.Contains(err.Error(), "PRIVATE") || calls != 1 {
			t.Fatal("bad local config reached authentication")
		}
	}
}

func TestPolicyRoutingForwardsOptionsAndCreatesOneController(t *testing.T) {
	root := t.TempDir()
	c := &routingCloud{account: "10001", region: "ap-guangzhou"}
	var opens atomic.Int32
	input := map[string]string{"tencent_group_id": "asg-one", "node_class": "game-server", "datacenter": "dc1", "node_pool": "pool1", "node_selector_strategy": "oldest_create_index", "node_drain_deadline": "10m", "node_drain_ignore_system_jobs": "true", "node_purge": "true", "instance_id_meta_key": "cvm_id", "scale_in_protection": "true"}
	expected, _ := (targetconfig.Agent{}).Policy(input)
	p := routeTarget(t, root, c, func(cfg live.Config, cloud live.Cloud, n live.Nomad) (groupController, error) {
		opens.Add(1)
		a, ok := n.(*nomadsdk.Adapter)
		if !ok {
			t.Error("runtime did not supply actual SDK adapter")
			return nil, errors.New("SDK adapter missing")
		}
		nc, _ := nomadclient.New(nomadclient.Config{Address: "http://127.0.0.1:4646"})
		defer nc.Close()
		if cfg.GroupID != "asg-one" || cfg.Region != c.region || cfg.DrainDeadline != 10*time.Minute || !cfg.ScaleInProtection || a.InstanceMetaKey() != "cvm_id" || a.PolicyDigest() != nomadsdk.New(nc, expected).PolicyDigest() {
			t.Error("policy was not forwarded to engine/SDK")
		}
		if !strings.HasPrefix(cfg.Dir, filepath.Join(root, "accounts", c.account, c.region)+string(filepath.Separator)) {
			t.Error("state directory was not scoped to verified account")
		}
		return &routingEngine{cfg: cfg}, nil
	})
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _, _ = p.Status(input) }()
	}
	wg.Wait()
	if readyRoute(t, p, input).Count != 0 || opens.Load() != 1 {
		t.Fatal("same group initialized more than once")
	}
	if err := p.Scale(sdk.ScalingAction{Count: 1}, input); err != nil {
		t.Fatal(err)
	}
	if readyRoute(t, p, input).Count != 1 {
		t.Fatal("Scale result routed to wrong controller")
	}
	input["node_class"] = "different"
	if _, err := p.Status(input); err != ErrRoutePolicyChanged {
		t.Fatal("changed policy reinterpreted persistent controller")
	}
}

func TestPolicyRoutingGroupIsolationAndDryRun(t *testing.T) {
	c := &routingCloud{account: "10001", region: "ap-guangzhou"}
	root := t.TempDir()
	entered := make(chan struct{}, 1)
	block := make(chan struct{})
	var mu sync.Mutex
	engines := map[string]*routingEngine{}
	p := routeTarget(t, root, c, func(cfg live.Config, _ live.Cloud, _ live.Nomad) (groupController, error) {
		e := &routingEngine{cfg: cfg}
		if cfg.GroupID == "asg-one" {
			e.block = block
			e.entered = entered
		}
		mu.Lock()
		engines[cfg.GroupID] = e
		mu.Unlock()
		return e, nil
	})
	if err := p.Scale(sdk.ScalingAction{Count: sdk.StrategyActionMetaValueDryRunCount}, map[string]string{"tencent_group_id": "asg-never"}); err != nil {
		t.Fatal(err)
	}
	if c.reads.Load() != 0 {
		t.Fatal("dry run initialized or observed cloud group")
	}
	a := map[string]string{"tencent_group_id": "asg-one"}
	b := map[string]string{"tencent_group_id": "asg-two"}
	readyRoute(t, p, a)
	readyRoute(t, p, b)
	finished := make(chan error, 1)
	go func() { finished <- p.Scale(sdk.ScalingAction{Count: 1}, a) }()
	<-entered
	done := make(chan error, 1)
	go func() { done <- p.Scale(sdk.ScalingAction{Count: 2}, b) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("group A blocked group B")
	}
	if readyRoute(t, p, b).Count != 2 {
		t.Fatal("group state crossed routes")
	}
	same := make(chan error, 1)
	go func() { same <- p.Scale(sdk.ScalingAction{Count: 0}, a) }()
	select {
	case err := <-same:
		if !errors.Is(err, live.ErrBusy) {
			t.Fatalf("concurrent same-group action: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("same-group request waited behind the running action")
	}
	if readyRoute(t, p, a).Count != 0 {
		t.Fatal("Status blocked or observed another group")
	}
	close(block)
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "accounts", "10001", c.region, "asg-never")); !os.IsNotExist(err) {
		t.Fatal("dry run persisted route")
	}
}

func TestDryRunConfigSuppressesActionsAndPreservesRoute(t *testing.T) {
	c := &routingCloud{account: "10001", region: "ap-guangzhou"}
	var opens atomic.Int32
	var controller *routingEngine
	p := routeTarget(t, t.TempDir(), c, func(cfg live.Config, _ live.Cloud, _ live.Nomad) (groupController, error) {
		opens.Add(1)
		controller = &routingEngine{cfg: cfg}
		return controller, nil
	})
	input := map[string]string{"tencent_group_id": "asg-one", "dry-run": "true"}
	for _, count := range []int64{sdk.StrategyActionMetaValueDryRunCount, 5} {
		if err := p.Scale(sdk.ScalingAction{Count: count}, input); err != nil {
			t.Fatal(err)
		}
	}
	if opens.Load() != 0 || c.reads.Load() != 0 || c.writes.Load() != 0 {
		t.Fatal("dry-run Scale initialized a route or reached the cloud")
	}
	if s := readyRoute(t, p, input); s.Count != 0 || !s.Ready {
		t.Fatal("dry-run hid real target status")
	}
	if err := p.Scale(sdk.ScalingAction{Count: 5}, input); err != nil || controller.calls.Load() != 0 {
		t.Fatal("dry-run invoked an existing controller")
	}
	input["dry-run"] = "false"
	if err := p.Scale(sdk.ScalingAction{Count: 1}, input); err != nil {
		t.Fatal(err)
	}
	if s := readyRoute(t, p, input); s.Count != 1 || opens.Load() != 1 || controller.calls.Load() != 1 {
		t.Fatal("explicit false did not reuse the original route for a normal action")
	}
	input["dry-run"] = "true"
	if err := p.Scale(sdk.ScalingAction{Count: 0}, input); err != nil {
		t.Fatal(err)
	}
	if s := readyRoute(t, p, input); s.Count != 1 || opens.Load() != 1 || controller.calls.Load() != 1 || c.writes.Load() != 0 {
		t.Fatal("dry-run transition changed capacity or persistent route identity")
	}
}

func TestPolicyRoutingDuplicateOwnerAndRestartRecovery(t *testing.T) {
	root := t.TempDir()
	a := map[string]string{"tencent_group_id": "asg-one", "node_purge": "true"}
	makeOpen := func(cfg live.Config, _ live.Cloud, _ live.Nomad) (groupController, error) {
		return &routingEngine{cfg: cfg}, nil
	}
	p := routeTarget(t, root, &routingCloud{account: "10001", region: "ap-guangzhou"}, makeOpen)
	readyRoute(t, p, a)
	q := routeTarget(t, root, &routingCloud{account: "10001", region: "ap-guangzhou"}, makeOpen)
	deadline := time.Now().Add(time.Second)
	var err error
	for time.Now().Before(deadline) {
		_, err = q.Status(a)
		if err != ErrRouteInitializing {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if err == nil || err == ErrRouteInitializing {
		t.Fatal("second controller was not rejected")
	}
	q.Close()
	p.Close()
	started := make(chan *routingEngine, 1)
	r := routeTarget(t, root, &routingCloud{account: "10001", region: "ap-guangzhou"}, func(cfg live.Config, _ live.Cloud, _ live.Nomad) (groupController, error) {
		e := &routingEngine{cfg: cfg}
		started <- e
		return e, nil
	})
	select {
	case engine := <-started:
		if engine.cfg.DisableReconciliation {
			t.Fatal("startup route did not enable recovery")
		}
	case <-time.After(time.Second):
		t.Fatal("restart needed Status to discover recorded group")
	}
	readyRoute(t, r, a)
	// Another verified account can use the same group string without its state.
	other := routeTarget(t, root, &routingCloud{account: "20002", region: "ap-guangzhou"}, makeOpen)
	readyRoute(t, other, a)
	if _, err := os.Stat(filepath.Join(root, "accounts", "20002", "ap-guangzhou", "asg-one", "route.json")); err != nil {
		t.Fatal(err)
	}
}

func TestPolicyRoutingCloseCancelsInitialization(t *testing.T) {
	block := make(chan struct{})
	c := &routingCloud{account: "10001", region: "ap-guangzhou", blockGroup: "asg-one", block: block}
	p := routeTarget(t, t.TempDir(), c, func(cfg live.Config, _ live.Cloud, _ live.Nomad) (groupController, error) {
		return &routingEngine{cfg: cfg}, nil
	})
	if _, err := p.Status(map[string]string{"tencent_group_id": "asg-one", shared.PluginConfigKeyGRPCTimeout: "10ms"}); err != ErrRouteInitializing {
		t.Fatal(err)
	}
	closed := make(chan struct{})
	go func() { p.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("Close did not cancel group initialization")
	}
	if !c.closed.Load() {
		t.Fatal("authentication client was retained after close")
	}
}

// A failed startup attempt must retain its permission to reconcile an existing
// journal. A later Status may retry initialization, but must not demote it to a
// fresh observation-only route and leave pending work permanently stalled.
func TestPolicyRoutingStartupRetryRetainsRecovery(t *testing.T) {
	root := t.TempDir()
	policy := map[string]string{"tencent_group_id": "asg-one"}
	newCloud := func() *routingCloud { return &routingCloud{account: "10001", region: "ap-guangzhou"} }
	first := routeTarget(t, root, newCloud(), func(cfg live.Config, _ live.Cloud, _ live.Nomad) (groupController, error) {
		return &routingEngine{cfg: cfg}, nil
	})
	readyRoute(t, first, policy)
	first.Close()
	var calls atomic.Int32
	var secondReadOnly atomic.Bool
	restart := routeTarget(t, root, newCloud(), func(cfg live.Config, _ live.Cloud, _ live.Nomad) (groupController, error) {
		if calls.Add(1) == 1 {
			return nil, errors.New("temporary initialization failure")
		}
		secondReadOnly.Store(cfg.DisableReconciliation)
		return &routingEngine{cfg: cfg}, nil
	})
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := restart.Status(policy); err == nil {
			if calls.Load() != 2 || secondReadOnly.Load() {
				t.Fatal("startup recovery was lost after temporary initialization failure")
			}
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("startup route did not recover after retry")
}

func TestPolicyRoutingUsesRealEngineAndRestartsWithoutWrites(t *testing.T) {
	root := t.TempDir()
	cfg := map[string]string{"tencent_group_id": "asg-real"}
	cloud := &routingCloud{account: "10001", region: "ap-guangzhou"}
	target := routeTarget(t, root, cloud, nil)
	status := readyRoute(t, target, cfg)
	if !status.Ready || status.Count != 0 {
		t.Fatal("real engine did not publish empty cloud group")
	}
	if err := target.Scale(sdk.ScalingAction{Count: 0}, cfg); err == nil || err.Error() != sdk.NewTargetScalingNoOpError("target already at requested capacity").Error() {
		t.Fatalf("unexpected no-op result: %v", err)
	}
	dir := filepath.Join(root, "accounts", cloud.account, cloud.region, "asg-real")
	data, err := os.ReadFile(filepath.Join(dir, "live.json"))
	if err != nil {
		t.Fatal(err)
	}
	var state live.State
	if json.Unmarshal(data, &state) != nil || state.GroupID != "asg-real" || state.Pending != nil || state.Batch != nil {
		t.Fatal("actual journal has wrong identity or fabricated operation")
	}
	record, exists, err := readRouteRecord(dir)
	if err != nil || !exists || record.Policy.InstanceIDMetaKey != "cloud_instance_id" {
		t.Fatal("policy descriptor missing")
	}
	if cloud.writes.Load() != 0 {
		t.Fatal("Status or no-op made cloud writes")
	}
	target.Close()
	nextCloud := &routingCloud{account: "10001", region: "ap-guangzhou"}
	restarted := routeTarget(t, root, nextCloud, nil)
	status = readyRoute(t, restarted, cfg)
	if !status.Ready || status.Count != 0 || nextCloud.writes.Load() != 0 {
		t.Fatal("real restart changed cloud or lost readiness")
	}
}

func TestPolicyRoutingRejectsInvalidRecordsBeforeStartingControllers(t *testing.T) {
	for _, mode := range []string{"account", "region", "group", "selector", "symlink"} {
		t.Run(mode, func(t *testing.T) {
			root := t.TempDir()
			account := "10001"
			region := "ap-guangzhou"
			makeRecord := func(group string) routeRecord {
				p, err := (targetconfig.Agent{}).Policy(map[string]string{"tencent_group_id": group})
				if err != nil {
					t.Fatal(err)
				}
				return routeRecord{Version: 1, Account: account, Region: region, Policy: p, MaxCapacity: 2}
			}
			goodDir := filepath.Join(root, "accounts", account, region, "asg-good")
			badDir := filepath.Join(root, "accounts", account, region, "asg-zbad")
			for _, dir := range []string{goodDir, badDir} {
				if err := os.MkdirAll(dir, 0700); err != nil {
					t.Fatal(err)
				}
			}
			if err := writeRouteRecord(goodDir, makeRecord("asg-good")); err != nil {
				t.Fatal(err)
			}
			record := makeRecord("asg-zbad")
			switch mode {
			case "account":
				record.Account = "20002"
			case "region":
				record.Region = "ap-shanghai"
			case "group":
				record.Policy.GroupID = "asg-wrong"
			case "selector":
				record.Policy.Selector = "unknown"
			}
			if mode == "symlink" {
				if err := os.Symlink(filepath.Join(goodDir, "route.json"), filepath.Join(badDir, "route.json")); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := writeRouteRecord(badDir, record); err != nil {
					t.Fatal(err)
				}
			}
			cloud := &routingCloud{account: account, region: region}
			var opened atomic.Int32
			p := &Target{runtimeDeps: &runtimeDependencies{openCloud: func(context.Context, tencentcloud.AuthConfig) (accountCloud, error) { return cloud, nil }, openEngine: func(cfg live.Config, _ live.Cloud, _ live.Nomad) (groupController, error) {
				opened.Add(1)
				return &routingEngine{cfg: cfg}, nil
			}}}
			t.Cleanup(p.Close)
			if err := p.SetConfig(map[string]string{"tencent_region": region, "tencent_state_dir": root}); err == nil {
				t.Fatal("invalid saved route accepted")
			}
			if p.runtime != nil || opened.Load() != 0 || cloud.reads.Load() != 0 || !cloud.closed.Load() {
				t.Fatal("recovery started before all route identities were validated")
			}
		})
	}
}

func TestPolicyRoutingDoesNotAdoptJournalWithoutPolicy(t *testing.T) {
	root := t.TempDir()
	cloud := &routingCloud{account: "10001", region: "ap-guangzhou"}
	dir := filepath.Join(root, "accounts", cloud.account, cloud.region, "asg-orphan")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	marker := []byte("unowned journal: keep unchanged")
	if err := os.WriteFile(filepath.Join(dir, "live.json"), marker, 0600); err != nil {
		t.Fatal(err)
	}
	p := routeTarget(t, root, cloud, nil)
	deadline := time.Now().Add(time.Second)
	var got error
	for time.Now().Before(deadline) {
		_, got = p.Status(map[string]string{"tencent_group_id": "asg-orphan"})
		if got != ErrRouteInitializing {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if got == nil || got.Error() != "MissingRouteRecordForJournal" {
		t.Fatalf("unowned journal: %v", got)
	}
	data, err := os.ReadFile(filepath.Join(dir, "live.json"))
	if err != nil || string(data) != string(marker) || cloud.reads.Load() != 0 {
		t.Fatal("unowned journal was changed or used for cloud activity")
	}
}

func TestPolicyRoutingChoosesGroupContract(t *testing.T) {
	p := routeTarget(t, t.TempDir(), &routingCloud{account: "10001", region: "ap-guangzhou"}, func(cfg live.Config, _ live.Cloud, _ live.Nomad) (groupController, error) {
		if !cfg.GroupContract {
			t.Error("default target still uses the legacy cloud contract")
		}
		return &routingEngine{cfg: cfg}, nil
	})
	readyRoute(t, p, map[string]string{"tencent_group_id": "asg-one"})
}
