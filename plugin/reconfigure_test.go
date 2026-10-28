package plugin

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/nomad-autoscaler/sdk"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/live"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/tencentcloud"
)

func TestRepeatedAgentConfigPreservesInFlightAction(t *testing.T) {
	var authCalls, engineCalls atomic.Int32
	c := &routingCloud{account: "10001", region: "ap-guangzhou"}
	entered, release := make(chan struct{}, 1), make(chan struct{})
	var unblock sync.Once
	defer unblock.Do(func() { close(release) })
	var engine *routingEngine
	p := &Target{runtimeDeps: &runtimeDependencies{
		openCloud: func(context.Context, tencentcloud.AuthConfig) (accountCloud, error) {
			authCalls.Add(1)
			return c, nil
		},
		openEngine: func(cfg live.Config, _ live.Cloud, _ live.Nomad) (groupController, error) {
			engineCalls.Add(1)
			engine = &routingEngine{cfg: cfg, entered: entered, block: release}
			return engine, nil
		},
	}}
	t.Cleanup(p.Close)
	cfg := map[string]string{"tencent_region": c.region, "tencent_state_dir": t.TempDir()}
	if err := p.SetConfig(cfg); err != nil {
		t.Fatal(err)
	}
	policy := map[string]string{"tencent_group_id": "asg-reload"}
	readyRoute(t, p, policy)
	original := p.runtime
	done := make(chan error, 1)
	go func() { done <- p.Scale(sdk.ScalingAction{Count: 1}, policy) }()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("action not entered")
	}
	// Concurrent Agent reloads must not reopen the group, drop its lock or
	// cancel the already executing action. Use independently allocated maps.
	reloads := make(chan error, 8)
	for i := 0; i < cap(reloads); i++ {
		go func() {
			reloads <- p.SetConfig(map[string]string{"tencent_state_dir": cfg["tencent_state_dir"], "tencent_region": c.region})
		}()
	}
	for i := 0; i < cap(reloads); i++ {
		select {
		case err := <-reloads:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("unchanged reload blocked behind Scale")
		}
	}
	if p.runtime != original || engine.closed.Load() || c.closed.Load() || authCalls.Load() != 1 || engineCalls.Load() != 1 {
		t.Fatal("unchanged configuration replaced the active controller")
	}
	unblock.Do(func() { close(release) })
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("action failed to complete after reload")
	}
	if readyRoute(t, p, policy).Count != 1 || engine.calls.Load() != 1 || c.writes.Load() != 0 {
		t.Fatal("action was lost or replayed")
	}
}

func TestRepeatedAgentConfigRejectsChangesWithoutReplacingRuntime(t *testing.T) {
	cfg := map[string]string{"tencent_region": "ap-guangzhou", "tencent_state_dir": t.TempDir()}
	c := &routingCloud{account: "10001", region: "ap-guangzhou"}
	p := routeTarget(t, cfg["tencent_state_dir"], c, nil)
	original := p.runtime
	for _, key := range []string{"tencent_region", "retry_attempts", "nomad_token", "unknown"} {
		t.Run(key, func(t *testing.T) {
			changed := map[string]string{}
			for k, v := range cfg {
				changed[k] = v
			}
			changed[key] = "SYNTHETIC_PRIVATE_VALUE"
			if err := p.SetConfig(changed); err == nil || strings.Contains(err.Error(), changed[key]) {
				t.Fatal("configuration change accepted or exposed")
			}
			if p.runtime != original || c.closed.Load() {
				t.Fatal("failed reload disrupted existing runtime")
			}
			if err := p.SetConfig(cfg); err != nil {
				t.Fatal("rejected change poisoned subsequent identical config", err)
			}
		})
	}
	// Mutating the original map after initial configuration must not update
	// the accepted configuration identity by aliasing the caller's map.
	cfg["retry_attempts"] = "16"
	if err := p.SetConfig(cfg); err == nil {
		t.Fatal("mutated map accepted")
	}
}

func TestFailedInitialConfigCanRetryIdenticalValues(t *testing.T) {
	var calls int
	p := &Target{runtimeDeps: &runtimeDependencies{openCloud: func(context.Context, tencentcloud.AuthConfig) (accountCloud, error) {
		calls++
		if calls == 1 {
			return nil, errors.New("SyntheticInitialAuthenticationFailure")
		}
		return &routingCloud{account: "10001", region: "ap-guangzhou"}, nil
	}}}
	t.Cleanup(p.Close)
	cfg := map[string]string{"tencent_region": "ap-guangzhou", "tencent_state_dir": t.TempDir()}
	if p.SetConfig(cfg) == nil {
		t.Fatal("initial authentication unexpectedly succeeded")
	}
	if err := p.SetConfig(cfg); err != nil {
		t.Fatal(err)
	}
	if err := p.SetConfig(cfg); err != nil || calls != 2 || p.runtime == nil {
		t.Fatal("identical retry skipped initialization or reopened configured client")
	}
	cfg["retry_attempts"] = "16"
	if p.SetConfig(cfg) == nil {
		t.Fatal("caller map mutation changed accepted configuration")
	}
}
