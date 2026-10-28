// Package plugin exposes the Tencent target and legacy experimental backends.
package plugin

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/nomad-autoscaler/plugins/base"
	"github.com/hashicorp/nomad-autoscaler/plugins/target"
	"github.com/hashicorp/nomad-autoscaler/sdk"

	"github.com/huxunpei/nomad-autoscaler-tencent/internal/live"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadclient"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadsdk"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/simulation"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/targetconfig"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/tencentcloud"
)

const Name = "tencent-as"

type Target struct {
	runtime      *policyRuntime
	runtimeDeps  *runtimeDependencies
	mu           sync.RWMutex
	logger       hclog.Logger
	engine       *simulation.Engine
	group        string
	live         *live.Engine
	nomad        *nomadclient.Client
	protect      bool
	backend      string
	configSet    bool
	configDigest [32]byte
}

var _ target.Target = (*Target)(nil)

func Factory(logger hclog.Logger) interface{} { return &Target{logger: logger} }

func (t *Target) PluginInfo() (*base.PluginInfo, error) {
	return &base.PluginInfo{Name: Name, PluginType: sdk.PluginTypeTarget}, nil
}

func (t *Target) SetConfig(config map[string]string) (err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	digest := agentConfigDigest(config)
	if t.engine != nil || t.live != nil || t.runtime != nil {
		if t.configSet && digest == t.configDigest {
			return nil
		}
		return errors.New("prototype config reload requires plugin restart")
	}
	// The standard Agent repeats SetConfig on SIGHUP, even when the plugin's
	// configuration has not changed. Remember only a private in-memory digest
	// after successful initialization; never replace an active controller.
	defer func() {
		if err == nil {
			t.configSet, t.configDigest = true, digest
		}
	}()
	if _, explicit := config["tencent_backend"]; !explicit {
		return t.configureRuntime(config)
	}
	if config["tencent_backend"] == "live-experimental" {
		return t.configureLive(config)
	}
	if config["tencent_backend"] != "simulation" {
		return errors.New("explicit tencent_backend=simulation or live-experimental required")
	}
	if config["tencent_region"] == "" {
		return errors.New("tencent_region is required")
	}
	initial, err := integer(config, "tencent_simulation_initial", 1)
	if err != nil {
		return err
	}
	min, err := integer(config, "tencent_simulation_min", 1)
	if err != nil {
		return err
	}
	max, err := integer(config, "tencent_simulation_max", 5)
	if err != nil {
		return err
	}
	protect := false
	if raw, ok := config["scale_in_protection"]; ok {
		protect, err = strconv.ParseBool(raw)
		if err != nil {
			return errors.New("invalid scale_in_protection")
		}
	}
	interval := 100 * time.Millisecond
	if raw, ok := config["tencent_reconcile_interval"]; ok {
		interval, err = time.ParseDuration(raw)
		if err != nil || interval <= 0 {
			return errors.New("invalid tencent_reconcile_interval")
		}
	}
	fault := config["tencent_simulation_fault"]
	if fault == "" {
		fault = "none"
	}
	e, err := simulation.Open(simulation.Config{Dir: config["tencent_state_dir"], GroupID: config["tencent_simulation_group_id"], Initial: initial, Min: min, Max: max, Protect: protect, Fault: fault, ReconcileInterval: interval})
	if err != nil {
		return fmt.Errorf("simulation initialization failed: %w", err)
	}
	t.engine, t.group, t.backend = e, config["tencent_simulation_group_id"], "simulation"
	if t.logger != nil {
		t.logger.Info("simulation initialized; no cloud or Nomad API calls", "group", t.group)
	}
	return nil
}

func integer(config map[string]string, key string, fallback int64) (int64, error) {
	raw, ok := config[key]
	if !ok {
		return fallback, nil
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid %s", key)
	}
	return v, nil
}

func (t *Target) check(config map[string]string) error {
	if t.engine == nil && t.live == nil {
		return errors.New("plugin not configured")
	}
	if config["tencent_group_id"] != t.group {
		return errors.New("policy group does not match configured group")
	}
	if _, ok := config["scale_in_protection"]; ok {
		return errors.New("prototype supports scale_in_protection at Agent level only")
	}
	return nil
}

func (t *Target) Status(config map[string]string) (*sdk.TargetStatus, error) {
	t.mu.RLock()
	if t.runtime != nil {
		runtime := t.runtime
		t.mu.RUnlock()
		return runtime.Status(config)
	}
	defer t.mu.RUnlock()
	if err := t.check(config); err != nil {
		return nil, err
	}
	if t.live != nil {
		s, ready, err := t.live.Snapshot()
		if err != nil {
			return nil, err
		}
		meta := map[string]string{"tencent.backend": t.backend}
		if s.ErrorCode != "" {
			meta["tencent.error"] = s.ErrorCode
		}
		if s.LastEvent != "" {
			// Workflow phases are diagnostics, not provider event timestamps.
			meta["tencent.event"] = s.LastEvent
		}
		return &sdk.TargetStatus{Count: int64(len(s.Members)), Ready: ready, Meta: meta}, nil
	}
	s, ready, err := t.engine.Snapshot()
	if err != nil {
		return nil, err
	}
	meta := map[string]string{"tencent.backend": "simulation"}
	if s.LastEvent != "" {
		meta[sdk.TargetStatusMetaKeyLastEvent] = s.LastEvent
	}
	return &sdk.TargetStatus{Count: int64(len(s.Members)), Ready: ready, Meta: meta}, nil
}

func (t *Target) Scale(action sdk.ScalingAction, config map[string]string) error {
	t.mu.RLock()
	if t.runtime != nil {
		runtime := t.runtime
		t.mu.RUnlock()
		return runtime.Scale(action, config)
	}
	defer t.mu.RUnlock()
	if err := t.check(config); err != nil {
		return err
	}
	// The standard Agent still invokes Scale after marking a dry-run action.
	// Check the sentinel before clamping the requested capacity.
	if action.Count == sdk.StrategyActionMetaValueDryRunCount {
		if t.logger != nil {
			if t.live != nil {
				t.logger.Info("dry-run skipped live changes")
			} else {
				t.logger.Info("dry-run skipped simulation changes")
			}
		}
		return nil
	}
	var changed bool
	var err error
	if t.live != nil {
		if t.protect {
			s, _, err := t.live.Snapshot()
			if err != nil {
				return err
			}
			if action.Count < int64(len(s.Members)) {
				return errors.New("live scale-in protection enabled")
			}
		}
		changed, err = t.live.Scale(action.Count)
	} else {
		changed, err = t.engine.Scale(action.Count)
	}
	if err != nil {
		return err
	}
	if !changed {
		return sdk.NewTargetScalingNoOpError("target already at requested capacity")
	}
	if t.logger != nil {
		if t.live != nil {
			t.logger.Info("scale request accepted", "target", action.Count)
		} else {
			t.logger.Info("simulation scale complete", "target", action.Count)
		}
	}
	return nil
}

// Close exists for in-process tests. The OS releases the file lock on process exit.
func (t *Target) Close() {
	t.mu.Lock()
	if t.runtime != nil {
		runtime := t.runtime
		t.runtime = nil
		t.mu.Unlock()
		runtime.Close()
		return
	}
	defer t.mu.Unlock()
	if t.engine != nil {
		t.engine.Close()
		t.engine = nil
	}
	if t.live != nil {
		t.live.Close()
		t.live = nil
	}
	if t.nomad != nil {
		t.nomad.Close()
		t.nomad = nil
	}
}

// Live mode is an opt-in, capacity-bounded lab target. Credential values are
// for Tencent are read only from the scoped environment. Nomad connection
// settings may arrive through the standard Agent inheritance mechanism.
func (t *Target) configureLive(config map[string]string) error {
	group := config["tencent_group_id"]
	if group == "" || config["tencent_confirm_exclusive_group"] != group {
		return errors.New("live mode requires exact exclusive group confirmation")
	}
	for _, key := range []string{"secret_id", "secret_key", "tencent_secret_id", "tencent_secret_key", "tencent_token"} {
		if _, ok := config[key]; ok {
			return errors.New("credential values are forbidden in plugin configuration; use scoped environment")
		}
	}
	id, key := os.Getenv("TENCENT_AS_SECRET_ID"), os.Getenv("TENCENT_AS_SECRET_KEY")
	if id == "" || key == "" {
		return errors.New("explicit TENCENT_AS_SECRET_ID and TENCENT_AS_SECRET_KEY are required")
	}
	interval := 5 * time.Second
	if raw := config["tencent_reconcile_interval"]; raw != "" {
		v, err := time.ParseDuration(raw)
		if err != nil || v < time.Second || v > time.Minute {
			return errors.New("invalid live reconcile interval")
		}
		interval = v
	}
	deadline := 2 * time.Minute
	if raw := config["tencent_drain_deadline"]; raw != "" {
		v, err := time.ParseDuration(raw)
		if err != nil || v < time.Second || v > time.Hour {
			return errors.New("invalid live drain deadline")
		}
		deadline = v
	}
	protect := false
	if raw := config["scale_in_protection"]; raw != "" {
		v, err := strconv.ParseBool(raw)
		if err != nil {
			return errors.New("invalid scale_in_protection")
		}
		protect = v
	}
	capacity, err := integer(config, "tencent_capacity_limit", 1)
	if err != nil || capacity < 1 || capacity > live.MaxSupportedCapacity {
		return errors.New("invalid tencent_capacity_limit")
	}
	nomadConfig, err := nomadclient.ConfigFromMap(config, os.Getenv)
	if err != nil {
		return err
	}
	nc, err := nomadclient.New(nomadConfig)
	if err != nil {
		return err
	}
	scoped, err := nc.WithScope(group, config["tencent_region"], "cloud_instance_id")
	if err != nil {
		nc.Close()
		return err
	}
	policy, err := (targetconfig.Agent{}).Policy(map[string]string{"tencent_group_id": group, "node_drain_deadline": deadline.String()})
	if err != nil {
		nc.Close()
		return err
	}
	adapter := nomadsdk.New(scoped, policy)
	cloud, err := tencentcloud.New(tencentcloud.Config{Region: config["tencent_region"], SecretID: id, SecretKey: key, Token: os.Getenv("TENCENT_AS_TOKEN"), Timeout: 10 * time.Second})
	if err != nil {
		nc.Close()
		return err
	}
	e, err := live.Open(live.Config{MaxCapacity: capacity, ScaleInProtection: protect, Dir: config["tencent_state_dir"], GroupID: group, Region: config["tencent_region"], ReconcileInterval: interval, DrainDeadline: deadline}, cloud, adapter)
	if err != nil {
		nc.Close()
		return err
	}
	t.live, t.nomad, t.group, t.backend, t.protect = e, nc, group, "live-experimental", protect
	if t.logger != nil {
		t.logger.Info("experimental live target initialized", "group", group)
	}
	return nil
}
