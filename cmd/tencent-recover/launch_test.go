package main

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/live"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadsdk"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/targetconfig"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/tencentcloud"
	"github.com/huxunpei/nomad-autoscaler-tencent/plugin"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLaunchConfigurationCommandHelp(t *testing.T) {
	for _, command := range []string{"lc-preview", "lc-apply"} {
		var out bytes.Buffer
		if run([]string{command, "--help"}, &out, nil) != 0 || !strings.Contains(out.String(), "state-dir") {
			t.Fatalf("%s has no launch configuration acceptance entrypoint: %s", command, out.String())
		}
	}
}

type launchCLICloud struct{ *recordedCloud }

func (c launchCLICloud) AccountID() string { return "10001" }
func (c launchCLICloud) Region() string    { return c.s.Region }
func TestLaunchConfigurationCLIPlanApply(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "accounts", "10001", "ap-guangzhou", "asg-test")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	policy, err := (targetconfig.Agent{Region: "ap-guangzhou"}).Policy(map[string]string{"tencent_group_id": "asg-test"})
	if err != nil {
		t.Fatal(err)
	}
	lc := tencentcloud.LaunchConfiguration{ID: "asc-old", Status: "NORMAL", ChargeType: "POSTPAID_BY_HOUR", InstanceType: "S5.SMALL2", ImageID: "img-test", VersionNumber: 1}
	state := live.State{Version: 3, GroupID: "asg-test", Region: "ap-guangzhou", DrainDeadlineNanoseconds: int64(policy.DrainDeadline), Members: map[string]live.Member{}, LaunchConfiguration: &lc, NomadInstanceIDMetaKey: policy.InstanceIDMetaKey, NomadPolicySHA256: nomadsdk.New(nil, policy).PolicyDigest()}
	for name, v := range map[string]any{"live.json": state, "route.json": map[string]any{"version": 2, "account_id": "10001", "region": "ap-guangzhou", "policy": policy}} {
		raw, _ := json.Marshal(v)
		if err := os.WriteFile(filepath.Join(dir, name), raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
	next := lc
	next.ID = "asc-next"
	c := launchCLICloud{&recordedCloud{s: tencentcloud.Snapshot{Region: "ap-guangzhou", Group: tencentcloud.Group{ID: "asg-test", Min: 0, Max: 5, Desired: 0, Count: 0, Enabled: "ENABLED", Status: "NORMAL", Activity: "NOT_IN_ACTIVITY", RetryPolicy: "IMMEDIATE_RETRY", AllocationPolicy: "LAUNCH_CONFIGURATION", LaunchConfiguration: "asc-next"}, LaunchConfiguration: next, Members: []tencentcloud.Member{}, VMs: map[string]tencentcloud.VM{}, Policies: []tencentcloud.Policy{}}}}
	calls := 0
	factory := func(context.Context, string, string, string) (plugin.LaunchConfigurationCloud, live.RecoveryNomad, func(), error) {
		calls++
		return c, emptyNomad{}, func() {}, nil
	}
	plan := filepath.Join(t.TempDir(), "plan.json")
	var out bytes.Buffer
	args := []string{"lc-preview", "--state-dir", dir, "--region", "ap-guangzhou", "--plan", plan, "--operator", "tester", "--reference", "change/test"}
	if runLaunch(args, &out, factory) != 0 {
		t.Fatal(out.String())
	}
	if !strings.Contains(out.String(), "asc-old") || !strings.Contains(out.String(), "asc-next") {
		t.Fatal("preview missing before/after", out.String())
	}
	p, err := live.ReadRecoveryPlan(plan)
	if err != nil {
		t.Fatal(err)
	}
	args = []string{"lc-apply", "--state-dir", dir, "--region", "ap-guangzhou", "--plan", plan, "--confirm", p.Digest}
	out.Reset()
	if runLaunch(args, &out, factory) != 0 {
		t.Fatal(out.String())
	}
	raw, _ := os.ReadFile(filepath.Join(dir, "live.json"))
	if err = json.Unmarshal(raw, &state); err != nil || state.LaunchConfiguration.ID != "asc-next" || state.RecoveryAudit == nil || calls != 2 {
		t.Fatal("CLI did not commit acceptance")
	}
	out.Reset()
	if runLaunch(args, &out, factory) == 0 {
		t.Fatal("stale plan replay accepted")
	}
}
