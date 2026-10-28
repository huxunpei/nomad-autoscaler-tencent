package plugin

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/hashicorp/nomad-autoscaler/sdk"
)

func configured(t *testing.T) (*Target, string) {
	t.Helper()
	dir := t.TempDir()
	target := &Target{}
	if err := target.SetConfig(map[string]string{"tencent_backend": "simulation", "tencent_region": "ap-guangzhou", "tencent_state_dir": dir, "tencent_simulation_group_id": "asg-sim"}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(target.Close)
	return target, dir
}

func TestDryRunBeforeTargetClampAndNoOp(t *testing.T) {
	p, dir := configured(t)
	cfg := map[string]string{"tencent_group_id": "asg-sim"}
	before, err := os.ReadFile(filepath.Join(dir, "simulation.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Scale(sdk.ScalingAction{Count: sdk.StrategyActionMetaValueDryRunCount}, cfg); err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(filepath.Join(dir, "simulation.json"))
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Fatal("dry-run mutated state")
	}
	if err := p.Scale(sdk.ScalingAction{Count: 5}, cfg); err != nil {
		t.Fatal(err)
	}
	if err := p.Scale(sdk.ScalingAction{Count: 5}, cfg); err == nil {
		t.Fatal("expected standard no-op error")
	}
	s, err := p.Status(cfg)
	if err != nil || s.Count != 5 || !s.Ready || s.Meta[sdk.TargetStatusMetaKeyLastEvent] == "" {
		t.Fatalf("unexpected status: %+v %v", s, err)
	}
}

func TestRejectLiveBackendAndWrongGroup(t *testing.T) {
	p := &Target{}
	if err := p.SetConfig(map[string]string{"tencent_backend": "live"}); err == nil {
		t.Fatal("prototype must reject live mutations")
	}
	p, _ = configured(t)
	if _, err := p.Status(map[string]string{"tencent_group_id": "asg-other"}); err == nil {
		t.Fatal("wrong group accepted")
	}
	if err := p.Scale(sdk.ScalingAction{Count: 5}, map[string]string{"tencent_group_id": "asg-other"}); err == nil {
		t.Fatal("wrong group mutation accepted")
	}
}
