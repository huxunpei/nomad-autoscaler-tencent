package plugin

import (
	"context"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/hashicorp/nomad-autoscaler/sdk"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/live"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadclient"
)

type eventNomadFake struct{ statusNomadFake }

func (*eventNomadFake) ResolveMembers(context.Context, string, string, []string) ([]nomadclient.Node, error) {
	return []nomadclient.Node{{Identity: nomadclient.Identity{NodeID: "11111111-1111-1111-1111-111111111111", InstanceID: "ins-status1", GroupID: "asg-status1", Region: "ap-chengdu"}, Status: "ready", Eligibility: "eligible"}}, nil
}
func (*eventNomadFake) StartOwnedDrain(context.Context, nomadclient.Identity, string, time.Duration) (nomadclient.MutationResult, error) {
	return nomadclient.MutationResult{WriteAttempted: true, NodeModifyIndex: 1}, nil
}

func TestLiveStatusEventMetadataContract(t *testing.T) {
	engine, err := live.Open(live.Config{Dir: filepath.Join(t.TempDir(), "live"), GroupID: "asg-status1", Region: "ap-chengdu", ReconcileInterval: time.Hour, DrainDeadline: time.Minute}, &statusCloudFake{}, &eventNomadFake{})
	if err != nil {
		t.Fatal(err)
	}
	target := &Target{live: engine, group: "asg-status1", backend: "live-experimental"}
	defer target.Close()
	cfg := map[string]string{"tencent_group_id": "asg-status1"}
	if err := target.Scale(sdk.ScalingAction{Count: 0}, cfg); err != nil {
		t.Fatal(err)
	}
	status, err := target.Status(cfg)
	if err != nil {
		t.Fatal(err)
	}
	// The official Agent parses this optional provider-event timestamp for cooldown.
	if raw, exists := status.Meta[sdk.TargetStatusMetaKeyLastEvent]; exists {
		if _, err := strconv.ParseInt(raw, 10, 64); err != nil {
			t.Fatalf("official Agent cannot parse last-event timestamp: %v", err)
		}
		t.Fatal("live backend has no provider event timestamp and must omit this optional key")
	}
	if status.Meta["tencent.event"] != "draining" || status.Ready || status.Count != 1 {
		t.Fatalf("diagnostic event or pending capacity was lost: %+v", status)
	}
}

func TestSimulationEventRemainsTimestamp(t *testing.T) {
	target, _ := configured(t)
	cfg := map[string]string{"tencent_group_id": "asg-sim"}
	if err := target.Scale(sdk.ScalingAction{Count: 5}, cfg); err != nil {
		t.Fatal(err)
	}
	status, err := target.Status(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if ts, err := strconv.ParseInt(status.Meta[sdk.TargetStatusMetaKeyLastEvent], 10, 64); err != nil || ts <= 0 {
		t.Fatalf("simulation timestamp regressed: %+v %v", status, err)
	}
}
