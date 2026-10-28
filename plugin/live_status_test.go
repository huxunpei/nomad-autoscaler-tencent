package plugin

import (
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/nomad-autoscaler/sdk"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/live"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadclient"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/tencentcloud"
)

type statusCloudFake struct{ writes atomic.Int32 }

func (f *statusCloudFake) Observe(context.Context, string, string) (tencentcloud.Snapshot, error) {
	return tencentcloud.Snapshot{At: time.Now().UTC(), Region: "ap-chengdu",
		Group:    tencentcloud.Group{ID: "asg-status1", Min: 0, Max: 1, Desired: 1, Count: 1, Enabled: "ENABLED", Status: "NORMAL", Activity: "NOT_IN_ACTIVITY", RetryPolicy: "NO_RETRY", LaunchConfiguration: "asc-status1"},
		Members:  []tencentcloud.Member{{ID: "ins-status1", CreationType: "AUTO_CREATION", Lifecycle: "IN_SERVICE", AddedAt: "2026-10-15T00:00:00Z"}},
		VMs:      map[string]tencentcloud.VM{"ins-status1": {ID: "ins-status1", Returned: true, State: "RUNNING", ChargeType: "POSTPAID_BY_HOUR"}},
		Policies: []tencentcloud.Policy{}, LaunchConfiguration: tencentcloud.LaunchConfiguration{ID: "asc-status1", Status: "NORMAL", InstanceType: "S5.SMALL2", ChargeType: "POSTPAID_BY_HOUR", ImageID: "img-status1", VersionNumber: 1}}, nil
}
func (f *statusCloudFake) SetDesired(context.Context, string, int64) (tencentcloud.WriteResult, error) {
	f.writes.Add(1)
	return tencentcloud.WriteResult{}, errors.New("unexpected write")
}
func (f *statusCloudFake) RemoveExact(context.Context, string, string) (tencentcloud.WriteResult, error) {
	f.writes.Add(1)
	return tencentcloud.WriteResult{}, errors.New("unexpected write")
}
func (f *statusCloudFake) Activity(context.Context, string, string) (tencentcloud.Activity, error) {
	return tencentcloud.Activity{}, errors.New("unexpected activity read")
}

type statusNomadFake struct {
	invalid bool
	writes  atomic.Int32
}

func (f *statusNomadFake) ResolveMembers(context.Context, string, string, []string) ([]nomadclient.Node, error) {
	if !f.invalid {
		return nil, errors.New("private-response-must-not-leak")
	}
	return []nomadclient.Node{{Identity: nomadclient.Identity{NodeID: "11111111-1111-1111-1111-111111111111", InstanceID: "ins-status1", GroupID: "asg-wrong1", Region: "ap-chengdu"}, Status: "ready", Eligibility: "eligible"}}, nil
}
func (f *statusNomadFake) ReadNode(context.Context, string) (nomadclient.Node, error) {
	return nomadclient.Node{}, errors.New("unexpected node read")
}
func (f *statusNomadFake) StartOwnedDrain(context.Context, nomadclient.Identity, string, time.Duration) (nomadclient.MutationResult, error) {
	f.writes.Add(1)
	return nomadclient.MutationResult{}, errors.New("unexpected drain")
}
func (f *statusNomadFake) RestoreOwnedDrain(context.Context, nomadclient.Identity, string) (nomadclient.MutationResult, error) {
	f.writes.Add(1)
	return nomadclient.MutationResult{}, errors.New("unexpected restore")
}

func TestLiveStatusCountsUnregisteredOrInvalidMappingWithoutAdvancing(t *testing.T) {
	for _, tc := range []struct {
		name, reason string
		invalid      bool
	}{{"unregistered", "NomadMemberMappingUnavailable", false}, {"invalid-mapping", "NomadMemberMappingInvalid", true}} {
		t.Run(tc.name, func(t *testing.T) {
			cloud, nomad := &statusCloudFake{}, &statusNomadFake{invalid: tc.invalid}
			engine, err := live.Open(live.Config{Dir: filepath.Join(t.TempDir(), "live"), GroupID: "asg-status1", Region: "ap-chengdu", ReconcileInterval: time.Hour, DrainDeadline: time.Minute}, cloud, nomad)
			if err != nil {
				t.Fatal(err)
			}
			target := &Target{live: engine, group: "asg-status1", backend: "live-experimental"}
			defer target.Close()
			cfg := map[string]string{"tencent_group_id": "asg-status1"}
			status, err := target.Status(cfg)
			if err != nil || status == nil || status.Count != 1 || status.Ready || status.Meta["tencent.error"] != tc.reason {
				t.Fatalf("real member hidden or unsafe status: %+v %v", status, err)
			}
			if err = target.Scale(sdk.ScalingAction{Count: 0}, cfg); err == nil {
				t.Fatal("missing identity allowed scaling")
			}
			if cloud.writes.Load() != 0 || nomad.writes.Load() != 0 {
				t.Fatal("Status representation change enabled writes")
			}
		})
	}
}

func (n *statusNomadFake) TasksStopped(context.Context, string, bool) (bool, error) { return true, nil }
