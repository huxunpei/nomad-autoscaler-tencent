package plugin

import (
	"context"
	"errors"
	"testing"

	"github.com/huxunpei/nomad-autoscaler-tencent/internal/live"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadclient"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/tencentcloud"
)

type launchTestCloud struct{ *routingCloud }

func (c launchTestCloud) Observe(ctx context.Context, g, i string) (tencentcloud.Snapshot, error) {
	s, err := c.routingCloud.Observe(ctx, g, i)
	s.LaunchConfiguration.VersionNumber++
	s.Group.AllocationPolicy = "LAUNCH_CONFIGURATION"
	return s, err
}

type launchTestNomad struct{}

func (launchTestNomad) ResolveMembers(context.Context, string, string, []string) ([]nomadclient.Node, error) {
	return nil, errors.New("unexpected node read for empty group")
}
func TestLaunchConfigurationRouteBindingAndLocks(t *testing.T) {
	dir, target, c := idlePolicyFixture(t)
	ctx := context.Background()
	n := launchTestNomad{}
	callback := func(r *live.Recovery, h string) error {
		p, err := r.PreviewLaunchConfiguration(ctx, "test", "change/lc", h)
		if err != nil {
			return err
		}
		_, err = r.ApplyLaunchConfiguration(ctx, p, p.Digest, h)
		return err
	}
	if WithLaunchConfigurationMaintenance(ctx, dir, launchTestCloud{c}, n, callback) == nil {
		t.Fatal("live controller not excluded")
	}
	target.Close()
	account := c.account
	c.account = "10002"
	if WithLaunchConfigurationMaintenance(ctx, dir, launchTestCloud{c}, n, callback) == nil {
		t.Fatal("wrong account accepted")
	}
	c.account = account
	region := c.region
	c.region = "ap-shanghai"
	if WithLaunchConfigurationMaintenance(ctx, dir, launchTestCloud{c}, n, callback) == nil {
		t.Fatal("wrong region accepted")
	}
	c.region = region
	if err := WithLaunchConfigurationMaintenance(ctx, dir, launchTestCloud{c}, n, callback); err != nil {
		t.Fatal(err)
	}
	if c.writes.Load() != 0 {
		t.Fatal("maintenance wrote provider")
	}
}
