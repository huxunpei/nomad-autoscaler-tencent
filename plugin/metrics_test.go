package plugin

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/nomad-autoscaler/sdk"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/live"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/tencentcloud"
)

func TestMetricsRuntimeEndpointResultsAndShutdown(t *testing.T) {
	cloud := &routingCloud{account: "10001", region: "ap-chengdu"}
	p := &Target{runtimeDeps: &runtimeDependencies{openCloud: func(_ context.Context, cfg tencentcloud.AuthConfig) (accountCloud, error) {
		if cfg.Metrics == nil {
			t.Fatal("API collector not passed")
		}
		cfg.Metrics.API("RemoveInstances", true, true)
		return cloud, nil
	}, openEngine: func(cfg live.Config, _ live.Cloud, _ live.Nomad) (groupController, error) {
		return &loggedRoutingEngine{routingEngine: &routingEngine{cfg: cfg}, fail: cfg.GroupID == "asg-fail"}, nil
	}}}
	cfg := map[string]string{"tencent_region": "ap-chengdu", "tencent_state_dir": t.TempDir(), "tencent_metrics_listen_address": "127.0.0.1:0"}
	if err := p.SetConfig(cfg); err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	address := p.runtime.metricsServer.Address()
	if err := p.SetConfig(cfg); err != nil || p.runtime.metricsServer.Address() != address {
		t.Fatal("same config reload replaced listener")
	}
	for _, tc := range []struct {
		group string
		count int64
	}{{"asg-one", 1}, {"asg-one", 1}, {"asg-fail", 1}, {"asg-dry", sdk.StrategyActionMetaValueDryRunCount}} {
		_ = p.Scale(sdk.ScalingAction{Count: tc.count}, map[string]string{"tencent_group_id": tc.group})
	}
	reads, writes := cloud.reads.Load(), cloud.writes.Load()
	client := &http.Client{Timeout: time.Second}
	for i := 0; i < 3; i++ {
		response, err := client.Get("http://" + address + "/metrics")
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(response.Body)
		response.Body.Close()
		for _, line := range []string{`tencent_as_scale_requests_total{result="success"} 1`, `tencent_as_scale_requests_total{result="error"} 1`, `tencent_as_scale_requests_total{result="no_op"} 1`, `tencent_as_scale_requests_total{result="dry_run"} 1`, `tencent_as_api_throttles_total{action="RemoveInstances"} 1`} {
			if !strings.Contains(string(b), line+"\n") {
				t.Fatal("missing metric", line)
			}
		}
	}
	if cloud.reads.Load() != reads || cloud.writes.Load() != writes {
		t.Fatal("scrape called cloud")
	}
	p.Close()
	if response, err := client.Get("http://" + address + "/metrics"); err == nil {
		response.Body.Close()
		t.Fatal("runtime leaked listener")
	}
}
