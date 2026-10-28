package plugin

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/nomad-autoscaler/sdk"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/live"
)

type loggedRoutingEngine struct {
	*routingEngine
	fail bool
}

func (e *loggedRoutingEngine) ScaleAndWait(n int64, attempts int) (bool, error) {
	if e.fail {
		return false, errors.New("PRIVATE_ERROR_CONTENT")
	}
	return e.routingEngine.ScaleAndWait(n, attempts)
}

func TestScaleActionLogsUseAgentPolicyIdentityAndResult(t *testing.T) {
	for _, result := range []string{"success", "error", "no-op", "dry-run"} {
		t.Run(result, func(t *testing.T) {
			var output bytes.Buffer
			p := routeTarget(t, t.TempDir(), &routingCloud{account: "10001", region: "ap-guangzhou"}, func(cfg live.Config, _ live.Cloud, _ live.Nomad) (groupController, error) {
				return &loggedRoutingEngine{routingEngine: &routingEngine{cfg: cfg}, fail: result == "error"}, nil
			})
			p.runtime.logger = hclog.New(&hclog.LoggerOptions{Output: &output, JSONFormat: true, Level: hclog.Debug})
			count := int64(1)
			if result == "no-op" {
				count = 0
			}
			if result == "dry-run" {
				count = sdk.StrategyActionMetaValueDryRunCount
			}
			id := "11111111-2222-3333-4444-555555555555"
			action := sdk.ScalingAction{Count: count, Reason: "PRIVATE_REASON", Meta: map[string]any{"nomad_policy_id": id, "private": "PRIVATE_METADATA"}}
			err := p.Scale(action, map[string]string{"tencent_group_id": "asg-log"})
			if (err != nil) != (result == "error" || result == "no-op") {
				t.Fatalf("unexpected result: %v", err)
			}
			if strings.Contains(output.String(), "PRIVATE_") {
				t.Fatal("untrusted action/error contents leaked")
			}
			found := false
			for _, line := range strings.Split(output.String(), "\n") {
				if line == "" {
					continue
				}
				var row map[string]any
				if err := json.Unmarshal([]byte(line), &row); err != nil {
					t.Fatal(err)
				}
				if row["result"] == result {
					found = true
					if row["policy_id"] != id || row["group"] != "asg-log" || row["requested_count"] != float64(count) {
						t.Fatalf("wrong action identity: %v", row)
					}
				}
			}
			if !found {
				t.Fatalf("missing %s action result", result)
			}
		})
	}
}

func TestScaleActionLogsDoNotInventOrEchoInvalidPolicyIdentity(t *testing.T) {
	for _, id := range []any{nil, 123, "PRIVATE_METADATA", "asg-log", "11111111-2222-3333-4444-555555555555\nPRIVATE_METADATA"} {
		var output bytes.Buffer
		p := routeTarget(t, t.TempDir(), &routingCloud{account: "10001", region: "ap-guangzhou"}, func(cfg live.Config, _ live.Cloud, _ live.Nomad) (groupController, error) {
			return &routingEngine{cfg: cfg}, nil
		})
		p.runtime.logger = hclog.New(&hclog.LoggerOptions{Output: &output, JSONFormat: true})
		if err := p.Scale(sdk.ScalingAction{Count: 1, Meta: map[string]any{"nomad_policy_id": id}}, map[string]string{"tencent_group_id": "asg-log"}); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(output.String(), "PRIVATE_METADATA") || strings.Contains(output.String(), `"policy_id":`) {
			t.Fatal("invalid identity logged or invented")
		}
		if !strings.Contains(output.String(), `"policy_id_available":false`) {
			t.Fatal("missing identity was hidden")
		}
	}
}

func TestConcurrentActionLogsKeepPolicyAndGroupPaired(t *testing.T) {
	var output bytes.Buffer
	p := routeTarget(t, t.TempDir(), &routingCloud{account: "10001", region: "ap-guangzhou"}, func(cfg live.Config, _ live.Cloud, _ live.Nomad) (groupController, error) {
		return &routingEngine{cfg: cfg}, nil
	})
	p.runtime.logger = hclog.New(&hclog.LoggerOptions{Output: &output, JSONFormat: true})
	ids := map[string]string{"asg-one": "11111111-1111-1111-1111-111111111111", "asg-two": "22222222-2222-2222-2222-222222222222"}
	var wg sync.WaitGroup
	for group, id := range ids {
		wg.Add(1)
		go func(group, id string) {
			defer wg.Done()
			if err := p.Scale(sdk.ScalingAction{Count: 1, Meta: map[string]any{"nomad_policy_id": id}}, map[string]string{"tencent_group_id": group}); err != nil {
				t.Error(err)
			}
		}(group, id)
	}
	wg.Wait()
	seen := map[string]bool{}
	for _, line := range strings.Split(output.String(), "\n") {
		if line == "" {
			continue
		}
		var row map[string]any
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			t.Fatal(err)
		}
		group, _ := row["group"].(string)
		if ids[group] == "" || row["policy_id"] != ids[group] {
			t.Fatalf("crossed action log context: %v", row)
		}
		if row["result"] == "success" {
			seen[group] = true
		}
	}
	if len(seen) != 2 {
		t.Fatal("missing group result")
	}
}
