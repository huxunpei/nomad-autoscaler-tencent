package plugin

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/nomad-autoscaler/plugins/shared"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/live"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/tencentcloud"
)

func TestDefaultRuntimeAuthenticationFailure(t *testing.T) {
	for _, key := range []string{"TENCENTCLOUD_SECRET_ID", "TENCENTCLOUD_SECRET_KEY", "TENCENTCLOUD_TOKEN"} {
		t.Setenv(key, "")
	}
	for _, tc := range []struct {
		name, code string
		file       bool
	}{
		{"empty_api_key", "InvalidCredentialMaterial", false},
		{"missing_file", "CredentialFileUnavailable", true},
		{"invalid_file", "InvalidCredentialFile", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			cfg := map[string]string{"tencent_region": "ap-guangzhou", "tencent_state_dir": root, "tencent_auth_method": "api_key"}
			if tc.file {
				cfg["tencent_credentials_file"] = filepath.Join(root, "credentials.json")
				if tc.name == "invalid_file" {
					if err := os.WriteFile(cfg["tencent_credentials_file"], []byte("{"), 0600); err != nil {
						t.Fatal(err)
					}
				}
			}
			// No runtime dependency injection: exercise the production interface
			// conversion, which previously turned a nil *Authenticated into a
			// non-nil accountCloud and panicked during cleanup.
			p := &Target{logger: hclog.NewNullLogger()}
			defer p.Close()
			err := p.SetConfig(cfg)
			var authErr *tencentcloud.Error
			if !errors.As(err, &authErr) || authErr.Code != tc.code {
				t.Fatalf("wanted selected credential source error %s, got %v", tc.code, err)
			}
			if p.runtime != nil || p.configSet {
				t.Fatal("authentication failure retained a configured runtime")
			}
			if _, err := os.Stat(filepath.Join(root, "accounts")); !os.IsNotExist(err) {
				t.Fatal("authentication failure initialized group state")
			}
		})
	}
}

type observationFailureCloud struct {
	*routingCloud
	err error
}

func (c *observationFailureCloud) Observe(context.Context, string, string) (tencentcloud.Snapshot, error) {
	c.reads.Add(1)
	return tencentcloud.Snapshot{}, c.err
}

func TestStartupObservationLogsCauseWithoutChangingRetry(t *testing.T) {
	for _, tc := range []struct {
		name, cause string
		err         error
	}{
		{"missing_group", "GroupMissingOrIncomplete", &tencentcloud.Error{Code: "GroupMissingOrIncomplete", Action: "DescribeAutoScalingGroups"}},
		{"denied_read", "UnauthorizedOperation", fmt.Errorf("private diagnostic: %w", &tencentcloud.Error{Code: "UnauthorizedOperation"})},
		{"opaque_error", "", errors.New("private diagnostic must not reach logs")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var logs bytes.Buffer
			var engines atomic.Int32
			cloud := &observationFailureCloud{routingCloud: &routingCloud{account: "10001", region: "ap-guangzhou"}, err: tc.err}
			p := &Target{logger: hclog.New(&hclog.LoggerOptions{Output: &logs, JSONFormat: true}), runtimeDeps: &runtimeDependencies{
				interval:  5 * time.Millisecond,
				openCloud: func(context.Context, tencentcloud.AuthConfig) (accountCloud, error) { return cloud, nil },
				openEngine: func(live.Config, live.Cloud, live.Nomad) (groupController, error) {
					engines.Add(1)
					return nil, errors.New("unexpected engine initialization")
				},
			}}
			defer p.Close()
			if err := p.SetConfig(map[string]string{"tencent_region": cloud.region, "tencent_state_dir": t.TempDir()}); err != nil {
				t.Fatal(err)
			}
			status, err := p.Status(map[string]string{"tencent_group_id": "asg-missing", shared.PluginConfigKeyGRPCTimeout: "100ms"})
			p.Close() // Join the initializer before reading its log buffer.
			if status != nil || err != ErrRouteInitializing || cloud.reads.Load() < 2 || engines.Load() != 0 || cloud.writes.Load() != 0 {
				t.Fatal("failed observation changed the wait/retry or no-write boundary", status, err)
			}
			if strings.Contains(logs.String(), "private diagnostic") {
				t.Fatal("raw observation error leaked into logs")
			}
			warnings := 0
			for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
				var row map[string]interface{}
				if err := json.Unmarshal([]byte(line), &row); err != nil {
					t.Fatal(err)
				}
				if row["@message"] != "group controller initialization will retry" {
					continue
				}
				warnings++
				if row["code"] != "GroupInitializationObservationFailed" || row["group"] != "asg-missing" {
					t.Fatal("lost existing warning fields", row)
				}
				if tc.cause != "" && row["cause_code"] != tc.cause {
					t.Fatal("missing structured observation cause", row)
				}
				if tc.cause == "" && row["cause_code"] != nil {
					t.Fatal("unstructured error treated as a safe cause code", row)
				}
			}
			if warnings < 2 {
				t.Fatal("initialization stopped retrying")
			}
		})
	}
}
