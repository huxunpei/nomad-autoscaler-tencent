package main

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/huxunpei/nomad-autoscaler-tencent/internal/live"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadclient"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/tencentcloud"
)

// These response-shape fixtures use synthetic identifiers and timestamps.
// The fake reader refreshes observations; this is not live-cloud acceptance.
type recordedCloud struct {
	s      tencentcloud.Snapshot
	a      tencentcloud.Activity
	writes int
}

func (c *recordedCloud) Observe(context.Context, string, string) (tencentcloud.Snapshot, error) {
	s := c.s
	s.At = time.Now().UTC()
	return s, nil
}
func (c *recordedCloud) Activity(context.Context, string, string) (tencentcloud.Activity, error) {
	return c.a, nil
}

type emptyNomad struct{}

func (emptyNomad) ResolveMembers(context.Context, string, string, []string) ([]nomadclient.Node, error) {
	panic("deleted group must not map nodes")
}
func TestCLIRecordedRemovalPreviewApply(t *testing.T) {
	testCLIRecordedRemovalPreviewApply(t, false)
}

func TestCLIGroupContractRemovalPreviewApply(t *testing.T) {
	testCLIRecordedRemovalPreviewApply(t, true)
}

func testCLIRecordedRemovalPreviewApply(t *testing.T, groupContract bool) {
	t.Helper()
	root := t.TempDir()
	dir := filepath.Join(root, "state")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile("testdata/recorded-remove-unknown.json")
	if err != nil {
		t.Fatal(err)
	}
	var state live.State
	if err = json.Unmarshal(raw, &state); err != nil {
		t.Fatal(err)
	}
	if groupContract {
		// Synthetic v3 journal using historical removal observations. This does
		// not turn the historical fixture into a real v3 integration result.
		state.Version = 3
		state.MaxCapacity = 0
		state.Batch = &live.Batch{
			ID: "live-1234567890abcdef1234567890abcdef", Kind: "shrink", Phase: "running",
			Target: 0, InitialCount: 1, InitialMembers: []string{state.Pending.InstanceID},
			Selected: []live.Candidate{{Identity: nomadclient.Identity{
				NodeID: state.Pending.NodeID, InstanceID: state.Pending.InstanceID,
				GroupID: state.GroupID, Region: state.Region,
			}}},
			Results: []live.Operation{}, StartedAt: state.Pending.StartedAt,
		}
		raw, err = json.MarshalIndent(state, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
	}
	if err = os.WriteFile(filepath.Join(dir, "live.json"), raw, 0600); err != nil {
		t.Fatal(err)
	}
	raw, err = os.ReadFile("testdata/recorded-remove-activity.json")
	if err != nil {
		t.Fatal(err)
	}
	var activity struct {
		ActivityId, AutoScalingGroupId, ActivityType, StartTime, EndTime, StatusCode string
		RelatedInstanceSet                                                           []struct{ InstanceId, InstanceStatus string }
	}
	if err = json.Unmarshal(raw, &activity); err != nil {
		t.Fatal(err)
	}
	c := &recordedCloud{s: *state.Cloud, a: tencentcloud.Activity{ID: activity.ActivityId, GroupID: activity.AutoScalingGroupId, Type: activity.ActivityType, StartedAt: activity.StartTime, EndedAt: activity.EndTime, Status: activity.StatusCode}}
	for _, v := range activity.RelatedInstanceSet {
		c.a.RelatedInstances = append(c.a.RelatedInstances, tencentcloud.RelatedInstance{ID: v.InstanceId, Status: v.InstanceStatus})
	}
	factory := func(string, string) (live.RecoveryCloud, live.RecoveryNomad, func(), error) {
		return c, emptyNomad{}, func() {}, nil
	}
	plan := filepath.Join(root, "plan.json")
	common := []string{"--state-dir", dir, "--group", state.GroupID, "--region", state.Region, "--drain-deadline", "120s"}
	if groupContract {
		common = append(common, "--group-contract")
	}
	args := append([]string{"preview"}, common...)
	args = append(args, "--activity", activity.ActivityId, "--operator", "tester", "--reference", "TEST-123", "--plan", plan)
	var out bytes.Buffer
	if exit := run(args, &out, factory); exit != 0 {
		t.Fatal(out.String())
	}
	var p live.RecoveryPlan
	if err = json.Unmarshal(out.Bytes(), &p); err != nil {
		t.Fatal(err)
	}
	if p.Decision.Action != "confirm-remove-success" || p.Decision.Count != 0 {
		t.Fatal(p.Decision)
	}
	if p.Scope.GroupContract != groupContract {
		t.Fatal("recovery plan did not bind the journal contract")
	}
	out.Reset()
	args = append([]string{"apply"}, common...)
	args = append(args, "--plan", plan, "--confirm", p.Digest)
	if exit := run(args, &out, factory); exit != 0 {
		t.Fatal(out.String())
	}
	raw, _ = os.ReadFile(filepath.Join(dir, "live.json"))
	state = live.State{}
	if err = json.Unmarshal(raw, &state); err != nil {
		t.Fatal(err)
	}
	if state.Pending != nil || state.LastOperation.ActivityID != "asa-synthetic" || state.LastOperation.RequestID != "" || state.RecoveryAudit == nil {
		t.Fatal("missing IDs invented or resolution incomplete")
	}
	out.Reset()
	if exit := run(args, &out, factory); exit == 0 {
		t.Fatal("accepted reused plan")
	}
}
func TestCLIRejectsArgumentsWithoutLeaking(t *testing.T) {
	factory := func(string, string) (live.RecoveryCloud, live.RecoveryNomad, func(), error) {
		t.Fatal("unexpected clients")
		return nil, nil, nil, nil
	}
	for _, args := range [][]string{nil, {"force-unlock"}, {"preview", "--secret-key=never-echo-me"}, {"apply"}, {"repair", "--state-dir", "/missing", "--group", "asg-test", "--region", "ap-chengdu", "--confirm", "bad"}} {
		var out bytes.Buffer
		if exit := run(args, &out, factory); exit == 0 || strings.Contains(out.String(), "never-echo-me") {
			t.Fatal(exit, out.String())
		}
	}
}

func TestCLIHelpNeedsNoCredentials(t *testing.T) {
	factory := func(string, string) (live.RecoveryCloud, live.RecoveryNomad, func(), error) {
		t.Fatal("help opened clients")
		return nil, nil, nil, nil
	}
	for _, args := range [][]string{{"--help"}, {"-h"}, {"help"}, {"preview", "--help"}, {"apply", "--help"}, {"repair", "--help"}} {
		var out bytes.Buffer
		if exit := run(args, &out, factory); exit != 0 || !strings.Contains(out.String(), "tencent-recover") {
			t.Fatal(exit, out.String())
		}
	}
}

func TestRecoveryTransportMatchesPluginPolicy(t *testing.T) {
	t.Setenv("TENCENT_AS_SECRET_ID", "test-id")
	t.Setenv("TENCENT_AS_SECRET_KEY", "test-key")
	for _, address := range []string{"http://192.0.2.1:4646", "http://example.invalid:4646", "http://localhost:4646"} {
		_, _, _, err := actualClients("ap-chengdu", address)
		if err == nil || err.Error() != "PlainHTTPRequiresLoopback" {
			t.Fatalf("unsafe address accepted: %v", err)
		}
	}
	for _, address := range []string{"http://127.0.0.1:4646", "http://[::1]:4646", "https://nomad.example.invalid:4646"} {
		_, _, closeClients, err := actualClients("ap-chengdu", address)
		if err != nil {
			t.Fatal(err)
		}
		closeClients()
	}
}

func TestRecoveryUsesSharedNomadEnvironmentTLSAndScope(t *testing.T) {
	t.Setenv("TENCENT_AS_SECRET_ID", "synthetic-id")
	t.Setenv("TENCENT_AS_SECRET_KEY", "synthetic-key")
	s := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Nomad-Token") != "synthetic-token" || r.URL.Query().Get("region") != "region-one" || r.URL.Query().Get("namespace") != "team-one" {
			t.Error("shared connection configuration missing")
		}
		if r.URL.Path != "/v1/nodes" {
			t.Error("unexpected request path")
		}
		w.Write([]byte("[]"))
	}))
	defer s.Close()
	ca := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: s.Certificate().Raw}), 0600); err != nil {
		t.Fatal(err)
	}
	for k, v := range map[string]string{"NOMAD_ADDR": s.URL, "NOMAD_CACERT": ca, "NOMAD_CAPATH": "", "NOMAD_CLIENT_CERT": "", "NOMAD_CLIENT_KEY": "", "NOMAD_TLS_SERVER_NAME": "", "NOMAD_SKIP_VERIFY": "false", "NOMAD_TOKEN": "synthetic-token", "NOMAD_REGION": "region-one", "NOMAD_NAMESPACE": "team-one"} {
		t.Setenv(k, v)
	}
	_, n, closeClients, err := actualClients("ap-chengdu", "")
	if err != nil {
		t.Fatal(err)
	}
	defer closeClients()
	// This exercises only the Nomad mock; the cloud client is never called.
	if _, err = n.ResolveMembers(context.Background(), "asg-example", "ap-chengdu", nil); err != nil {
		t.Fatal(err)
	}
}
