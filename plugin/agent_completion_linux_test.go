package plugin

import (
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadtest"

	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/nomad-autoscaler/plugins"
	"github.com/hashicorp/nomad-autoscaler/sdk"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/live"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/tencentcloud"
)

// This helper exists only in the test binary. The production target, router,
// live engine, journal and SDK are unchanged; only cloud authentication/API is
// replaced by a loopback fixture. It never uses real credentials or cloud APIs.
type agentFixtureCloud struct{ url string }

// Only this test wrapper opts into the absolute CAS production path.
type agentActivityFixtureCloud struct{ *agentFixtureCloud }

func (c *agentActivityFixtureCloud) SetDesiredCAS(ctx context.Context, group string, n, origin int64) (r tencentcloud.WriteResult, err error) {
	err = c.call(ctx, http.MethodPost, "/fixture/desired-cas", map[string]int64{"count": n, "origin": origin}, &r)
	return
}

func (*agentFixtureCloud) AccountID() string { return "10001" }
func (*agentFixtureCloud) Region() string    { return "ap-guangzhou" }
func (*agentFixtureCloud) Close()            {}
func (c *agentFixtureCloud) call(ctx context.Context, method, path string, input, output any) error {
	b, err := json.Marshal(input)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, method, c.url+path, bytes.NewReader(b))
	if err != nil {
		return err
	}
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("fixture HTTP %d", resp.StatusCode)
	}
	return json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(output)
}
func (c *agentFixtureCloud) Observe(ctx context.Context, g, id string) (s tencentcloud.Snapshot, err error) {
	err = c.call(ctx, http.MethodGet, "/fixture/cloud", map[string]string{"group_id": g, "tracked_instance_id": id}, &s)
	return
}
func (c *agentFixtureCloud) SetDesired(ctx context.Context, g string, n int64) (r tencentcloud.WriteResult, err error) {
	var response struct {
		tencentcloud.WriteResult
		Rejection *tencentcloud.Error
	}
	err = c.call(ctx, http.MethodPost, "/fixture/desired", map[string]int64{"count": n}, &response)
	r = response.WriteResult
	if err == nil && response.Rejection != nil {
		err = response.Rejection
	}
	return
}
func (c *agentFixtureCloud) RemoveExact(ctx context.Context, group, id string) (r tencentcloud.WriteResult, err error) {
	err = c.call(ctx, http.MethodPost, "/fixture/remove", map[string]string{"group_id": group, "instance_id": id}, &r)
	return
}
func (c *agentFixtureCloud) Activity(ctx context.Context, group, id string) (r tencentcloud.Activity, err error) {
	err = c.call(ctx, http.MethodPost, "/fixture/activity", map[string]string{"group_id": group, "activity_id": id}, &r)
	return
}

// Activity history remains a test-only cloud transport; production settlement
// validates the same original failure, later exact removal and stable snapshots.
func (c *agentFixtureCloud) ActivitiesSince(ctx context.Context, group string, since time.Time) (r []tencentcloud.Activity, err error) {
	err = c.call(ctx, http.MethodPost, "/fixture/activities", map[string]any{"group_id": group, "since": since}, &r)
	return
}

// Record the actual Target status response at the RPC boundary. This makes
// Agent cooldown failures distinguishable from a missing plugin timestamp.
type observedAgentTarget struct {
	*Target
	endpoint string
}

// Only the loopback fixture receives these synthetic requests. Recording both
// sides of the RPC shows which policy fields the unmodified Agent supplies.
type agentObservedRequest struct {
	Method         string
	Config         map[string]string
	Action         *sdk.ScalingAction
	CredentialKeys []string
}

func (p *observedAgentTarget) observeRequest(method string, values map[string]string, action *sdk.ScalingAction) error {
	var ack bool
	safe := make(map[string]string, len(values))
	var credentialKeys []string
	for key, value := range values {
		switch key {
		case "nomad_token", "nomad_http-auth":
			if value != "" {
				credentialKeys = append(credentialKeys, key)
			}
		default:
			safe[key] = value
		}
	}
	return (&agentFixtureCloud{url: p.endpoint}).call(context.Background(), http.MethodPost, "/fixture/request", agentObservedRequest{Method: method, Config: safe, Action: action, CredentialKeys: credentialKeys}, &ack)
}

func (p *observedAgentTarget) SetConfig(values map[string]string) error {
	if err := p.observeRequest("SetConfig", values, nil); err != nil {
		return err
	}
	return p.Target.SetConfig(values)
}

func (p *observedAgentTarget) Scale(action sdk.ScalingAction, values map[string]string) error {
	if err := p.observeRequest("Scale", values, &action); err != nil {
		return err
	}
	return p.Target.Scale(action, values)
}

func (p *observedAgentTarget) Status(values map[string]string) (*sdk.TargetStatus, error) {
	if err := p.observeRequest("Status", values, nil); err != nil {
		return nil, err
	}
	status, err := p.Target.Status(values)
	if err != nil {
		return status, err
	}
	var ack bool
	if err := (&agentFixtureCloud{url: p.endpoint}).call(context.Background(), http.MethodPost, "/fixture/status", status, &ack); err != nil {
		return nil, err
	}
	return status, nil
}

func TestStandardAgentPluginHelper(t *testing.T) {
	if os.Getenv("TENCENT_AGENT_TEST_HELPER") != "1" {
		t.Skip("subprocess only")
	}
	endpoint := os.Getenv("TENCENT_AGENT_TEST_URL")
	if !strings.HasPrefix(endpoint, "http://127.0.0.1:") {
		t.Fatal("fixture must be loopback")
	}
	plugins.Serve(func(logger hclog.Logger) interface{} {
		return &observedAgentTarget{endpoint: endpoint, Target: &Target{logger: logger, runtimeDeps: &runtimeDependencies{
			interval: 20 * time.Millisecond,
			openCloud: func(context.Context, tencentcloud.AuthConfig) (accountCloud, error) {
				if os.Getenv("TENCENT_AGENT_TEST_SCALE_OUT") == "1" {
					return &agentActivityFixtureCloud{&agentFixtureCloud{url: endpoint}}, nil
				}
				return &agentFixtureCloud{url: endpoint}, nil
			},
		}}}
	})
	os.Exit(0)
}

type agentFixture struct {
	activityGrowth                    bool
	holdGrowthActivity                bool
	unknownGrowthResponse             bool
	registrationRecovery              bool
	registrationReleased              bool
	increments                        []int64
	targets, origins                  []int64
	growthActivities                  map[string]tencentcloud.Activity
	growthJournal                     string
	failedBeforeNext                  bool
	failCloudReads, cloudReadFailures int
	failedReadBeforeWrite             bool
	rejectWrites                      bool
	rejectedWrites                    int
	mu                                sync.Mutex
	count, desired                    int64
	hold                              bool
	added                             time.Time
	writes                            []time.Time
	statuses                          []sdk.TargetStatus
	requests                          []agentObservedRequest
	groupMin, groupMax                int64
}

func (f *agentFixture) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if nomadtest.DisabledACL(w, r) {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	emit := func(v any) { _ = json.NewEncoder(w).Encode(v) }
	min, max := f.groupMin, f.groupMax
	if max == 0 {
		min, max = 1, 3
	}
	switch {
	case r.URL.Path == "/fixture/desired-cas" && f.activityGrowth && r.Method == http.MethodPost:
		var v struct{ Count, Origin int64 }
		if json.NewDecoder(r.Body).Decode(&v) != nil || v.Count <= f.count || v.Count > max || v.Origin != f.desired {
			http.Error(w, "invalid fixture absolute target or assertion", 400)
			return
		}
		if len(f.increments) == 1 {
			var s live.State
			raw, _ := os.ReadFile(f.growthJournal)
			_ = json.Unmarshal(raw, &s)
			f.failedBeforeNext = s.LastBatch != nil && s.LastBatch.Phase == "failed" && s.LastCompletedAt == 0 &&
				s.Pending != nil && s.Pending.CloudIntent != nil && s.Pending.CloudIntent.Action == "ModifyDesiredCapacity" &&
				s.Pending.CloudIntent.Desired != nil && s.Pending.CloudIntent.Desired.Origin == 5 && s.Pending.Target == 5 &&
				s.GrowthBackoff != nil && s.GrowthBackoff.Failures == 1 && !time.Now().Before(s.GrowthBackoff.RetryAfter)
		}
		before := f.count
		f.increments = append(f.increments, v.Count-before)
		f.targets, f.origins = append(f.targets, v.Count), append(f.origins, v.Origin)
		added, status := v.Count-before, "SUCCESSFUL"
		if len(f.increments) == 1 && !f.registrationRecovery {
			added, status = 2, "PARTIALLY_SUCCESSFUL"
		}
		f.count += added
		f.desired = v.Count
		f.added = time.Now().UTC()
		f.writes = append(f.writes, f.added)
		stamp := f.added.Format(time.RFC3339)
		a := tencentcloud.Activity{ID: fmt.Sprintf("asa-growth%d", len(f.increments)), GroupID: "asg-agent", Type: "SCALE_OUT", Status: status, StartedAt: stamp, EndedAt: stamp,
			RequestIDs: []tencentcloud.RequestTrace{{Action: "DescribeAutoScalingActivities", RequestID: "12345678-1234-1234-1234-123456789abc"}}}
		for i := before; i < f.count; i++ {
			a.RelatedInstances = append(a.RelatedInstances, tencentcloud.RelatedInstance{ID: fmt.Sprintf("ins-agent%d", i), Status: "SUCCESSFUL"})
		}
		if f.growthActivities == nil {
			f.growthActivities = map[string]tencentcloud.Activity{}
		}
		f.growthActivities[a.ID] = a
		result := tencentcloud.WriteResult{Action: "ModifyDesiredCapacity", Attempted: true, RequestID: "12345678-1234-1234-1234-123456789abc"}
		if f.registrationRecovery || f.unknownGrowthResponse {
			result.RequestID = ""
		}
		emit(result)
	case r.URL.Path == "/fixture/activities" && f.activityGrowth:
		if f.holdGrowthActivity && len(f.writes) > 0 {
			http.Error(w, "temporary activity history outage", 503)
			return
		}
		var v struct{ Since time.Time }
		if json.NewDecoder(r.Body).Decode(&v) != nil {
			http.Error(w, "bad activity history", 400)
			return
		}
		history := []tencentcloud.Activity{}
		for _, a := range f.growthActivities {
			at, _ := time.Parse(time.RFC3339, a.StartedAt)
			if !at.Before(v.Since) {
				history = append(history, a)
			}
		}
		emit(history)
	case r.URL.Path == "/fixture/activity" && f.activityGrowth:
		if f.holdGrowthActivity {
			http.Error(w, "temporary activity query outage", 503)
			return
		}
		var v struct {
			ActivityID string `json:"activity_id"`
		}
		if json.NewDecoder(r.Body).Decode(&v) != nil {
			http.Error(w, "bad activity", 400)
			return
		}
		a, ok := f.growthActivities[v.ActivityID]
		if !ok {
			http.NotFound(w, r)
			return
		}
		emit(a)
	case r.URL.Path == "/fixture/request":
		var request agentObservedRequest
		if json.NewDecoder(r.Body).Decode(&request) != nil {
			http.Error(w, "bad request observation", 400)
			return
		}
		f.requests = append(f.requests, request)
		emit(true)
	case r.URL.Path == "/fixture/status":
		var status sdk.TargetStatus
		if json.NewDecoder(r.Body).Decode(&status) != nil {
			http.Error(w, "bad status", 400)
			return
		}
		f.statuses = append(f.statuses, status)
		emit(true)
	case r.URL.Path == "/fixture/desired" && r.Method == http.MethodPost:
		var v struct{ Count int64 }
		if json.NewDecoder(r.Body).Decode(&v) != nil || v.Count < f.count || v.Count > max {
			http.Error(w, "invalid fixture target", 400)
			return
		}
		if f.rejectWrites {
			f.rejectedWrites++
			emit(struct {
				tencentcloud.WriteResult
				Rejection *tencentcloud.Error
			}{
				tencentcloud.WriteResult{Action: "ModifyDesiredCapacity", Attempted: true, RequestID: "12345678-1234-1234-1234-123456789abc"},
				&tencentcloud.Error{Action: "ModifyDesiredCapacity", Code: "AuthFailure.UnauthorizedOperation", RequestID: "12345678-1234-1234-1234-123456789abc"},
			})
			return
		}
		f.desired = v.Count
		f.added = time.Now().UTC()
		f.writes = append(f.writes, f.added)
		if !f.hold {
			f.count = f.desired
		}
		emit(tencentcloud.WriteResult{Attempted: true, RequestID: "12345678-1234-1234-1234-123456789abc"})
	case r.URL.Path == "/fixture/cloud":
		if f.failCloudReads > 0 {
			f.failCloudReads--
			f.cloudReadFailures++
			f.failedReadBeforeWrite = len(f.writes) == 0
			http.Error(w, "temporary fixture read", 503)
			return
		}
		s := tencentcloud.Snapshot{At: time.Now().UTC(), Region: "ap-guangzhou", Group: tencentcloud.Group{ID: "asg-agent", Min: min, Max: max, Count: f.count, Desired: f.desired, Enabled: "ENABLED", Status: "NORMAL", Activity: "NOT_IN_ACTIVITY", RetryPolicy: "NO_RETRY", LaunchConfiguration: "asc-test"}, Members: []tencentcloud.Member{}, VMs: map[string]tencentcloud.VM{}, Policies: []tencentcloud.Policy{}, LaunchConfiguration: tencentcloud.LaunchConfiguration{ID: "asc-test", Status: "NORMAL", ChargeType: "POSTPAID_BY_HOUR", InstanceType: "S5.SMALL2", ImageID: "img-test", VersionNumber: 1}}
		if f.activityGrowth {
			s.Group.AllocationPolicy = "LAUNCH_CONFIGURATION"
			s.Group.RetryPolicy = "IMMEDIATE_RETRY"
		}
		if f.count != f.desired && !f.activityGrowth {
			s.Group.Activity = "IN_ACTIVITY"
		}
		for i := int64(0); i < f.count; i++ {
			id := fmt.Sprintf("ins-agent%d", i)
			s.Members = append(s.Members, tencentcloud.Member{ID: id, CreationType: "AUTO_CREATION", Lifecycle: "IN_SERVICE", AddedAt: f.added.Format(time.RFC3339)})
			s.VMs[id] = tencentcloud.VM{ID: id, State: "RUNNING", Returned: true, ChargeType: "POSTPAID_BY_HOUR"}
		}
		emit(s)
	case r.URL.Path == "/v1/nodes":
		nodes := []map[string]any{}
		registered := f.count
		if f.registrationRecovery && !f.registrationReleased {
			registered = 1
		}
		for i := int64(0); i < registered; i++ {
			nodes = append(nodes, map[string]any{"ID": fmt.Sprintf("11111111-1111-1111-1111-%012d", i)})
		}
		emit(nodes)
	case strings.HasPrefix(r.URL.Path, "/v1/node/"):
		id := strings.TrimPrefix(r.URL.Path, "/v1/node/")
		index, err := strconv.ParseInt(id[len(id)-12:], 10, 64)
		if err != nil || index < 0 || index >= f.count {
			http.NotFound(w, r)
			return
		}
		emit(map[string]any{"ID": id, "Meta": map[string]string{"cloud_instance_id": fmt.Sprintf("ins-agent%d", index)}, "Status": "ready", "SchedulingEligibility": "eligible", "Drain": false, "DrainStrategy": nil})
	case r.URL.Path == "/api/v1/query_range":
		start, _ := strconv.ParseFloat(r.URL.Query().Get("start"), 64)
		end, _ := strconv.ParseFloat(r.URL.Query().Get("end"), 64)
		metric := "140"
		if f.activityGrowth {
			metric = "350"
		}
		emit(map[string]any{"status": "success", "data": map[string]any{"resultType": "matrix", "result": []any{map[string]any{"metric": map[string]string{}, "values": []any{[]any{start, metric}, []any{end, metric}}}}}})
	default:
		http.Error(w, "unexpected fixture endpoint", 404)
	}
}

type agentLog struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (b *agentLog) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.Write(p)
}
func (b *agentLog) text() string { b.mu.Lock(); defer b.mu.Unlock(); return b.buffer.String() }

func TestStandardAgentCompletion(t *testing.T)           { verifyStandardAgentCompletion(t, false) }
func TestStandardAgentDryRun(t *testing.T)               { verifyStandardAgentCompletion(t, false) }
func TestStandardAgentExplicitDryRunFalse(t *testing.T)  { verifyStandardAgentCompletion(t, false) }
func TestStandardAgentLongPositiveDeadline(t *testing.T) { verifyStandardAgentCompletion(t, false) }
func TestStandardAgentNoDrainDeadline(t *testing.T)      { verifyStandardAgentCompletion(t, false) }
func TestStandardAgentForcedDrainDeadline(t *testing.T)  { verifyStandardAgentCompletion(t, false) }
func TestStandardAgentConfiguredRestartCooldownTwoMinutes(t *testing.T) {
	verifyStandardAgentCompletion(t, true)
}
func TestStandardAgentConfiguredRestartCooldownFiveMinutes(t *testing.T) {
	verifyStandardAgentCompletion(t, true)
}
func TestStandardAgentPolicyBoundaryWarning(t *testing.T) {
	verifyStandardAgentScenario(t, false, true, false, false)
}
func TestStandardAgentCompletionAndRestartCooldown(t *testing.T) {
	verifyStandardAgentCompletion(t, true)
}

// This is a transport diagnostic, not a passing policy-boundary test. A passing
// probe establishes that the current release does not supply complete bounds.
func TestStandardAgentPolicyBoundaryTransportProbe(t *testing.T) {
	verifyStandardAgentScenario(t, false, true, false, false)
}

func verifyStandardAgentCompletion(t *testing.T, checkRestart bool) {
	t.Helper()
	verifyStandardAgentScenario(t, checkRestart, false, false, false)
}

// This diagnostic proves disk policy contents need not equal the Agent's
// current policy. It also exercises identical SetConfig calls from SIGHUP.
func TestStandardAgentPolicyFileVersionProbe(t *testing.T) {
	verifyStandardAgentScenario(t, false, true, true, false)
}

// Exercises normal Agent evaluation after a definite, synchronous cloud rejection.
// Cloud credentials and APIs remain synthetic; this is not live IAM acceptance.
func TestStandardAgentStartupReadRetry(t *testing.T) {
	verifyStandardAgentScenario(t, false, false, false, false)
}

func TestStandardAgentObservability(t *testing.T) {
	verifyStandardAgentScenario(t, false, false, false, false)
}

func TestStandardAgentRejectedWriteNextEvaluation(t *testing.T) {
	verifyStandardAgentScenario(t, false, false, false, true)
}

func TestStandardAgentGrowthRecoveryBeforeRegistration(t *testing.T) {
	verifyStandardAgentScenario(t, false, false, false, false)
}

func TestStandardAgentUnknownGrowthRestartRecovery(t *testing.T) {
	verifyStandardAgentScenario(t, false, false, false, false)
}

func TestStandardAgentPartialGrowthNextEvaluation(t *testing.T) {
	verifyStandardAgentScenario(t, false, false, false, false)
}

func verifyStandardAgentScenario(t *testing.T, checkRestart, policyProbe, reloadProbe, rejectProbe bool) {
	t.Helper()
	configuredCooldown := t.Name() == "TestStandardAgentConfiguredRestartCooldownTwoMinutes" || t.Name() == "TestStandardAgentConfiguredRestartCooldownFiveMinutes"
	boundaryWarning := t.Name() == "TestStandardAgentPolicyBoundaryWarning"
	cooldown := 6 * time.Second
	if configuredCooldown {
		cooldown = 2 * time.Minute
		if t.Name() == "TestStandardAgentConfiguredRestartCooldownFiveMinutes" {
			cooldown = 5 * time.Minute
		}
	}
	metricsProbe := t.Name() == "TestStandardAgentObservability"
	startupProbe := t.Name() == "TestStandardAgentStartupReadRetry"
	unknownRestartProbe := t.Name() == "TestStandardAgentUnknownGrowthRestartRecovery"
	growthProbe := t.Name() == "TestStandardAgentPartialGrowthNextEvaluation" || unknownRestartProbe
	registrationProbe := t.Name() == "TestStandardAgentGrowthRecoveryBeforeRegistration"
	dryRunProbe := t.Name() == "TestStandardAgentDryRun"
	explicitDryRunFalse := t.Name() == "TestStandardAgentExplicitDryRunFalse"
	binary := os.Getenv("NOMAD_AUTOSCALER_TEST_BIN")
	if binary == "" {
		t.Skip("requires a separately installed standard Nomad Autoscaler release")
	}
	data, err := os.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	version, err := exec.Command(binary, "version").CombinedOutput()
	if err != nil {
		t.Fatal(err)
	}
	expectedVersion := os.Getenv("NOMAD_AUTOSCALER_TEST_VERSION")
	if expectedVersion == "" {
		expectedVersion = "v0.5.0"
	}
	if expectedVersion != "v0.5.0" && expectedVersion != "v0.6.0" {
		t.Fatalf("unsupported diagnostic version: %s", expectedVersion)
	}
	if !strings.Contains(string(version), expectedVersion) {
		t.Fatalf("unverified Agent version: %s", version)
	}
	t.Logf("standard Agent=%s sha256=%x; cloud/Nomad/APM are loopback fixtures", strings.TrimSpace(string(version)), sha256.Sum256(data))
	root := t.TempDir()
	pluginDir := filepath.Join(root, "plugins")
	policyDir := filepath.Join(root, "policies")
	stateDir := filepath.Join(root, "state")
	for _, dir := range []string{pluginDir, policyDir, stateDir} {
		if err := os.Mkdir(dir, 0700); err != nil {
			t.Fatal(err)
		}
	}
	fixture := &agentFixture{count: 1, desired: 1, hold: true, added: time.Now().Add(-time.Hour), rejectWrites: rejectProbe}
	if growthProbe || registrationProbe {
		fixture.registrationRecovery = registrationProbe
		fixture.activityGrowth = true
		fixture.hold = false
		fixture.groupMin = 1
		fixture.groupMax = 5
	}
	if unknownRestartProbe {
		fixture.holdGrowthActivity = true
		fixture.unknownGrowthResponse = true
	}
	if startupProbe {
		fixture.failCloudReads = 1
	}
	if policyProbe {
		fixture.count, fixture.desired, fixture.groupMin, fixture.groupMax = 3, 3, 2, 10
	}
	server := httptest.NewServer(fixture)
	defer server.Close()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	quote := func(s string) string { return "'" + strings.ReplaceAll(s, "'", "'\"'\"'") + "'" }
	wrapper := "#!/bin/sh\nexec " + quote(executable) + " -test.run=^TestStandardAgentPluginHelper$\n"
	if err = os.WriteFile(filepath.Join(pluginDir, Name), []byte(wrapper), 0700); err != nil {
		t.Fatal(err)
	}
	config := fmt.Sprintf(`nomad { address = %q }
apm "prometheus" {
 driver = "prometheus"
 config = { address = %q }
}
target "tencent-as" {
 driver = "tencent-as"
 config = { tencent_region = "ap-guangzhou", tencent_state_dir = %q, retry_attempts = "200" }
}
`, server.URL, server.URL, stateDir)
	if boundaryWarning {
		config = strings.Replace(config, `retry_attempts = "200"`, `retry_attempts = "200", policy_dir = `+strconv.Quote(policyDir), 1)
	}
	if metricsProbe {
		config = strings.Replace(config, `retry_attempts = "200"`, `retry_attempts = "200", tencent_metrics_listen_address = "127.0.0.1:0"`, 1)
	}
	configPath := filepath.Join(root, "agent.hcl")
	if err = os.WriteFile(configPath, []byte(config), 0600); err != nil {
		t.Fatal(err)
	}
	policy := `scaling "cluster" {
 enabled = true
 min = 1
 max = 3
 policy {
  cooldown = "6s"
  evaluation_interval = "200ms"
  check "cpu" {
   source = "prometheus"
   query = "fixture_cpu"
   strategy "target-value" { target = 70 }
  }
  target "tencent-as" { tencent_group_id = "asg-agent" }
 }
}
`
	if policyProbe {
		policy = `scaling "cluster" {
 enabled = true
 min = 1
 max = 20
 policy {
  cooldown = "6s"
  evaluation_interval = "200ms"
  check "fixed" {
   strategy "fixed-value" { value = 5 }
  }
  target "tencent-as" { tencent_group_id = "asg-agent" }
 }
}
`
	}
	if growthProbe || registrationProbe {
		policy = strings.Replace(policy, "max = 3", "max = 5", 1)
	}
	if configuredCooldown {
		policy = strings.Replace(policy, `cooldown = "6s"`, `cooldown = "`+cooldown.String()+`"`, 1)
		policy = strings.Replace(policy, `target "tencent-as" { tencent_group_id = "asg-agent" }`, `target "tencent-as" {
   tencent_group_id = "asg-agent"
   restart_cooldown = "`+cooldown.String()+`"
  }`, 1)
	}

	deadlineMode := ""
	if dryRunProbe || explicitDryRunFalse {
		policy = strings.Replace(policy, `target "tencent-as" { tencent_group_id = "asg-agent" }`,
			`target "tencent-as" {
   tencent_group_id = "asg-agent"
   dry-run = "`+strconv.FormatBool(dryRunProbe)+`"
  }`, 1)
	}
	switch t.Name() {
	case "TestStandardAgentLongPositiveDeadline":
		deadlineMode = "48h"
	case "TestStandardAgentNoDrainDeadline":
		deadlineMode = "0"
	case "TestStandardAgentForcedDrainDeadline":
		deadlineMode = "-1s"
	}
	if deadlineMode != "" {
		policy = strings.Replace(policy, `target "tencent-as" { tencent_group_id = "asg-agent" }`, `target "tencent-as" {
   tencent_group_id = "asg-agent"
   node_drain_deadline = "`+deadlineMode+`"
  }`, 1)
	}
	if err = os.WriteFile(filepath.Join(policyDir, "cluster.hcl"), []byte(policy), 0600); err != nil {
		t.Fatal(err)
	}
	journal := filepath.Join(stateDir, "accounts", "10001", "ap-guangzhou", "asg-agent", "live.json")
	fixture.mu.Lock()
	fixture.growthJournal = journal
	fixture.mu.Unlock()
	readState := func() live.State { var s live.State; b, _ := os.ReadFile(journal); _ = json.Unmarshal(b, &s); return s }
	var logs []*agentLog
	var currentAgent *exec.Cmd
	start := func() func() {
		log := &agentLog{}
		logs = append(logs, log)
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		port := listener.Addr().(*net.TCPAddr).Port
		if err := listener.Close(); err != nil {
			t.Fatal(err)
		}
		cmd := exec.Command(binary, "agent", "-config="+configPath, "-plugin-dir="+pluginDir, "-policy-dir="+policyDir, "-policy-source-disable-nomad", "-http-bind-address=127.0.0.1", "-http-bind-port="+strconv.Itoa(port), "-log-level=DEBUG", "-log-json")
		cmd.Env = append(os.Environ(), "TENCENT_AGENT_TEST_HELPER=1", "TENCENT_AGENT_TEST_URL="+server.URL)
		if growthProbe || registrationProbe {
			cmd.Env = append(cmd.Env, "TENCENT_AGENT_TEST_SCALE_OUT=1")
		}
		cmd.Stdout, cmd.Stderr = log, log
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		currentAgent = cmd
		done := make(chan error, 1)
		go func() { done <- cmd.Wait() }()
		var once sync.Once
		stop := func() {
			once.Do(func() {
				_ = cmd.Process.Signal(os.Interrupt)
				select {
				case err := <-done:
					if err != nil {
						t.Errorf("Agent exit: %v", err)
					}
				case <-time.After(5 * time.Second):
					_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
					<-done
					t.Error("Agent failed to stop")
				}
			})
		}
		t.Cleanup(stop)
		return stop
	}
	defer func() {
		for i, log := range logs {
			t.Logf("Agent run %d:\n%s", i+1, log.text())
		}
	}()
	wait := func(label string, deadline time.Time, condition func() bool) {
		t.Helper()
		for time.Now().Before(deadline) {
			if condition() {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
		snapshot, _ := json.Marshal(readState())
		t.Fatalf("%s; synthetic journal=%s", label, snapshot)
	}
	writes := func() int { fixture.mu.Lock(); defer fixture.mu.Unlock(); return len(fixture.writes) }
	stop := start()
	if dryRunProbe {
		wait("dry-run did not complete through the official Agent", time.Now().Add(15*time.Second), func() bool {
			return strings.Contains(logs[0].text(), `"result":"dry-run"`)
		})
		stop()
		fixture.mu.Lock()
		defer fixture.mu.Unlock()
		var sawStatus, sawScale bool
		for _, request := range fixture.requests {
			if request.Method == "Status" && request.Config["dry-run"] == "true" {
				sawStatus = true
			}
			if request.Method == "Scale" && request.Config["dry-run"] == "true" && request.Action != nil && request.Action.Count == sdk.StrategyActionMetaValueDryRunCount {
				sawScale = true
			}
		}
		s := readState()
		if !sawStatus || !sawScale || len(fixture.writes) != 0 || len(fixture.increments) != 0 || fixture.count != 1 || fixture.desired != 1 ||
			s.Pending != nil || s.Batch != nil || s.LastCompletedAt != 0 || len(s.Members) != 1 {
			t.Fatal("dry-run lost actual Status or created a cloud action/completion record")
		}
		t.Log("official Agent dry-run=true reached target Status and Scale, preserved Count=Desired=1 and created no cloud writes, pending batch or success timestamp; cloud/Nomad/APM are synthetic")
		return
	}
	if registrationProbe {
		waiting := func() bool {
			s := readState()
			if writes() != 1 || s.Pending != nil || s.LastCompletedAt == 0 || len(s.Members) != 5 ||
				s.LastOperation == nil || !s.LastOperation.DesiredAccepted || s.LastOperation.RequestID != "" ||
				s.LastOperation.CloudIntent == nil || s.LastOperation.CloudIntent.Desired == nil ||
				s.LastOperation.CloudIntent.Desired.Origin != 1 || s.LastOperation.Target != 5 || s.Desired != 5 {
				return false
			}
			for i := 1; i < 5; i++ {
				if s.Members[fmt.Sprintf("ins-agent%d", i)].RegistrationState != "waiting" {
					return false
				}
			}
			fixture.mu.Lock()
			defer fixture.mu.Unlock()
			if len(fixture.statuses) == 0 {
				return false
			}
			last := fixture.statuses[len(fixture.statuses)-1]
			return last.Count == 5 && !last.Ready
		}
		wait("known cloud success remained blocked by absent Nomad registrations", time.Now().Add(25*time.Second), waiting)
		stamp := readState().LastCompletedAt
		stop()
		fixture.mu.Lock()
		prior := len(fixture.statuses)
		fixture.mu.Unlock()
		stop = start()
		wait("restart lost registration waiting state", time.Now().Add(20*time.Second), func() bool {
			fixture.mu.Lock()
			newStatus := len(fixture.statuses) > prior
			fixture.mu.Unlock()
			return newStatus && waiting() && readState().LastCompletedAt == stamp
		})
		fixture.mu.Lock()
		fixture.registrationReleased = true
		prior = len(fixture.statuses)
		fixture.mu.Unlock()
		wait("late Nomad registration did not release readiness", time.Now().Add(20*time.Second), func() bool {
			s := readState()
			for _, m := range s.Members {
				if !m.Joined || !m.EverJoined || m.RegistrationState != "registered" {
					return false
				}
			}
			fixture.mu.Lock()
			defer fixture.mu.Unlock()
			if len(fixture.statuses) <= prior {
				return false
			}
			last := fixture.statuses[len(fixture.statuses)-1]
			return last.Count == 5 && last.Ready && s.LastCompletedAt == stamp
		})
		stop()
		fixture.mu.Lock()
		increments := append([]int64(nil), fixture.increments...)
		fixture.mu.Unlock()
		if !reflect.DeepEqual(increments, []int64{4}) || readState().Pending != nil || !reflect.DeepEqual(fixture.targets, []int64{5}) || !reflect.DeepEqual(fixture.origins, []int64{1}) {
			t.Fatal("cloud mutation replayed during registration recovery")
		}
		t.Log("unmodified Agent: absolute CAS response lost -> persisted origin=1 target=5 accepted from Desired readback -> running cloud count=5 completes without fabricated RequestId -> four unregistered clients keep Status unready across restart -> late registration makes Status ready; one cloud write, same completion timestamp; cloud/Nomad/APM synthetic")
		return
	}
	if unknownRestartProbe {
		wait("first growth did not persist its unknown outcome", time.Now().Add(15*time.Second), func() bool {
			s := readState()
			return writes() == 1 && s.Pending != nil && s.Pending.DesiredAccepted && s.Pending.RequestID == "" &&
				s.Pending.CloudIntent != nil && s.Pending.CloudIntent.Desired != nil && s.Pending.CloudIntent.Desired.Origin == 1 && s.Desired == 5
		})
		stop()
		fixture.mu.Lock()
		priorRequests, priorStatuses := len(fixture.requests), len(fixture.statuses)
		fixture.mu.Unlock()
		stop = start()
		wait("restarted Agent never queried Status", time.Now().Add(15*time.Second), func() bool {
			fixture.mu.Lock()
			defer fixture.mu.Unlock()
			for _, request := range fixture.requests[priorRequests:] {
				if request.Method == "Status" {
					return true
				}
			}
			return false
		})
		// Keep the outcome unresolved over multiple natural evaluation ticks.
		time.Sleep(600 * time.Millisecond)
		fixture.mu.Lock()
		sawUnready := false
		for _, status := range fixture.statuses[priorStatuses:] {
			if status.Count == 3 && !status.Ready {
				sawUnready = true
			}
		}
		mutationsWhileUnknown := len(fixture.writes)
		fixture.holdGrowthActivity = false
		fixture.unknownGrowthResponse = false
		fixture.mu.Unlock()
		wait("background result recovery did not restore the same Agent's natural evaluation after backoff", time.Now().Add(3*time.Minute), func() bool {
			s := readState()
			return writes() == 2 && s.Pending == nil && s.LastBatch != nil && s.LastBatch.Phase == "completed" && len(s.Members) == 5
		})
		stop()
		fixture.mu.Lock()
		defer fixture.mu.Unlock()
		if !sawUnready || mutationsWhileUnknown != 1 || !fixture.failedBeforeNext || !reflect.DeepEqual(fixture.increments, []int64{4, 2}) ||
			!reflect.DeepEqual(fixture.targets, []int64{5, 5}) || !reflect.DeepEqual(fixture.origins, []int64{1, 5}) || readState().RecoveryAudit == nil {
			t.Fatal("unknown startup lost truthful unready Status, replay protection or audited partial-failure recovery")
		}
		t.Log("official Agent restarted after a lost CAS response: Desired=5 proven by readback, Count=3 Ready=false while activity history unavailable; authoritative partial failure recovered in background; after persistent two-minute backoff the same Agent naturally resent absolute 5 with origin=5 and added only 2; no replay or second restart; cloud/Nomad/APM synthetic")
		return
	}
	if growthProbe {
		wait("partial growth did not persist its failed batch and backoff", time.Now().Add(15*time.Second), func() bool {
			s := readState()
			return s.Pending == nil && s.LastBatch != nil && s.LastBatch.Phase == "failed" && s.GrowthBackoff != nil
		})
		failed := readState()
		if writes() != 1 || failed.Desired != 5 || len(failed.Members) != 3 || failed.LastCompletedAt != 0 || failed.GrowthBackoff.Failures != 1 || time.Until(failed.GrowthBackoff.RetryAfter) < 110*time.Second {
			t.Fatal("partial growth lost Desired=5, truthful count, or two-minute failure backoff")
		}
		wait("partial failure did not return Ready=true Count=3", time.Now().Add(5*time.Second), func() bool {
			fixture.mu.Lock()
			defer fixture.mu.Unlock()
			for _, status := range fixture.statuses {
				if status.Ready && status.Count == 3 {
					return true
				}
			}
			return false
		})
		for time.Now().Before(failed.GrowthBackoff.RetryAfter.Add(-100 * time.Millisecond)) {
			if writes() != 1 || readState().LastCompletedAt != 0 {
				t.Fatal("backoff allowed an early cloud write or success timestamp")
			}
			time.Sleep(50 * time.Millisecond)
		}
		wait("partial growth did not permit the same Agent's next evaluation", failed.GrowthBackoff.RetryAfter.Add(10*time.Second), func() bool {
			s := readState()
			return writes() == 2 && s.Pending == nil && s.LastBatch != nil && s.LastBatch.Phase == "completed" && len(s.Members) == 5 && s.LastCompletedAt > 0
		})
		stop()
		fixture.mu.Lock()
		if !reflect.DeepEqual(fixture.increments, []int64{4, 2}) || !reflect.DeepEqual(fixture.targets, []int64{5, 5}) || !reflect.DeepEqual(fixture.origins, []int64{1, 5}) ||
			!fixture.failedBeforeNext || fixture.count != 5 || fixture.desired != 5 || readState().GrowthBackoff != nil {
			fixture.mu.Unlock()
			t.Fatal("partial growth replayed, lost failure, or used incorrect baseline")
		}
		fixture.mu.Unlock()
		if readState().RecoveryAudit == nil {
			t.Fatal("partial failure audit missing")
		}
		t.Log("same unmodified Agent: absolute target=5 origin=1 -> partial Count=3 Desired=5 with failed batch and no cooldown -> two-minute backoff, Ready=true -> same absolute target=5 origin=5 -> Count=Desired=5 success and cleared backoff; cloud/Nomad/APM are synthetic")
		return
	}
	if rejectProbe {
		wait("rejected action was not returned as failure", time.Now().Add(20*time.Second), func() bool {
			s := readState()
			return s.LastBatch != nil && s.LastBatch.Phase == "failed" && strings.Contains(logs[0].text(), "scale request finished")
		})
		s := readState()
		if s.Pending != nil || s.Batch != nil || s.ManualReview || s.LastCompletedAt != 0 || writes() != 0 || s.LastOperation.CloudRejection == nil {
			t.Fatal("rejected action remained unknown or published completion")
		}
		fixture.mu.Lock()
		if fixture.count != 1 || fixture.desired != 1 || fixture.rejectedWrites < 1 {
			fixture.mu.Unlock()
			t.Fatal("rejected fixture changed capacity")
		}
		fixture.rejectWrites = false
		fixture.mu.Unlock()
		t.Log("known cloud rejection returned failure without capacity change or completion timestamp; releasing failure for the next normal evaluation")
	}
	wait("first scale not issued", time.Now().Add(20*time.Second), func() bool { return writes() == 1 })
	time.Sleep(350 * time.Millisecond) // exceeds the 150ms Status RPC timeout
	if s := readState(); s.LastCompletedAt != 0 || s.Pending == nil || strings.Contains(logs[0].text(), "scale request completed") {
		t.Fatal("Agent scale reported completion before cloud settled")
	}
	fixture.mu.Lock()
	fixture.hold = false
	fixture.count = fixture.desired
	fixture.mu.Unlock()
	wait("first scale not completed", time.Now().Add(5*time.Second), func() bool {
		return readState().LastCompletedAt > 0 && strings.Contains(logs[0].text(), "scale request completed")
	})
	stamp := readState().LastCompletedAt
	if deadlineMode != "" {
		expected, err := time.ParseDuration(deadlineMode)
		if err != nil {
			t.Fatal(err)
		}
		if readState().DrainDeadlineNanoseconds != int64(expected) {
			t.Fatal("actual Agent policy duration changed")
		}
		fixture.mu.Lock()
		matched := false
		for _, request := range fixture.requests {
			if request.Method == "Scale" && request.Config["node_drain_deadline"] == deadlineMode {
				matched = true
			}
		}
		fixture.mu.Unlock()
		if !matched {
			t.Fatal("actual Agent did not forward exact policy deadline")
		}
		t.Logf("actual Agent accepted %s through Status/Scale during model growth; durable policy retained exact duration; this probe does not exercise shrinking", deadlineMode)
	}

	if startupProbe {
		fixture.mu.Lock()
		failures, beforeWrite := fixture.cloudReadFailures, fixture.failedReadBeforeWrite
		scaleCalls := 0
		for _, request := range fixture.requests {
			if request.Method == "Scale" {
				scaleCalls++
			}
		}
		fixture.mu.Unlock()
		if failures != 1 || !beforeWrite || scaleCalls != 1 || !strings.Contains(logs[0].text(), "group controller initialization will retry") {
			t.Fatal("standard Agent did not exercise startup retry before its one accepted action", failures, beforeWrite, scaleCalls)
		}
		t.Log("standard Agent startup: one failed initialization read, plugin retry log forwarded, one normal Scale call completed; no manual retry")
	}

	if metricsProbe {
		state := readState()
		batch := state.LastBatch
		if batch == nil || batch.Source == nil || batch.Source.PolicyID == "" {
			t.Fatal("missing durable Agent identity")
		}
		address := ""
		matched := false
		for _, line := range strings.Split(logs[0].text(), "\n") {
			var entry map[string]any
			if json.Unmarshal([]byte(line), &entry) != nil {
				continue
			}
			if entry["@message"] == "Tencent metrics endpoint started" {
				address, _ = entry["address"].(string)
			}
			if entry["@message"] == "scaling lifecycle" && entry["result"] == "completed" && entry["operation_id"] == batch.ID && entry["policy_id"] == batch.Source.PolicyID && entry["old_count"] == float64(1) && entry["requested_count"] == float64(2) && entry["effective_count"] == float64(2) && entry["observed_new_count"] == float64(2) {
				matched = true
			}
		}
		if !matched || !strings.HasPrefix(address, "127.0.0.1:") {
			t.Fatal("actual Agent did not forward lifecycle/endpoint logs")
		}
		client := &http.Client{Timeout: time.Second}
		response, err := client.Get("http://" + address + "/metrics")
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(response.Body)
		response.Body.Close()
		if err != nil || response.StatusCode != 200 || !strings.Contains(string(body), `tencent_as_scale_requests_total{result="success"} 1`) {
			t.Fatal("actual plugin process metrics missing")
		}
		t.Logf("actual plugin metrics via HTTP: %s", body)
	}

	if rejectProbe {
		fixture.mu.Lock()
		attempts := 0
		for _, req := range fixture.requests {
			if req.Method == "Scale" {
				attempts++
			}
		}
		fixture.mu.Unlock()
		if attempts < 2 || readState().LastBatch.Phase != "completed" {
			t.Fatal("normal Agent did not recover on its next evaluation")
		}
		t.Logf("standard Agent recovery: %d Scale RPC calls, one accepted cloud write, failed then successful batch, no manual CLI", attempts)
	}
	if reloadProbe {
		scales := func() []agentObservedRequest {
			fixture.mu.Lock()
			defer fixture.mu.Unlock()
			var out []agentObservedRequest
			for _, req := range fixture.requests {
				if req.Method == "Scale" {
					out = append(out, req)
				}
			}
			return out
		}
		setConfigs := func() int {
			fixture.mu.Lock()
			defer fixture.mu.Unlock()
			n := 0
			for _, req := range fixture.requests {
				if req.Method == "SetConfig" {
					n++
				}
			}
			return n
		}
		resetCount := func(n int64) { fixture.mu.Lock(); fixture.count = n; fixture.desired = n; fixture.mu.Unlock() }
		first := scales()
		if len(first) != 1 || first[0].Action == nil || first[0].Action.Count != 5 {
			t.Fatal("missing original uncapped action")
		}
		originalID, _ := first[0].Action.Meta["nomad_policy_id"].(string)
		if originalID == "" {
			t.Fatal("missing original policy ID")
		}
		path := filepath.Join(policyDir, "cluster.hcl")
		changed := strings.Replace(policy, "max = 20", "max = 3", 1)
		if changed == policy {
			t.Fatal("probe did not change file max")
		}
		if err := os.WriteFile(path, []byte(changed), 0600); err != nil {
			t.Fatal(err)
		}
		resetCount(3)
		wait("unchanged Agent did not reevaluate cached policy", time.Now().Add(15*time.Second), func() bool { return writes() == 2 && readState().LastCompletedAt > stamp })
		beforeReload := scales()
		disk, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if len(beforeReload) != 2 || beforeReload[1].Action.Count != 5 || !bytes.Equal(disk, []byte(changed)) {
			t.Fatal("disk/cache difference not reproduced")
		}
		if beforeReload[1].Action.Meta["nomad_policy_id"] != originalID || !reflect.DeepEqual(beforeReload[1].Config, first[0].Config) {
			t.Fatal("unreloaded policy identity or target config changed")
		}
		t.Log("DIAGNOSTIC: disk max=3, loaded Agent max=20, next Scale count=5; rereading the same file would validate a different policy version")

		stamp = readState().LastCompletedAt
		priorConfigCalls := setConfigs()
		resetCount(2)
		if err := currentAgent.Process.Signal(syscall.SIGHUP); err != nil {
			t.Fatal(err)
		}
		wait("Agent did not repeat SetConfig on reload", time.Now().Add(5*time.Second), func() bool {
			return setConfigs() > priorConfigCalls && strings.Contains(logs[0].text(), "reloading plugins")
		})
		wait("reloaded Agent did not reevaluate policy", time.Now().Add(15*time.Second), func() bool { return writes() == 3 && readState().LastCompletedAt > stamp })
		afterReload := scales()
		if len(afterReload) != 3 || afterReload[2].Action.Count != 5 || afterReload[2].Action.Meta["nomad_policy_id"] != originalID || !reflect.DeepEqual(afterReload[2].Config, first[0].Config) {
			t.Fatal("unexpected policy cache contract for tested release; inspect upstream source")
		}
		if strings.Contains(logs[0].text(), "failed to reload plugins") || strings.Contains(logs[0].text(), "reload requires plugin restart") {
			t.Fatal("unchanged plugin configuration rejected during SIGHUP")
		}
		t.Log("DIAGNOSTIC: SIGHUP rereads the directory but the tested release retains existing policy max=20; policy ID and target config stay identical; repeated SetConfig succeeded without replacing the target")
		stop()
		renamed := filepath.Join(policyDir, "renamed.hcl")
		if err := os.Rename(path, renamed); err != nil {
			t.Fatal(err)
		}
		resetCount(2)
		stamp = readState().LastCompletedAt
		stopRestarted := start()
		wait("renamed policy did not evaluate after restart", time.Now().Add(20*time.Second), func() bool { return writes() == 4 && readState().LastCompletedAt > stamp })
		stopRestarted()
		restarted := scales()
		if len(restarted) != 4 || restarted[3].Action.Count != 3 || restarted[3].Action.Meta["nomad_policy_id"] == originalID || !reflect.DeepEqual(restarted[3].Config, first[0].Config) {
			t.Fatal("renamed/restarted policy correlation assumption changed")
		}
		t.Log("DIAGNOSTIC: rename plus Agent restart changes policy ID while target config is unchanged; UUID cannot identify the file or its min/max")
		for _, req := range restarted {
			for _, key := range []string{"min", "max", "policy_min", "policy_max"} {
				if _, ok := req.Config[key]; ok {
					t.Fatal("transport now includes complete bounds; inspect the new contract")
				}
			}
		}
		t.Log("Transport diagnostic: file contents can differ from the loaded Agent policy; policy_dir checks are advisory only. All cloud/Nomad/APM state is synthetic.")
		return
	}
	if configuredCooldown {
		time.Sleep(30 * time.Second)
	}
	stop()
	if boundaryWarning {
		if !strings.Contains(logs[0].text(), "policy bounds exceed cloud group limits; scaling uses cloud limits") || !strings.Contains(logs[0].text(), "cluster.hcl#cluster") {
			t.Fatal("standard Agent did not forward the file policy boundary warning")
		}
		t.Log("policy-boundary runtime: policy=1..20 group=2..10 target=5 emitted the advisory warning through the unmodified Agent")
	}
	if !checkRestart {
		if policyProbe {
			fixture.mu.Lock()
			requests := append([]agentObservedRequest(nil), fixture.requests...)
			count := fixture.count
			fixture.mu.Unlock()
			seen := map[string]bool{}
			var policyID string
			for _, request := range requests {
				if seen[request.Method] {
					continue
				}
				seen[request.Method] = true
				b, err := json.Marshal(request)
				if err != nil {
					t.Fatal(err)
				}
				t.Logf("actual synthetic RPC request: %s", b)
				for _, key := range []string{"min", "max", "policy_id", "policy_min", "policy_max"} {
					if _, ok := request.Config[key]; ok {
						t.Fatalf("transport contract changed, inspect %s", key)
					}
				}
				if request.Action != nil {
					id, ok := request.Action.Meta["nomad_policy_id"].(string)
					if request.Action.Count != 5 || len(request.Action.Meta) != 1 || !ok || id == "" {
						t.Fatalf("unexpected uncapped fixed action: %+v", request.Action)
					}
					policyID = id
				}
			}
			if !seen["SetConfig"] || !seen["Status"] || !seen["Scale"] || count != 5 || writes() != 1 {
				t.Fatalf("incomplete transport probe: methods=%v count=%d writes=%d", seen, count, writes())
			}
			matchedLog := false
			for _, line := range strings.Split(logs[0].text(), "\n") {
				var entry map[string]any
				if json.Unmarshal([]byte(line), &entry) == nil && entry["@message"] == "scale request completed" && entry["policy_id"] == policyID && entry["group"] == "asg-agent" && entry["result"] == "success" {
					matchedLog = true
				}
			}
			if !matchedLog {
				t.Fatal("plugin completion log did not preserve the Agent policy ID")
			}
			t.Logf("Transport diagnostic: policy=1..20 group=2..10 requested=5; complete policy bounds absent from Target RPC; policy ID=%s supplied in ScalingAction.Meta and preserved in plugin log", policyID)
		}
		t.Logf("standard Agent waited for cloud completion; persisted last_event=%d; first cloud write count=%d", stamp, writes())
		return
	}
	fixture.mu.Lock()
	priorStatusCount := len(fixture.statuses)
	fixture.mu.Unlock()
	stop2 := start()
	wait("restarted Agent did not receive target status", time.Now().Add(3*time.Second), func() bool {
		fixture.mu.Lock()
		defer fixture.mu.Unlock()
		return len(fixture.statuses) > priorStatusCount
	})
	fixture.mu.Lock()
	firstStatus := fixture.statuses[priorStatusCount]
	fixture.mu.Unlock()
	if firstStatus.Count != 2 || firstStatus.Meta[sdk.TargetStatusMetaKeyLastEvent] != strconv.FormatInt(stamp, 10) {
		t.Fatalf("restart status did not carry persisted completion: %+v", firstStatus)
	}
	t.Logf("restart actual Target.Status: count=%d ready=%t last_event=%s; completion=%d", firstStatus.Count, firstStatus.Ready, firstStatus.Meta[sdk.TargetStatusMetaKeyLastEvent], stamp)
	until := time.Unix(0, stamp).Add(cooldown)
	if !time.Now().Before(until.Add(-time.Second)) {
		t.Fatal("restart did not leave a measurable cooldown window")
	}
	for time.Now().Before(until.Add(-100 * time.Millisecond)) {
		if configuredCooldown {
			fixture.mu.Lock()
			scales := 0
			for _, request := range fixture.requests {
				if request.Method == "Scale" {
					scales++
				}
			}
			fixture.mu.Unlock()
			if scales != 1 {
				t.Fatal("restart cooldown allowed an early Scale RPC", scales)
			}
		}
		if writes() != 1 || readState().LastCompletedAt != stamp {
			fixture.mu.Lock()
			observedWrites := append([]time.Time(nil), fixture.writes...)
			fixture.mu.Unlock()
			t.Fatalf("restart bypassed cooldown: completion=%d expected_not_before=%d observed_writes=%v journal_completion=%d", stamp, until.UnixNano(), observedWrites, readState().LastCompletedAt)
		}
		time.Sleep(20 * time.Millisecond)
	}
	wait("no evaluation after cooldown", until.Add(5*time.Second), func() bool { return writes() == 2 })
	fixture.mu.Lock()
	second := fixture.writes[1]
	fixture.mu.Unlock()
	if second.Before(until.Add(-50 * time.Millisecond)) {
		t.Fatal("second action preceded cooldown deadline")
	}
	wait("second action not completed", time.Now().Add(5*time.Second), func() bool { return readState().LastCompletedAt > stamp })
	stop2()
	t.Logf("first_completion_unix_ns=%d second_write_unix_ns=%d cooldown=%s; two original operations, no duplicate cloud write", stamp, second.UnixNano(), cooldown)
}
