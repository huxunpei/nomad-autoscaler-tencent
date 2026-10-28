package plugin

import (
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
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/huxunpei/nomad-autoscaler-tencent/internal/live"
)

// AWS is the real built-in Target from the unmodified Agent release. Only its
// service endpoint is overridden using the AWS SDK's documented environment
// setting. Tencent uses the existing external RPC helper and real router/engine.
// All credentials and HTTP responses below are synthetic and loopback-only.
type multiProviderAWS struct {
	mu      sync.Mutex
	count   int
	actions []string
	writes  []int
	invalid []string
}

func (f *multiProviderAWS) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fail := func(reason string) { f.invalid = append(f.invalid, reason); http.Error(w, reason, 400) }
	if r.Method != http.MethodPost || r.ParseForm() != nil || r.Form.Get("Version") != "2011-01-01" || r.Header.Get("Authorization") == "" {
		fail("unexpected synthetic AWS request envelope")
		return
	}
	action := r.Form.Get("Action")
	f.actions = append(f.actions, action)
	group := r.Form.Get("AutoScalingGroupName")
	if action == "DescribeAutoScalingGroups" {
		group = r.Form.Get("AutoScalingGroupNames.member.1")
	}
	if group != "aws-isolated" {
		fail("AWS received another provider's group")
		return
	}
	result := ""
	switch action {
	case "DescribeAutoScalingGroups":
		instances := ""
		for i := 0; i < f.count; i++ {
			instances += fmt.Sprintf("<member><InstanceId>i-synthetic%d</InstanceId><HealthStatus>Healthy</HealthStatus><LifecycleState>InService</LifecycleState><ProtectedFromScaleIn>false</ProtectedFromScaleIn></member>", i)
		}
		result = fmt.Sprintf("<AutoScalingGroups><member><AutoScalingGroupName>aws-isolated</AutoScalingGroupName><MinSize>1</MinSize><MaxSize>10</MaxSize><DesiredCapacity>%d</DesiredCapacity><AvailabilityZones><member>us-east-1a</member></AvailabilityZones><Instances>%s</Instances></member></AutoScalingGroups>", f.count, instances)
	case "DescribeScalingActivities":
		result = "<Activities/>"
	case "DescribeInstanceRefreshes":
		result = "<InstanceRefreshes/>"
	case "UpdateAutoScalingGroup":
		n, err := strconv.Atoi(r.Form.Get("DesiredCapacity"))
		if err != nil || n <= f.count || n > 10 {
			fail("unexpected synthetic AWS target")
			return
		}
		f.count = n
		f.writes = append(f.writes, n)
	default:
		fail("unexpected AWS action")
		return
	}
	w.Header().Set("Content-Type", "text/xml")
	fmt.Fprintf(w, `<%sResponse xmlns="http://autoscaling.amazonaws.com/doc/2011-01-01/"><%sResult>%s</%sResult><ResponseMetadata><RequestId>synthetic-aws-request</RequestId></ResponseMetadata></%sResponse>`, action, action, result, action, action)
}

func TestStandardAgentMultiProviderIsolation(t *testing.T) {
	verifyStandardAgentMultiProvider(t, false)
}

// This remains an acceptance regression, not an expected-failure diagnostic.
// Official v0.6.0 currently fails its valid same-ID file policy update.
func TestStandardAgentMultiProviderPolicyReload(t *testing.T) {
	verifyStandardAgentMultiProvider(t, true)
}

func verifyStandardAgentMultiProvider(t *testing.T, checkReload bool) {
	t.Helper()
	binary := os.Getenv("NOMAD_AUTOSCALER_TEST_BIN")
	if binary == "" {
		t.Skip("requires separately installed standard v0.6.0 release with AWS SDK V2")
	}
	data, err := os.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	version, err := exec.Command(binary, "version").CombinedOutput()
	if err != nil || !strings.Contains(string(version), "v0.6.0") {
		t.Fatal("requires verified standard v0.6.0 Agent")
	}
	t.Logf("unmodified Agent=%s sha256=%x; Tencent cloud/Nomad and AWS APIs are synthetic loopback services", strings.TrimSpace(string(version)), sha256.Sum256(data))
	root := t.TempDir()
	pluginDir := filepath.Join(root, "plugins")
	policyDir := filepath.Join(root, "policies")
	stateDir := filepath.Join(root, "state")
	for _, dir := range []string{pluginDir, policyDir, stateDir} {
		if err := os.Mkdir(dir, 0700); err != nil {
			t.Fatal(err)
		}
	}
	tencent := &agentFixture{count: 1, desired: 1, hold: true, added: time.Now().Add(-time.Hour)}
	tencentServer := httptest.NewServer(tencent)
	defer tencentServer.Close()
	aws := &multiProviderAWS{count: 3}
	awsServer := httptest.NewServer(aws)
	defer awsServer.Close()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	quote := func(s string) string { return "'" + strings.ReplaceAll(s, "'", "'\"'\"'") + "'" }
	if err := os.WriteFile(filepath.Join(pluginDir, Name), []byte("#!/bin/sh\nexec "+quote(executable)+" -test.run=^TestStandardAgentPluginHelper$\n"), 0700); err != nil {
		t.Fatal(err)
	}
	config := fmt.Sprintf(`nomad { address = %q }
apm "prometheus" {
 driver = "prometheus"
 config = { address = %q }
}
target "tencent-as" {
 driver = "tencent-as"
 config = { tencent_region = "ap-guangzhou", tencent_state_dir = %q, retry_attempts = "2000", tencent_metrics_listen_address = "127.0.0.1:0" }
}
target "aws-asg" {
 driver = "aws-asg"
 config = { aws_region = "us-east-1", aws_access_key_id = "SYNTHETIC_AWS_ID", aws_secret_access_key = "synthetic-loopback-only", retry_attempts = "2" }
}
`, tencentServer.URL, tencentServer.URL, stateDir)
	configPath := filepath.Join(root, "agent.hcl")
	if err := os.WriteFile(configPath, []byte(config), 0600); err != nil {
		t.Fatal(err)
	}
	policy := func(name, target, values string, n int) string {
		return fmt.Sprintf(`scaling %q {
 enabled = true
 min = 1
 max = 10
 policy {
  cooldown = "100ms"
  evaluation_interval = "200ms"
  check "desired" {
   source = "prometheus"
   query = "fixture_cpu"
   strategy "fixed-value" { value = %d }
  }
  target %q {
   %s
  }
 }
}
`, name, n, target, values)
	}
	writePolicy := func(name, body string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(policyDir, name+".hcl"), []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
	}
	writePolicy("tencent", policy("tencent-only", "tencent-as", `tencent_group_id = "asg-agent"`, 2))
	writePolicy("aws", policy("aws-only", "aws-asg", `aws_asg_name = "aws-isolated"
   node_class = "aws-lab"`, 4))
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	log := &agentLog{}
	cmd := exec.Command(binary, "agent", "-config="+configPath, "-plugin-dir="+pluginDir, "-policy-dir="+policyDir, "-policy-source-disable-nomad", "-http-bind-address=127.0.0.1", "-http-bind-port="+strconv.Itoa(port), "-log-level=DEBUG", "-log-json")
	// Deliberate allowlist: no inherited cloud credentials, profiles, proxies or
	// metadata sources. AWS SDK V2 is forced to the local fixture for all services.
	cmd.Env = []string{"PATH=/usr/bin:/bin", "HOME=" + root, "TENCENT_AGENT_TEST_HELPER=1", "TENCENT_AGENT_TEST_URL=" + tencentServer.URL, "AWS_ENDPOINT_URL=" + awsServer.URL, "AWS_EC2_METADATA_DISABLED=true", "AWS_CONFIG_FILE=/dev/null", "AWS_SHARED_CREDENTIALS_FILE=/dev/null", "AWS_MAX_ATTEMPTS=1"}
	cmd.Stdout, cmd.Stderr = log, log
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
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
	defer func() { t.Logf("single Agent multi-provider log:\n%s", log.text()) }()
	wait := func(label string, condition func() bool) {
		t.Helper()
		deadline := time.Now().Add(15 * time.Second)
		for time.Now().Before(deadline) {
			if condition() {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Fatal(label)
	}
	journal := filepath.Join(stateDir, "accounts", "10001", "ap-guangzhou", "asg-agent", "live.json")
	readState := func() live.State { var s live.State; b, _ := os.ReadFile(journal); _ = json.Unmarshal(b, &s); return s }
	wait("both provider actions were not routed", func() bool {
		tencent.mu.Lock()
		tc := len(tencent.writes)
		tencent.mu.Unlock()
		aws.mu.Lock()
		ac := len(aws.writes)
		aws.mu.Unlock()
		return tc == 1 && ac == 1 && strings.Contains(log.text(), "successfully performed and verified scaling out")
	})
	// AWS must complete while the Tencent cloud remains unsettled. A shared
	// provider lock, swapped status or accidental target dispatch fails here.
	if s := readState(); s.Pending == nil || s.LastCompletedAt != 0 {
		t.Fatal("Tencent completion/state was contaminated by AWS success")
	}
	tencent.mu.Lock()
	if tencent.count != 1 || tencent.desired != 2 {
		tencent.mu.Unlock()
		t.Fatal("Tencent target was not independent")
	}
	tencent.hold = false
	tencent.count = tencent.desired
	tencent.mu.Unlock()
	wait("Tencent action did not finish", func() bool { return readState().LastCompletedAt > 0 })
	initialState := readState()
	if initialState.LastBatch == nil || initialState.LastOperation == nil {
		t.Fatal("missing completed Tencent operation")
	}
	stamp := readState().LastCompletedAt
	if checkReload {
		writePolicy("aws", policy("aws-only", "aws-asg", `aws_asg_name = "aws-isolated"
   node_class = "aws-lab"`, 5))
		if err := cmd.Process.Signal(syscall.SIGHUP); err != nil {
			t.Fatal(err)
		}
		wait("AWS policy update was not evaluated", func() bool { aws.mu.Lock(); defer aws.mu.Unlock(); return len(aws.writes) == 2 })
	}
	metricsAddress := ""
	for _, line := range strings.Split(log.text(), "\n") {
		var v map[string]any
		if json.Unmarshal([]byte(line), &v) == nil && v["@message"] == "Tencent metrics endpoint started" {
			metricsAddress, _ = v["address"].(string)
		}
	}
	if !strings.HasPrefix(metricsAddress, "127.0.0.1:") {
		t.Fatal("Tencent's isolated metrics listener was not started")
	}
	response, err := (&http.Client{Timeout: time.Second}).Get("http://" + metricsAddress + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	metrics, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil || response.StatusCode != 200 || !strings.Contains(string(metrics), `tencent_as_scale_requests_total{result="success"} 1`+"\n") || strings.Contains(string(metrics), "aws-isolated") {
		t.Fatal("AWS actions contaminated Tencent's result metrics")
	}
	stop()
	expectedCount, expectedWrites := 4, 1
	if checkReload {
		expectedCount, expectedWrites = 5, 2
	}
	aws.mu.Lock()
	if aws.count != expectedCount || len(aws.writes) != expectedWrites || aws.writes[0] != 4 || (checkReload && aws.writes[1] != 5) || len(aws.invalid) > 0 {
		t.Errorf("AWS routing/endpoint failures: count=%d writes=%v invalid=%v", aws.count, aws.writes, aws.invalid)
	}
	actions := append([]string(nil), aws.actions...)
	aws.mu.Unlock()
	tencent.mu.Lock()
	if tencent.count != 2 || tencent.desired != 2 || len(tencent.writes) != 1 {
		t.Error("AWS update changed Tencent capacity")
	}
	scaleCalls := 0
	statusCalls := 0
	policyID := ""
	for _, request := range tencent.requests {
		if request.Method == "Status" || request.Method == "Scale" {
			if request.Config["tencent_group_id"] != "asg-agent" || request.Config["aws_asg_name"] != "" {
				t.Error("another provider's config entered Tencent RPC")
			}
		}
		if request.Method == "Status" {
			statusCalls++
		}
		if request.Method == "Scale" {
			scaleCalls++
			if request.Action == nil || request.Action.Count != 2 {
				t.Error("AWS target count entered Tencent Scale")
			} else {
				policyID, _ = request.Action.Meta["nomad_policy_id"].(string)
			}
		}
	}
	tencent.mu.Unlock()
	if scaleCalls != 1 || statusCalls < 2 || policyID == "" {
		t.Fatal("missing distinct normal Tencent policy evaluation", scaleCalls, statusCalls)
	}
	finalState := readState()
	if finalState.LastBatch == nil || finalState.LastOperation == nil || initialState.LastBatch.ID != finalState.LastBatch.ID || initialState.LastOperation.ID != finalState.LastOperation.ID || finalState.LastCompletedAt != stamp {
		t.Fatal("AWS policy update replaced Tencent operation/completion")
	}
	for _, required := range []string{"DescribeAutoScalingGroups", "DescribeScalingActivities", "DescribeInstanceRefreshes", "UpdateAutoScalingGroup"} {
		if !strings.Contains(strings.Join(actions, ","), required) {
			t.Error("missing actual built-in AWS action", required)
		}
	}
	// The Agent has stopped, so its external plugin listener must also close.
	if !strings.HasPrefix(metricsAddress, "127.0.0.1:") {
		t.Fatal("Tencent's isolated metrics listener was not started")
	}
	if response, err := (&http.Client{Timeout: time.Second}).Get("http://" + metricsAddress + "/metrics"); err == nil {
		response.Body.Close()
		t.Fatal("plugin metrics listener survived Agent shutdown")
	}
	t.Logf("AWS actual SDK actions=%v; AWS desired=3→%d, Tencent=1→2 with one isolated policy=%s; reload-check=%t", actions, expectedCount, policyID, checkReload)
}
