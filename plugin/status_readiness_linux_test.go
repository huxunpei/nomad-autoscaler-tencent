package plugin

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
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
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/tencentcloud"
)

const readinessForeignNode = "99999999-9999-9999-9999-999999999999"

// This fixture adds externally owned drain state to the existing real-Target
// RPC helper. No recovery operation exists in its new journal. The foreign
// node is valid and visible to Nomad discovery but absent from cloud membership.
type readinessFixture struct {
	base                      *agentFixture
	mu                        sync.Mutex
	targetBlocked, cloudBusy  bool
	strategyOnly              bool
	foreignReads, nomadWrites int
}

func (f *readinessFixture) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	blocked, busy, strategyOnly := f.targetBlocked, f.cloudBusy, f.strategyOnly
	if strings.HasPrefix(r.URL.Path, "/v1/") && r.Method != http.MethodGet {
		f.nomadWrites++
		f.mu.Unlock()
		http.Error(w, "Status must not mutate Nomad", 405)
		return
	}
	foreign := r.URL.Path == "/v1/node/"+readinessForeignNode
	if foreign {
		f.foreignReads++
	}
	f.mu.Unlock()
	if foreign {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"ID": readinessForeignNode, "Meta": map[string]string{"cloud_instance_id": "ins-foreign"}, "Status": "ready", "SchedulingEligibility": "ineligible", "Drain": true, "DrainStrategy": map[string]any{"Deadline": int64(time.Minute)}, "LastDrain": map[string]any{"Status": "draining", "Meta": map[string]string{"drained_by": "operator"}}})
		return
	}
	intercept := r.URL.Path == "/fixture/cloud" || r.URL.Path == "/v1/nodes" || strings.HasPrefix(r.URL.Path, "/v1/node/")
	if !intercept {
		f.base.ServeHTTP(w, r)
		return
	}
	recorded := httptest.NewRecorder()
	f.base.ServeHTTP(recorded, r)
	if recorded.Code != 200 {
		for k, values := range recorded.Header() {
			w.Header()[k] = values
		}
		w.WriteHeader(recorded.Code)
		_, _ = w.Write(recorded.Body.Bytes())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	switch {
	case r.URL.Path == "/fixture/cloud":
		var snap tencentcloud.Snapshot
		if json.Unmarshal(recorded.Body.Bytes(), &snap) != nil {
			http.Error(w, "fixture snapshot invalid", 500)
			return
		}
		if busy {
			snap.Group.Activity = "IN_ACTIVITY"
		}
		_ = json.NewEncoder(w).Encode(snap)
	case r.URL.Path == "/v1/nodes":
		var nodes []map[string]any
		if json.Unmarshal(recorded.Body.Bytes(), &nodes) != nil {
			http.Error(w, "fixture nodes invalid", 500)
			return
		}
		nodes = append(nodes, map[string]any{"ID": readinessForeignNode})
		_ = json.NewEncoder(w).Encode(nodes)
	default:
		var node map[string]any
		if json.Unmarshal(recorded.Body.Bytes(), &node) != nil {
			http.Error(w, "fixture node invalid", 500)
			return
		}
		if blocked {
			node["Drain"] = !strategyOnly
			node["DrainStrategy"] = map[string]any{"Deadline": int64(time.Minute)}
			node["SchedulingEligibility"] = "ineligible"
			node["LastDrain"] = map[string]any{"Status": "draining", "Meta": map[string]string{"drained_by": "operator"}}
		}
		_ = json.NewEncoder(w).Encode(node)
	}
}

func TestStandardAgentIdlePoolReadiness(t *testing.T) {
	binary := os.Getenv("NOMAD_AUTOSCALER_TEST_BIN")
	if binary == "" {
		t.Skip("requires separately installed official v0.6.0 release")
	}
	data, err := os.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	version, err := exec.Command(binary, "version").CombinedOutput()
	if err != nil || !strings.Contains(string(version), "v0.6.0") {
		t.Fatal("requires verified official v0.6.0 Agent")
	}
	t.Logf("unmodified Agent=%s sha256=%x; cloud, Nomad and APM are loopback fixtures", strings.TrimSpace(string(version)), sha256.Sum256(data))
	for _, strategyOnly := range []bool{false, true} {
		name := "drain-flag"
		if strategyOnly {
			name = "strategy-with-flag-false"
		}
		t.Run(name, func(t *testing.T) { verifyIdlePoolReadiness(t, binary, strategyOnly) })
	}
}

func verifyIdlePoolReadiness(t *testing.T, binary string, strategyOnly bool) {
	t.Helper()
	root := t.TempDir()
	pluginDir := filepath.Join(root, "plugins")
	policyDir := filepath.Join(root, "policies")
	stateDir := filepath.Join(root, "state")
	for _, dir := range []string{pluginDir, policyDir, stateDir} {
		if err := os.Mkdir(dir, 0700); err != nil {
			t.Fatal(err)
		}
	}
	base := &agentFixture{count: 1, desired: 1, added: time.Now().Add(-time.Hour)}
	fixture := &readinessFixture{base: base, targetBlocked: true, strategyOnly: strategyOnly}
	server := httptest.NewServer(fixture)
	defer server.Close()
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
 config = { tencent_region = "ap-guangzhou", tencent_state_dir = %q, retry_attempts = "200" }
}
`, server.URL, server.URL, stateDir)
	configPath := filepath.Join(root, "agent.hcl")
	if err := os.WriteFile(configPath, []byte(config), 0600); err != nil {
		t.Fatal(err)
	}
	policy := `scaling "readiness" {
 enabled = true
 min = 1
 max = 3
 policy {
  cooldown = "1s"
  evaluation_interval = "200ms"
  check "desired" {
   source = "prometheus"
   query = "fixture_cpu"
   strategy "fixed-value" { value = 2 }
  }
  target "tencent-as" { tencent_group_id = "asg-agent" }
 }
}
`
	if err := os.WriteFile(filepath.Join(policyDir, "cluster.hcl"), []byte(policy), 0600); err != nil {
		t.Fatal(err)
	}
	journal := filepath.Join(stateDir, "accounts", "10001", "ap-guangzhou", "asg-agent", "live.json")
	if _, err := os.Stat(journal); !os.IsNotExist(err) {
		t.Fatal("test did not start without a journal")
	}
	readState := func() live.State { var s live.State; b, _ := os.ReadFile(journal); _ = json.Unmarshal(b, &s); return s }
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
	cmd.Env = []string{"PATH=/usr/bin:/bin", "HOME=" + root, "TENCENT_AGENT_TEST_HELPER=1", "TENCENT_AGENT_TEST_URL=" + server.URL}
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
	defer func() { t.Logf("actual Agent readiness log:\n%s", log.text()) }()
	wait := func(label string, condition func() bool) {
		t.Helper()
		deadline := time.Now().Add(12 * time.Second)
		for time.Now().Before(deadline) {
			if condition() {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Fatal(label)
	}
	statusCount := func() int { base.mu.Lock(); defer base.mu.Unlock(); return len(base.statuses) }
	assertObservedBlocked := func(label string) {
		t.Helper()
		s := readState()
		if s.Pending != nil || s.Batch != nil || s.ManualReview || s.LastCompletedAt != 0 || len(s.Members) != 1 {
			t.Fatal(label, "invented a local operation or lost cloud membership")
		}
		base.mu.Lock()
		defer base.mu.Unlock()
		if len(base.writes) != 0 {
			t.Fatal(label, "mutated cloud before readiness")
		}
		for _, request := range base.requests {
			if request.Method == "Scale" {
				t.Fatal(label, "Agent scaled while target unready")
			}
		}
		for _, status := range base.statuses {
			if status.Count != 1 || status.Ready {
				t.Fatal(label, "published ready/incorrect count while blocked")
			}
		}
		fixture.mu.Lock()
		defer fixture.mu.Unlock()
		if fixture.nomadWrites != 0 || fixture.foreignReads == 0 {
			t.Fatal(label, "mutated Nomad or failed to actually observe the foreign drained node")
		}
	}
	wait("initial external drain was not observed", func() bool {
		s := readState()
		return statusCount() >= 3 && len(s.Members) == 1 && !s.Members["ins-agent0"].Eligible
	})
	assertObservedBlocked("existing operator drain with empty journal")
	t.Log("checkpoint: existing operator drain blocks Ready with Count=1, no local operation, no Scale or cloud/Nomad mutation; foreign drained node also observed")
	// Keep at least one blocker throughout the transition so the test never
	// relies on an atomic cloud+Nomad read that the real APIs do not provide.
	fixture.mu.Lock()
	fixture.cloudBusy = true
	fixture.mu.Unlock()
	wait("cloud activity was not observed", func() bool { s := readState(); return s.Cloud != nil && s.Cloud.Group.Activity == "IN_ACTIVITY" })
	before := statusCount()
	fixture.mu.Lock()
	fixture.targetBlocked = false
	fixture.mu.Unlock()
	wait("cloud-only blocking status was not observed", func() bool {
		s := readState()
		return s.Cloud != nil && s.Cloud.Group.Activity == "IN_ACTIVITY" && s.Members["ins-agent0"].Eligible && statusCount() >= before+2
	})
	assertObservedBlocked("cloud activity after operator clears drain")
	t.Log("checkpoint: operator cleared target drain, cloud still IN_ACTIVITY, Ready remains false without writes or drain adoption")
	fixture.mu.Lock()
	fixture.cloudBusy = false
	fixture.mu.Unlock()
	wait("foreign drain incorrectly blocked normal Agent evaluation", func() bool {
		s := readState()
		return s.LastCompletedAt > 0 && s.Pending == nil && s.Batch == nil && strings.Contains(log.text(), "scale request completed")
	})
	stop()
	base.mu.Lock()
	if base.count != 2 || base.desired != 2 || len(base.writes) != 1 {
		t.Error("expected one ordinary Agent grow after both local blockers cleared")
	}
	scales := 0
	readySeen := false
	for _, request := range base.requests {
		if request.Method == "Scale" {
			scales++
			if request.Action == nil || request.Action.Count != 2 || request.Config["tencent_group_id"] != "asg-agent" {
				t.Error("normal action was not scoped to the original group")
			}
		}
	}
	for _, status := range base.statuses {
		if status.Ready {
			readySeen = true
		}
		if status.Count < 1 || status.Count > 2 {
			t.Error("foreign node contaminated cloud Count")
		}
	}
	base.mu.Unlock()
	fixture.mu.Lock()
	writes, foreignReads, blocked := fixture.nomadWrites, fixture.foreignReads, fixture.targetBlocked
	fixture.mu.Unlock()
	if scales != 1 || !readySeen || writes != 0 || foreignReads < 2 || blocked {
		t.Fatal("missing isolated ready/action/read-only evidence", scales, readySeen, writes, foreignReads)
	}
	t.Logf("checkpoint: target recovered, foreign node remains drained, normal Agent grew 1→2 once; Nomad mutations=0, foreign node reads=%d", foreignReads)
}
