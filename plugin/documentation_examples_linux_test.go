package plugin

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadtest"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/tencentcloud"
)

type documentationFixture struct {
	mu       sync.Mutex
	requests []agentObservedRequest
	statuses int
	writes   int
	errors   []string
}

func (f *documentationFixture) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if nomadtest.DisabledACL(w, r) {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	emit := func(v any) { _ = json.NewEncoder(w).Encode(v) }
	const node = "11111111-1111-1111-1111-111111111111"
	view := map[string]any{"ID": node, "ModifyIndex": uint64(1), "Status": "ready", "SchedulingEligibility": "eligible", "Drain": false, "DrainStrategy": nil, "NodeClass": "workers", "Datacenter": "dc1", "NodePool": "default", "Meta": map[string]string{"cloud_instance_id": "ins-example"}}
	switch r.URL.Path {
	case "/fixture/request":
		var v agentObservedRequest
		if json.NewDecoder(r.Body).Decode(&v) != nil {
			f.errors = append(f.errors, "request observation invalid")
			http.Error(w, "bad observation", 400)
			return
		}
		f.requests = append(f.requests, v)
		emit(true)
	case "/fixture/status":
		f.statuses++
		emit(true)
	case "/fixture/cloud":
		emit(tencentcloud.Snapshot{At: time.Now().UTC(), Region: "ap-guangzhou", Group: tencentcloud.Group{ID: "asg-example", Min: 1, Max: 5, Count: 1, Desired: 1, Enabled: "ENABLED", Status: "NORMAL", Activity: "NOT_IN_ACTIVITY", RetryPolicy: "NO_RETRY", LaunchConfiguration: "asc-example"}, Members: []tencentcloud.Member{{ID: "ins-example", CreationType: "AUTO_CREATION", Lifecycle: "IN_SERVICE", AddedAt: time.Now().Add(-time.Hour).UTC().Format(time.RFC3339)}}, VMs: map[string]tencentcloud.VM{"ins-example": {ID: "ins-example", State: "RUNNING", Returned: true, ChargeType: "POSTPAID_BY_HOUR"}}, Policies: []tencentcloud.Policy{}, LaunchConfiguration: tencentcloud.LaunchConfiguration{ID: "asc-example", Status: "NORMAL", ChargeType: "POSTPAID_BY_HOUR", InstanceType: "S5.SMALL2", ImageID: "img-example", VersionNumber: 1}})
	case "/v1/nodes":
		w.Header().Set("X-Nomad-Index", "1")
		emit([]any{view})
	case "/v1/node/" + node:
		w.Header().Set("X-Nomad-Index", "1")
		emit(view)
	case "/v1/node/" + node + "/allocations":
		w.Header().Set("X-Nomad-Index", "1")
		emit([]any{})
	case "/api/v1/query_range":
		start, _ := strconv.ParseFloat(r.URL.Query().Get("start"), 64)
		end, _ := strconv.ParseFloat(r.URL.Query().Get("end"), 64)
		emit(map[string]any{"status": "success", "data": map[string]any{"resultType": "matrix", "result": []any{map[string]any{"metric": map[string]string{}, "values": []any{[]any{start, "70"}, []any{end, "70"}}}}}})
	case "/fixture/desired", "/fixture/remove":
		f.writes++
		http.Error(w, "writes forbidden in documentation check", 500)
	default:
		f.errors = append(f.errors, r.URL.Path)
		http.Error(w, "unexpected fixture path", 404)
	}
}

// The unmodified Agent parses actual files. Only paths/addresses are relocated;
// the second phase enables an isolated copy against a no-op cloud/APM model.
func TestStandardAgentDocumentationExamples(t *testing.T) {
	binary := os.Getenv("NOMAD_AUTOSCALER_TEST_BIN")
	if binary == "" {
		t.Skip("requires a pinned standard Agent on the Linux test host")
	}
	expected := os.Getenv("NOMAD_AUTOSCALER_TEST_SHA256")
	data, err := os.ReadFile(binary)
	if err != nil || !filepath.IsAbs(binary) {
		t.Fatal("Agent binary unavailable")
	}
	sum := sha256.Sum256(data)
	if len(expected) != 64 || hex.EncodeToString(sum[:]) != expected {
		t.Fatal("Agent SHA256 pin mismatch")
	}
	version, err := exec.Command(binary, "version").CombinedOutput()
	if err != nil || !strings.Contains(string(version), "v0.6.0") {
		t.Fatal("expected pinned official v0.6.0")
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	agentRaw, err := os.ReadFile("../examples/autoscaler.hcl")
	if err != nil {
		t.Fatal(err)
	}
	report := map[string]any{"go_version": runtime.Version(), "agent_version": strings.TrimSpace(string(version)), "agent_sha256": expected, "scope": "official parser + actual Target config + synthetic no-op cloud/Nomad/APM; no credentials or actual scaling"}
	cases := []map[string]any{}
	for _, name := range []string{"cluster-fixed.hcl", "cluster-prometheus.hcl"} {
		for _, active := range []bool{false, true} {
			t.Run(name+"/"+strconv.FormatBool(active), func(t *testing.T) {
				raw, err := os.ReadFile(filepath.Join("../examples", name))
				if err != nil {
					t.Fatal(err)
				}
				if !strings.Contains(string(raw), "enabled = false") {
					t.Fatal("shipped example must be disabled")
				}
				root := t.TempDir()
				plugins := filepath.Join(root, "plugins")
				policies := filepath.Join(root, "policies")
				state := filepath.Join(root, "state")
				for _, p := range []string{plugins, policies, state} {
					if err := os.Mkdir(p, 0700); err != nil {
						t.Fatal(err)
					}
				}
				fixture := &documentationFixture{}
				server := httptest.NewServer(fixture)
				defer server.Close()
				cfg := strings.NewReplacer("/opt/nomad-autoscaler/plugins", plugins, "/etc/nomad-autoscaler/policies", policies, "/var/lib/nomad-autoscaler/tencent-as", state, "http://127.0.0.1:4646", server.URL, "http://127.0.0.1:9090", server.URL).Replace(string(agentRaw))
				listener, err := net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				port := listener.Addr().(*net.TCPAddr).Port
				_ = listener.Close()
				cfg = strings.Replace(cfg, "bind_port = 8080", "bind_port = "+strconv.Itoa(port), 1)
				configPath := filepath.Join(root, "agent.hcl")
				if err := os.WriteFile(configPath, []byte(cfg), 0600); err != nil {
					t.Fatal(err)
				}
				policy := string(raw)
				if active {
					policy = strings.Replace(policy, "enabled = false", "enabled = true", 1)
				}
				if err := os.WriteFile(filepath.Join(policies, name), []byte(policy), 0600); err != nil {
					t.Fatal(err)
				}
				quote := func(s string) string { return "'" + strings.ReplaceAll(s, "'", "'\"'\"'") + "'" }
				wrapper := "#!/bin/sh\nexec " + quote(executable) + " -test.run='^TestStandardAgentPluginHelper$'\n"
				if err := os.WriteFile(filepath.Join(plugins, Name), []byte(wrapper), 0700); err != nil {
					t.Fatal(err)
				}
				logs := &agentLog{}
				cmd := exec.Command(binary, "agent", "-config="+configPath, "-log-level=DEBUG", "-log-json")
				cmd.Env = []string{"PATH=/usr/bin:/bin", "HOME=" + root, "TENCENT_AGENT_TEST_HELPER=1", "TENCENT_AGENT_TEST_URL=" + server.URL, "CHECKPOINT_DISABLE=1"}
				cmd.Stdout = logs
				cmd.Stderr = logs
				cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
				if err := cmd.Start(); err != nil {
					t.Fatal(err)
				}
				done := make(chan struct{})
				go func() { _ = cmd.Wait(); close(done) }()
				stop := func() {
					select {
					case <-done:
						return
					default:
					}
					_ = cmd.Process.Signal(os.Interrupt)
					select {
					case <-done:
					case <-time.After(8 * time.Second):
						_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
						<-done
						t.Error("Agent did not stop")
					}
				}
				defer stop()
				ready := false
				until := time.Now().Add(20 * time.Second)
				for time.Now().Before(until) {
					select {
					case <-done:
						t.Fatalf("Agent exited: %s", logs.text())
					default:
					}
					fixture.mu.Lock()
					statuses := fixture.statuses
					fixture.mu.Unlock()
					if strings.Contains(logs.text(), "found policy in file") && (!active || statuses > 0) {
						ready = true
						break
					}
					time.Sleep(20 * time.Millisecond)
				}
				if !ready {
					t.Fatalf("example not loaded/validated: %s", logs.text())
				}
				time.Sleep(300 * time.Millisecond)
				stop()
				fixture.mu.Lock()
				defer fixture.mu.Unlock()
				configured, validated := false, !active
				for _, r := range fixture.requests {
					if r.Method == "SetConfig" && r.Config["tencent_region"] == "ap-guangzhou" {
						configured = true
					}
					if r.Method == "Status" && r.Config["tencent_group_id"] == "asg-example" && r.Config["node_drain_deadline"] == "10m" {
						validated = true
					}
				}
				if !configured || !validated || fixture.writes != 0 || len(fixture.errors) != 0 {
					t.Fatalf("example config/no-op assertions: configured=%v validated=%v writes=%d errors=%v", configured, validated, fixture.writes, fixture.errors)
				}
				s := sha256.Sum256(raw)
				cases = append(cases, map[string]any{"file": name, "source_sha256": hex.EncodeToString(s[:]), "activated_copy": active, "pid": cmd.Process.Pid, "process_terminal": cmd.ProcessState != nil, "data_directory": root, "agent_log": logs.text(), "target_status_calls": fixture.statuses, "cloud_writes": fixture.writes, "config_validated": validated})
			})
		}
	}
	report["cases"] = cases
	report["passed"] = !t.Failed()
	if path := os.Getenv("NOMAD_DOCUMENTATION_REPORT"); path != "" {
		if !filepath.IsAbs(path) {
			t.Fatal("report must be absolute")
		}
		raw, err := json.MarshalIndent(report, "", "  ")
		if err != nil || os.WriteFile(path, raw, 0600) != nil {
			t.Fatal("report write failed")
		}
	}
}
