package live

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/hashicorp/nomad/api"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadclient"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/tencentcloud"
)

// Uses an unmodified official Agent and a test-only external target executable.
// Target routing, SDK, journal and Nomad are real; only cloud authentication/API
// is replaced by an explicit loopback model. This is not live AS acceptance.
func TestRealNomadStandardAgentInheritsTLSACLAndDrains(t *testing.T) {
	verifyRealNomadStandardAgent(t, false, false)
}

func TestRealNomadStandardAgentFailedDeleteAndNextEvaluation(t *testing.T) {
	verifyRealNomadStandardAgent(t, true, false)
}

func TestRealNomadStandardAgentPreDeleteAbortAndNextEvaluation(t *testing.T) {
	verifyRealNomadStandardAgent(t, false, true)
}

func verifyRealNomadStandardAgent(t *testing.T, failedDelete, preDeleteAbort bool) {
	t.Helper()
	nomad, agent, helper := os.Getenv("NOMAD_REAL_BINARY"), os.Getenv("NOMAD_AUTOSCALER_TEST_BIN"), os.Getenv("NOMAD_AGENT_HELPER_BINARY")
	if nomad == "" || agent == "" || helper == "" {
		t.Skip("requires separately pinned real Nomad, official Agent and test-only plugin executable on the Linux test host")
	}
	pin := func(path, expected string) string {
		t.Helper()
		data, e := os.ReadFile(path)
		if e != nil || !filepath.IsAbs(path) {
			t.Fatal("required test executable unavailable")
		}
		h := sha256.Sum256(data)
		got := hex.EncodeToString(h[:])
		if expected != "" && got != expected {
			t.Fatal("test executable pin mismatch")
		}
		return got
	}
	if !digestPattern.MatchString(os.Getenv("NOMAD_REAL_SHA256")) || !digestPattern.MatchString(os.Getenv("NOMAD_AUTOSCALER_TEST_SHA256")) {
		t.Fatal("real runtime binaries require explicit SHA256 pins")
	}
	report := map[string]any{"started_at": time.Now().UTC(), "scope": "official Agent + actual Nomad TLS/ACL/task lifecycle; cloud authentication/API are loopback models", "nomad_binary_sha256": pin(nomad, os.Getenv("NOMAD_REAL_SHA256")), "agent_binary_sha256": pin(agent, os.Getenv("NOMAD_AUTOSCALER_TEST_SHA256")), "test_plugin_sha256": pin(helper, "")}
	version, e := exec.Command(agent, "version").CombinedOutput()
	if e != nil || !strings.Contains(string(version), "v0.6.0") {
		t.Fatal("requires verified official v0.6.0")
	}
	report["agent_version"] = strings.TrimSpace(string(version))
	dir, err := os.MkdirTemp("", "na-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	report["data_directory"] = dir
	recoveryScenario := failedDelete || preDeleteAbort
	purge := os.Getenv("NOMAD_REAL_AGENT_PURGE") == "true" && !recoveryScenario
	report["failed_delete_scenario"] = failedDelete
	report["pre_delete_abort_scenario"] = preDeleteAbort
	report["node_purge"] = purge
	controllerInstance := testInstance
	if purge {
		controllerInstance = "ins-control"
	}
	pki := newNomadRuntimePKI(t, dir)
	address, cmd, done := startRealNomadACLTLS(t, nomad, dir, pki, report, controllerInstance)
	var admin *api.Client
	var currentAgent *exec.Cmd
	var agentDone <-chan struct{}
	var spare *exec.Cmd
	var spareDone <-chan struct{}
	var spareAddress string
	stopSpare := func() {
		if spare == nil {
			return
		}
		select {
		case <-spareDone:
			return
		default:
		}
		_ = spare.Process.Signal(os.Interrupt)
		select {
		case <-spareDone:
		case <-time.After(8 * time.Second):
			_ = syscall.Kill(-spare.Process.Pid, syscall.SIGKILL)
			<-spareDone
			t.Error("selected Nomad client did not stop")
		}
	}

	var server *httptest.Server
	logs := &realAgentRuntimeLog{}
	secrets := []string{}
	jobID := "nomad-spec-agent-poststop"
	jobSubmitted := false
	extraJobs := []string{}
	stopAgent := func() {
		if currentAgent == nil {
			return
		}
		select {
		case <-agentDone:
			return
		default:
		}
		_ = currentAgent.Process.Signal(os.Interrupt)
		select {
		case <-agentDone:
		case <-time.After(8 * time.Second):
			_ = syscall.Kill(-currentAgent.Process.Pid, syscall.SIGKILL)
			<-agentDone
			t.Error("official Agent did not stop gracefully")
		}
	}
	defer func() {
		stopAgent()
		if server != nil {
			server.Close()
		}
		report["agent_process_terminal"] = currentAgent == nil || currentAgent.ProcessState != nil
		terminal := !jobSubmitted
		if admin != nil && jobSubmitted {
			for _, extra := range extraJobs {
				_, _, _ = admin.Jobs().Deregister(extra, true, nil)
			}
			_, _, _ = admin.Jobs().Deregister(jobID, true, nil)
			until := time.Now().Add(15 * time.Second)
			for time.Now().Before(until) {
				terminal = true
				for _, ownedJob := range append([]string{jobID}, extraJobs...) {
					rows, _, e := admin.Jobs().Allocations(ownedJob, false, nil)
					terminal = terminal && e == nil
					for _, row := range rows {
						a, _, e := admin.Allocations().Info(row.ID, nil)
						if e != nil {
							terminal = false
							continue
						}
						if a.ClientStatus != "complete" && a.ClientStatus != "failed" && a.ClientStatus != "lost" {
							terminal = false
						}
						for _, state := range a.TaskStates {
							if state == nil || state.State != "dead" {
								terminal = false
							}
						}
					}
				}
				if terminal {
					break
				}
				time.Sleep(100 * time.Millisecond)
			}
		}
		stopSpare()
		if spare != nil {
			report["selected_client_process_terminal"] = spare.ProcessState != nil
			conn, e := net.DialTimeout("tcp", strings.TrimPrefix(spareAddress, "http://"), 200*time.Millisecond)
			if e == nil {
				_ = conn.Close()
			}
			report["selected_client_listener_closed"] = e != nil
		}
		report["owned_tasks_terminal"] = terminal
		if !terminal {
			t.Error("owned tasks did not stop")
		}
		_ = cmd.Process.Signal(os.Interrupt)
		select {
		case <-done:
		case <-time.After(8 * time.Second):
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
			<-done
			t.Error("Nomad did not stop gracefully")
		}
		report["nomad_process_terminal"] = cmd.ProcessState != nil
		mounts, _ := os.ReadFile("/proc/self/mountinfo")
		unmounted := []string{}
		for _, line := range strings.Split(string(mounts), "\n") {
			f := strings.Fields(line)
			if len(f) > 5 && strings.HasPrefix(f[4], dir+"/") && strings.Contains(f[4], "/data/alloc/") && strings.Contains(line, " - tmpfs tmpfs ") {
				if e := syscall.Unmount(f[4], 0); e != nil {
					t.Error("owned mount cleanup failed")
				} else {
					unmounted = append(unmounted, f[4])
				}
			}
		}
		report["owned_tmpfs_unmounted"] = unmounted
		raw, _ := os.ReadFile(filepath.Join(dir, "nomad.log"))
		redact := func(raw string) string {
			lines := strings.Split(raw, "\n")
			sensitive := regexp.MustCompile(`(?i)token|secret|credential|password|-----BEGIN`)
			for i, line := range lines {
				for _, value := range secrets {
					line = strings.ReplaceAll(line, value, "<redacted>")
				}
				if sensitive.MatchString(line) {
					line = "[credential-related log line omitted]"
				}
				lines[i] = line
			}
			return strings.Join(lines, "\n")
		}
		report["nomad_log"] = redact(string(raw))
		report["agent_log"] = redact(logs.text())
		conn, e := net.DialTimeout("tcp", strings.TrimPrefix(address, "https://"), 200*time.Millisecond)
		if e == nil {
			_ = conn.Close()
		}
		report["https_listener_closed"] = e != nil
		if e := os.RemoveAll(dir); e != nil {
			t.Error("private test directory cleanup failed")
		}
		_, e = os.Stat(dir)
		report["private_key_and_data_directory_removed"] = os.IsNotExist(e)
		report["test_failed"] = t.Failed()
		report["ended_at"] = time.Now().UTC()
		data := jsonBytes(report)
		for _, value := range secrets {
			if strings.Contains(string(data), value) {
				t.Error("ephemeral Nomad credential leaked into report")
				return
			}
		}
		if path := os.Getenv("NOMAD_REAL_AGENT_REPORT"); path != "" {
			if !filepath.IsAbs(path) {
				t.Error("report must be absolute")
			} else if e := os.WriteFile(path, data, 0600); e != nil {
				t.Error("report write failed")
			}
		}
	}()
	cfg := nomadclient.Config{Address: address, CACert: pki.ca, ClientCert: pki.clientCert, ClientKey: pki.clientKey, TLSServerName: "server.global.nomad", Timeout: 5 * time.Second}
	transport, err := nomadclient.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer transport.Close()
	apiConfig := transport.SDKConfig(nil)
	apiConfig.Namespace = "default"
	admin, err = api.NewClient(apiConfig)
	if err != nil {
		t.Fatal(err)
	}
	waitRealNomad(t, 20*time.Second, "isolated TLS leader", func() bool { leader, e := admin.Status().Leader(); return e == nil && leader != "" })
	management, _, err := admin.ACLTokens().Bootstrap(nil)
	if err != nil {
		t.Fatal("isolated bootstrap failed")
	}
	secrets = append(secrets, management.SecretID)
	admin.SetSecretID(management.SecretID)
	if _, err = admin.ACLPolicies().Upsert(&api.ACLPolicy{Name: "nomad-spec-agent", Rules: `node { policy = "write" } namespace "*" { capabilities = ["read-job"] }`}, nil); err != nil {
		t.Fatal("isolated policy creation failed")
	}
	token, _, err := admin.ACLTokens().Create(&api.ACLToken{Name: "nomad-spec-agent", Type: "client", Policies: []string{"nomad-spec-agent"}}, nil)
	if err != nil {
		t.Fatal("isolated token creation failed")
	}
	secrets = append(secrets, token.SecretID)
	report["nomad_client_accessor_id"] = token.AccessorID
	var node *api.Node
	waitRealNomad(t, 40*time.Second, "real ready raw_exec node", func() bool {
		nodes, _, e := admin.Nodes().List(nil)
		if e != nil || len(nodes) != 1 {
			return false
		}
		n, _, e := admin.Nodes().Info(nodes[0].ID, nil)
		if e != nil || n.Status != "ready" || n.Meta["cloud_instance_id"] != controllerInstance {
			return false
		}
		driver := n.Drivers["raw_exec"]
		if driver == nil || !driver.Detected || !driver.Healthy {
			return false
		}
		node = n
		return true
	})
	if purge || recoveryScenario {
		rpc, e := admin.Status().Leader()
		if e != nil {
			t.Fatal("RPC leader unavailable")
		}
		spareInstance := testInstance
		if recoveryScenario {
			spareInstance = "ins-spare"
		}
		spareAddress, spare, spareDone = startRealNomadClientForRecovery(t, nomad, filepath.Join(dir, "c"), rpc, report, spareInstance)
		waitRealNomad(t, 40*time.Second, "separate target client registered with ACL server", func() bool {
			rows, _, e := admin.Nodes().List(nil)
			if e != nil {
				return false
			}
			for _, row := range rows {
				n, _, e := admin.Nodes().Info(row.ID, nil)
				if e != nil || n.Meta["cloud_instance_id"] != spareInstance || n.Status != "ready" {
					continue
				}
				driver := n.Drivers["raw_exec"]
				if driver != nil && driver.Detected && driver.Healthy {
					if !recoveryScenario {
						node = n
					}
					return true
				}
			}
			return false
		})
	}
	report["node_id"] = node.ID
	job := realRecoveryJob(jobID, node.ID, "batch", true)
	job.TaskGroups[0].Tasks[0].Config["args"] = []string{"15"}
	if _, _, err = admin.Jobs().Register(job, nil); err != nil {
		t.Fatal("owned job registration failed")
	}
	jobSubmitted = true
	allocationID := ""
	waitRealNomad(t, 15*time.Second, "real batch running before Agent", func() bool {
		rows, _, e := admin.Jobs().Allocations(jobID, false, nil)
		if e != nil {
			return false
		}
		for _, a := range rows {
			if a.ClientStatus == "running" && a.NodeID == node.ID {
				allocationID = a.ID
				return true
			}
		}
		return false
	})
	report["allocation_id"] = allocationID
	stateDir := filepath.Join(dir, "state")
	pluginDir := filepath.Join(dir, "plugins")
	policyDir := filepath.Join(dir, "policies")
	for _, path := range []string{stateDir, pluginDir, policyDir} {
		if e := os.Mkdir(path, 0700); e != nil {
			t.Fatal(e)
		}
	}
	journal := filepath.Join(stateDir, "accounts", "10001", "ap-guangzhou", testGroup, "live.json")
	readState := func() State { var s State; raw, _ := os.ReadFile(journal); _ = json.Unmarshal(raw, &s); return s }
	fixture := &realAgentCloudFixture{cloud: newCloud(), admin: admin, nodeID: node.ID, allocationID: allocationID, journal: journal}
	fixture.cloud.addMember()
	fixture.failedDelete, fixture.preDeleteAbort = failedDelete, preDeleteAbort
	if recoveryScenario {
		fixture.cloud.snap.Group.Count, fixture.cloud.snap.Group.Desired = 2, 2
		fixture.cloud.snap.Group.Min, fixture.cloud.snap.Group.Max = 1, 3
		fixture.cloud.snap.Members = append(fixture.cloud.snap.Members, tencentcloud.Member{ID: "ins-spare", CreationType: "AUTO_CREATION", Lifecycle: "IN_SERVICE", Protected: true, AddedAt: time.Now().UTC().Format(time.RFC3339)})
		fixture.cloud.snap.VMs["ins-spare"] = tencentcloud.VM{ID: "ins-spare", State: "RUNNING", ChargeType: "POSTPAID_BY_HOUR", Returned: true}
	}
	if purge {
		fixture.afterRemoval = stopSpare
	}
	server = httptest.NewServer(fixture)
	quote := func(value string) string { return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'" }
	// Removing all Nomad fallback env from the plugin proves that credentials
	// arrive via the Agent's SetConfig inheritance, not via inherited process env.
	wrapper := "#!/bin/sh\nunset NOMAD_ADDR NOMAD_TOKEN NOMAD_HTTP_AUTH NOMAD_CACERT NOMAD_CAPATH NOMAD_CLIENT_CERT NOMAD_CLIENT_KEY NOMAD_TLS_SERVER_NAME NOMAD_NAMESPACE NOMAD_REGION NOMAD_SKIP_VERIFY NOMAD_ALLOC_ID\nexec " + quote(helper) + " -test.run='^TestStandardAgentPluginHelper$'\n"
	if err = os.WriteFile(filepath.Join(pluginDir, "tencent-as"), []byte(wrapper), 0700); err != nil {
		t.Fatal(err)
	}
	agentConfig := fmt.Sprintf(`nomad {
 address = %q
 namespace = "default"
 ca_cert = %q
 client_cert = %q
 client_key = %q
 tls_server_name = "server.global.nomad"
}
target "tencent-as" {
 driver = "tencent-as"
 config = { tencent_region = "ap-guangzhou", tencent_state_dir = %q, retry_attempts = "200" }
}
`, address, pki.ca, pki.clientCert, pki.clientKey, stateDir)
	if recoveryScenario {
		agentConfig += fmt.Sprintf(`
apm "prometheus" {
 driver = "prometheus"
 config = { address = %q }
}
`, server.URL)
	}
	configPath := filepath.Join(dir, "autoscaler.hcl")
	if err = os.WriteFile(configPath, []byte(agentConfig), 0600); err != nil {
		t.Fatal(err)
	}
	policy := fmt.Sprintf(`scaling "real-nomad" {
 enabled = true
 min = 0
 max = 1
 policy {
  cooldown = "30s"
  evaluation_interval = "200ms"
  check "fixed" {
   strategy "fixed-value" { value = 0 }
  }
  target "tencent-as" {
   tencent_group_id = %q
   node_drain_deadline = "30s"
   node_purge = %q
  }
 }
}
`, testGroup, strconv.FormatBool(purge))
	if recoveryScenario {
		policy = fmt.Sprintf(`scaling "real-nomad" {
 enabled = true
 min = 1
 max = 3
 policy {
  cooldown = "2s"
  evaluation_interval = "200ms"
  check "capacity" {
   source = "prometheus"
   query = "fixture_capacity"
   strategy "pass-through" {}
  }
  target "tencent-as" {
   tencent_group_id = %q
   node_drain_deadline = "30s"
   node_purge = "false"
   scale_in_protection = "true"
  }
 }
}
`, testGroup)
	}
	if err = os.WriteFile(filepath.Join(policyDir, "cluster.hcl"), []byte(policy), 0600); err != nil {
		t.Fatal(err)
	}
	listener, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	agentPort := listener.Addr().(*net.TCPAddr).Port
	_ = listener.Close()
	currentAgent = exec.Command(agent, "agent", "-config="+configPath, "-plugin-dir="+pluginDir, "-policy-dir="+policyDir, "-policy-source-disable-nomad", "-http-bind-address=127.0.0.1", "-http-bind-port="+strconv.Itoa(agentPort), "-log-level=DEBUG", "-log-json")
	currentAgent.Env = []string{"PATH=/usr/bin:/bin", "HOME=" + dir, "NOMAD_TOKEN=" + token.SecretID, "TENCENT_AGENT_TEST_HELPER=1", "TENCENT_AGENT_TEST_URL=" + server.URL}
	currentAgent.Stdout = logs
	currentAgent.Stderr = logs
	currentAgent.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err = currentAgent.Start(); err != nil {
		t.Fatal("official Agent start failed")
	}
	report["agent_pid"] = currentAgent.Process.Pid
	finished := make(chan struct{})
	agentDone = finished
	go func() { _ = currentAgent.Wait(); close(finished) }()
	if preDeleteAbort {
		// Change only the cloud model's lower bound after the actual Nomad
		// node is draining a real running task. No deletion has been submitted.
		waitRealNomad(t, 12*time.Second, "real owned drain before modeled lower-bound change", func() bool {
			n, _, ne := admin.Nodes().Info(node.ID, nil)
			a, _, ae := admin.Allocations().Info(allocationID, nil)
			s := readState()
			if ne != nil || ae != nil || s.Pending == nil || n.LastDrain == nil || n.LastDrain.Meta["tencent_operation_id"] != s.Pending.ID || !n.Drain || n.SchedulingEligibility != "ineligible" || a.TaskStates["main"] == nil || a.TaskStates["main"].State != "running" {
				return false
			}
			fixture.mu.Lock()
			defer fixture.mu.Unlock()
			if fixture.removeCalls != 0 || fixture.desiredCalls != 0 {
				t.Fatal("cloud write preceded pre-delete guard injection")
			}
			fixture.cloud.snap.Group.Min = 2
			report["lower_bound_changed_after_real_drain"] = map[string]any{"node_id": node.ID, "operation_id": s.Pending.ID, "at": time.Now().UTC(), "min": 2}
			return true
		})
		waitRealNomad(t, 55*time.Second, "pre-delete abort automatically restores real node", func() bool {
			s := readState()
			n, _, e := admin.Nodes().Info(node.ID, nil)
			return s.Pending == nil && s.Batch == nil && s.LastBatch != nil && s.LastBatch.Phase == "failed" && s.LastOperation != nil && s.LastOperation.AbortReason == "pre-delete-group-limits-changed" && s.LastOperation.DrainOutcome == "restored" && e == nil && n.SchedulingEligibility == "eligible" && !n.Drain && n.DrainStrategy == nil
		})
		restored := readState()
		failedBatch := restored.LastBatch.ID
		report["pre_delete_restored_state"] = restored
		if restored.ManualReview || restored.LastCompletedAt != 0 || restored.LastBatch.CompletedAt != 0 || restored.LastOperation.RequestID != "" || restored.LastOperation.ActivityID != "" || restored.RecoveryAudit == nil {
			t.Fatal("pre-delete abort lost failure, acquired cloud acceptance or needs manual recovery")
		}
		waitRealNomad(t, 10*time.Second, "official Agent receives pre-delete failure", func() bool {
			return strings.Contains(logs.text(), "ScaleActionFailed")
		})
		nextID := "nomad-spec-predelete-restored-work"
		extraJobs = append(extraJobs, nextID)
		next := realRecoveryJob(nextID, node.ID, "batch", false)
		next.TaskGroups[0].Tasks[0].Config["args"] = []string{"2"}
		if _, _, e := admin.Jobs().Register(next, nil); e != nil {
			t.Fatal("pre-delete restored-node job registration failed")
		}
		nextAllocation := ""
		waitRealNomad(t, 15*time.Second, "new task runs on exact pre-delete restored node", func() bool {
			rows, _, e := admin.Jobs().Allocations(nextID, false, nil)
			for _, row := range rows {
				if e == nil && row.NodeID == node.ID && row.ClientStatus == "running" {
					nextAllocation = row.ID
					return true
				}
			}
			return false
		})
		report["restored_new_allocation_id"], report["restored_new_allocation_node_id"] = nextAllocation, node.ID
		waitRealNomad(t, 12*time.Second, "new task completes before lower-bound recovery", func() bool {
			a, _, e := admin.Allocations().Info(nextAllocation, nil)
			return e == nil && a.ClientStatus == "complete" && a.TaskStates["main"] != nil && a.TaskStates["main"].State == "dead"
		})
		fixture.mu.Lock()
		before := fixture.summaryLocked()
		fixture.cloud.snap.Group.Min = 1
		fixture.mu.Unlock()
		report["cloud_before_guard_recovery"] = before
		if before["remove_calls"] != 0 || before["desired_calls"] != 0 || before["unexpected_writes"] != 0 || len(before["errors"].([]string)) != 0 {
			t.Fatal("cloud write or fixture error before guard recovery")
		}
		// Restore the modeled lower bound only. The same Agent and unchanged
		// policy must naturally request the next shrink and finish it once.
		waitRealNomad(t, 25*time.Second, "same Agent completes next shrink after pre-delete abort", func() bool {
			s := readState()
			return s.Pending == nil && s.Batch == nil && s.LastBatch != nil && s.LastBatch.ID != failedBatch && s.LastBatch.Kind == "shrink" && s.LastBatch.Phase == "completed" && s.LastCompletedAt > 0 && strings.Contains(logs.text(), "scale request completed")
		})
		stopAgent()
		fixture.mu.Lock()
		summary := fixture.summaryLocked()
		fixture.mu.Unlock()
		report["cloud_model"], report["journal"] = summary, readState()
		report["same_agent_without_reload_or_manual_recovery"] = true
		if summary["remove_calls"] != 1 || summary["desired_calls"] != 0 || summary["unexpected_writes"] != 0 || summary["config_with_token"] != true || summary["inherited_connection"] != true || len(summary["errors"].([]string)) != 0 {
			t.Error("pre-delete recovery next-action write or inheritance mismatch")
		}
		report["passed"] = !t.Failed()
		return
	}
	if failedDelete {
		drainObserved, poststopObserved := false, false
		waitRealNomad(t, 55*time.Second, "failed removal and real owned eligibility restoration", func() bool {
			a, _, e := admin.Allocations().Info(allocationID, nil)
			n, _, ne := admin.Nodes().Info(node.ID, nil)
			if e == nil && ne == nil {
				if n.SchedulingEligibility == "ineligible" && a.TaskStates["main"] != nil && a.TaskStates["main"].State == "running" {
					drainObserved = true
				}
				if a.TaskStates["poststop"] != nil && a.TaskStates["poststop"].State == "running" {
					poststopObserved = true
				}
			}
			s := readState()
			return s.Pending != nil && s.Pending.Phase == "failed-delete" && s.Pending.DrainOutcome == "restored" && ne == nil && n.SchedulingEligibility == "eligible" && s.RecoveryAudit != nil
		})
		restored := readState()
		report["restored_failed_state"] = restored
		report["drain_while_main_running"], report["poststop_observed_running"] = drainObserved, poststopObserved
		if !drainObserved || !poststopObserved || restored.LastCompletedAt != 0 || restored.Pending.ActivityStatus != "FAILED" || restored.Pending.InstanceResult != "FAILED" {
			t.Fatal("failed action lost original outcome or published success")
		}
		failedOperation, failedBatch := restored.Pending.ID, restored.Batch.ID
		nextID := "nomad-spec-agent-restored-work"
		extraJobs = append(extraJobs, nextID)
		next := realRecoveryJob(nextID, node.ID, "batch", false)
		next.TaskGroups[0].Tasks[0].Config["args"] = []string{"2"}
		if _, _, e := admin.Jobs().Register(next, nil); e != nil {
			t.Fatal("restored-node job registration failed")
		}
		nextAllocation := ""
		waitRealNomad(t, 15*time.Second, "new real task running on restored exact node", func() bool {
			rows, _, e := admin.Jobs().Allocations(nextID, false, nil)
			for _, row := range rows {
				if e == nil && row.NodeID == node.ID && row.ClientStatus == "running" {
					nextAllocation = row.ID
					return true
				}
			}
			return false
		})
		report["restored_new_allocation_id"] = nextAllocation
		report["restored_new_allocation_node_id"] = node.ID
		waitRealNomad(t, 12*time.Second, "restored new task stopped before modeled later removal", func() bool {
			a, _, e := admin.Allocations().Info(nextAllocation, nil)
			return e == nil && a.ClientStatus == "complete" && a.TaskStates["main"] != nil && a.TaskStates["main"].State == "dead"
		})
		fixture.mu.Lock()
		if fixture.removeCalls != 1 || fixture.desiredCalls != 0 || len(fixture.errors) != 0 {
			fixture.mu.Unlock()
			t.Fatal("cloud write repeated while failure pending")
		}
		fixture.settleFailedDeleteLocked()
		fixture.mu.Unlock()
		waitRealNomad(t, 20*time.Second, "original failed batch settled by background worker", func() bool {
			s := readState()
			return s.Pending == nil && s.Batch == nil && s.LastBatch != nil && s.LastBatch.ID == failedBatch && s.LastBatch.Phase == "failed"
		})
		settled := readState()
		report["settled_failed_state"] = settled
		if settled.LastCompletedAt != 0 || settled.LastBatch.CompletedAt != 0 || settled.ManualReview || settled.LastOperation.ID != failedOperation || settled.LastOperation.ActivityID != testActivity || settled.LastOperation.ActivityStatus != "FAILED" || settled.LastOperation.DeleteSettlement == nil || settled.LastOperation.AbsentObservations != 2 {
			t.Fatal("settlement rewrote failure or lacks exact later-removal evidence")
		}
		// Change only the APM observation. Same process, policy and plugin instance
		// must issue the next request through its normal evaluation loop.
		fixture.mu.Lock()
		fixture.growMetric = true
		fixture.mu.Unlock()
		waitRealNomad(t, 25*time.Second, "same official Agent completes next normal growth evaluation", func() bool {
			s := readState()
			return s.Pending == nil && s.Batch == nil && s.LastBatch != nil && s.LastBatch.ID != failedBatch && s.LastBatch.Kind == "grow" && s.LastBatch.Phase == "completed" && s.LastCompletedAt > 0 && strings.Contains(logs.text(), "scale request completed")
		})
		stopAgent()
		fixture.mu.Lock()
		summary := fixture.summaryLocked()
		fixture.mu.Unlock()
		report["cloud_model"] = summary
		report["journal"] = readState()
		report["same_agent_without_reload_or_manual_recovery"] = true
		if summary["remove_calls"] != 1 || summary["desired_calls"] != 1 || summary["unexpected_writes"] != 0 || summary["config_with_token"] != true || summary["inherited_connection"] != true {
			t.Error("failed-delete Agent recovery write/inheritance mismatch")
		}
		report["passed"] = !t.Failed()
		return
	}
	drainWhileRunning, poststopRunning := false, false
	waitRealNomad(t, 55*time.Second, "official Agent real drain and model removal completion", func() bool {
		a, _, e := admin.Allocations().Info(allocationID, nil)
		n, _, ne := admin.Nodes().Info(node.ID, nil)
		if e == nil && ne == nil {
			if n.SchedulingEligibility == "ineligible" && a.TaskStates["main"] != nil && a.TaskStates["main"].State == "running" {
				drainWhileRunning = true
			}
			if a.TaskStates["poststop"] != nil && a.TaskStates["poststop"].State == "running" {
				poststopRunning = true
			}
		}
		s := readState()
		return s.LastCompletedAt > 0 && s.Pending == nil && s.Batch == nil && s.LastBatch != nil && s.LastBatch.Phase == "completed" && strings.Contains(logs.text(), "scale request completed")
	})
	stopAgent()
	final := readState()
	fixture.mu.Lock()
	summary := fixture.summaryLocked()
	fixture.mu.Unlock()
	report["cloud_model"] = summary
	report["journal"] = final
	report["drain_while_main_running"] = drainWhileRunning
	report["poststop_observed_running"] = poststopRunning
	if !drainWhileRunning || !poststopRunning {
		t.Error("actual job lifecycle was not observed during Agent action")
	}
	if summary["remove_calls"] != 1 || summary["unexpected_writes"] != 0 || summary["config_with_token"] != true || summary["inherited_connection"] != true {
		t.Error("Agent inheritance/model exact-write assertions failed")
	}
	if final.LastCompletedAt == 0 || final.LastOperation == nil || final.RecoveryAudit == nil {
		t.Error("Agent completion/post evidence missing")
	} else {
		record, e := readRecord(filepath.Dir(journal), *final.RecoveryAudit)
		if e != nil || record.Plan.Decision.Action != "confirm-sdk-post" || len(record.Evidence) != 1 || record.Evidence[0].Post == nil || record.Evidence[0].Post.Purge != purge || (purge && record.Evidence[0].Post.Outcome != "purged") || (!purge && record.Evidence[0].Post.Outcome != "purge-disabled") {
			t.Error("real SDK post audit missing")
		} else {
			report["post_audit"] = record.Evidence[0].Post
		}
	}
	if purge {
		checkConfig := cfg
		checkConfig.Token = token.SecretID
		check, e := nomadclient.New(checkConfig)
		if e != nil {
			t.Fatal("post client unavailable")
		}
		defer check.Close()
		_, e = check.ReadNode(context.Background(), node.ID)
		var apiErr *nomadclient.Error
		gone := errors.As(e, &apiErr) && apiErr.HTTPStatus == 404
		if !gone {
			t.Error("purge=true did not remove the exact real Nomad record")
		}
		report["purge_true_node_absent"] = gone
	} else {
		nodeAfter, _, e := admin.Nodes().Info(node.ID, nil)
		retained := e == nil && nodeAfter.SchedulingEligibility == "ineligible"
		if !retained {
			t.Error("purge=false node retention did not match contract")
		}
		report["purge_false_node_retained"] = retained
	}
	rawJournal, _ := os.ReadFile(journal)
	rawConfig, _ := os.ReadFile(configPath)
	for _, secret := range secrets {
		if bytes.Contains(rawJournal, []byte(secret)) || bytes.Contains(rawConfig, []byte(secret)) {
			t.Error("ephemeral credential persisted")
		}
	}
	report["plugin_nomad_environment_cleared"] = true
	report["passed"] = !t.Failed()
}

type realAgentRuntimeLog struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *realAgentRuntimeLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}
func (l *realAgentRuntimeLog) text() string { l.mu.Lock(); defer l.mu.Unlock(); return l.b.String() }

type realAgentCloudFixture struct {
	mu                                                     sync.Mutex
	cloud                                                  *fakeCloud
	admin                                                  *api.Client
	nodeID, allocationID, journal                          string
	removeCalls, unexpectedWrites, statusCalls, scaleCalls int
	configToken, inherited                                 bool
	beforeRemoval                                          any
	removedAt                                              time.Time
	afterRemoval                                           func()
	errors                                                 []string
	failedDelete, settled, growMetric, preDeleteAbort      bool
	desiredCalls, historyCalls                             int
	history                                                []tencentcloud.Activity
}

func (f *realAgentCloudFixture) summaryLocked() map[string]any {
	return map[string]any{"remove_calls": f.removeCalls, "desired_calls": f.desiredCalls, "history_calls": f.historyCalls, "settled": f.settled, "unexpected_writes": f.unexpectedWrites, "status_calls": f.statusCalls, "scale_calls": f.scaleCalls, "config_with_token": f.configToken, "inherited_connection": f.inherited, "before_removal": f.beforeRemoval, "removed_at": f.removedAt, "errors": append([]string{}, f.errors...)}
}
func (f *realAgentCloudFixture) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	emit := func(value any) { _ = json.NewEncoder(w).Encode(value) }
	reject := func(why string) {
		f.errors = append(f.errors, why)
		http.Error(w, "isolated cloud model rejected request", 500)
	}
	switch r.URL.Path {
	case "/fixture/request":
		var request struct {
			Method         string
			Config         map[string]string
			CredentialKeys []string
			Action         *struct{ Count int64 }
		}
		if json.NewDecoder(r.Body).Decode(&request) != nil {
			reject("invalid observation")
			return
		}
		if request.Method == "SetConfig" {
			for _, key := range request.CredentialKeys {
				if key == "nomad_token" {
					f.configToken = true
				}
			}
			f.inherited = request.Config["nomad_address"] != "" && request.Config["nomad_ca-cert"] != "" && request.Config["nomad_client-cert"] != "" && request.Config["nomad_client-key"] != "" && request.Config["nomad_tls-server-name"] == "server.global.nomad"
			if request.Config["nomad_token"] != "" {
				reject("observation contains credential")
				return
			}
		}
		if request.Method == "Scale" {
			f.scaleCalls++
			if request.Action == nil || (!f.failedDelete && !f.preDeleteAbort && request.Action.Count != 0) || ((f.failedDelete || f.preDeleteAbort) && request.Action.Count != 1 && request.Action.Count != 2) || request.Config["tencent_group_id"] != testGroup {
				reject("wrong Agent action")
				return
			}
		}
		emit(true)
	case "/fixture/status":
		f.statusCalls++
		emit(true)
	case "/fixture/cloud":
		var request struct {
			Group   string `json:"group_id"`
			Tracked string `json:"tracked_instance_id"`
		}
		if json.NewDecoder(r.Body).Decode(&request) != nil || request.Group != testGroup {
			reject("wrong group read")
			return
		}
		s, e := f.cloud.Observe(r.Context(), testGroup, request.Tracked)
		if e != nil {
			reject("model observation failed")
			return
		}
		s.Region = "ap-guangzhou"
		emit(s)
	case "/fixture/remove":
		var request struct {
			Group    string `json:"group_id"`
			Instance string `json:"instance_id"`
		}
		if json.NewDecoder(r.Body).Decode(&request) != nil || request.Group != testGroup || request.Instance != testInstance || f.removeCalls != 0 {
			reject("wrong or repeated exact removal")
			return
		}
		a, _, e := f.admin.Allocations().Info(f.allocationID, nil)
		n, _, ne := f.admin.Nodes().Info(f.nodeID, nil)
		var state State
		raw, re := os.ReadFile(f.journal)
		if re != nil || json.Unmarshal(raw, &state) != nil || state.Pending == nil || state.Pending.Phase != "remove-intent" || state.Pending.NodeID != f.nodeID || state.Pending.InstanceID != testInstance {
			reject("missing durable exact intent")
			return
		}
		if e != nil || ne != nil || a.ClientStatus != "complete" || n.Drain || n.DrainStrategy != nil || n.SchedulingEligibility != "ineligible" || n.LastDrain == nil || n.LastDrain.Meta["tencent_operation_id"] != state.Pending.ID {
			reject("removal preceded real drain")
			return
		}
		for _, name := range []string{"main", "poststop"} {
			if a.TaskStates[name] == nil || a.TaskStates[name].State != "dead" {
				reject("removal preceded real task completion")
				return
			}
		}
		f.beforeRemoval = map[string]any{"node_id": n.ID, "allocation_id": a.ID, "main_finished_at": a.TaskStates["main"].FinishedAt, "poststop_finished_at": a.TaskStates["poststop"].FinishedAt, "operation_id": state.Pending.ID}
		result, e := f.cloud.RemoveExact(context.Background(), testGroup, testInstance)
		if e != nil {
			reject("model exact removal failed")
			return
		}
		if f.afterRemoval != nil {
			f.afterRemoval()
		}
		if f.failedDelete {
			f.cloud.fail()
			f.cloud.snap.Group.Desired = 1
			f.cloud.snap.Members[0].Lifecycle = "TERMINATION_FAILED"
			a := &f.cloud.activity
			a.Type, a.StartedAt, a.EndedAt = "REMOVE_INSTANCES", state.Pending.StartedAt.Truncate(time.Second).Format(time.RFC3339), time.Now().UTC().Format(time.RFC3339)
			a.RequestIDs = []tencentcloud.RequestTrace{{Action: "DescribeAutoScalingActivities", RequestID: testRequest}}
			f.history = []tencentcloud.Activity{copyJSON(*a)}
		} else if f.preDeleteAbort {
			// This scenario starts with two members and removes only the
			// selected one; preserve the protected spare and exact capacity.
			f.cloud.snap.Members = []tencentcloud.Member{f.cloud.snap.Members[1]}
			delete(f.cloud.snap.VMs, testInstance)
			f.cloud.snap.Group.Count, f.cloud.snap.Group.Desired = 1, 1
			f.cloud.activity.Status = "SUCCESSFUL"
			f.cloud.activity.RelatedInstances[0].Status = "SUCCESSFUL"
		} else {
			f.cloud.complete()
		}
		f.removeCalls++
		f.removedAt = time.Now().UTC()
		emit(result)
	case "/fixture/activity":
		var request struct {
			Group    string `json:"group_id"`
			Activity string `json:"activity_id"`
		}
		if json.NewDecoder(r.Body).Decode(&request) != nil {
			reject("invalid activity read")
			return
		}
		a, e := f.cloud.Activity(r.Context(), request.Group, request.Activity)
		if e != nil {
			reject("wrong activity read")
			return
		}
		emit(a)
	case "/api/v1/query_range":
		if !f.failedDelete && !f.preDeleteAbort {
			reject("unexpected metrics read")
			return
		}
		start, _ := strconv.ParseFloat(r.URL.Query().Get("start"), 64)
		end, _ := strconv.ParseFloat(r.URL.Query().Get("end"), 64)
		metric := "1"
		if f.growMetric {
			metric = "2"
		}
		emit(map[string]any{"status": "success", "data": map[string]any{"resultType": "matrix", "result": []any{map[string]any{"metric": map[string]string{}, "values": []any{[]any{start, metric}, []any{end, metric}}}}}})
	case "/fixture/activities":
		var request struct {
			Group string    `json:"group_id"`
			Since time.Time `json:"since"`
		}
		if !f.failedDelete || json.NewDecoder(r.Body).Decode(&request) != nil || request.Group != testGroup || request.Since.IsZero() {
			reject("invalid activity history")
			return
		}
		f.historyCalls++
		emit(copyJSON(f.history))
	case "/fixture/desired":
		if f.failedDelete {
			var request struct{ Count int64 }
			var state State
			raw, e := os.ReadFile(f.journal)
			if json.NewDecoder(r.Body).Decode(&request) != nil || request.Count != 2 || !f.settled || !f.growMetric || f.desiredCalls != 0 || e != nil || json.Unmarshal(raw, &state) != nil || state.Pending == nil || state.Pending.Kind != "grow" || state.Pending.Phase != "grow-intent" || state.LastBatch == nil || state.LastBatch.Phase != "failed" || state.LastCompletedAt != 0 {
				reject("unexpected next Agent growth")
				return
			}
			added := time.Now().UTC().Format(time.RFC3339)
			f.cloud.snap.Members = append(f.cloud.snap.Members, tencentcloud.Member{ID: "ins-new", CreationType: "AUTO_CREATION", Lifecycle: "IN_SERVICE", AddedAt: added})
			f.cloud.snap.VMs["ins-new"] = tencentcloud.VM{ID: "ins-new", State: "RUNNING", ChargeType: "POSTPAID_BY_HOUR", Returned: true}
			f.cloud.snap.Group.Count, f.cloud.snap.Group.Desired = 2, 2
			f.desiredCalls++
			emit(tencentcloud.WriteResult{Attempted: true, RequestID: testRequest, Action: "ModifyDesiredCapacity"})
			return
		}
		f.unexpectedWrites++
		reject("unexpected capacity write")
	default:
		reject("unexpected endpoint")
	}
}

// This models a separately evidenced later deletion, not cancellation or
// restoration of TERMINATION_FAILED. The real Nomad process is not a CVM.
func (f *realAgentCloudFixture) settleFailedDeleteLocked() {
	f.cloud.snap.Members = []tencentcloud.Member{f.cloud.snap.Members[1]}
	delete(f.cloud.snap.VMs, testInstance)
	f.cloud.snap.Group.Count, f.cloud.snap.Group.Desired = 1, 1
	later := copyJSON(f.history[0])
	later.ID, later.Status = "asa-later", "SUCCESSFUL"
	later.StartedAt, later.EndedAt = time.Now().UTC().Format(time.RFC3339), time.Now().UTC().Format(time.RFC3339)
	later.RelatedInstances[0].Status = "SUCCESSFUL"
	f.history = append(f.history, later)
	f.settled = true
}
