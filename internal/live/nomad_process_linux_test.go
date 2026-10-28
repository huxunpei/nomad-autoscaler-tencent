package live

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/hashicorp/nomad/api"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadclient"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadsdk"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/targetconfig"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/tencentcloud"
)

// Opt-in integration with a real isolated Nomad server/client and raw_exec
// tasks. Cloud outcomes are explicitly modelled; this is not real AS/CVM or
// the official Autoscaler Agent acceptance. Never read cloud credentials.
func TestRealNomadSubmittedFailureRestoresNewTaskAfterLostACKAndRestart(t *testing.T) {
	binary := os.Getenv("NOMAD_REAL_BINARY")
	if binary == "" {
		t.Skip("requires separately provisioned real Nomad binary on the Linux test host")
	}
	expected := os.Getenv("NOMAD_REAL_SHA256")
	if !filepath.IsAbs(binary) || !digestPattern.MatchString(expected) {
		t.Fatal("real Nomad requires absolute binary path and pinned SHA256")
	}
	raw, err := os.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(raw)
	if hex.EncodeToString(hash[:]) != expected {
		t.Fatal("real Nomad binary hash mismatch")
	}
	report := map[string]any{"nomad_binary_sha256": expected, "cloud": "synthetic failed RemoveInstances and later asynchronous deletion", "transport_fault": "actual eligible response deliberately malformed after Nomad committed it", "started_at": time.Now().UTC()}
	directory, err := os.MkdirTemp("", "nr-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(directory); err != nil {
			t.Error(err)
		}
	})
	address, cmd, nomadDone := startRealNomadForRecovery(t, binary, directory, report)
	direct, err := api.NewClient(&api.Config{Address: address})
	if err != nil {
		t.Fatal(err)
	}
	jobPrefix := "nomad-spec-" + strings.ReplaceAll(filepath.Base(directory), "_", "-")
	// All submitted jobs are explicit test-owned names. Deregister before stopping
	// our process; do not touch a pre-existing Nomad cluster or host service.
	var jobs []string
	var node *api.Node
	var spareCmd *exec.Cmd
	var spareDone <-chan struct{}
	var spareAddress string
	var engine *Engine
	defer func() {
		if engine != nil {
			engine.Close()
		}
		for _, id := range jobs {
			_, _, _ = direct.Jobs().Deregister(id, true, nil)
		}
		deadline := time.Now().Add(12 * time.Second)
		terminal := false
		for time.Now().Before(deadline) {
			allocs, _, e := direct.Allocations().List(nil)
			terminal = e == nil
			for _, a := range allocs {
				if !strings.HasPrefix(a.JobID, jobPrefix) {
					continue
				}
				if a.ClientStatus != "complete" && a.ClientStatus != "failed" && a.ClientStatus != "lost" {
					terminal = false
				}
			}
			if terminal {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		report["owned_allocations_terminal_before_stop"] = terminal
		if spareCmd != nil {
			_ = spareCmd.Process.Signal(os.Interrupt)
			select {
			case <-spareDone:
			case <-time.After(10 * time.Second):
				_ = syscall.Kill(-spareCmd.Process.Pid, syscall.SIGKILL)
				<-spareDone
			}
			report["spare_process_terminal"] = spareCmd.ProcessState != nil
			spareConn, e := net.DialTimeout("tcp", strings.TrimPrefix(spareAddress, "http://"), 200*time.Millisecond)
			if e == nil {
				_ = spareConn.Close()
			}
			report["spare_http_listener_closed"] = e != nil
		}
		_ = cmd.Process.Signal(os.Interrupt)
		select {
		case <-nomadDone:
		case <-time.After(10 * time.Second):
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
			<-nomadDone
		}
		report["nomad_process_terminal"] = cmd.ProcessState != nil
		mounts, _ := os.ReadFile("/proc/self/mountinfo")
		unmounted := []string{}
		for _, line := range strings.Split(string(mounts), "\n") {
			fields := strings.Fields(line)
			if len(fields) > 5 && strings.HasPrefix(fields[4], directory+"/") && strings.Contains(fields[4], "/data/alloc/") && strings.Contains(line, " - tmpfs tmpfs ") {
				if e := syscall.Unmount(fields[4], 0); e != nil {
					t.Error("test-owned tmpfs cleanup", e)
				} else {
					unmounted = append(unmounted, fields[4])
				}
			}
		}
		report["test_tmpfs_unmounted"] = unmounted
		conn, e := net.DialTimeout("tcp", strings.TrimPrefix(address, "http://"), 200*time.Millisecond)
		if e == nil {
			_ = conn.Close()
		}
		report["http_listener_closed"] = e != nil
		nomadLog, _ := os.ReadFile(filepath.Join(directory, "nomad.log"))
		report["nomad_log"] = string(nomadLog)
		report["test_failed"] = t.Failed()
		report["ended_at"] = time.Now().UTC()
		if output := os.Getenv("NOMAD_REAL_REPORT"); output != "" {
			if !filepath.IsAbs(output) {
				t.Error("report path must be absolute")
			} else if e := os.WriteFile(output, jsonBytes(report), 0600); e != nil {
				t.Error(e)
			}
		}
		if !terminal {
			t.Error("test-owned task cleanup did not reach terminal state")
		}
	}()
	waitRealNomad(t, 50*time.Second, "ready raw_exec client", func() bool {
		rows, _, e := direct.Nodes().List(nil)
		if e != nil || len(rows) != 1 {
			return false
		}
		n, _, e := direct.Nodes().Info(rows[0].ID, nil)
		if e != nil || n.Status != "ready" || n.SchedulingEligibility != "eligible" || n.Meta["cloud_instance_id"] != testInstance {
			return false
		}
		driver := n.Drivers["raw_exec"]
		if driver == nil || !driver.Detected || !driver.Healthy {
			return false
		}
		node = n
		return true
	})
	report["node_id"] = node.ID
	seed := jobPrefix + "-seed"
	jobs = append(jobs, seed)
	seedJob := realRecoveryJob(seed, node.ID, "service", true)
	seedJob.Constraints = nil
	if _, _, err = direct.Jobs().Register(seedJob, nil); err != nil {
		t.Fatal("register seed job", err)
	}
	var original *api.Allocation
	waitRealNomad(t, 30*time.Second, "seed task running", func() bool {
		rows, _, e := direct.Jobs().Allocations(seed, false, nil)
		if e != nil {
			return false
		}
		for _, row := range rows {
			a, _, e := direct.Allocations().Info(row.ID, nil)
			if e == nil && a.NodeID == node.ID && a.TaskStates["main"] != nil && a.TaskStates["main"].State == "running" {
				original = a
				return true
			}
		}
		return false
	})
	report["original_allocation"] = original
	spareAddress, spareCmd, spareDone = startRealNomadClientForRecovery(t, binary, filepath.Join(directory, "spare"), report["rpc_address"].(string), report)
	waitRealNomad(t, 40*time.Second, "second real client for service migration", func() bool {
		rows, _, e := direct.Nodes().List(nil)
		if e != nil || len(rows) != 2 {
			return false
		}
		for _, n := range rows {
			if n.ID != node.ID && n.Status == "ready" && n.SchedulingEligibility == "eligible" {
				return true
			}
		}
		return false
	})
	var mu sync.Mutex
	writes := []map[string]any{}
	requests := []map[string]any{}
	lostRestore := false
	defer func() {
		mu.Lock()
		report["http_request_summaries"] = copyJSON(requests)
		report["nomad_writes"] = copyJSON(writes)
		mu.Unlock()
	}()
	upstream, _ := url.Parse(address)
	proxy := httputil.NewSingleHostReverseProxy(upstream)
	proxy.ModifyResponse = func(resp *http.Response) error {
		data, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
		if err != nil {
			return err
		}
		_ = resp.Body.Close()
		resp.Body = io.NopCloser(bytes.NewReader(data))
		resp.ContentLength = int64(len(data))
		resp.Header.Del("Content-Length")
		row := map[string]any{"path": resp.Request.URL.Path, "query": resp.Request.URL.RawQuery, "status": resp.StatusCode, "bytes": len(data), "content_encoding": resp.Header.Get("Content-Encoding")}
		if strings.HasSuffix(resp.Request.URL.Path, "/allocations") {
			var list []map[string]any
			_ = json.Unmarshal(data, &list)
			shapes := []map[string]any{}
			for _, a := range list {
				shapes = append(shapes, map[string]any{"id": a["ID"], "node_id": a["NodeID"], "namespace": a["Namespace"], "job_id": a["JobID"], "job_present": a["Job"] != nil, "resources_present": a["Resources"] != nil, "task_states_present": a["TaskStates"] != nil, "client_status": a["ClientStatus"]})
			}
			row["allocations"] = shapes
		}
		mu.Lock()
		requests = append(requests, row)
		mu.Unlock()

		if resp.Request.Context().Value(realNomadRestoreKey{}) == true {
			mu.Lock()
			drop := !lostRestore
			lostRestore = true
			mu.Unlock()
			if drop {
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
				resp.Body = io.NopCloser(strings.NewReader("{lost-eligible-response"))
				resp.ContentLength = int64(len("{lost-eligible-response"))
				resp.Header.Del("Content-Length")
				resp.Header.Del("Content-Encoding")
			}
		}
		return nil
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<16))
			_ = r.Body.Close()
			r.Body = io.NopCloser(bytes.NewReader(body))
			var payload struct {
				MarkEligible bool
				Meta         map[string]string
			}
			_ = json.Unmarshal(body, &payload)
			mu.Lock()
			writes = append(writes, map[string]any{"at": time.Now().UTC(), "method": r.Method, "path": r.URL.Path, "mark_eligible": payload.MarkEligible, "operation_id": payload.Meta["tencent_operation_id"]})
			mu.Unlock()
			if payload.MarkEligible {
				r = r.WithContext(context.WithValue(r.Context(), realNomadRestoreKey{}, true))
			}
		}
		proxy.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	client, err := nomadclient.New(nomadclient.Config{Address: server.URL, Timeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	client, err = client.WithScope(testGroup, testRegion, "cloud_instance_id")
	if err != nil {
		t.Fatal(err)
	}
	policy, err := (targetconfig.Agent{}).Policy(map[string]string{"tencent_group_id": testGroup, "node_drain_deadline": "15s", "node_purge": "false"})
	if err != nil {
		t.Fatal(err)
	}
	adapter := nomadsdk.New(client, policy)
	c := &realProcessCloud{settlementCloud: &settlementCloud{multiCloud: &multiCloud{fakeCloud: newCloud(), activities: map[string]tencentcloud.Activity{}}}}
	c.addMember()
	var poststopAt, deleteAt time.Time
	c.beforeRemove = func() error {
		alloc, _, e := direct.Allocations().Info(original.ID, nil)
		if e != nil {
			return e
		}
		task := alloc.TaskStates["poststop"]
		if task == nil || task.State != "dead" || task.FinishedAt.IsZero() {
			return fmt.Errorf("cloud deletion before real poststop finished")
		}
		poststopAt = task.FinishedAt
		deleteAt = time.Now().UTC()
		report["allocation_at_cloud_remove"] = alloc
		return nil
	}
	cfg := Config{Dir: filepath.Join(directory, "journal"), GroupID: testGroup, Region: testRegion, GroupContract: true, DrainDeadline: 15 * time.Second, ReconcileInterval: time.Hour, StabilizationInterval: 100 * time.Millisecond, DisableReconciliation: true}
	engine, err = Open(cfg, c, adapter)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { engine.Close() })
	changed, scaleErr := engine.ScaleAndWait(0, 15)
	report["state_after_scale_call"] = copyJSON(engine.state)
	if scaleErr != nil {
		report["scale_call_error"] = scaleErr.Error()
	}
	if !changed || scaleErr == nil || scaleErr.Error() != "ScaleActionFailed" {
		t.Fatalf("real drain/failed model deletion: changed=%v error=%v event=%s", changed, scaleErr, engine.state.LastEvent)
	}
	report["scale_error"] = scaleErr.Error()
	report["poststop_finished_at"] = poststopAt
	report["cloud_remove_called_at"] = deleteAt
	if poststopAt.IsZero() || deleteAt.Before(poststopAt) || c.writeCount() != 1 || engine.state.Pending == nil || engine.state.Pending.Phase != "failed-delete" || engine.state.LastCompletedAt != 0 {
		t.Fatal("poststop/failed operation evidence incomplete")
	}
	opID := engine.state.Pending.ID
	report["operation_id"] = opID
	report["state_before_restart"] = copyJSON(engine.state)
	if engine.state.Pending.DrainOutcome != "" {
		t.Fatal("lost ACK was already confirmed before restart")
	}
	engine.Close()
	cfg.DisableReconciliation = false
	cfg.ReconcileInterval = 50 * time.Millisecond
	engine, err = Open(cfg, c, adapter)
	if err != nil {
		t.Fatal("restart after lost eligible ACK", err)
	}
	waitRealNomad(t, 5*time.Second, "background confirmation of actual restored node", func() bool {
		s, _, _ := engine.Snapshot()
		return s.Pending != nil && s.Pending.DrainOutcome == "restored" && !s.ManualReview && s.LastCompletedAt == 0
	})
	report["restoration_confirmed_by_background_worker"] = true
	engine.Close()
	cfg.DisableReconciliation = true
	cfg.ReconcileInterval = time.Hour
	engine, err = Open(cfg, c, adapter)
	if err != nil {
		t.Fatal("reopen after background restoration", err)
	}
	engine.EnableReconciliation()
	restored, _, err := direct.Nodes().Info(node.ID, nil)
	if err != nil || restored.SchedulingEligibility != "eligible" || restored.DrainStrategy != nil {
		t.Fatal("real node not scheduling again", err)
	}
	restoredAt := time.Now().UTC()
	restoredView, _ := client.ReadNode(context.Background(), node.ID)
	report["restored_node"] = restoredView
	report["restored_read_at"] = restoredAt
	c.mu.Lock()
	if len(c.snap.Members) != 1 || c.snap.Members[0].Lifecycle != "TERMINATION_FAILED" {
		t.Error("model failure member incorrectly normalized")
	}
	report["cloud_while_restored"] = copyJSON(c.snap)
	c.mu.Unlock()
	newcomer := jobPrefix + "-new"
	jobs = append(jobs, newcomer)
	if _, _, err = direct.Jobs().Register(realRecoveryJob(newcomer, node.ID, "batch", false), nil); err != nil {
		t.Fatal("submit new task", err)
	}
	var fresh *api.Allocation
	waitRealNomad(t, 25*time.Second, "new task executing on restored node", func() bool {
		rows, _, e := direct.Jobs().Allocations(newcomer, false, nil)
		if e != nil {
			return false
		}
		for _, row := range rows {
			a, _, e := direct.Allocations().Info(row.ID, nil)
			if e != nil {
				continue
			}
			task := a.TaskStates["main"]
			if a.NodeID == node.ID && task != nil && !task.StartedAt.Before(restoredAt) && (task.State == "running" || (task.State == "dead" && !task.Failed)) {
				fresh = a
				return true
			}
		}
		return false
	})
	report["new_allocation_on_restored_node"] = fresh
	// The dedicated Nomad node is still physically alive. Only the cloud model
	// now presents an authoritative later retry deletion; purge=false is explicit.
	laterDeletion(t, engine, c.settlementCloud)
	for i := 0; i < 5 && engine.state.Pending != nil; i++ {
		if err = engine.reconcileOnce(); err != nil {
			t.Fatal("model async retry settlement", err)
		}
	}
	if engine.state.Pending != nil || engine.state.Batch != nil || engine.state.LastBatch == nil || engine.state.LastBatch.Phase != "failed" || engine.state.LastCompletedAt != 0 || engine.checkRecoveryAudit() != nil || c.writeCount() != 1 {
		t.Fatal("failed batch did not settle without replay", engine.state.LastEvent)
	}
	report["state_after_model_retry"] = copyJSON(engine.state)
	// This proves the next engine action is admitted; it does not claim the
	// standard Autoscaler Agent or real CVM expansion ran in this integration.
	changed, err = engine.ScaleAndWait(1, 15)
	if !changed || err != nil {
		t.Fatal("next engine expansion", changed, err)
	}
	report["state_after_next_engine_action"] = copyJSON(engine.state)
	mu.Lock()
	report["nomad_writes"] = copyJSON(writes)
	restores, drains := 0, 0
	for _, w := range writes {
		if strings.HasSuffix(w["path"].(string), "/drain") {
			if w["mark_eligible"].(bool) {
				restores++
			} else {
				drains++
			}
		}
	}
	mu.Unlock()
	if restores != 1 || drains != 1 {
		t.Fatal("real Nomad mutation replay", drains, restores)
	}
	if !lostRestore {
		t.Fatal("lost ACK injection never reached real restore")
	}
	report["cloud_write_history"] = copyJSON(c.writes)
	report["passed"] = true
}

type realNomadRestoreKey struct{}
type realProcessCloud struct {
	*settlementCloud
	beforeRemove func() error
}

func (c *realProcessCloud) RemoveExact(ctx context.Context, group, id string) (tencentcloud.WriteResult, error) {
	if err := c.beforeRemove(); err != nil {
		return tencentcloud.WriteResult{}, err
	}
	result, err := c.multiCloud.RemoveExact(ctx, group, id)
	if err != nil {
		return result, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	at := time.Now().UTC().Format(time.RFC3339)
	a := c.activities[result.ActivityID]
	a.Status = "FAILED"
	a.Type = "REMOVE_INSTANCES"
	a.StartedAt = at
	a.EndedAt = at
	a.RelatedInstances[0].Status = "FAILED"
	a.RequestIDs = []tencentcloud.RequestTrace{{Action: "DescribeAutoScalingActivities", RequestID: testRequest}}
	c.activities[result.ActivityID] = a
	for i := range c.snap.Members {
		if c.snap.Members[i].ID == id {
			c.snap.Members[i].Lifecycle = "TERMINATION_FAILED"
		}
	}
	return result, nil
}
func waitRealNomad(t *testing.T, limit time.Duration, what string, fn func() bool) {
	t.Helper()
	until := time.Now().Add(limit)
	for time.Now().Before(until) {
		if fn() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("timed out waiting for " + what)
}
func realRecoveryJob(id, node, kind string, poststop bool) *api.Job {
	one := 1
	cpu, mem := 20, 32
	kill := 2 * time.Second
	main := &api.Task{Name: "main", Driver: "raw_exec", Config: map[string]any{"command": "/bin/sleep", "args": []string{"120"}}, Resources: &api.Resources{CPU: &cpu, MemoryMB: &mem}, KillTimeout: &kill}
	if kind == "batch" {
		main.Config["args"] = []string{"4"}
	}
	tasks := []*api.Task{main}
	if poststop {
		tasks = append(tasks, &api.Task{Name: "poststop", Driver: "raw_exec", Config: map[string]any{"command": "/bin/sleep", "args": []string{"3"}}, Resources: &api.Resources{CPU: &cpu, MemoryMB: &mem}, Lifecycle: &api.TaskLifecycle{Hook: "poststop"}})
	}
	name := "work"
	return &api.Job{ID: &id, Name: &id, Type: &kind, Datacenters: []string{"dc1"}, Constraints: []*api.Constraint{{LTarget: "${node.unique.id}", Operand: "=", RTarget: node}}, TaskGroups: []*api.TaskGroup{{Name: &name, Count: &one, Tasks: tasks, Networks: []*api.NetworkResource{{Mode: "host"}}}}}
}
func startRealNomadForRecovery(t *testing.T, binary, directory string, report map[string]any) (string, *exec.Cmd, <-chan struct{}) {
	t.Helper()
	freePort := func() int {
		l, e := net.Listen("tcp", "127.0.0.1:0")
		if e != nil {
			t.Fatal(e)
		}
		p := l.Addr().(*net.TCPAddr).Port
		_ = l.Close()
		return p
	}
	httpPort, rpcPort, serfPort := freePort(), freePort(), freePort()
	address := fmt.Sprintf("http://127.0.0.1:%d", httpPort)
	config := fmt.Sprintf(`bind_addr = "127.0.0.1"
log_level = "WARN"
ports { http = %d rpc = %d serf = %d }
advertise { http = "127.0.0.1:%d" rpc = "127.0.0.1:%d" serf = "127.0.0.1:%d" }
client {
 network_interface = "lo"
 meta { cloud_instance_id = "ins-test1" }
}
plugin "raw_exec" { config { enabled = true } }
consul { auto_advertise = false server_auto_join = false client_auto_join = false }
`, httpPort, rpcPort, serfPort, httpPort, rpcPort, serfPort)
	path := filepath.Join(directory, "agent.hcl")
	if err := os.WriteFile(path, []byte(config), 0600); err != nil {
		t.Fatal(err)
	}
	logFile := filepath.Join(directory, "nomad.log")
	log, err := os.OpenFile(logFile, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(binary, "agent", "-dev", "-config="+path, "-data-dir="+filepath.Join(directory, "data"), "-bind=127.0.0.1")
	cmd.Env = []string{"PATH=/usr/bin:/bin", "HOME=" + directory}
	cmd.Stdout = log
	cmd.Stderr = log
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err = cmd.Start(); err != nil {
		_ = log.Close()
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { _ = cmd.Wait(); _ = log.Close(); close(done) }()
	// This fallback also covers failures before the caller installs normal cleanup.
	t.Cleanup(func() {
		select {
		case <-done:
		default:
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
				<-done
			}
		}
		raw, _ := os.ReadFile(logFile)
		report["nomad_log"] = string(raw)
	})
	report["http_address"] = address
	report["rpc_address"] = fmt.Sprintf("127.0.0.1:%d", rpcPort)
	report["nomad_pid"] = cmd.Process.Pid
	return address, cmd, done
}

func startRealNomadClientForRecovery(t *testing.T, binary, directory, rpc string, report map[string]any, instanceID ...string) (string, *exec.Cmd, <-chan struct{}) {
	t.Helper()
	if err := os.Mkdir(directory, 0700); err != nil {
		t.Fatal(err)
	}
	free := func() int {
		l, e := net.Listen("tcp", "127.0.0.1:0")
		if e != nil {
			t.Fatal(e)
		}
		p := l.Addr().(*net.TCPAddr).Port
		_ = l.Close()
		return p
	}
	httpPort, rpcPort, serfPort := free(), free(), free()
	instance := "ins-spare"
	if len(instanceID) == 1 {
		instance = instanceID[0]
	}
	config := fmt.Sprintf(`bind_addr = "127.0.0.1"
name = "nomad-spec-spare"
log_level = "WARN"
ports { http = %d rpc = %d serf = %d }
advertise { http = "127.0.0.1:%d" rpc = "127.0.0.1:%d" serf = "127.0.0.1:%d" }
client {
 enabled = true
 servers = ["%s"]
 network_interface = "lo"
 meta { cloud_instance_id = %q }
}
plugin "raw_exec" { config { enabled = true } }
consul { auto_advertise = false server_auto_join = false client_auto_join = false }
`, httpPort, rpcPort, serfPort, httpPort, rpcPort, serfPort, rpc, instance)
	path := filepath.Join(directory, "agent.hcl")
	if err := os.WriteFile(path, []byte(config), 0600); err != nil {
		t.Fatal(err)
	}
	logFile := filepath.Join(directory, "nomad.log")
	log, err := os.OpenFile(logFile, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(binary, "agent", "-client", "-config="+path, "-data-dir="+filepath.Join(directory, "data"))
	cmd.Env = []string{"PATH=/usr/bin:/bin", "HOME=" + directory}
	cmd.Stdout = log
	cmd.Stderr = log
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err = cmd.Start(); err != nil {
		_ = log.Close()
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { _ = cmd.Wait(); _ = log.Close(); close(done) }()
	t.Cleanup(func() {
		select {
		case <-done:
		default:
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
				<-done
			}
		}
	})
	report["spare_nomad_pid"] = cmd.Process.Pid
	return fmt.Sprintf("http://127.0.0.1:%d", httpPort), cmd, done
}
