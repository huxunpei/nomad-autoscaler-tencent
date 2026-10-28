package live

import (
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadtest"

	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadclient"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadsdk"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/targetconfig"
)

func TestSDKHTTPSystemJobPurgeAndTaskCompletionMatrix(t *testing.T) {
	for _, ignore := range []string{"default", "false", "true"} {
		for _, purge := range []string{"default", "false", "true"} {
			t.Run("ignore-"+ignore+"/purge-"+purge, func(t *testing.T) {
				runSDKHTTPSystemJobScenario(t, ignore, purge, false)
			})
		}
	}
	// Selecting a system-only node does not set the separate drain exemption.
	t.Run("empty-ignore-selector-with-default-drain-and-10m", func(t *testing.T) {
		runSDKHTTPSystemJobScenario(t, "default", "default", true)
	})
}

func runSDKHTTPSystemJobScenario(t *testing.T, ignore, purge string, selectorExemption bool) {
	t.Helper()
	t.Setenv("NOMAD_ALLOC_ID", "")
	options := map[string]string{"tencent_group_id": testGroup}
	if ignore != "default" {
		options["node_drain_ignore_system_jobs"] = ignore
	}
	if purge != "default" {
		options["node_purge"] = purge
	}
	if selectorExemption {
		options["node_selector_strategy"] = "empty_ignore_system"
		options["node_drain_deadline"] = "10m"
	}
	policy, err := (targetconfig.Agent{}).Policy(options)
	if err != nil {
		t.Fatal(err)
	}
	expectedDeadline, expectedSelector := 15*time.Minute, "least_busy"
	if selectorExemption {
		expectedDeadline, expectedSelector = 10*time.Minute, "empty_ignore_system"
	}
	if policy.DrainDeadline != expectedDeadline || policy.Selector != expectedSelector || policy.IgnoreSystemJobs != (ignore == "true") || policy.Purge != (purge == "true") {
		t.Fatal("configuration contract default or override changed")
	}
	cfg := config(t)
	cfg.GroupContract = true
	cfg.DrainDeadline = policy.DrainDeadline
	cfg.StabilizationInterval = 10 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	cfg.Context = ctx
	defer cancel()
	c := newCloud()
	c.addMember()
	var mu sync.Mutex
	var serviceStopped, systemStopped atomic.Bool
	drainCount, purgeCount, index := 0, 0, 100
	opID := ""
	purged := false
	drainSeen := make(chan struct{})
	checkJournal := func(phase string) bool {
		b, readErr := os.ReadFile(filepath.Join(cfg.Dir, "live.json"))
		var s State
		return readErr == nil && json.Unmarshal(b, &s) == nil && s.Pending != nil && s.Pending.Phase == phase && s.Pending.InstanceID == testInstance && s.Pending.NodeID == testNode && s.Pending.SDKLifecycle && s.NomadPolicySHA256 != ""
	}
	c.beforeWrite = func(phase string) error {
		if phase != "remove-intent" || !checkJournal(phase) || !serviceStopped.Load() || (!policy.IgnoreSystemJobs && !systemStopped.Load()) {
			return fmt.Errorf("required task or durable deletion precondition not met")
		}
		return nil
	}
	allocation := func(id, kind, task string, stopped bool) map[string]any {
		status, state, desired := "running", "running", "run"
		if stopped {
			status, state, desired = "complete", "dead", "stop"
		}
		if kind == "service" {
			status, desired = "complete", "stop"
		}
		return map[string]any{"ID": id, "NodeID": testNode, "Namespace": "team-two", "ClientStatus": status, "DesiredStatus": desired, "TaskGroup": "group", "Job": map[string]any{"Type": kind, "TaskGroups": []any{map[string]any{"Name": "group", "Tasks": []any{map[string]string{"Name": task}}}}}, "Resources": map[string]int{"CPU": 10, "MemoryMB": 10}, "TaskStates": map[string]any{task: map[string]string{"State": state}}}
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if nomadtest.DisabledACL(w, r) {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		index++
		w.Header().Set("X-Nomad-Index", fmt.Sprint(index))
		send := func(v any) { _ = json.NewEncoder(w).Encode(v) }
		node := map[string]any{"ID": testNode, "Status": "ready", "SchedulingEligibility": "eligible", "Meta": map[string]string{"cloud_instance_id": testInstance}, "NodeResources": map[string]any{"Cpu": map[string]int{"CpuShares": 1000}, "Memory": map[string]int{"MemoryMB": 1000}}}
		if drainCount > 0 {
			node["SchedulingEligibility"] = "ineligible"
			status := "complete"
			if !policy.IgnoreSystemJobs && !systemStopped.Load() {
				node["Drain"] = true
				node["DrainStrategy"] = map[string]any{"Deadline": int64(policy.DrainDeadline), "IgnoreSystemJobs": false}
				status = "draining"
			}
			node["LastDrain"] = map[string]any{"Status": status, "Meta": map[string]string{"drained_by": drainOwner, "tencent_operation_id": opID}}
		}
		switch r.URL.Path {
		case "/v1/nodes":
			if purged {
				send([]any{})
			} else {
				send([]any{node})
			}
		case "/v1/node/" + testNode:
			if purged {
				http.Error(w, "not found", 404)
			} else {
				send(node)
			}
		case "/v1/node/" + testNode + "/allocations":
			if r.URL.Query().Get("namespace") != "*" {
				t.Error("system scenario lost namespace completeness")
			}
			send([]any{allocation(testNode, "service", "poststop", serviceStopped.Load()), allocation("22222222-2222-2222-2222-222222222222", "system", "system-main", systemStopped.Load())})
		case "/v1/node/" + testNode + "/drain":
			var p struct {
				NodeID    string
				DrainSpec *struct {
					Deadline         time.Duration
					IgnoreSystemJobs bool
				}
				MarkEligible bool
				Meta         map[string]string
			}
			if !checkJournal("drain-intent") || json.NewDecoder(r.Body).Decode(&p) != nil || p.NodeID != testNode || p.DrainSpec == nil || p.DrainSpec.Deadline != policy.DrainDeadline || p.DrainSpec.IgnoreSystemJobs != policy.IgnoreSystemJobs || p.MarkEligible || p.Meta["drained_by"] != drainOwner || p.Meta["tencent_operation_id"] == "" {
				t.Error("system drain changed policy or preceded durable exact intent")
				http.Error(w, "bad drain", 400)
				return
			}
			opID = p.Meta["tencent_operation_id"]
			drainCount++
			if drainCount == 1 {
				close(drainSeen)
			}
			send(map[string]int{"NodeModifyIndex": index})
		case "/v1/node/" + testNode + "/purge":
			if !policy.Purge || !checkJournal("post-scale") {
				t.Error("purge option or durable post intent lost")
			}
			c.mu.Lock()
			absent := len(c.snap.Members) == 0 && c.snap.Group.Count == 0 && c.snap.Group.Desired == 0
			c.mu.Unlock()
			if !absent {
				t.Error("purge preceded confirmed cloud absence")
			}
			purgeCount++
			purged = true
			send(map[string]any{"EvalIDs": []string{}})
		default:
			t.Errorf("unexpected system scenario route: %s", r.URL.Path)
			http.Error(w, "unexpected", 404)
		}
	}))
	defer server.Close()
	raw, err := nomadclient.New(nomadclient.Config{Address: server.URL, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	scoped, err := raw.WithScope(testGroup, testRegion, "cloud_instance_id")
	if err != nil {
		t.Fatal(err)
	}
	adapter := nomadsdk.New(scoped, policy)
	e, err := Open(cfg, c, adapter)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { cancel(); e.Close() }()
	type outcome struct {
		changed bool
		err     error
	}
	done := make(chan outcome, 1)
	go func() { changed, err := e.ScaleAndWait(0, 200); done <- outcome{changed, err} }()
	select {
	case <-drainSeen:
	case v := <-done:
		t.Fatal("drain not reached", v.err)
	case <-ctx.Done():
		t.Fatal("drain not reached before context")
	}
	assertNoDeletion := func(why string) {
		select {
		case v := <-done:
			t.Fatalf("action returned while %s: %v", why, v.err)
		case <-time.After(120 * time.Millisecond):
		}
		if c.writeCount() != 0 {
			t.Fatal("deleted while " + why)
		}
	}
	assertNoDeletion("service poststop still running")
	serviceStopped.Store(true)
	if !policy.IgnoreSystemJobs {
		assertNoDeletion("system job still running")
		systemStopped.Store(true)
	}
	until := time.Now().Add(4 * time.Second)
	for c.writeCount() == 0 && time.Now().Before(until) {
		select {
		case v := <-done:
			t.Fatal("action failed before cloud deletion", v.err)
		default:
		}
		time.Sleep(5 * time.Millisecond)
	}
	if c.writeCount() != 1 {
		t.Fatal("exact deletion did not follow required task completion")
	}
	if policy.IgnoreSystemJobs && systemStopped.Load() {
		t.Fatal("test stopped the exempt system job")
	}
	c.complete()
	select {
	case v := <-done:
		if !v.changed || v.err != nil {
			t.Fatal("system/purge workflow failed", v.err)
		}
	case <-ctx.Done():
		t.Fatal("workflow did not complete")
	}
	s, _, err := e.Snapshot()
	if err != nil || s.Pending != nil || s.Batch != nil || s.LastBatch == nil || s.LastBatch.Phase != "completed" || s.LastCompletedAt <= 0 {
		t.Fatal("missing completed batch", err)
	}
	mu.Lock()
	defer mu.Unlock()
	expectedPurge := 0
	if policy.Purge {
		expectedPurge = 1
	}
	if drainCount != 1 || purgeCount != expectedPurge || purged != policy.Purge || c.writeCount() != 1 {
		t.Fatal("policy changed lifecycle write counts", drainCount, purgeCount, c.writeCount())
	}
	t.Logf("deadline=%s ignoreSystem=%t purge=%t selector=%s; service poststop blocked deletion; system still running at deletion=%t", policy.DrainDeadline, policy.IgnoreSystemJobs, policy.Purge, policy.Selector, !systemStopped.Load())
}
