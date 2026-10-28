package live

import (
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadtest"

	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadclient"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadsdk"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/targetconfig"
)

// This runs the engine against the actual upstream SDK over a synthetic HTTP
// server. It proves the selected identity and durable phase cross the real SDK
// request path, rather than only testing an interface double.
func TestSDKHTTPWorkflowPersistsBeforeDrainAndPurgesOnlyAfterCloudAbsence(t *testing.T) {
	for _, tc := range []struct {
		name          string
		deadline      time.Duration
		groupContract bool
	}{{"original-one-second", time.Second, false}, {"group-contract-forty-eight-hours", 48 * time.Hour, true}, {"group-contract-no-deadline", 0, true}, {"group-contract-forced-drain", -time.Second, true}} {
		t.Run(tc.name, func(t *testing.T) { runSDKHTTPDeadlineWorkflow(t, tc.deadline, tc.groupContract) })
	}
}
func TestSDKHTTPV1SingleOperationMigratesAndCompletes(t *testing.T) {
	runSDKHTTPDeadlineWorkflowMode(t, time.Second, false, true)
}
func runSDKHTTPDeadlineWorkflow(t *testing.T, deadline time.Duration, groupContract bool) {
	runSDKHTTPDeadlineWorkflowMode(t, deadline, groupContract, false)
}
func runSDKHTTPDeadlineWorkflowMode(t *testing.T, deadline time.Duration, groupContract, migrateV1 bool) {
	t.Setenv("NOMAD_ALLOC_ID", "")
	cfg := config(t)
	cfg.DrainDeadline = deadline
	cfg.GroupContract = groupContract
	c := newCloud()
	c.addMember()
	var mu sync.Mutex
	drained, purged := false, false
	drainWrites, purgeWrites, index := 0, 0, 100
	ownerOp := ""
	checkJournal := func(phase string) {
		b, err := os.ReadFile(filepath.Join(cfg.Dir, "live.json"))
		var s State
		if err != nil || json.Unmarshal(b, &s) != nil || s.Pending == nil || s.Pending.Phase != phase || !s.Pending.SDKLifecycle || s.Pending.InstanceID != testInstance || s.NomadPolicySHA256 == "" {
			t.Error("SDK request preceded exact durable intent")
		}
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
		if drained {
			node["SchedulingEligibility"] = "ineligible"
			node["LastDrain"] = map[string]any{"Status": "complete", "Meta": map[string]string{"drained_by": drainOwner, "tencent_operation_id": ownerOp}}
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
				t.Error("namespace lost")
			}
			status, state := "running", "running"
			if drained {
				status, state = "complete", "dead"
			}
			send([]any{map[string]any{"ID": testNode, "NodeID": testNode, "Namespace": "default", "ClientStatus": status, "TaskGroup": "group", "Job": map[string]any{"Type": "service", "TaskGroups": []any{map[string]any{"Name": "group", "Tasks": []any{map[string]string{"Name": "poststop"}}}}}, "Resources": map[string]int{"CPU": 10, "MemoryMB": 10}, "TaskStates": map[string]any{"poststop": map[string]string{"State": state}}}})
		case "/v1/node/" + testNode + "/drain":
			checkJournal("drain-intent")
			var body struct {
				NodeID    string
				DrainSpec *struct{ Deadline time.Duration }
				Meta      map[string]string
			}
			if json.NewDecoder(r.Body).Decode(&body) != nil || body.NodeID != testNode || body.DrainSpec == nil || body.DrainSpec.Deadline != deadline {
				t.Error("SDK NodeID payload lost")
			}
			ownerOp = body.Meta["tencent_operation_id"]
			if body.Meta["drained_by"] != drainOwner || ownerOp == "" {
				t.Error("ownership missing")
			}
			drained = true
			drainWrites++
			send(map[string]int{"NodeModifyIndex": index})
		case "/v1/node/" + testNode + "/purge":
			checkJournal("post-scale")
			c.mu.Lock()
			if len(c.snap.Members) != 0 || c.snap.Group.Desired != 0 {
				t.Error("purge before settled deletion")
			}
			c.mu.Unlock()
			purged = true
			purgeWrites++
			send(map[string]any{"EvalIDs": []string{}})
		default:
			t.Errorf("unexpected route %s", r.URL.Path)
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
	policy, err := (targetconfig.Agent{}).Policy(map[string]string{"tencent_group_id": testGroup, "node_drain_deadline": deadline.String(), "node_purge": "true"})
	if err != nil {
		t.Fatal(err)
	}
	adapter := nomadsdk.New(scoped, policy)
	e, err := Open(cfg, c, adapter)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { e.Close() }()
	if _, err = e.Scale(0); err != nil {
		t.Fatal(err)
	}
	if migrateV1 {
		original, _, snapErr := e.Snapshot()
		if snapErr != nil || original.Version != 1 || original.Batch != nil || original.Pending == nil || original.NomadInstanceIDMetaKey != "cloud_instance_id" {
			t.Fatal("SDK HTTP source was not v1")
		}
		e.Close()
		cfg.GroupContract = true
		e, err = Open(cfg, c, adapter)
		if err != nil {
			t.Fatal("SDK HTTP v1 migration", err)
		}
		state, _, _ := e.Snapshot()
		if state.LegacyContract == nil || state.LegacyContract.SourceVersion != 1 || !reflect.DeepEqual(state.Pending, original.Pending) || state.LastCompletedAt != 0 {
			t.Fatal("SDK HTTP migration changed original drain")
		}
		e.Close()
		session, recoveryErr := OpenRecovery(cfg, c, adapter)
		if recoveryErr != nil {
			t.Fatal("SDK HTTP v1 recovery reader", recoveryErr)
		}
		session.Close()
		e, err = Open(cfg, c, adapter)
		if err != nil {
			t.Fatal("SDK HTTP v1 restart", err)
		}
		mu.Lock()
		writes := drainWrites
		mu.Unlock()
		if writes != 1 || c.writeCount() != 0 {
			t.Fatal("SDK HTTP migration replayed providers")
		}
	}
	if groupContract {
		e.Close()
		session, recoveryErr := OpenRecovery(cfg, c, adapter)
		if recoveryErr != nil {
			t.Fatal("long deadline recovery entry rejected", recoveryErr)
		}
		if session.e.state.DrainDeadlineNanoseconds != int64(deadline) || session.e.state.Pending == nil {
			t.Fatal("durable long drain intent lost")
		}
		session.Close()
		e, err = Open(cfg, c, adapter)
		if err != nil {
			t.Fatal("long deadline restart rejected", err)
		}
	}
	step(t, e)
	c.complete()
	for i := 0; i < 8; i++ {
		step(t, e)
		state, _, _ := e.Snapshot()
		if state.Pending == nil && state.Batch == nil {
			break
		}
	}
	s, _, err := e.Snapshot()
	if err != nil || s.Pending != nil || s.LastOperation == nil || s.LastOperation.InstanceID != testInstance {
		t.Fatal("workflow not complete", err)
	}
	if migrateV1 && (s.LastBatch == nil || s.LastBatch.Phase != "completed" || s.LastCompletedAt <= 0 || s.LegacyContract != nil || e.checkRecoveryAudit() != nil) {
		t.Fatal("SDK HTTP v1 did not complete through post audit")
	}
	mu.Lock()
	defer mu.Unlock()
	if drainWrites != 1 || purgeWrites != 1 || c.writeCount() != 1 {
		t.Fatal("unexpected write count", drainWrites, purgeWrites, c.writeCount())
	}
}
