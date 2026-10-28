package live

import (
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadtest"

	"encoding/json"
	"fmt"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadclient"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadsdk"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/targetconfig"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestUnknownSDKDrainHTTPActualSDKLostDrainAndRestoreResponses(t *testing.T) {
	for _, crash := range []bool{false, true} {
		t.Run(fmt.Sprintf("exit-before-sdk-return-%v", crash), func(t *testing.T) { unknownSDKDrainHTTP(t, crash) })
	}
}
func unknownSDKDrainHTTP(t *testing.T, crash bool) {
	t.Setenv("NOMAD_ALLOC_ID", "")
	cfg := config(t)
	cfg.GroupContract = true
	cfg.DrainDeadline = time.Second
	c := newCloud()
	c.addMember()
	var mu sync.Mutex
	drains, restores := 0, 0
	drained, restored := false, false
	nodeVersion := uint64(100)
	ownerOp := ""
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if nomadtest.DisabledACL(w, r) {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		w.Header().Set("X-Nomad-Index", "9000") // This query index is not the node's ModifyIndex.
		send := func(v any) { _ = json.NewEncoder(w).Encode(v) }
		node := map[string]any{"ID": testNode, "ModifyIndex": nodeVersion, "Status": "ready", "SchedulingEligibility": "eligible", "Meta": map[string]string{"cloud_instance_id": testInstance}, "NodeResources": map[string]any{"Cpu": map[string]int{"CpuShares": 1000}, "Memory": map[string]int{"MemoryMB": 1000}}}
		if drained || restored {
			status := "canceled"
			if drained {
				node["Drain"] = true
				node["DrainStrategy"] = map[string]int64{"Deadline": int64(time.Second)}
				node["SchedulingEligibility"] = "ineligible"
				status = "draining"
			}
			node["LastDrain"] = map[string]any{"Status": status, "Meta": map[string]string{"drained_by": drainOwner, "tencent_operation_id": ownerOp}}
		}
		switch r.URL.Path {
		case "/v1/nodes":
			send([]any{node})
		case "/v1/node/" + testNode:
			send(node)
		case "/v1/node/" + testNode + "/allocations":
			send([]any{})
		case "/v1/node/" + testNode + "/drain":
			var body struct {
				MarkEligible bool
				DrainSpec    json.RawMessage
				Meta         map[string]string
			}
			if json.NewDecoder(r.Body).Decode(&body) != nil {
				t.Error("bad SDK drain payload")
			}
			raw, err := os.ReadFile(filepath.Join(cfg.Dir, "live.json"))
			var state State
			if err != nil || json.Unmarshal(raw, &state) != nil || state.Pending == nil || state.Pending.NomadDrainIntent == nil || state.Pending.CloudIntent != nil {
				t.Error("missing durable SDK origin")
				http.Error(w, "invalid intent", 500)
				return
			}
			if body.Meta["drained_by"] != drainOwner || body.Meta["tencent_operation_id"] != state.Pending.ID {
				t.Error("wrong SDK drain owner")
			}
			ownerOp = state.Pending.ID
			if body.MarkEligible {
				if r.Method != http.MethodPost || state.Pending.Phase != "aborting" || state.Pending.AbortReason != "sdk-drain-response-lost" || string(body.DrainSpec) != "null" || state.RecoveryAudit == nil {
					t.Error("restore outside audited failure")
				}
				record, err := readRecord(cfg.Dir, *state.RecoveryAudit)
				if err != nil || record.Plan.Decision.Action != "begin-node-restore" {
					t.Error("restore preceded start audit")
				}
				restores++
				drained = false
				restored = true
				nodeVersion++
			} else {
				if r.Method != http.MethodPut || state.Pending.Phase != "drain-intent" {
					t.Error("wrong SDK pre phase")
				}
				drains++
				drained = true
				nodeVersion++
			}
			_, _ = fmt.Fprint(w, "{lost-response")
		default:
			t.Errorf("unexpected HTTP operation %s %s", r.Method, r.URL.Path)
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
	policy, err := (targetconfig.Agent{}).Policy(map[string]string{"tencent_group_id": testGroup, "node_drain_deadline": "1s"})
	if err != nil {
		t.Fatal(err)
	}
	adapter := nomadsdk.New(scoped, policy)
	e, err := Open(cfg, c, adapter)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if crash {
		e.Close()
		input := filepath.Join(t.TempDir(), "sdk-exit.json")
		if err = os.WriteFile(input, jsonBytes(unknownDrainSDKExitInput{cfg.Dir, server.URL, c.snap}), 0600); err != nil {
			t.Fatal(err)
		}
		cmd := exec.Command(os.Args[0], "-test.run=^TestUnknownSDKDrainExitBeforeReturnProcessHelper$")
		cmd.Env = append(os.Environ(), "NOMAD_UNKNOWN_DRAIN_SDK_EXIT_FIXTURE="+input)
		out, err := cmd.CombinedOutput()
		if err == nil || cmd.ProcessState.ExitCode() != 55 {
			t.Fatalf("SDK process did not exit before return: %v %s", err, out)
		}
		raw, err := os.ReadFile(filepath.Join(cfg.Dir, "live.json"))
		var intent State
		if err != nil || json.Unmarshal(raw, &intent) != nil || intent.Pending == nil || intent.Pending.Phase != "drain-intent" || intent.Pending.Unknown || intent.Pending.NomadDrainIntent == nil || intent.Pending.CloudIntent != nil {
			t.Fatal("SDK exit did not preserve original pre-call intent")
		}
		e, err = Open(cfg, c, adapter)
		if err != nil {
			t.Fatal(err)
		}
		defer e.Close()
		if e.state.Pending == nil || !e.state.Pending.Unknown {
			t.Fatal("restart did not classify uncertain original intent")
		}
	} else {
		if changed, err := e.ScaleAndWait(0, 3); !changed || err != ErrUnknown {
			t.Fatal("lost SDK drain response not unknown", changed, err)
		}
	}

	step(t, e)
	if e.state.Pending == nil || e.state.Pending.Phase != "aborting" || e.state.Pending.NodeModifyIndex != 101 {
		t.Fatal("did not adopt actual node ModifyIndex")
	}
	e.Close()
	e, err = Open(cfg, c, adapter)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	step(t, e)
	if e.state.Pending != nil || e.state.Batch != nil || e.state.LastBatch == nil || e.state.LastBatch.Phase != "failed" || e.state.LastCompletedAt != 0 || e.state.LastOperation.DrainOutcome != "restored" || c.writeCount() != 0 {
		t.Fatal("actual SDK failure not restored")
	}
	if err = e.checkRecoveryAudit(); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if drains != 1 || restores != 1 || drained || !restored || nodeVersion != 102 {
		t.Fatal("drain or restore was replayed", drains, restores, nodeVersion)
	}
}
