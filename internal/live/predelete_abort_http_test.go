package live

import (
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadtest"

	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadclient"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadsdk"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/targetconfig"
)

// Exercise the actual SDK drain and the actual Nomad restoration client. The
// HTTP service loses the restore response after applying it; ScaleAndWait must
// observe the restored node and return failure, with zero cloud deletions.
func TestPreDeleteAbortSDKHTTPRestoresAfterLostResponseAndReturnsFailure(t *testing.T) {
	t.Setenv("NOMAD_ALLOC_ID", "")
	cfg := config(t)
	cfg.GroupContract = true
	cfg.DrainDeadline = time.Second
	cfg.StabilizationInterval = 5 * time.Millisecond
	c := newCloud()
	c.addMember()
	var mu sync.Mutex
	drained, restored := false, false
	drains, restores, index := 0, 0, 100
	ownerOp := ""
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if nomadtest.DisabledACL(w, r) {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		index++
		w.Header().Set("X-Nomad-Index", fmt.Sprint(index))
		send := func(v any) { _ = json.NewEncoder(w).Encode(v) }
		node := map[string]any{"ID": testNode, "Status": "ready", "SchedulingEligibility": "eligible", "Meta": map[string]string{"cloud_instance_id": testInstance},
			"NodeResources": map[string]any{"Cpu": map[string]int{"CpuShares": 1000}, "Memory": map[string]int{"MemoryMB": 1000}}}
		if drained || restored {
			status := "complete"
			if drained {
				node["SchedulingEligibility"] = "ineligible"
			} else {
				status = "canceled"
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
				t.Error("invalid drain payload")
			}
			raw, err := os.ReadFile(filepath.Join(cfg.Dir, "live.json"))
			var s State
			if err != nil || json.Unmarshal(raw, &s) != nil || s.Pending == nil || s.Pending.InstanceID != testInstance {
				t.Error("missing durable operation")
				http.Error(w, "missing intent", 500)
				return
			}
			if body.Meta["drained_by"] != drainOwner || body.Meta["tencent_operation_id"] != s.Pending.ID {
				t.Error("ownership lost")
			}
			ownerOp = s.Pending.ID
			if body.MarkEligible {
				if r.Method != http.MethodPost || s.Pending.Phase != "aborting" || s.Pending.AbortReason == "" || string(body.DrainSpec) != "null" {
					t.Error("restore preceded durable abort or wrong restore payload")
				}
				restores++
				drained = false
				restored = true
				_, _ = w.Write([]byte("{lost-response"))
				return
			}
			if r.Method != http.MethodPut || s.Pending.Phase != "drain-intent" || !s.Pending.SDKLifecycle {
				t.Error("SDK pre did not own drain")
			}
			drains++
			drained = true
			restored = false
			if drains == 1 {
				c.mu.Lock()
				c.snap.Group.Min = 1
				c.mu.Unlock()
			}
			send(map[string]int{"NodeModifyIndex": index})
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
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
	e, err := Open(cfg, c, nomadsdk.New(scoped, policy))
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if changed, err := e.ScaleAndWait(0, 3); !changed || err == nil || err.Error() != "ScaleActionFailed" {
		t.Fatal("aborted action returned success or wrong failure", changed, err)
	}
	if e.state.Pending != nil || e.state.ManualReview || e.state.LastCompletedAt != 0 || e.state.LastOperation.DrainOutcome != "restored" || c.writeCount() != 0 {
		t.Fatal("abort was not durably reconciled")
	}
	mu.Lock()
	if drains != 1 || restores != 1 || drained || !restored {
		t.Error("restore was repeated or not observed", drains, restores)
	}
	mu.Unlock()
	c.mu.Lock()
	c.snap.Group.Min = 0
	c.mu.Unlock()
	if changed, err := e.Scale(0); !changed || err != nil {
		t.Fatal("later scale required manual recovery", changed, err)
	}
}
