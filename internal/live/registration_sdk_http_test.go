package live

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadclient"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadsdk"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadtest"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/targetconfig"
)

func TestRegistrationGraceSDKHTTPDrainsOnlyRegisteredSelection(t *testing.T) {
	t.Setenv("NOMAD_ALLOC_ID", "")
	cfg, c, _ := registrationFixture(t)
	var mu sync.Mutex
	drained := false
	op := ""
	writes := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if nomadtest.DisabledACL(w, r) {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		w.Header().Set("X-Nomad-Index", "100")
		node := map[string]any{"ID": testNode, "Status": "ready", "SchedulingEligibility": "eligible", "Meta": map[string]string{"cloud_instance_id": "ins-multi1"}, "NodeResources": map[string]any{"Cpu": map[string]int{"CpuShares": 1000}, "Memory": map[string]int{"MemoryMB": 1000}}}
		if drained {
			node["SchedulingEligibility"] = "ineligible"
			node["LastDrain"] = map[string]any{"Status": "complete", "Meta": map[string]string{"drained_by": drainOwner, "tencent_operation_id": op}}
		}
		send := func(v any) { _ = json.NewEncoder(w).Encode(v) }
		switch r.URL.Path {
		case "/v1/nodes":
			send([]any{node})
		case "/v1/node/" + testNode:
			send(node)
		case "/v1/node/" + testNode + "/allocations":
			send([]any{})
		case "/v1/node/" + testNode + "/drain":
			var body struct {
				NodeID    string
				DrainSpec *struct{ Deadline time.Duration }
				Meta      map[string]string
			}
			if json.NewDecoder(r.Body).Decode(&body) != nil || body.NodeID != testNode || body.DrainSpec == nil {
				t.Error("unexpected drain request")
			}
			op = body.Meta["tencent_operation_id"]
			drained = true
			writes++
			send(map[string]int{"NodeModifyIndex": 100})
		default:
			t.Error("unexpected endpoint", r.URL.Path)
			http.Error(w, "unexpected", 404)
		}
	}))
	defer server.Close()
	raw, err := nomadclient.New(nomadclient.Config{Address: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	scoped, err := raw.WithScope(testGroup, testRegion, "cloud_instance_id")
	if err != nil {
		t.Fatal(err)
	}
	policy, err := (targetconfig.Agent{}).Policy(map[string]string{"tencent_group_id": testGroup, "node_drain_deadline": cfg.DrainDeadline.String()})
	if err != nil {
		t.Fatal(err)
	}
	e := openMulti(t, cfg, c, nomadsdk.New(scoped, policy))
	if changed, err := e.Scale(1); !changed || err != nil {
		t.Fatal("SDK did not select remaining registered node", err)
	}
	for i := 0; i < 10; i++ {
		if err := e.reconcileOnce(); err != nil {
			t.Fatal(err)
		}
		s, _, _ := e.Snapshot()
		if s.Pending != nil && s.Pending.Phase == "removing" {
			break
		}
	}
	if s, _, _ := e.Snapshot(); s.Pending == nil || s.Pending.InstanceID != "ins-multi1" || s.Pending.Phase != "removing" {
		t.Fatal("exact removal not submitted")
	}
	c.terminal(true)
	for i := 0; i < 20; i++ {
		if err := e.reconcileOnce(); err != nil {
			t.Fatal(err)
		}
		s, _, _ := e.Snapshot()
		if s.Pending == nil && s.Batch == nil {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	step(t, e) // Completion is published before the next ready observation.
	s, ready, err := e.Snapshot()
	if err != nil || !ready || s.Pending != nil || s.Batch != nil || len(s.Members) != 1 || s.Members["ins-multi0"].RegistrationState != "timed_out" {
		t.Fatal("partial registration workflow did not settle", s.LastEvent, err)
	}
	mu.Lock()
	defer mu.Unlock()
	if writes != 1 || len(c.writes) != 1 || c.writes[0] != "remove:ins-multi1" {
		t.Fatal(fmt.Sprint("incorrect deletion: ", c.writes))
	}
}
