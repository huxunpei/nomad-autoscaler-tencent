package live

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadclient"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadsdk"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/targetconfig"
)

func TestRegistrationGraceHTTPExpiryAndRestart(t *testing.T) {
	c := newCloud()
	c.addMember()
	c.snap.Members[0].AddedAt = time.Now().Add(-time.Hour).UTC().Format(time.RFC3339)
	var joined atomic.Bool
	var failRead atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Error("registration observation issued a write")
			http.Error(w, "unexpected", 500)
			return
		}
		if failRead.Load() {
			http.Error(w, "synthetic error", 403)
			return
		}
		node := map[string]any{"ID": testNode, "Status": "ready", "SchedulingEligibility": "eligible", "Meta": map[string]string{"cloud_instance_id": testInstance}}
		switch r.URL.Path {
		case "/v1/nodes":
			nodes := []any{}
			if joined.Load() {
				nodes = append(nodes, node)
			}
			_ = json.NewEncoder(w).Encode(nodes)
		case "/v1/node/" + testNode:
			_ = json.NewEncoder(w).Encode(node)
		default:
			t.Error("unexpected read", r.URL.Path)
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
	policy, _ := (targetconfig.Agent{}).Policy(map[string]string{"tencent_group_id": testGroup})
	adapter := nomadsdk.New(scoped, policy)
	cfg := config(t)
	cfg.GroupContract = true
	e, err := Open(cfg, c, adapter)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { e.Close() }()
	check := func(wantReady bool) {
		t.Helper()
		s, ready, err := e.Snapshot()
		if err != nil || ready != wantReady || len(s.Members) != 1 {
			t.Fatalf("unexpected registration readiness: ready=%v error=%v state=%s", ready, err, s.ErrorCode)
		}
	}
	check(true)
	// Timeout only releases unrelated operations. It never selects this VM.
	if changed, err := e.Scale(0); changed || err == nil || c.writeCount() != 0 {
		t.Fatal("absent VM was selected for deletion", changed, err)
	}
	e.Close()
	e, err = Open(cfg, c, adapter)
	if err != nil {
		t.Fatal(err)
	}
	check(true)
	// An ACL failure is not evidence of continued absence.
	failRead.Store(true)
	_ = e.reconcileOnce()
	check(false)
	failRead.Store(false)
	joined.Store(true)
	if err := e.reconcileOnce(); err != nil {
		t.Fatal(err)
	}
	check(true)
	if s, _, _ := e.Snapshot(); !s.Members[testInstance].Joined {
		t.Fatal("late registration not recognized")
	}
	// A node that was registered and then disappears cannot use initial grace.
	joined.Store(false)
	_ = e.reconcileOnce()
	check(false)
	e.Close()
	e, err = Open(cfg, c, adapter)
	if err != nil {
		t.Fatal(err)
	}
	check(false)
	if c.writeCount() != 0 {
		t.Fatal("observation wrote to cloud")
	}
}
