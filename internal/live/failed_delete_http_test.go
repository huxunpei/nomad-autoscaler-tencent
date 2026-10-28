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
	"testing"
	"time"

	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadclient"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadsdk"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/targetconfig"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/tencentcloud"
)

type failedDeleteHTTPCloud struct {
	loseWriteResponse bool
	*fakeCloud
	history []tencentcloud.Activity
}

func (c *failedDeleteHTTPCloud) ActivitiesSince(context.Context, string, time.Time) ([]tencentcloud.Activity, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return copyJSON(c.history), nil
}

func (c *failedDeleteHTTPCloud) RemoveExact(ctx context.Context, g, id string) (tencentcloud.WriteResult, error) {
	result, err := c.fakeCloud.RemoveExact(ctx, g, id)
	c.fail()
	if c.loseWriteResponse {
		c.mu.Lock()
		stamp := time.Now().UTC().Format(time.RFC3339)
		c.activity.Type = "REMOVE_INSTANCES"
		c.activity.StartedAt = stamp
		c.activity.EndedAt = stamp
		c.activity.RequestIDs = []tencentcloud.RequestTrace{{Action: "DescribeAutoScalingActivities", RequestID: testRequest}}
		c.history = []tencentcloud.Activity{copyJSON(c.activity)}
		c.mu.Unlock()
		return tencentcloud.WriteResult{Attempted: true, Unknown: true}, err
	}
	return result, err
}

// The real SDK drains and the real Nomad client restores through HTTP. The
// failed cloud is synthetic; no live resource is involved. Restart must read
// back the lost restoration response without another write or fake success.
func TestSubmittedFailureSDKHTTPRestoresAfterLostResponseAndRestart(t *testing.T) {
	submittedFailureSDKHTTP(t, false)
}
func TestDiscoveredDeletionFailureSDKHTTPRestoresAndSettles(t *testing.T) {
	submittedFailureSDKHTTP(t, true)
}
func submittedFailureSDKHTTP(t *testing.T, loseWriteResponse bool) {
	t.Setenv("NOMAD_ALLOC_ID", "")
	cfg := config(t)
	cfg.GroupContract = true
	cfg.DrainDeadline = time.Second
	cfg.StabilizationInterval = 5 * time.Millisecond
	c := &failedDeleteHTTPCloud{fakeCloud: newCloud(), loseWriteResponse: loseWriteResponse}
	c.addMember()
	var mu sync.Mutex
	drained, restored, purged := false, false, false
	drains, restores, purges, index := 0, 0, 0, 100
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
			if purged {
				send([]any{})
				return
			}
			send([]any{node})
		case "/v1/node/" + testNode:
			if purged {
				http.NotFound(w, r)
				return
			}
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
				if loseWriteResponse && (s.Pending.ActivityDiscovery == nil || s.RecoveryAudit == nil) {
					t.Error("restore preceded discovery audit")
				}
				if r.Method != http.MethodPost || s.Pending.Phase != "failed-delete" || s.Pending.ActivityStatus != "FAILED" || string(body.DrainSpec) != "null" {
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
			send(map[string]int{"NodeModifyIndex": index})
		case "/v1/node/" + testNode + "/purge":
			raw, err := os.ReadFile(filepath.Join(cfg.Dir, "live.json"))
			var state State
			if err != nil || json.Unmarshal(raw, &state) != nil || state.Pending == nil || state.Pending.Phase != "failed-delete-post" || state.Pending.DeleteSettlement == nil || state.Pending.ActivityStatus != "FAILED" || state.Pending.AbsentObservations != 2 {
				t.Error("purge preceded durable settlement")
			}
			purges++
			purged = true
			_, _ = w.Write([]byte("{lost-purge-response"))
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
	policy, err := (targetconfig.Agent{}).Policy(map[string]string{"tencent_group_id": testGroup, "node_drain_deadline": "1s", "node_purge": "true"})
	if err != nil {
		t.Fatal(err)
	}
	e, err := Open(cfg, c, nomadsdk.New(scoped, policy))
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	want := "ScaleActionFailed"
	if loseWriteResponse {
		want = ErrUnknown.Error()
	}
	if changed, err := e.ScaleAndWait(0, 3); !changed || err == nil || err.Error() != want {
		t.Fatal("aborted action returned success or wrong failure", changed, err)
	}
	if loseWriteResponse {
		step(t, e)
		if _, err := e.completionState(e.state.Batch.ID); err == nil || err.Error() != "ScaleActionFailed" {
			t.Fatal("discovered failure was not reported", err)
		}
	}

	e.Close()
	e, err = Open(cfg, c, nomadsdk.New(scoped, policy))
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	step(t, e)
	if e.state.Pending == nil || e.state.Pending.Phase != "failed-delete" || e.state.Pending.DrainOutcome != "restored" || e.state.ManualReview || e.state.LastCompletedAt != 0 || c.writeCount() != 1 {
		t.Fatal("lost recovery or claimed cloud settlement")
	}
	mu.Lock()
	if drains != 1 || restores != 1 || drained || !restored {
		t.Error("restore repeated or unobserved", drains, restores)
	}
	mu.Unlock()
	if _, ready, _ := e.Snapshot(); ready {
		t.Fatal("restoration incorrectly enabled new cloud action")
	}

	// Simulate a later exact cloud removal after scheduling was restored. The
	// actual SDK purge applies through HTTP, loses its response, then resumes by
	// reading the 404 after restart. This remains a synthetic cloud/Nomad server.
	stamp := e.state.Pending.StartedAt.Truncate(time.Second).Format(time.RFC3339)
	c.mu.Lock()
	if c.activity.StartedAt != "" {
		stamp = c.activity.StartedAt
	}
	c.activity.Type = "REMOVE_INSTANCES"
	c.activity.StartedAt = stamp
	c.activity.EndedAt = stamp
	c.activity.RequestIDs = []tencentcloud.RequestTrace{{Action: "DescribeAutoScalingActivities", RequestID: testRequest}}
	later := copyJSON(c.activity)
	later.ID = "asa-later"
	later.Status = "SUCCESSFUL"
	later.RelatedInstances[0].Status = "SUCCESSFUL"
	c.history = []tencentcloud.Activity{c.activity, later}
	c.snap.Group.Count = 0
	c.snap.Group.Desired = 0
	c.snap.Members = []tencentcloud.Member{}
	c.snap.VMs = map[string]tencentcloud.VM{}
	c.mu.Unlock()
	step(t, e)
	if err := e.reconcileOnce(); err == nil || err.Error() != "SDKPostIncomplete" {
		t.Fatal("lost purge response was not retained", err)
	}
	if e.state.Pending == nil || e.state.Pending.Phase != "failed-delete-post" {
		t.Fatal("lost post obligation")
	}
	e.Close()
	e, err = Open(cfg, c, nomadsdk.New(scoped, policy))
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	step(t, e)
	if e.state.Pending != nil || e.state.Batch != nil || e.state.ManualReview || e.state.LastBatch.Phase != "failed" || e.state.LastCompletedAt != 0 || c.writeCount() != 1 {
		t.Fatal("failed cloud settlement did not close safely")
	}
	mu.Lock()
	defer mu.Unlock()
	if purges != 1 || restores != 1 || drains != 1 {
		t.Fatal("SDK/HTTP replayed a completed mutation", purges, restores, drains)
	}
}
