package tencentcloud_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/huxunpei/nomad-autoscaler-tencent/internal/live"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadclient"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/tencentcloud"
)

// The production adapter and live engine run together. Only provider replies
// and Nomad observations are synthetic; no real endpoint or credential is used.
type rateEngineCloud struct {
	*tencentcloud.Client
	snap tencentcloud.Snapshot
}

func (c *rateEngineCloud) Observe(_ context.Context, _, tracked string) (tencentcloud.Snapshot, error) {
	b, _ := json.Marshal(c.snap)
	var s tencentcloud.Snapshot
	_ = json.Unmarshal(b, &s)
	s.At = time.Now().UTC()
	if tracked != "" {
		if _, ok := s.VMs[tracked]; !ok {
			s.VMs[tracked] = tencentcloud.VM{ID: tracked}
		}
	}
	return s, nil
}
func (c *rateEngineCloud) Activity(context.Context, string, string) (tencentcloud.Activity, error) {
	kind := "REMOVE_INSTANCES"
	if c.snap.Group.Count > 0 {
		kind = "SCALE_OUT"
	}
	stamp := time.Now().UTC().Format(time.RFC3339)
	return tencentcloud.Activity{ID: "asa-test", GroupID: "asg-test", Type: kind, StartedAt: stamp, EndedAt: stamp,
		Status: "SUCCESSFUL", RelatedInstances: []tencentcloud.RelatedInstance{{ID: "ins-one", Status: "SUCCESSFUL"}},
		RequestIDs: []tencentcloud.RequestTrace{{Action: "DescribeAutoScalingActivities", RequestID: "11111111-2222-3333-4444-555555555555"}}}, nil
}
func (*rateEngineCloud) ActivitiesSince(context.Context, string, time.Time) ([]tencentcloud.Activity, error) {
	return nil, nil
}

type rateEngineNomad struct {
	node     nomadclient.Node
	restores int
}

func (n *rateEngineNomad) ResolveMembers(_ context.Context, _, _ string, ids []string) ([]nomadclient.Node, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	return []nomadclient.Node{n.node}, nil
}
func (n *rateEngineNomad) ReadNode(context.Context, string) (nomadclient.Node, error) {
	return n.node, nil
}
func (*rateEngineNomad) TasksStopped(context.Context, string, bool) (bool, error) { return true, nil }
func (*rateEngineNomad) ActiveAllocations(context.Context, string) (int, error)   { return 0, nil }
func (n *rateEngineNomad) StartOwnedDrain(_ context.Context, _ nomadclient.Identity, op string, _ time.Duration) (nomadclient.MutationResult, error) {
	n.node.Eligibility = "ineligible"
	n.node.LastDrain = &nomadclient.DrainRecord{Owner: "nomad-autoscaler-tencent", OperationID: op, Status: "complete"}
	return nomadclient.MutationResult{WriteAttempted: true, NodeModifyIndex: 1, After: &n.node}, nil
}
func (n *rateEngineNomad) RestoreOwnedDrain(_ context.Context, _ nomadclient.Identity, op string) (nomadclient.MutationResult, error) {
	if n.node.LastDrain == nil || n.node.LastDrain.OperationID != op {
		return nomadclient.MutationResult{}, errors.New("wrong drain owner")
	}
	n.restores++
	n.node.Eligibility = "eligible"
	n.node.LastDrain.Status = "canceled"
	return nomadclient.MutationResult{WriteAttempted: true, NodeModifyIndex: 2, After: &n.node}, nil
}

type rateEngineTransport func(*http.Request) (*http.Response, error)

func (f rateEngineTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestRateLimitProductionAdapterEngineRecovery(t *testing.T) {
	for _, mode := range []string{"grow-success", "grow-exhausted", "shrink-exhausted", "shrink-unknown"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			shrink := strings.HasPrefix(mode, "shrink")
			count, target := int64(0), int64(1)
			if shrink {
				count, target = 1, 0
			}
			c := &rateEngineCloud{snap: tencentcloud.Snapshot{Region: "ap-chengdu", Group: tencentcloud.Group{ID: "asg-test", Min: 0, Max: 2, Desired: count, Count: count, Enabled: "ENABLED", Status: "NORMAL", Activity: "NOT_IN_ACTIVITY", RetryPolicy: "IMMEDIATE_RETRY", LaunchConfiguration: "asc-test"}, VMs: map[string]tencentcloud.VM{}, LaunchConfiguration: tencentcloud.LaunchConfiguration{ID: "asc-test", Status: "NORMAL", ChargeType: "POSTPAID_BY_HOUR", InstanceType: "S5.SMALL2", ImageID: "img-test", SecurityGroupIDs: []string{"sg-test"}, VersionNumber: 1}}}
			c.snap.Group.AllocationPolicy = "LAUNCH_CONFIGURATION"
			setCount := func(value int64) {
				c.snap.Group.Count = value
				c.snap.Group.Desired = value
				c.snap.Members = nil
				c.snap.VMs = map[string]tencentcloud.VM{}
				if value == 1 {
					c.snap.Members = []tencentcloud.Member{{ID: "ins-one", CreationType: "AUTO_CREATION", Lifecycle: "IN_SERVICE", AddedAt: time.Now().UTC().Format(time.RFC3339)}}
					c.snap.VMs["ins-one"] = tencentcloud.VM{ID: "ins-one", State: "RUNNING", Returned: true, ChargeType: "POSTPAID_BY_HOUR"}
				}
			}
			setCount(count)
			n := &rateEngineNomad{node: nomadclient.Node{Identity: nomadclient.Identity{NodeID: "11111111-1111-1111-1111-111111111111", InstanceID: "ins-one", GroupID: "asg-test", Region: "ap-chengdu"}, Status: "ready", Eligibility: "eligible"}}
			cfg := live.Config{Dir: filepath.Join(t.TempDir(), "journal"), GroupID: "asg-test", Region: "ap-chengdu", GroupContract: true, DisableReconciliation: true, ReconcileInterval: time.Hour, DrainDeadline: time.Minute, StabilizationInterval: time.Millisecond}
			calls, accepted := 0, 0
			allow := false
			intent := ""
			timeout := 6 * time.Second
			if mode == "shrink-unknown" {
				timeout = time.Second
			}
			client, err := tencentcloud.NewRateLimitTestClient(tencentcloud.Config{Region: "ap-chengdu", SecretID: "synthetic-id", SecretKey: "synthetic-key", Timeout: timeout}, rateEngineTransport(func(r *http.Request) (*http.Response, error) {
				calls++
				var s live.State
				b, readErr := os.ReadFile(filepath.Join(cfg.Dir, "live.json"))
				if readErr != nil || json.Unmarshal(b, &s) != nil || s.Pending == nil || s.Pending.CloudIntent == nil {
					t.Fatal("API attempt preceded durable intent")
				}
				if intent != "" && intent != s.Pending.ID {
					t.Fatal("retry created a different logical operation")
				}
				intent = s.Pending.ID
				action := r.Header.Get("X-TC-Action")
				want := "ModifyDesiredCapacity"
				if shrink {
					want = "RemoveInstances"
				}
				if action != want {
					return nil, fmt.Errorf("unexpected API %s", action)
				}
				if !shrink {
					var body map[string]interface{}
					if json.NewDecoder(r.Body).Decode(&body) != nil || body["DesiredCapacity"] != float64(target) || body["AssertOriginDesiredCapacity"] != float64(count) || len(body) != 3 {
						return nil, fmt.Errorf("absolute CAS request missing")
					}
				}
				body := ""
				switch {
				case mode == "shrink-unknown" && calls == 2:
					<-r.Context().Done()
					return nil, r.Context().Err()
				case !allow && !(mode == "grow-success" && calls == 2):
					body = `{"Response":{"Error":{"Code":"RequestLimitExceeded","Message":"synthetic-throttle"},"RequestId":"11111111-2222-3333-4444-555555555555"}}`
				default:
					accepted++
					setCount(target)
					body = `{"Response":{"RequestId":"11111111-2222-3333-4444-555555555555","ActivityId":"asa-test"}}`
				}
				return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
			}))
			if err != nil {
				t.Fatal(err)
			}
			c.Client = client
			open := func() *live.Engine {
				e, err := live.Open(cfg, c, n)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(e.Close)
				return e
			}
			e := open()
			changed, scaleErr := e.ScaleAndWait(target, 15)
			s, _, _ := e.Snapshot()
			if !changed {
				t.Fatal("no action attempted", scaleErr)
			}
			if mode == "grow-success" {
				if scaleErr != nil || calls != 2 || accepted != 1 || s.Pending != nil || s.LastCompletedAt == 0 {
					t.Fatalf("successful retry not completed: %v %+v calls=%d", scaleErr, s, calls)
				}
				return
			}
			if scaleErr == nil || s.LastCompletedAt != 0 {
				t.Fatal("failed action reported success", scaleErr)
			}
			if mode == "shrink-unknown" {
				if calls != 2 || accepted != 0 || s.Pending == nil || !s.Pending.Unknown || n.restores != 0 || n.node.Eligibility != "ineligible" {
					t.Fatal("ambiguous second request released or repeated", s)
				}
				e.Close()
				e = open()
				s, _, _ = e.Snapshot()
				if s.Pending == nil || !s.Pending.Unknown || calls != 2 {
					t.Fatal("unknown replayed on restart")
				}
				return
			}
			if calls != 4 || accepted != 0 || s.Pending != nil || s.Batch != nil || s.ManualReview || s.LastBatch == nil || s.LastBatch.Phase != "failed" {
				t.Fatalf("exhausted rejection stayed locked: %+v calls=%d", s, calls)
			}
			if shrink && (n.restores != 1 || n.node.Eligibility != "eligible") {
				t.Fatal("surviving drained node not restored")
			}
			e.Close()
			allow = true
			intent = ""
			e = open()
			if !shrink {
				if _, err := e.ScaleAndWait(target, 15); err == nil || err.Error() != "GrowthRetryBackoffActive" || calls != 4 {
					t.Fatal("failed grow did not back off", err, calls)
				}
				e.Close()
				// Advance the persisted deadline to simulate elapsed wall time.
				raw, err := os.ReadFile(filepath.Join(cfg.Dir, "live.json"))
				if err != nil {
					t.Fatal(err)
				}
				var timed live.State
				if json.Unmarshal(raw, &timed) != nil || timed.GrowthBackoff == nil {
					t.Fatal("missing durable backoff")
				}
				timed.GrowthBackoff.RetryAfter = time.Now().Add(-time.Second)
				raw, err = json.Marshal(timed)
				if err != nil {
					t.Fatal(err)
				}
				if err = os.WriteFile(filepath.Join(cfg.Dir, "live.json"), raw, 0600); err != nil {
					t.Fatal(err)
				}
				e = open()
			}
			if _, err = e.ScaleAndWait(target, 15); err != nil {
				t.Fatal("next normal action blocked", err)
			}
			s, _, _ = e.Snapshot()
			if calls != 5 || accepted != 1 || s.Pending != nil || s.LastCompletedAt == 0 || c.snap.Group.Count != target {
				t.Fatalf("next action duplicated capacity or failed: %+v calls=%d", s, calls)
			}
		})
	}
}
