package nomadsdk

import (
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadtest"

	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadclient"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/targetconfig"
)

const first = "11111111-1111-1111-1111-111111111111"
const second = "22222222-2222-2222-2222-222222222222"
const foreign = "33333333-3333-3333-3333-333333333333"
const operation = "live-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

type fixture struct {
	nodeReadStatus                        int
	mu                                    sync.Mutex
	drained, purged                       bool
	drainWrites, purgeWrites              int
	taskRunning, system                   bool
	authFailed                            bool
	barrier                               atomic.Bool
	index                                 atomic.Int64
	duplicate                             bool
	purgeLost                             bool
	paged, selfDenied, malformedResources bool
}

func identity(id string) nomadclient.Identity {
	iid := "ins-first"
	if id == second {
		iid = "ins-second"
	}
	if id == foreign {
		iid = "ins-foreign"
	}
	return nomadclient.Identity{NodeID: id, InstanceID: iid, GroupID: "asg-example", Region: "ap-guangzhou"}
}
func (f *fixture) serve(t *testing.T) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if nomadtest.DisabledACL(w, r) {
			return
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		w.Header().Set("X-Nomad-Index", fmt.Sprint(100+f.index.Add(1)))
		if r.Header.Get("X-Nomad-Token") != "synthetic-token" {
			t.Error("SDK lost inherited ACL token")
		}
		send := func(v any) { json.NewEncoder(w).Encode(v) }
		node := func(id string) map[string]any {
			iid := identity(id).InstanceID
			if f.duplicate && id == foreign {
				iid = "ins-first"
			}
			n := map[string]any{"ID": id, "NodeClass": "workers", "Datacenter": "dc1", "NodePool": "pool1", "Status": "ready", "SchedulingEligibility": "eligible", "Meta": map[string]string{"custom_instance": iid}, "CreateIndex": 100, "NodeResources": map[string]any{"Cpu": map[string]int{"CpuShares": 1000}, "Memory": map[string]int{"MemoryMB": 1000}}}
			if id == foreign {
				n["NodeClass"] = "foreign"
				n["Status"] = "initializing"
			}
			if f.drained && id == second {
				n["SchedulingEligibility"] = "ineligible"
				n["LastDrain"] = map[string]any{"Status": "complete", "Meta": map[string]string{"drained_by": "nomad-autoscaler-tencent", "tencent_operation_id": operation}}
			}
			return n
		}
		switch {
		case r.URL.Path == "/v1/nodes":
			if f.paged {
				if r.URL.Query().Get("next_token") == "" {
					w.Header().Set("X-Nomad-NextToken", "page-two")
					send([]any{node(first)})
				} else {
					send([]any{node(second), node(foreign)})
				}
			} else {
				send([]any{node(first), node(second), node(foreign)})
			}
		case strings.HasPrefix(r.URL.Path, "/v1/allocation/"):
			if f.selfDenied {
				http.Error(w, "synthetic-private-body", 403)
				return
			}
			send(map[string]string{"ID": strings.TrimPrefix(r.URL.Path, "/v1/allocation/"), "NodeID": second})
		case r.URL.Path == "/v1/node/"+first:
			send(node(first))
		case r.URL.Path == "/v1/node/"+second:
			if f.nodeReadStatus != 0 {
				http.Error(w, "synthetic-private-body", f.nodeReadStatus)
				return
			}
			if f.purged {
				http.Error(w, "not found", 404)
				return
			}
			send(node(second))
		case r.URL.Path == "/v1/node/"+foreign:
			send(node(foreign))
		case strings.HasSuffix(r.URL.Path, "/allocations"):
			if f.authFailed {
				http.Error(w, "synthetic-private-body", 403)
				return
			}
			if f.malformedResources {
				send([]any{map[string]any{"ID": first, "NodeID": first, "Namespace": "default", "ClientStatus": "running", "Job": map[string]string{"Type": "service"}}})
				return
			}
			if r.URL.Query().Get("namespace") != "*" {
				t.Error("SDK allocation query is not namespace-complete")
			}
			id := first
			cpu := 900
			if strings.Contains(r.URL.Path, second) {
				id = second
				cpu = 10
			}
			if strings.Contains(r.URL.Path, foreign) {
				t.Error("SDK queried foreign allocation data")
			}
			status, taskState, kind := "running", "running", "service"
			if f.drained && id == second {
				status = "complete"
				taskState = "dead"
				if f.taskRunning {
					taskState = "running"
				}
			}
			if f.system {
				kind = "system"
				status = "running"
				taskState = "running"
			}
			send([]any{map[string]any{"ID": id, "NodeID": id, "Namespace": "team-two", "ClientStatus": status, "DesiredStatus": "stop", "TaskGroup": "group", "Job": map[string]any{"Type": kind, "TaskGroups": []any{map[string]any{"Name": "group", "Tasks": []any{map[string]any{"Name": "poststop"}}}}}, "Resources": map[string]int{"CPU": cpu, "MemoryMB": cpu}, "TaskStates": map[string]any{"poststop": map[string]string{"State": taskState}}}})
		case r.URL.Path == "/v1/node/"+second+"/drain":
			if !f.barrier.Load() {
				t.Error("SDK drained before durable barrier")
			}
			var p struct {
				Meta      map[string]string
				DrainSpec struct {
					Deadline         int64
					IgnoreSystemJobs bool
				}
			}
			if json.NewDecoder(r.Body).Decode(&p) != nil {
				t.Error("invalid drain payload")
			}
			if p.Meta["drained_by"] != "nomad-autoscaler-tencent" || p.Meta["tencent_operation_id"] != operation {
				t.Error("drain ownership missing")
			}
			if p.DrainSpec.Deadline != int64(time.Second) || p.DrainSpec.IgnoreSystemJobs != f.system {
				t.Error("drain policy lost")
			}
			f.drained = true
			f.drainWrites++
			send(map[string]any{"NodeModifyIndex": 100})
		case r.URL.Path == "/v1/node/"+second+"/purge":
			if !f.barrier.Load() {
				t.Error("SDK purged before durable barrier")
			}
			f.purged = true
			f.purgeWrites++
			if f.purgeLost {
				http.Error(w, "synthetic-private-body", 503)
				return
			}
			send(map[string]any{"EvalIDs": []string{}})
		default:
			t.Errorf("unexpected SDK request: %s %s", r.Method, r.URL.Path)
			http.Error(w, "unexpected", 404)
		}
	}
}
func setup(t *testing.T, f *fixture, options map[string]string) *Adapter {
	t.Helper()
	t.Setenv("NOMAD_ALLOC_ID", "")
	server := httptest.NewServer(f.serve(t))
	t.Cleanup(server.Close)
	c, err := nomadclient.New(nomadclient.Config{Address: server.URL, Token: "synthetic-token", Namespace: "team-one", Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	c, err = c.WithScope("asg-example", "ap-guangzhou", "custom_instance")
	if err != nil {
		t.Fatal(err)
	}
	cfg := map[string]string{"tencent_group_id": "asg-example", "instance_id_meta_key": "custom_instance", "node_drain_deadline": "1s"}
	for k, v := range options {
		cfg[k] = v
	}
	p, err := (targetconfig.Agent{}).Policy(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return New(c, p)
}
func TestSDKSelectionUsesResourcesAndCloudScopeWithEmptyFilters(t *testing.T) {
	a := setup(t, &fixture{}, nil)
	got, err := a.SelectScaleIn(context.Background(), []nomadclient.Identity{identity(first), identity(second)}, 1)
	if err != nil {
		t.Fatal(err)
	}
	// Both nodes have one allocation; only the SDK resource-based ranking picks second.
	if len(got) != 1 || got[0] != identity(second) {
		t.Fatalf("incorrect selection: %+v", got)
	}
	nodes, err := a.ResolveMembers(context.Background(), "asg-example", "ap-guangzhou", []string{"ins-first", "ins-second"})
	if err != nil || len(nodes) != 2 {
		t.Fatal(nodes, err)
	}
}
func TestSDKCombinedFiltersAndDuplicateMappingFailBeforeWrite(t *testing.T) {
	for _, tc := range []struct {
		name      string
		duplicate bool
		dc        string
	}{{"filter", false, "dc-other"}, {"duplicate", true, "dc1"}} {
		t.Run(tc.name, func(t *testing.T) {
			f := &fixture{duplicate: tc.duplicate}
			a := setup(t, f, map[string]string{"node_class": "workers", "datacenter": tc.dc, "node_pool": "pool1"})
			if _, err := a.SelectScaleIn(context.Background(), []nomadclient.Identity{identity(first), identity(second)}, 1); err == nil {
				t.Fatal("unsafe selection accepted")
			}
			if f.drainWrites != 0 {
				t.Fatal("selection changed Nomad")
			}
		})
	}
}
func TestSDKPreAndPostUseDurableBarriersAndWaitForPoststop(t *testing.T) {
	f := &fixture{taskRunning: true}
	a := setup(t, f, map[string]string{"node_purge": "true"})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		r, e := a.PreScaleIn(ctx, identity(second), operation, time.Second, func() error { f.barrier.Store(true); return nil })
		if e == nil && (!r.WriteAttempted || r.Unknown || r.NodeModifyIndex == 0) {
			e = errors.New("SDK acknowledgement lost")
		}
		done <- e
	}()
	select {
	case e := <-done:
		t.Fatalf("pre returned before poststop: %v", e)
	case <-time.After(150 * time.Millisecond):
	}
	f.mu.Lock()
	if f.drainWrites != 1 {
		t.Error("drain did not reach SDK")
	}
	f.taskRunning = false
	f.mu.Unlock()
	select {
	case e := <-done:
		if e != nil {
			t.Fatal(e)
		}
	case <-ctx.Done():
		t.Fatal("pre did not complete")
	}
	f.barrier.Store(false)
	if err := a.PostScaleIn(ctx, identity(second), operation, func() error { f.barrier.Store(true); return nil }); err != nil {
		t.Fatal(err)
	}
	if err := a.PostScaleIn(ctx, identity(second), operation, func() error { t.Error("already purged node attempted another write"); return nil }); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.drainWrites != 1 || f.purgeWrites != 1 {
		t.Fatal("unexpected write count")
	}
}
func TestSDKBarrierFailurePreventsDrainAndIgnoreSystemJobsWorks(t *testing.T) {
	f := &fixture{system: true}
	a := setup(t, f, map[string]string{"node_drain_ignore_system_jobs": "true"})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := a.PreScaleIn(ctx, identity(second), operation, time.Second, func() error { return errors.New("synthetic-private-error") }); err == nil || strings.Contains(err.Error(), "private") {
		t.Fatal(err)
	}
	if f.drainWrites != 0 {
		t.Fatal("failed barrier was crossed")
	}
	if _, err := a.PreScaleIn(ctx, identity(second), operation, time.Second, func() error { f.barrier.Store(true); return nil }); err != nil {
		t.Fatal(err)
	}
	if err := a.PostScaleIn(ctx, identity(second), operation, func() error { t.Error("purge disabled but barrier called"); return nil }); err != nil {
		t.Fatal(err)
	}
	if f.purgeWrites != 0 {
		t.Fatal("default purge was not disabled")
	}
}

func TestSDKReadErrorsAndCanceledContextCannotStartDrain(t *testing.T) {
	f := &fixture{authFailed: true}
	a := setup(t, f, nil)
	_, err := a.SelectScaleIn(context.Background(), []nomadclient.Identity{identity(first), identity(second)}, 1)
	if err == nil || strings.Contains(err.Error(), "private") {
		t.Fatal("allocation error was ignored or leaked", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result, err := a.PreScaleIn(ctx, identity(second), operation, time.Second, func() error { t.Error("canceled call crossed barrier"); return nil })
	if err == nil || result.WriteAttempted {
		t.Fatal("canceled call mutated")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.drainWrites != 0 {
		t.Fatal("read failure started drain")
	}
}
func TestSDKTaskTimeoutRetainsAcknowledgedDrain(t *testing.T) {
	f := &fixture{taskRunning: true}
	a := setup(t, f, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	result, err := a.PreScaleIn(ctx, identity(second), operation, time.Second, func() error { f.barrier.Store(true); return nil })
	if err == nil || !result.WriteAttempted || result.Unknown || !result.MonitoringIncomplete || result.NodeModifyIndex == 0 {
		t.Fatal("known drain incorrectly became unknown", result, err)
	}
}
func TestSDKPurgeBarrierFailureLeavesNodeForRetry(t *testing.T) {
	f := &fixture{drained: true}
	a := setup(t, f, map[string]string{"node_purge": "true"})
	err := a.PostScaleIn(context.Background(), identity(second), operation, func() error { return errors.New("synthetic-private-error") })
	if err == nil || strings.Contains(err.Error(), "private") {
		t.Fatal(err)
	}
	if f.purgeWrites != 0 {
		t.Fatal("purge crossed failed barrier")
	}
	if err = a.PostScaleIn(context.Background(), identity(second), operation, func() error { f.barrier.Store(true); return nil }); err != nil {
		t.Fatal(err)
	}
	if f.purgeWrites != 1 {
		t.Fatal("purge did not retry")
	}
}

func TestSDKLostPurgeResponseIsResolvedWithoutReplay(t *testing.T) {
	f := &fixture{drained: true, purgeLost: true}
	a := setup(t, f, map[string]string{"node_purge": "true"})
	if err := a.PostScaleIn(context.Background(), identity(second), operation, func() error { f.barrier.Store(true); return nil }); err == nil {
		t.Fatal("lost response reported success")
	}
	if err := a.PostScaleIn(context.Background(), identity(second), operation, func() error { t.Error("replayed purge"); return nil }); err != nil {
		t.Fatal(err)
	}
	if f.purgeWrites != 1 {
		t.Fatal("duplicate purge")
	}
}

func TestSDKPaginationAndSelfNodeExclusion(t *testing.T) {
	for _, denied := range []bool{false, true} {
		t.Run(fmt.Sprint("self-denied-", denied), func(t *testing.T) {
			f := &fixture{paged: true, selfDenied: denied}
			a := setup(t, f, nil)
			t.Setenv("NOMAD_ALLOC_ID", foreign)
			got, err := a.SelectScaleIn(context.Background(), []nomadclient.Identity{identity(first), identity(second)}, 1)
			if denied {
				if err == nil {
					t.Fatal("failed self-node lookup was silently ignored")
				}
				return
			}
			if err != nil || len(got) != 1 || got[0] != identity(first) {
				t.Fatal("SDK failed pagination or selected own node", got, err)
			}
		})
	}
}
func TestSDKMalformedResourcesFailBeforeUpstreamSelectorCanPanic(t *testing.T) {
	a := setup(t, &fixture{malformedResources: true}, nil)
	if _, err := a.SelectScaleIn(context.Background(), []nomadclient.Identity{identity(first), identity(second)}, 1); err == nil {
		t.Fatal("missing resources accepted")
	}
}
