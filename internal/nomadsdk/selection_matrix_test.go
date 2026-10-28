package nomadsdk

import (
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadtest"

	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadclient"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/targetconfig"
)

const matrixSystem = "44444444-4444-4444-4444-444444444444"
const matrixEmpty = "55555555-5555-5555-5555-555555555555"

type selectionMatrixNode struct {
	id, instance, class, dc, pool, job string
	index, cpu                         int
}

func selectionMatrixNodes() []selectionMatrixNode {
	return []selectionMatrixNode{
		{first, "ins-matrixa", "workers", "dc1", "pool1", "service", 500, 900},
		{second, "ins-matrixb", "workers", "dc1", "pool1", "service", 100, 10},
		{matrixSystem, "ins-matrixc", "workers", "dc1", "pool1", "system", 400, 5},
		{matrixEmpty, "ins-matrixd", "workers", "dc1", "pool1", "", 300, 0},
		// This node would win both empty and newest if cloud scoping were lost.
		{foreign, "ins-matrixforeign", "workers", "dc1", "pool1", "", 600, 0},
	}
}

func selectionMatrixAdapter(t *testing.T, nodes []selectionMatrixNode, options map[string]string) (*Adapter, func()) {
	t.Helper()
	t.Setenv("NOMAD_ALLOC_ID", "")
	metaKey := "cloud_instance_id"
	if options["instance_id_meta_key"] != "" {
		metaKey = options["instance_id_meta_key"]
	}
	var mu sync.Mutex
	resourceLists, allocationReads := 0, 0
	lookup := map[string]selectionMatrixNode{}
	for _, n := range nodes {
		lookup[n.id] = n
	}
	node := func(n selectionMatrixNode) map[string]any {
		return map[string]any{"ID": n.id, "NodeClass": n.class, "Datacenter": n.dc, "NodePool": n.pool, "Status": "ready", "SchedulingEligibility": "eligible", "CreateIndex": n.index, "Meta": map[string]string{metaKey: n.instance}, "NodeResources": map[string]any{"Cpu": map[string]int{"CpuShares": 1000}, "Memory": map[string]int{"MemoryMB": 1000}}}
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if nomadtest.DisabledACL(w, r) {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		w.Header().Set("X-Nomad-Index", "100")
		send := func(v any) { _ = json.NewEncoder(w).Encode(v) }
		if r.Method != http.MethodGet {
			t.Error("selection attempted mutation")
			http.Error(w, "unexpected write", 405)
			return
		}
		if r.URL.Path == "/v1/nodes" {
			if r.URL.Query().Get("resources") == "true" {
				resourceLists++
			}
			rows := []any{}
			for _, n := range nodes {
				rows = append(rows, node(n))
			}
			send(rows)
			return
		}
		if !strings.HasPrefix(r.URL.Path, "/v1/node/") {
			t.Error("unexpected matrix request")
			http.Error(w, "unexpected", 404)
			return
		}
		id := strings.TrimPrefix(r.URL.Path, "/v1/node/")
		allocs := strings.HasSuffix(id, "/allocations")
		id = strings.TrimSuffix(id, "/allocations")
		n, ok := lookup[id]
		if !ok {
			t.Error("unknown matrix node")
			http.Error(w, "missing", 404)
			return
		}
		if !allocs {
			send(node(n))
			return
		}
		allocationReads++
		if id == foreign {
			t.Error("SDK inspected allocations outside cloud membership")
		}
		if r.URL.Query().Get("namespace") != "*" {
			t.Error("matrix allocation query lost all namespaces")
		}
		if n.job == "" {
			send([]any{})
			return
		}
		// Both client and desired state are running: empty cannot treat this
		// as a server-terminal allocation and accidentally pass the test.
		send([]any{map[string]any{"ID": n.id, "NodeID": n.id, "Namespace": "team-two", "ClientStatus": "running", "DesiredStatus": "run", "TaskGroup": "group", "Job": map[string]string{"Type": n.job}, "Resources": map[string]int{"CPU": n.cpu, "MemoryMB": n.cpu}}})
	}))
	t.Cleanup(server.Close)
	client, err := nomadclient.New(nomadclient.Config{Address: server.URL, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	client, err = client.WithScope("asg-example", "ap-guangzhou", metaKey)
	if err != nil {
		t.Fatal(err)
	}
	values := map[string]string{"tencent_group_id": "asg-example"}
	for k, v := range options {
		values[k] = v
	}
	p, err := (targetconfig.Agent{}).Policy(values)
	if err != nil {
		t.Fatal(err)
	}
	return New(client, p), func() {
		mu.Lock()
		defer mu.Unlock()
		if resourceLists == 0 {
			t.Error("actual SDK resource-list request was not exercised")
		}
		t.Logf("actual SDK resource lists=%d allocation reads=%d; cloud-foreign allocation reads rejected", resourceLists, allocationReads)
	}
}

func matrixIdentities(nodes []selectionMatrixNode, ids ...string) []nomadclient.Identity {
	out := []nomadclient.Identity{}
	for _, id := range ids {
		for _, n := range nodes {
			if n.id == id {
				out = append(out, nomadclient.Identity{NodeID: id, InstanceID: n.instance, GroupID: "asg-example", Region: "ap-guangzhou"})
			}
		}
	}
	return out
}

func TestSDKSelectionStrategyExecutionMatrix(t *testing.T) {
	for _, tc := range []struct {
		name, strategy, want string
		ids                  []string
		sysbatch             bool
	}{
		{"default-least-busy", "", second, []string{first, second}, false},
		{"explicit-least-busy", "least_busy", second, []string{first, second}, false},
		{"newest", "newest_create_index", first, []string{first, second}, false},
		{"oldest", "oldest_create_index", second, []string{first, second}, false},
		{"empty", "empty", matrixEmpty, []string{second, matrixSystem, matrixEmpty}, false},
		{"empty-system-exempt", "empty_ignore_system", matrixSystem, []string{second, matrixSystem, matrixEmpty}, false},
		{"empty-no-candidate", "empty", "", []string{first, second}, false},
		{"system-exempt-does-not-ignore-sysbatch", "empty_ignore_system", matrixEmpty, []string{second, matrixSystem, matrixEmpty}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			nodes := selectionMatrixNodes()
			if tc.sysbatch {
				nodes[2].job = "sysbatch"
			}
			values := map[string]string{}
			if tc.strategy != "" {
				values["node_selector_strategy"] = tc.strategy
			}
			a, check := selectionMatrixAdapter(t, nodes, values)
			defer check()
			expectedStrategy := tc.strategy
			if expectedStrategy == "" {
				expectedStrategy = "least_busy"
			}
			if a.policy.Selector != expectedStrategy {
				t.Fatal("selector option/default changed")
			}
			got, err := a.SelectScaleIn(context.Background(), matrixIdentities(nodes, tc.ids...), 1)
			if tc.want == "" {
				if err == nil {
					t.Fatal("empty strategy selected occupied node")
				}
				return
			}
			if err != nil || len(got) != 1 || got[0].NodeID != tc.want {
				t.Fatalf("strategy=%s got=%+v err=%v want=%s", tc.strategy, got, err, tc.want)
			}
			t.Logf("strategy=%s selected=%s", a.policy.Selector, tc.want)
		})
	}
}

func TestSDKFilterAndMappingExecutionMatrix(t *testing.T) {
	for _, tc := range []struct {
		name           string
		options        map[string]string
		combined, miss bool
	}{
		{"empty-filters", nil, false, false},
		{"explicit-empty-filters", map[string]string{"node_class": "", "datacenter": "", "node_pool": ""}, false, false},
		{"class-only", map[string]string{"node_class": "workers"}, false, false},
		{"dc-only", map[string]string{"datacenter": "dc1"}, false, false},
		{"pool-only", map[string]string{"node_pool": "pool1"}, false, false},
		{"all-and", map[string]string{"node_class": "workers", "datacenter": "dc1", "node_pool": "pool1"}, true, false},
		{"class-miss", map[string]string{"node_class": "absent", "datacenter": "dc1", "node_pool": "pool1"}, true, true},
		{"dc-miss", map[string]string{"node_class": "workers", "datacenter": "absent", "node_pool": "pool1"}, true, true},
		{"pool-miss", map[string]string{"node_class": "workers", "datacenter": "dc1", "node_pool": "absent"}, true, true},
		{"custom-metadata-without-group-region", map[string]string{"instance_id_meta_key": "asset_instance", "node_class": "workers", "datacenter": "dc1", "node_pool": "pool1"}, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			nodes := selectionMatrixNodes()
			ids := []string{first, second}
			want := second
			if tc.combined {
				// Each nonmatching member fails exactly one different predicate.
				nodes[0].dc = "dc-other"
				nodes[1].class = "other"
				nodes[2].pool = "pool-other"
				ids = []string{first, second, matrixSystem, matrixEmpty}
				want = matrixEmpty
			} else {
				nodes[0].class = "other"
				nodes[0].dc = "dc-other"
				nodes[0].pool = "pool-other"
			}
			a, check := selectionMatrixAdapter(t, nodes, tc.options)
			defer check()
			got, err := a.SelectScaleIn(context.Background(), matrixIdentities(nodes, ids...), 1)
			if tc.miss {
				if err == nil {
					t.Fatal("AND filter mismatch ignored")
				}
				return
			}
			if err != nil || len(got) != 1 || got[0].NodeID != want {
				t.Fatal(fmt.Sprintf("filter/mapping changed: got=%+v want=%s err=%v", got, want, err))
			}
		})
	}
}
