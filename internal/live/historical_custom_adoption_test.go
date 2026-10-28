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
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadclient"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadsdk"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/targetconfig"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/tencentcloud"
)

// This is an acceptance probe against journals emitted by the actual pinned
// historical controller. Model hints never enter production code or justify
// replaying a provider mutation. Only region values were normalized after
// capture; JournalSHA256 binds the normalized archival bytes.
type historicalCustomModel struct {
	Commit, Name  string
	MaxCapacity   int64
	DrainDeadline time.Duration
	Snapshot      tencentcloud.Snapshot
	Nodes         []nomadclient.Node
	Activities    map[string]tencentcloud.Activity
	JournalSHA256 string
}

func TestHistoricalCustomJournalsRequireAutomaticAdoption(t *testing.T) {
	t.Setenv("NOMAD_ALLOC_ID", "")
	base := "testdata/historical-custom-d24948f"
	entries, err := os.ReadDir(base)
	if err != nil || len(entries) != 13 {
		t.Fatal("missing pinned historical fixtures", err)
	}
	report := []map[string]any{}
	for _, entry := range entries {
		t.Run(entry.Name(), func(t *testing.T) {
			name := entry.Name()
			raw, err := os.ReadFile(filepath.Join(base, name, "live.json"))
			if err != nil {
				t.Fatal(err)
			}
			var original State
			if json.Unmarshal(raw, &original) != nil {
				t.Fatal("invalid source")
			}
			data, err := os.ReadFile(filepath.Join(base, name, "model.json"))
			var model historicalCustomModel
			if err != nil || json.Unmarshal(data, &model) != nil || model.Commit != "d24948f491db5d0bad77da28f2a6c05f6617c377" || sha(raw) != model.JournalSHA256 || original.NomadPolicySHA256 != "" {
				t.Fatal("historical fixture provenance mismatch")
			}
			var writes atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if nomadtest.DisabledACL(w, r) {
					return
				}
				if r.Method != http.MethodGet {
					writes.Add(1)
					http.Error(w, "no provider mutations during read-only adoption", 405)
					return
				}
				w.Header().Set("X-Nomad-Index", "100")
				send := func(v any) { _ = json.NewEncoder(w).Encode(v) }
				node := func(n nomadclient.Node) map[string]any {
					out := map[string]any{"ID": n.NodeID, "Status": n.Status, "Drain": n.Drain, "SchedulingEligibility": n.Eligibility, "Meta": map[string]string{"cloud_instance_id": n.InstanceID, "tencent_group_id": n.GroupID, "tencent_region": n.Region}, "NodeResources": map[string]any{"Cpu": map[string]int{"CpuShares": 1000}, "Memory": map[string]int{"MemoryMB": 1000}}}
					if n.HasDrainStrategy {
						out["DrainStrategy"] = map[string]any{"Deadline": int64(model.DrainDeadline), "IgnoreSystemJobs": false}
					}
					if n.LastDrain != nil {
						out["LastDrain"] = map[string]any{"Status": n.LastDrain.Status, "Meta": map[string]string{"drained_by": n.LastDrain.Owner, "tencent_operation_id": n.LastDrain.OperationID}}
					}
					return out
				}
				if r.URL.Path == "/v1/nodes" {
					rows := []any{}
					for _, n := range model.Nodes {
						rows = append(rows, node(n))
					}
					send(rows)
					return
				}
				if strings.HasSuffix(r.URL.Path, "/allocations") {
					send([]any{})
					return
				}
				for _, n := range model.Nodes {
					if r.URL.Path == "/v1/node/"+n.NodeID {
						send(node(n))
						return
					}
				}
				http.Error(w, "missing", 404)
			}))
			defer server.Close()
			client, err := nomadclient.New(nomadclient.Config{Address: server.URL, Timeout: time.Second})
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			client, err = client.WithScope(original.GroupID, original.Region, "cloud_instance_id")
			if err != nil {
				t.Fatal(err)
			}
			policy, err := (targetconfig.Agent{}).Policy(map[string]string{"tencent_group_id": original.GroupID, "node_drain_deadline": model.DrainDeadline.String()})
			if err != nil {
				t.Fatal(err)
			}
			cfg := Config{Dir: filepath.Join(t.TempDir(), "journal"), GroupID: original.GroupID, Region: original.Region, GroupContract: true, DrainDeadline: model.DrainDeadline, ReconcileInterval: time.Hour, DisableReconciliation: true}
			if err := os.Mkdir(cfg.Dir, 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(cfg.Dir, "live.json"), raw, 0600); err != nil {
				t.Fatal(err)
			}
			cloud := &fakeCloud{snap: model.Snapshot}
			e, openErr := Open(cfg, cloud, nomadsdk.New(client, policy))
			after, readErr := os.ReadFile(filepath.Join(cfg.Dir, "live.json"))
			observed := ""
			if openErr != nil {
				observed = openErr.Error()
			}
			report = append(report, map[string]any{"name": name, "current_error": observed, "source_version": original.Version, "source_event": original.LastEvent, "source_manual_review": original.ManualReview, "input_unchanged": readErr == nil && string(after) == string(raw), "cloud_writes": cloud.writeCount(), "nomad_writes": writes.Load()})
			if openErr != nil {
				if e != nil {
					e.Close()
				}
				t.Fatalf("automatic historical adoption missing: %v", openErr)
			}
			defer e.Close()
			state, _, _ := e.Snapshot()
			if state.Version != 3 || state.LastCompletedAt != 0 || cloud.writeCount() != 0 || writes.Load() != 0 {
				t.Fatal("adoption manufactured success or changed provider state")
			}
			if original.Pending != nil {
				op := state.Pending
				want := original.Pending
				if op == nil || op.ID != want.ID || op.Kind != want.Kind || op.Target != want.Target || op.InstanceID != want.InstanceID || op.NodeID != want.NodeID || op.RequestID != want.RequestID || op.ActivityID != want.ActivityID || !op.StartedAt.Equal(want.StartedAt) || op.SDKLifecycle {
					t.Fatal("original operation lost or falsely relabeled as SDK pre")
				}
			}
			if original.Batch != nil && (state.Batch == nil || state.Batch.ID != original.Batch.ID || !reflect.DeepEqual(state.Batch.Selected, original.Batch.Selected)) {
				t.Fatal("original batch selection changed")
			}
			t.Logf("historical %s adopted without replays", name)
		})
	}
	if output := os.Getenv("NOMAD_LEGACY_ADOPTION_REPORT"); output != "" {
		if !filepath.IsAbs(output) {
			t.Fatal("report path must be absolute")
		}
		if err := os.WriteFile(output, jsonBytes(report), 0600); err != nil {
			t.Fatal(err)
		}
	}
	t.Log(fmt.Sprint("historical replay gate executed against all 13 fixtures"))
}
