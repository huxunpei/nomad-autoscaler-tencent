package live

import (
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadtest"

	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadclient"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadsdk"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/targetconfig"
)

// Effect tests derive fresh synthetic times from the pinned historical output.
// All timestamps move by one offset, before migration starts. The archival
// adoption gate separately uses and verifies the exact original journal bytes.
func historicalCustomInput(t *testing.T, name string, age time.Duration) (State, historicalCustomModel, []byte) {
	t.Helper()
	base := filepath.Join("testdata/historical-custom-d24948f", name)
	raw, err := os.ReadFile(filepath.Join(base, "live.json"))
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(base, "model.json"))
	if err != nil {
		t.Fatal(err)
	}
	var old State
	var model historicalCustomModel
	if json.Unmarshal(raw, &old) != nil || json.Unmarshal(data, &model) != nil || sha(raw) != model.JournalSHA256 {
		t.Fatal("bad historical source")
	}
	anchor := old.ObservedAt
	if old.Pending != nil {
		anchor = old.Pending.StartedAt
	} else if old.Batch != nil {
		anchor = old.Batch.StartedAt
	} else if old.LastOperation != nil {
		anchor = old.LastOperation.StartedAt
	}
	offset := time.Now().UTC().Add(-age).Truncate(time.Second).Sub(anchor.Truncate(time.Second))
	var shift func(any) any
	shift = func(v any) any {
		switch x := v.(type) {
		case string:
			if at, err := time.Parse(time.RFC3339Nano, x); err == nil {
				return at.Add(offset).UTC().Format(time.RFC3339Nano)
			}
		case map[string]any:
			for k, v := range x {
				x[k] = shift(v)
			}
		case []any:
			for i, v := range x {
				x[i] = shift(v)
			}
		}
		return v
	}
	move := func(b []byte) []byte {
		var v any
		if json.Unmarshal(b, &v) != nil {
			t.Fatal("invalid historical JSON")
		}
		return jsonBytes(shift(v))
	}
	raw = move(raw)
	data = move(data)
	if json.Unmarshal(raw, &old) != nil || json.Unmarshal(data, &model) != nil {
		t.Fatal("bad shifted input")
	}
	model.JournalSHA256 = sha(raw)
	return old, model, raw
}

type legacyHTTPHarness struct {
	metaKey                   string
	mapping                   map[string]string
	readUnavailable, unstable bool
	reads                     uint64
	mu                        sync.Mutex
	nodes                     map[string]nomadclient.Node
	purged                    map[string]bool
	writes                    []string
	blockedJob                string
	cloud                     *settlementCloud
	directory                 string
	deadline                  time.Duration
	t                         *testing.T
}

func (h *legacyHTTPHarness) serve(w http.ResponseWriter, r *http.Request) {
	if nomadtest.DisabledACL(w, r) {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	w.Header().Set("X-Nomad-Index", "101")
	send := func(v any) { _ = json.NewEncoder(w).Encode(v) }
	if h.readUnavailable {
		http.Error(w, "temporary model read", 503)
		return
	}
	h.reads++
	view := func(n nomadclient.Node) map[string]any {
		v := map[string]any{"ID": n.NodeID, "ModifyIndex": uint64(101), "Status": n.Status, "Drain": n.Drain, "SchedulingEligibility": n.Eligibility, "Meta": map[string]string{"cloud_instance_id": n.InstanceID, "tencent_group_id": n.GroupID, "tencent_region": n.Region}, "NodeResources": map[string]any{"Cpu": map[string]int{"CpuShares": 1000}, "Memory": map[string]int{"MemoryMB": 1000}}}
		key := h.metaKey
		if key == "" {
			key = "cloud_instance_id"
		}
		iid := n.InstanceID
		if v, ok := h.mapping[n.NodeID]; ok {
			iid = v
		}
		v["Meta"] = map[string]string{key: iid, "tencent_group_id": n.GroupID, "tencent_region": n.Region}
		if h.unstable {
			v["ModifyIndex"] = 100 + h.reads
		}
		if n.HasDrainStrategy {
			v["DrainStrategy"] = map[string]any{"Deadline": int64(time.Minute), "IgnoreSystemJobs": false}
		}
		if n.LastDrain != nil {
			v["LastDrain"] = map[string]any{"Status": n.LastDrain.Status, "Meta": map[string]string{"drained_by": n.LastDrain.Owner, "tencent_operation_id": n.LastDrain.OperationID}}
		}
		return v
	}
	if r.URL.Path == "/v1/nodes" {
		rows := []any{}
		for id, n := range h.nodes {
			if !h.purged[id] {
				rows = append(rows, view(n))
			}
		}
		send(rows)
		return
	}
	for id, n := range h.nodes {
		prefix := "/v1/node/" + n.NodeID
		if r.URL.Path == prefix && r.Method == http.MethodGet {
			if h.purged[id] {
				http.Error(w, "missing", 404)
			} else {
				send(view(n))
			}
			return
		}
		if r.URL.Path == prefix+"/allocations" {
			if r.URL.Query().Get("namespace") != "*" {
				h.t.Error("all namespace task check lost")
			}
			rows := []any{}
			if h.blockedJob != "" {
				rows = append(rows, map[string]any{"ID": n.NodeID, "NodeID": n.NodeID, "Namespace": "default", "ClientStatus": "running", "TaskGroup": "group", "Job": map[string]any{"Type": h.blockedJob, "TaskGroups": []any{map[string]any{"Name": "group", "Tasks": []any{map[string]any{"Name": "poststop", "Lifecycle": map[string]any{"Hook": "poststop"}}}}}}, "TaskStates": map[string]any{"poststop": map[string]string{"State": "running"}}, "Resources": map[string]int{"CPU": 10, "MemoryMB": 10}})
			}
			send(rows)
			return
		}
		if r.URL.Path == prefix+"/drain" {
			var body struct {
				NodeID    string
				DrainSpec *struct {
					Deadline         time.Duration
					IgnoreSystemJobs bool
				}
				MarkEligible bool
				Meta         map[string]string
			}
			if json.NewDecoder(r.Body).Decode(&body) != nil {
				h.t.Error("bad drain payload")
			}
			raw, _ := os.ReadFile(filepath.Join(h.directory, "live.json"))
			var state State
			if json.Unmarshal(raw, &state) != nil || state.Pending == nil || state.Pending.NodeID != n.NodeID || body.Meta["drained_by"] != drainOwner || body.Meta["tencent_operation_id"] != state.Pending.ID {
				h.t.Error("drain before exact durable ownership")
				http.Error(w, "bad ownership", 400)
				return
			}
			if body.DrainSpec == nil {
				if r.Method != http.MethodPost || !body.MarkEligible || state.RecoveryAudit == nil {
					h.t.Error("unrecorded restore")
				}
				n.Drain = false
				n.HasDrainStrategy = false
				n.Eligibility = "eligible"
				n.LastDrain.Status = "canceled"
				h.writes = append(h.writes, "restore:"+id)
			} else {
				if r.Method != http.MethodPut || state.Pending.Phase != "drain-intent" || !state.Pending.SDKLifecycle || state.Pending.LegacyOrigin != nil || body.DrainSpec.Deadline != h.deadline {
					h.t.Error("old pre replayed or new SDK intent missing")
				}
				if h.deadline <= 0 && (state.Pending.DrainWait == nil || state.Batch == nil || state.Batch.DrainWait == nil) {
					h.t.Error("new SDK nonpositive wait not durable before drain")
				}
				n.Drain = false
				n.HasDrainStrategy = false
				n.Eligibility = "ineligible"
				n.LastDrain = &nomadclient.DrainRecord{Status: "complete", Owner: drainOwner, OperationID: body.Meta["tencent_operation_id"]}
				h.writes = append(h.writes, "pre:"+id)
			}
			n.ModifyIndex = 101
			h.nodes[id] = n
			send(map[string]int{"NodeModifyIndex": 101})
			return
		}
		if r.URL.Path == prefix+"/purge" {
			raw, _ := os.ReadFile(filepath.Join(h.directory, "live.json"))
			var state State
			if r.Method != http.MethodPut || json.Unmarshal(raw, &state) != nil || state.Pending == nil || (state.Pending.Phase != "post-scale" && state.Pending.Phase != "failed-delete-post") || state.Pending.AbsentObservations != 2 {
				h.t.Error("purge before durable settlement")
			}
			h.cloud.mu.Lock()
			vm := h.cloud.snap.VMs[id]
			_, present := cloudMember(h.cloud.snap, id)
			h.cloud.mu.Unlock()
			if present || vm.Returned {
				h.t.Error("purge before actual cloud absence")
			}
			h.purged[id] = true
			h.writes = append(h.writes, "post:"+id)
			send(map[string]any{"EvalIDs": []string{}})
			return
		}
	}
	h.t.Errorf("unexpected historical HTTP route %s %s", r.Method, r.URL.Path)
	http.Error(w, "unexpected", 404)
}
func legacyHTTPOpen(t *testing.T, name string, age time.Duration, deadline ...time.Duration) (Config, *settlementCloud, *legacyHTTPHarness, State, *Engine) {
	t.Helper()
	t.Setenv("NOMAD_ALLOC_ID", "")
	old, model, raw := historicalCustomInput(t, name, age)
	cfg := Config{Dir: filepath.Join(t.TempDir(), "journal"), GroupID: old.GroupID, Region: old.Region, GroupContract: true, DrainDeadline: 15 * time.Minute, ReconcileInterval: time.Hour, DisableReconciliation: true}
	if len(deadline) > 0 {
		cfg.DrainDeadline = deadline[0]
	}
	if err := os.Mkdir(cfg.Dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(cfg.Dir, "live.json"), raw, 0600); err != nil {
		t.Fatal(err)
	}
	c := &settlementCloud{multiCloud: &multiCloud{fakeCloud: &fakeCloud{snap: model.Snapshot}, activities: model.Activities}}
	h := &legacyHTTPHarness{nodes: map[string]nomadclient.Node{}, purged: map[string]bool{}, cloud: c, directory: cfg.Dir, deadline: cfg.DrainDeadline, t: t}
	for _, n := range model.Nodes {
		n.ModifyIndex = 101
		h.nodes[n.InstanceID] = n
	}
	server := httptest.NewServer(http.HandlerFunc(h.serve))
	t.Cleanup(server.Close)
	client, err := nomadclient.New(nomadclient.Config{Address: server.URL, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	client, err = client.WithScope(old.GroupID, old.Region, "cloud_instance_id")
	if err != nil {
		t.Fatal(err)
	}
	policy, err := (targetconfig.Agent{}).Policy(map[string]string{"tencent_group_id": old.GroupID, "node_drain_deadline": cfg.DrainDeadline.String(), "node_purge": "true", "node_drain_ignore_system_jobs": "true"})
	if err != nil {
		t.Fatal(err)
	}
	adapter := nomadsdk.New(client, policy)
	e, err := Open(cfg, c, adapter)
	if err != nil {
		t.Fatal("historical HTTP adoption", err)
	}
	t.Cleanup(func() { e.Close() })
	if c.writeCount() != 0 || len(h.writes) != 0 || e.checkRecoveryAudit() != nil {
		t.Fatal("adoption replayed providers or invalid audit")
	}
	// Restart through both entrypoints before exercising any effects.
	e.Close()
	r, err := OpenRecovery(cfg, c, adapter)
	if err != nil {
		t.Fatal("historical recovery reader", err)
	}
	r.Close()
	e, err = Open(cfg, c, adapter)
	if err != nil {
		t.Fatal("historical restart", err)
	}
	return cfg, c, h, old, e
}
func runLegacyToTerminal(t *testing.T, e *Engine, c *settlementCloud) {
	t.Helper()
	e.EnableReconciliation()
	for i := 0; i < 12; i++ {
		err := e.reconcileOnce()
		if err != nil && err != ErrUnknown {
			t.Fatalf("reconciliation %d: %v (%s)", i, err, e.state.LastEvent)
		}
		c.terminal(true)
		c.mu.Lock()
		for id, a := range c.activities {
			if a.Status == "SUCCESSFUL" && a.EndedAt == "" {
				a.EndedAt = time.Now().UTC().Format(time.RFC3339)
				c.activities[id] = a
			}
		}
		c.mu.Unlock()
		if e.state.Pending == nil && e.state.Batch == nil {
			return
		}
	}
	t.Fatal("historical operation still pending", e.state.LastEvent)
}
func TestLegacyCustomHTTPDrainingRetainsTaskContractAndSDKPost(t *testing.T) {
	for _, job := range []string{"service", "system"} {
		t.Run(job, func(t *testing.T) {
			_, c, h, old, e := legacyHTTPOpen(t, "v1-draining", 5*time.Second)
			h.mu.Lock()
			n := h.nodes[testInstance]
			n.Drain = false
			n.HasDrainStrategy = false
			n.LastDrain.Status = "complete"
			h.nodes[testInstance] = n
			h.blockedJob = job
			h.mu.Unlock()
			e.EnableReconciliation()
			for i := 0; i < 2; i++ {
				_ = e.reconcileOnce()
			}
			if c.writeCount() != 0 || len(h.writes) != 0 || e.state.Pending == nil {
				t.Fatal("ignored original custom system/poststop contract")
			}
			if !e.operationDrainUntil(e.state.Pending).Equal(old.Pending.StartedAt.Add(time.Minute+time.Minute)) || e.state.DrainDeadlineNanoseconds != int64(15*time.Minute) {
				t.Fatal("original drain deadline reinterpreted")
			}
			h.mu.Lock()
			h.blockedJob = ""
			h.mu.Unlock()
			runLegacyToTerminal(t, e, c)
			if c.writeCount() != 1 || !reflect.DeepEqual(h.writes, []string{"post:" + testInstance}) || e.state.LastOperation.SDKLifecycle || e.state.LastOperation.LegacyOrigin == nil || e.state.LastCompletedAt <= 0 || e.state.LegacyContract != nil || e.checkRecoveryAudit() != nil {
				t.Fatal("old pre replayed or SDK post missing", h.writes, e.state.LastEvent)
			}
		})
	}
}
func TestLegacyCustomHTTPKnownDeletionNeverReplaysCloud(t *testing.T) {
	for _, name := range []string{"v1-removing", "v1-delete-unknown", "v2-removing", "v2-delete-unknown"} {
		t.Run(name, func(t *testing.T) {
			_, c, h, old, e := legacyHTTPOpen(t, name, 5*time.Second)
			c.terminal(true)
			runLegacyToTerminal(t, e, c)
			want := 0
			if old.Batch != nil {
				want = len(old.Batch.Selected) - 1
			}
			if c.writeCount() != want || e.state.LastBatch.Phase != "completed" || e.checkRecoveryAudit() != nil {
				t.Fatal("known delete replayed or not completed", c.writes, e.state.LastEvent)
			}
			post, pre := 0, 0
			for _, w := range h.writes {
				if strings.HasPrefix(w, "post:") {
					post++
				}
				if strings.HasPrefix(w, "pre:") {
					pre++
				}
			}
			if pre != want || post != want+1 {
				t.Fatal("SDK lifecycle count", h.writes)
			}
		})
	}
}
func TestLegacyCustomHTTPBetweenChildrenKeepsPrefixAndStartsOriginalNext(t *testing.T) {
	_, c, h, old, e := legacyHTTPOpen(t, "v2-between-children", 5*time.Second)
	prefix := copyJSON(old.Batch.Results)
	next := old.Batch.Selected[len(prefix)]
	runLegacyToTerminal(t, e, c)
	if c.writeCount() != 1 || !reflect.DeepEqual(e.state.LastBatch.Results[:len(prefix)], prefix) || !reflect.DeepEqual(h.writes, []string{"pre:" + next.InstanceID, "post:" + next.InstanceID}) || e.checkRecoveryAudit() != nil {
		t.Fatal("historical prefix/selection changed", h.writes)
	}
}
func TestLegacyCustomHTTPFailedRestoredWaitsForAsyncCloudRetry(t *testing.T) {
	_, c, h, old, e := legacyHTTPOpen(t, "v1-failed-restored", 5*time.Second)
	e.EnableReconciliation()
	_ = e.reconcileOnce()
	_ = e.reconcileOnce()
	if e.state.Pending == nil || e.state.Pending.DrainOutcome != "restored" || len(h.writes) != 0 || c.writeCount() != 0 || e.state.ManualReview || e.state.LastCompletedAt != 0 {
		t.Fatal("already-restored survivor mishandled", e.state.LastEvent)
	}
	n := h.nodes[old.LastOperation.InstanceID]
	if n.Eligibility != "eligible" {
		t.Fatal("survivor stopped receiving work")
	}
	laterDeletion(t, e, c)
	runLegacyToTerminal(t, e, c)
	if c.writeCount() != 0 || !reflect.DeepEqual(h.writes, []string{"post:" + n.InstanceID}) || e.state.LastBatch.Phase != "failed" || e.state.LastCompletedAt != 0 || e.state.LastOperation.ActivityID != old.LastOperation.ActivityID || e.state.LastOperation.SDKLifecycle || e.checkRecoveryAudit() != nil {
		t.Fatal("failed old batch fabricated success or dropped post", h.writes, e.state.LastEvent)
	}
	if ok, err := e.Scale(1); !ok || err != nil {
		t.Fatal("next action blocked", ok, err)
	}
	if !reflect.DeepEqual(c.writes, []string{"desired1"}) {
		t.Fatal("unexpected cloud replay", c.writes)
	}
}
func TestLegacyCustomHTTPUnknownDrainRestoresOnlyObservedOwner(t *testing.T) {
	for _, name := range []string{"v1-drain-unknown", "v1-drain-intent"} {
		t.Run(name, func(t *testing.T) {
			_, c, h, _, e := legacyHTTPOpen(t, name, 5*time.Second)
			if name == "v1-drain-intent" {
				n := h.nodes[testInstance]
				n.Eligibility = "ineligible"
				n.LastDrain = &nomadclient.DrainRecord{Owner: drainOwner, OperationID: e.state.Pending.ID, Status: "complete"}
				h.nodes[testInstance] = n
			}
			runLegacyToTerminal(t, e, c)
			if c.writeCount() != 0 || !reflect.DeepEqual(h.writes, []string{"restore:" + testInstance}) || e.state.LastCompletedAt != 0 || e.state.LastBatch.Phase != "failed" || e.state.LastOperation.SDKLifecycle || e.state.LastOperation.NomadDrainIntent != nil || e.checkRecoveryAudit() != nil {
				t.Fatal("unknown custom drain replayed or false SDK intent", h.writes, e.state.LastEvent)
			}
		})
	}
}
func TestLegacyCustomHTTPUnknownDrainRejectsMissingOwnerAndCloudIntent(t *testing.T) {
	for _, mode := range []string{"eligible-unowned", "other-owner", "other-operation", "node-down", "cloud-member-missing", "remove-intent"} {
		t.Run(mode, func(t *testing.T) {
			name := "v1-drain-unknown"
			if mode == "remove-intent" {
				name = "v1-remove-intent"
			}
			_, c, h, _, e := legacyHTTPOpen(t, name, 5*time.Second)
			h.mu.Lock()
			n := h.nodes[testInstance]
			switch mode {
			case "eligible-unowned":
				n.Eligibility = "eligible"
				n.Drain = false
				n.HasDrainStrategy = false
				n.LastDrain = nil
			case "other-owner":
				n.LastDrain.Owner = "operator"
			case "other-operation":
				n.LastDrain.OperationID = "live-other"
			case "node-down":
				n.Status = "down"
			case "cloud-member-missing":
				c.snap.Members = nil
				c.snap.Group.Count = 0
				delete(c.snap.VMs, testInstance)
			}
			h.nodes[testInstance] = n
			h.mu.Unlock()
			e.EnableReconciliation()
			for i := 0; i < 2; i++ {
				_ = e.reconcileOnce()
			}
			if c.writeCount() != 0 || len(h.writes) != 0 || e.state.Pending == nil || !e.state.Pending.Unknown || e.state.LastCompletedAt != 0 {
				t.Fatal("ambiguous source replayed", mode, e.state.LastEvent)
			}
		})
	}
}
func TestLegacyCustomHTTPExpiredOriginalDeadlineRestoresWithoutDeletion(t *testing.T) {
	_, c, h, _, e := legacyHTTPOpen(t, "v1-draining", 3*time.Minute)
	e.EnableReconciliation()
	runLegacyToTerminal(t, e, c)
	if c.writeCount() != 0 || !reflect.DeepEqual(h.writes, []string{"restore:" + testInstance}) || e.state.LastCompletedAt != 0 || e.state.LastOperation.AbortReason != "drain-completion-timeout" || e.state.LastBatch.Phase != "failed" {
		t.Fatal("new deadline extended old drain", h.writes, e.state.LastEvent)
	}
}

func TestLegacyCustomHTTPAcknowledgedGrowCompletesWithoutResubmission(t *testing.T) {
	for _, name := range []string{"v1-grow", "v2-grow"} {
		t.Run(name, func(t *testing.T) {
			_, c, h, old, e := legacyHTTPOpen(t, name, 5*time.Second)
			runLegacyToTerminal(t, e, c)
			if c.writeCount() != 0 || len(h.writes) != 0 || e.state.LastCompletedAt <= 0 || e.state.LastOperation.ID != old.Pending.ID || e.state.LastOperation.SDKLifecycle || e.state.LastOperation.LegacyOrigin == nil || e.checkRecoveryAudit() != nil {
				t.Fatal("acknowledged grow resubmitted or lost origin", e.state.LastEvent)
			}
		})
	}
}
func TestLegacyCustomHTTPInterruptedRestoreRetainsAsyncFailure(t *testing.T) {
	_, c, h, old, e := legacyHTTPOpen(t, "v1-restore-intent", 5*time.Second)
	e.EnableReconciliation()
	_ = e.reconcileOnce()
	_ = e.reconcileOnce()
	if e.state.Pending == nil || e.state.Pending.DrainOutcome != "restored" || !reflect.DeepEqual(h.writes, []string{"restore:" + testInstance}) || c.writeCount() != 0 || e.state.LastCompletedAt != 0 {
		t.Fatal("interrupted legacy restore not reconciled", e.state.LastEvent, h.writes)
	}
	laterDeletion(t, e, c)
	runLegacyToTerminal(t, e, c)
	if c.writeCount() != 0 || !reflect.DeepEqual(h.writes, []string{"restore:" + testInstance, "post:" + testInstance}) || e.state.LastBatch.Phase != "failed" || e.state.LastCompletedAt != 0 || e.state.LastOperation.ActivityID != old.Pending.ActivityID || e.checkRecoveryAudit() != nil {
		t.Fatal("old restore fabricated success or replayed", h.writes, e.state.LastEvent)
	}
}
func TestLegacyCustomAdmissionRejectsUnsupportedContractChanges(t *testing.T) {
	for _, mode := range []string{"mapping-change", "wrong-group", "bad-origin-hash", "sdk-already-bound", "modern-intent", "unrelated-manual"} {
		t.Run(mode, func(t *testing.T) {
			old, _, raw := historicalCustomInput(t, "v1-draining", 5*time.Second)
			cfg := Config{GroupID: old.GroupID, Region: old.Region, GroupContract: true, DrainDeadline: 15 * time.Minute}
			binding := LegacyCustomBinding{PolicySHA256: strings.Repeat("a", 64), InstanceMetaKey: "cloud_instance_id", DrainDeadline: cfg.DrainDeadline}
			hash := sha(raw)
			switch mode {
			case "mapping-change":
				binding.InstanceMetaKey = "other_instance_id"
			case "wrong-group":
				cfg.GroupID = "asg-other"
			case "bad-origin-hash":
				hash = "bad"
			case "sdk-already-bound":
				old.NomadPolicySHA256 = strings.Repeat("b", 64)
			case "modern-intent":
				old.Pending.NomadDrainIntent = &NomadDrainIntent{At: old.Pending.StartedAt}
			case "unrelated-manual":
				old.ManualReview = true
			}
			before := jsonBytes(old)
			if _, err := legacyCustomAfter(cfg, old, hash, binding); err == nil {
				t.Fatal("unsafe historical adoption accepted", mode)
			}
			if !reflect.DeepEqual(before, jsonBytes(old)) {
				t.Fatal("rejected admission changed source")
			}
		})
	}
}

func TestLegacyCustomBackupRejectsRewrittenOriginalIntent(t *testing.T) {
	for _, mode := range []string{"origin-deadline", "origin-phase", "started-at", "request-id", "activity-id", "sdk-flag", "drop-origin"} {
		t.Run(mode, func(t *testing.T) {
			_, _, _, _, e := legacyHTTPOpen(t, "v1-removing", 5*time.Second)
			switch op := e.state.Pending; mode {
			case "origin-deadline":
				op.LegacyOrigin.DrainDeadline += time.Second
			case "origin-phase":
				op.LegacyOrigin.Phase = "draining"
			case "started-at":
				op.StartedAt = op.StartedAt.Add(time.Second)
			case "request-id":
				op.RequestID = "22222222-2222-3333-4444-555555555555"
			case "activity-id":
				op.ActivityID = "asa-other"
			case "sdk-flag":
				op.SDKLifecycle = true
			case "drop-origin":
				op.LegacyOrigin = nil
			}
			if e.checkLegacyContractBackup() == nil {
				t.Fatal("rewritten legacy source accepted", mode)
			}
		})
	}
}
