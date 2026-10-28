package live

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadclient"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/tencentcloud"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type multiCloud struct {
	*fakeCloud
	activities map[string]tencentcloud.Activity
	removed    string
}

func (c *multiCloud) SetDesired(ctx context.Context, group string, count int64) (tencentcloud.WriteResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.writes = append(c.writes, fmt.Sprint("desired", count))
	for i := len(c.snap.Members); i < int(count); i++ {
		id := fmt.Sprintf("ins-multi%d", i)
		c.snap.Members = append(c.snap.Members, tencentcloud.Member{ID: id, CreationType: "AUTO_CREATION", Lifecycle: "IN_SERVICE", AddedAt: time.Now().UTC().Format(time.RFC3339)})
		c.snap.VMs[id] = tencentcloud.VM{ID: id, State: "RUNNING", Returned: true, ChargeType: "POSTPAID_BY_HOUR"}
	}
	c.snap.Group.Count = count
	c.snap.Group.Desired = count
	return tencentcloud.WriteResult{Attempted: true, RequestID: testRequest}, nil
}
func (c *multiCloud) RemoveExact(ctx context.Context, g, id string) (tencentcloud.WriteResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.writes = append(c.writes, "remove:"+id)
	a := fmt.Sprintf("asa-multi%d", len(c.writes))
	c.removed = id
	c.activities[a] = tencentcloud.Activity{ID: a, GroupID: g, Status: "RUNNING", RelatedInstances: []tencentcloud.RelatedInstance{{ID: id, Status: "RUNNING"}}}
	c.snap.Group.Desired--
	return tencentcloud.WriteResult{Attempted: true, Unknown: c.removeUnknown, RequestID: testRequest, ActivityID: a}, nil
}
func (c *multiCloud) Activity(ctx context.Context, g, id string) (tencentcloud.Activity, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return copyJSON(c.activities[id]), nil
}
func (c *multiCloud) terminal(success bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for id, a := range c.activities {
		if a.Status != "RUNNING" {
			continue
		}
		a.Status = "FAILED"
		a.RelatedInstances[0].Status = "FAILED"
		if success {
			a.Status = "SUCCESSFUL"
			a.RelatedInstances[0].Status = "SUCCESSFUL"
			members := []tencentcloud.Member{}
			for _, m := range c.snap.Members {
				if m.ID != a.RelatedInstances[0].ID {
					members = append(members, m)
				}
			}
			c.snap.Members = members
			delete(c.snap.VMs, a.RelatedInstances[0].ID)
			c.snap.Group.Count = int64(len(members))
			c.snap.Group.Desired = int64(len(members))
		}
		c.activities[id] = a
	}
}

type multiNomad struct {
	nodes  map[string]nomadclient.Node
	load   map[string]int
	writes []string
}

func (n *multiNomad) ResolveMembers(ctx context.Context, g, r string, ids []string) ([]nomadclient.Node, error) {
	out := []nomadclient.Node{}
	for _, id := range ids {
		out = append(out, copyJSON(n.nodes[id]))
	}
	return out, nil
}
func (n *multiNomad) ReadNode(ctx context.Context, id string) (nomadclient.Node, error) {
	for _, v := range n.nodes {
		if v.NodeID == id {
			return copyJSON(v), nil
		}
	}
	return nomadclient.Node{}, fmt.Errorf("missing")
}
func (n *multiNomad) ActiveAllocations(ctx context.Context, id string) (int, error) {
	return n.load[id], nil
}
func (n *multiNomad) StartOwnedDrain(ctx context.Context, id nomadclient.Identity, op string, d time.Duration) (nomadclient.MutationResult, error) {
	v := n.nodes[id.InstanceID]
	n.writes = append(n.writes, "drain:"+id.InstanceID)
	v.Eligibility = "ineligible"
	v.LastDrain = &nomadclient.DrainRecord{Owner: drainOwner, OperationID: op, Status: "complete"}
	n.nodes[id.InstanceID] = v
	return nomadclient.MutationResult{WriteAttempted: true, NodeModifyIndex: 1, After: &v}, nil
}
func (n *multiNomad) RestoreOwnedDrain(ctx context.Context, id nomadclient.Identity, op string) (nomadclient.MutationResult, error) {
	v := n.nodes[id.InstanceID]
	if v.LastDrain.OperationID != op {
		return nomadclient.MutationResult{}, fmt.Errorf("owner")
	}
	n.writes = append(n.writes, "restore:"+id.InstanceID)
	v.Eligibility = "eligible"
	v.LastDrain.Status = "canceled"
	n.nodes[id.InstanceID] = v
	return nomadclient.MutationResult{WriteAttempted: true, NodeModifyIndex: 2, After: &v}, nil
}
func multiFixture(t *testing.T, count, max int64) (Config, *multiCloud, *multiNomad, *Engine) {
	t.Helper()
	c := &multiCloud{fakeCloud: newCloud(), activities: map[string]tencentcloud.Activity{}}
	c.snap.Group.Max = max
	n := &multiNomad{nodes: map[string]nomadclient.Node{}, load: map[string]int{}}
	for i := 0; i < int(max); i++ {
		id := fmt.Sprintf("ins-multi%d", i)
		node := fmt.Sprintf("%08d-1111-1111-1111-111111111111", i+1)
		n.nodes[id] = nomadclient.Node{Identity: nomadclient.Identity{NodeID: node, InstanceID: id, GroupID: testGroup, Region: testRegion}, Status: "ready", Eligibility: "eligible"}
	}
	if count > 0 {
		_, _ = c.SetDesired(context.Background(), testGroup, count)
		c.writes = nil
	}
	cfg := config(t)
	cfg.MaxCapacity = max
	e := openMulti(t, cfg, c, n)
	return cfg, c, n, e
}
func multiState(t *testing.T, e *Engine) State {
	t.Helper()
	s, _, err := e.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	return s
}
func completeMultiDelete(t *testing.T, e *Engine, c *multiCloud) {
	t.Helper()
	step(t, e)
	if multiState(t, e).Pending.Phase != "removing" {
		t.Fatal("not removing")
	}
	c.terminal(true)
	step(t, e)
	step(t, e)
}

func TestMultiGrowAndLeastBusyShrink(t *testing.T) {
	_, c, n, e := multiFixture(t, 0, 2)
	if changed, err := e.Scale(2); !changed || err != nil {
		t.Fatal(changed, err)
	}
	step(t, e)
	s := multiState(t, e)
	if len(s.Members) != 2 || s.Pending != nil || s.LastBatch.Target != 2 {
		t.Fatal(s)
	}
	n.load[n.nodes["ins-multi0"].NodeID] = 4
	if changed, err := e.Scale(1); !changed || err != nil {
		t.Fatal(changed, err)
	}
	if multiState(t, e).Pending.InstanceID != "ins-multi1" {
		t.Fatal("busy selected")
	}
	completeMultiDelete(t, e, c)
	s = multiState(t, e)
	if len(s.Members) != 1 || s.Batch != nil || s.LastBatch.Results[0].InstanceID != "ins-multi1" {
		t.Fatal(s)
	}
}
func TestMultiSerialPartialFailureRestoresOnlySurvivor(t *testing.T) {
	_, c, n, e := multiFixture(t, 3, 3)
	if _, err := e.Scale(0); err != nil {
		t.Fatal(err)
	}
	completeMultiDelete(t, e, c)
	s := multiState(t, e)
	if s.Batch == nil || len(s.Batch.Results) != 1 || s.Pending != nil {
		t.Fatal("must persist completed child before next", s)
	}
	step(t, e)
	step(t, e)
	c.terminal(false)
	step(t, e)
	s = multiState(t, e)
	if !s.ManualReview || s.Batch != nil || s.LastBatch.Phase != "failed" || len(s.LastBatch.Results) != 2 {
		t.Fatal(s)
	}
	if n.nodes["ins-multi1"].Eligibility != "eligible" || n.nodes["ins-multi2"].Eligibility != "eligible" || n.nodes["ins-multi2"].LastDrain != nil {
		t.Fatal(n)
	}
	before := c.writeCount()
	step(t, e)
	if c.writeCount() != before {
		t.Fatal("replayed after failure")
	}
}
func TestMultiBatchBoundaryRestartAndUnknownFreeze(t *testing.T) {
	cfg, c, n, e := multiFixture(t, 2, 2)
	if _, err := e.Scale(0); err != nil {
		t.Fatal(err)
	}
	completeMultiDelete(t, e, c)
	e.Close()
	e = openMulti(t, cfg, c, n)
	step(t, e)
	c.removeUnknown = true
	if err := e.reconcileOnce(); err != ErrUnknown {
		t.Fatal(err)
	}
	s, ready, err := e.Snapshot()
	if err != ErrUnknown || ready || len(s.Batch.Results) != 1 {
		t.Fatal(s, ready, err)
	}
	before := c.writeCount()
	e.Close()
	e = openMulti(t, cfg, c, n)
	step(t, e)
	if c.writeCount() != before {
		t.Fatal("unknown write replay")
	}
}
func TestMultiProtectionAndScaleInFlag(t *testing.T) {
	_, c, _, e := multiFixture(t, 2, 2)
	c.snap.Members[0].Protected = true
	if _, err := e.Scale(1); err != nil {
		t.Fatal(err)
	}
	if multiState(t, e).Pending.InstanceID != "ins-multi1" {
		t.Fatal("selected protected")
	}
}
func TestMultiLegacyMigrationRejectsPending(t *testing.T) {
	c := newCloud()
	n := newNomad()
	cfg := config(t)
	e := openMulti(t, cfg, c, n)
	if _, err := e.Scale(1); err != nil {
		t.Fatal(err)
	}
	e.Close()
	before, _ := os.ReadFile(filepath.Join(cfg.Dir, "live.json"))
	cfg.MaxCapacity = 2
	c.snap.Group.Max = 2
	if x, err := Open(cfg, c, n); err == nil {
		x.Close()
		t.Fatal("upgraded in-flight v1")
	}
	after, _ := os.ReadFile(filepath.Join(cfg.Dir, "live.json"))
	if string(before) != string(after) {
		t.Fatal("changed pending legacy journal")
	}
}

func openMulti(t *testing.T, cfg Config, c Cloud, n Nomad) *Engine {
	t.Helper()
	e, err := Open(cfg, c, n)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(e.Close)
	return e
}

func TestMultiAdditionalGrowAndSerialCompletion(t *testing.T) {
	_, c, _, e := multiFixture(t, 1, 3)
	// Existing members may predate this operation; only new members need the provenance time check.
	c.snap.Members[0].AddedAt = time.Now().Add(-24 * time.Hour).UTC().Format(time.RFC3339)
	if _, err := e.Scale(3); err != nil {
		t.Fatal(err)
	}
	step(t, e)
	if s := multiState(t, e); len(s.Members) != 3 || s.Batch != nil {
		t.Fatal(s)
	}
	if _, err := e.Scale(0); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if i > 0 {
			step(t, e)
		}
		completeMultiDelete(t, e, c)
	}
	s := multiState(t, e)
	if len(s.Members) != 0 || s.Batch != nil || len(s.LastBatch.Results) != 3 || s.LastBatch.Phase != "completed" {
		t.Fatal(s)
	}
	for i, r := range s.LastBatch.Results {
		if r.InstanceID != fmt.Sprintf("ins-multi%d", i) || r.AbsentObservations != 2 {
			t.Fatal("tie or confirmation mismatch", r)
		}
	}
}
func TestMultiAllScaleInProtected(t *testing.T) {
	cfg, c, n, e := multiFixture(t, 2, 2)
	e.Close()
	cfg.ScaleInProtection = true
	e = openMulti(t, cfg, c, n)
	for _, target := range []int64{0, 1} {
		if changed, err := e.Scale(target); changed || err == nil {
			t.Fatal("protected shrink accepted", target)
		}
	}
	if c.writeCount() != 0 || len(n.writes) != 0 {
		t.Fatal("protection wrote")
	}
}
func TestMultiUnselectableBeforeAnyMutation(t *testing.T) {
	_, c, n, e := multiFixture(t, 2, 2)
	c.snap.Members[0].Protected = true
	if changed, err := e.Scale(0); changed || err == nil {
		t.Fatal("insufficient candidates accepted")
	}
	if c.writeCount() != 0 || len(n.writes) != 0 {
		t.Fatal("partial drain before candidate check")
	}
}
func TestMultiPreDeleteAndNextChildRecheck(t *testing.T) {
	for _, stage := range []string{"current-protection", "next-protection", "membership-drift"} {
		t.Run(stage, func(t *testing.T) {
			_, c, n, e := multiFixture(t, 2, 2)
			if _, err := e.Scale(0); err != nil {
				t.Fatal(err)
			}
			if stage == "current-protection" {
				c.snap.Members[0].Protected = true
				step(t, e)
				if c.writeCount() != 0 {
					t.Fatal("deleted after protection change")
				}
				return
			}
			completeMultiDelete(t, e, c)
			before := c.writeCount()
			drains := len(n.writes)
			if stage == "next-protection" {
				c.snap.Members[0].Protected = true
			} else {
				old := c.snap.Members[0].ID
				c.snap.Members[0].ID = "ins-replaced"
				vm := c.snap.VMs[old]
				delete(c.snap.VMs, old)
				vm.ID = "ins-replaced"
				c.snap.VMs[vm.ID] = vm
				node := n.nodes[old]
				node.InstanceID = vm.ID
				n.nodes[vm.ID] = node
			}
			step(t, e)
			if c.writeCount() != before || len(n.writes) != drains || multiState(t, e).Pending != nil {
				t.Fatal("advanced across changed state")
			}
		})
	}
}
func TestMultiRemovingRestartRetainsExactActivity(t *testing.T) {
	cfg, c, n, e := multiFixture(t, 2, 2)
	_, _ = e.Scale(1)
	step(t, e)
	old := multiState(t, e).Pending
	before := c.writeCount()
	e.Close()
	e = openMulti(t, cfg, c, n)
	s := multiState(t, e)
	if s.Pending.ActivityID != old.ActivityID || s.Pending.ID != old.ID {
		t.Fatal("lost identity")
	}
	c.terminal(true)
	step(t, e)
	step(t, e)
	if c.writeCount() != before || multiState(t, e).Batch != nil {
		t.Fatal("replayed or not completed")
	}
}
func TestMultiLegacyIdleUpgradeAndConfigBinding(t *testing.T) {
	c := newCloud()
	n := newNomad()
	cfg := config(t)
	e := openTest(t, cfg, c, n)
	e.Close()
	before, _ := os.ReadFile(filepath.Join(cfg.Dir, "live.json"))
	cfg.MaxCapacity = 2
	c.snap.Group.Max = 2
	e = openMulti(t, cfg, c, n)
	if multiState(t, e).Version != 2 {
		t.Fatal("not upgraded")
	}
	backup, _ := os.ReadFile(filepath.Join(cfg.Dir, "live-v1.backup.json"))
	if string(backup) != string(before) {
		t.Fatal("backup changed")
	}
	e.Close()
	cfg.MaxCapacity = 3
	if x, err := Open(cfg, c, n); err == nil {
		x.Close()
		t.Fatal("capacity binding changed silently")
	}
}
func TestMultiCorruptBatchRejectedWithoutWrites(t *testing.T) {
	for _, kind := range []string{"duplicate-selection", "wrong-target", "foreign-node", "duplicate-results", "missing-batch"} {
		t.Run(kind, func(t *testing.T) {
			cfg, c, n, e := multiFixture(t, 2, 2)
			_, _ = e.Scale(0)
			completeMultiDelete(t, e, c)
			s := multiState(t, e)
			e.Close()
			switch kind {
			case "duplicate-selection":
				s.Batch.Selected[1] = s.Batch.Selected[0]
			case "wrong-target":
				s.Batch.Target = 2
			case "foreign-node":
				s.Batch.Selected[1].GroupID = "asg-foreign"
			case "duplicate-results":
				s.Batch.Results = append(s.Batch.Results, s.Batch.Results[0])
			case "missing-batch":
				s.Pending = &Operation{ID: "live-11111111111111111111111111111111", Kind: "shrink", Phase: "draining", Target: 0, InstanceID: s.Batch.Selected[1].InstanceID, NodeID: s.Batch.Selected[1].NodeID, StartedAt: time.Now().UTC()}
				s.Batch = nil
			}
			data, _ := json.Marshal(s)
			if err := os.WriteFile(filepath.Join(cfg.Dir, "live.json"), data, 0600); err != nil {
				t.Fatal(err)
			}
			before := c.writeCount()
			if x, err := Open(cfg, c, n); err == nil {
				x.Close()
				t.Fatal("accepted corrupt batch")
			}
			if c.writeCount() != before {
				t.Fatal("corrupt journal caused write")
			}
		})
	}
}
func TestMultiUnknownIntentOnRestartKeepsBatchFrozen(t *testing.T) {
	cfg, c, n, e := multiFixture(t, 2, 2)
	_, _ = e.Scale(1)
	s := multiState(t, e)
	e.Close()
	s.Pending.Phase = "remove-intent"
	data, _ := json.Marshal(s)
	_ = os.WriteFile(filepath.Join(cfg.Dir, "live.json"), data, 0600)
	before := c.writeCount()
	e = openMulti(t, cfg, c, n)
	s, ready, err := e.Snapshot()
	if err != ErrUnknown || ready || s.Batch == nil {
		t.Fatal(s, ready, err)
	}
	step(t, e)
	if c.writeCount() != before {
		t.Fatal("intent replayed")
	}
}

func TestMultiProtectionEnabledAcrossRestartBlocksNewDelete(t *testing.T) {
	cfg, c, n, e := multiFixture(t, 2, 2)
	_, _ = e.Scale(0)
	e.Close()
	cfg.ScaleInProtection = true
	e = openMulti(t, cfg, c, n)
	before := c.writeCount()
	step(t, e)
	if c.writeCount() != before || multiState(t, e).Pending.Phase != "draining" {
		t.Fatal("protection did not stop pending deletion")
	}
}

// Deletion protection forbids another delete; it does not forbid restoring
// scheduling after an exact failed deletion and an owned drain.
func TestMultiProtectedFailedSurvivorResumesWithoutClearingProtection(t *testing.T) {
	for _, mode := range []string{"as", "cvm", "both"} {
		t.Run(mode, func(t *testing.T) {
			cfg, c, n, e := multiFixture(t, 2, 2)
			if _, err := e.Scale(0); err != nil {
				t.Fatal(err)
			}
			completeMultiDelete(t, e, c)
			step(t, e)
			step(t, e)
			c.terminal(false)
			id := "ins-multi1"
			c.snap.Members[0].Lifecycle = "TERMINATION_FAILED"
			if mode != "cvm" {
				c.snap.Members[0].Protected = true
			}
			if mode != "as" {
				vm := c.snap.VMs[id]
				vm.Protected = true
				c.snap.VMs[id] = vm
			}
			before := c.writeCount()
			step(t, e)
			s, ready, err := e.Snapshot()
			if err != nil || ready || !s.ManualReview || s.Pending != nil || s.Batch != nil || s.LastBatch.Phase != "failed" || len(s.LastBatch.Results) != 2 {
				t.Fatalf("failed survivor was not recovered: %+v ready=%v err=%v", s, ready, err)
			}
			if n.nodes[id].Eligibility != "eligible" || n.nodes[id].LastDrain.Status != "canceled" || c.writeCount() != before {
				t.Fatal("restore altered cloud or left node unschedulable")
			}
			if (mode != "cvm" && !c.snap.Members[0].Protected) || (mode != "as" && !c.snap.VMs[id].Protected) {
				t.Fatal("cleared protection")
			}
			e.Close()
			e = openMulti(t, cfg, c, n)
			step(t, e)
			if c.writeCount() != before || !multiState(t, e).ManualReview {
				t.Fatal("restarted controller replayed cloud deletion")
			}
		})
	}
}

func (n *multiNomad) TasksStopped(context.Context, string, bool) (bool, error) { return true, nil }
