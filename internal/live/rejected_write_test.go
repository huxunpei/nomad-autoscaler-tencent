package live

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadclient"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/tencentcloud"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type rejectedCloud struct {
	*multiCloud
	reject bool
	code   string
	calls  int
	after  func()
}

func (c *rejectedCloud) rejection(action string) (tencentcloud.WriteResult, error) {
	c.calls++
	if c.after != nil {
		f := c.after
		c.after = nil
		f()
	}
	return tencentcloud.WriteResult{Action: action, Attempted: true, RequestID: testRequest}, &tencentcloud.Error{Action: action, Code: c.code, RequestID: testRequest}
}
func (c *rejectedCloud) SetDesired(ctx context.Context, g string, n int64) (tencentcloud.WriteResult, error) {
	if c.reject {
		return c.rejection("ModifyDesiredCapacity")
	}
	return c.multiCloud.SetDesired(ctx, g, n)
}
func (c *rejectedCloud) RemoveExact(ctx context.Context, g, id string) (tencentcloud.WriteResult, error) {
	if c.reject {
		return c.rejection("RemoveInstances")
	}
	return c.multiCloud.RemoveExact(ctx, g, id)
}

func TestRejectedCloudGrowFailsWithoutUnknownOrCooldown(t *testing.T) {
	cfg, base, n, e := groupContractFixture(t, 1, 0, 3, false)
	e.Close()
	cfg.StabilizationInterval = time.Millisecond
	c := &rejectedCloud{multiCloud: base, reject: true, code: "AuthFailure.UnauthorizedOperation"}
	e = openMulti(t, cfg, c, n)
	changed, err := e.ScaleAndWait(3, 2)
	if !changed || err == nil || errors.Is(err, ErrUnknown) || e.state.Pending != nil || e.state.Batch != nil || e.state.LastBatch == nil || e.state.LastBatch.Phase != "failed" || e.state.LastCompletedAt != 0 || e.state.ManualReview || c.calls != 1 {
		t.Fatalf("rejection did not finish as failure: changed=%v err=%v state=%+v", changed, err, e.state)
	}
	e.Close()
	c.reject = false
	e = openMulti(t, cfg, c, n)
	if changed, err = e.ScaleAndWait(3, 2); !changed || err != nil {
		t.Fatal("next action blocked", changed, err)
	}
	if c.calls != 1 || base.writeCount() != 1 || e.state.LastCompletedAt == 0 {
		t.Fatal("replayed rejection or missing subsequent completion")
	}
}

type rejectedNomad struct {
	*multiNomad
	dir             string
	loseAck, refuse bool
	after           func()
}

func (n *rejectedNomad) RestoreOwnedDrain(ctx context.Context, id nomadclient.Identity, op string) (nomadclient.MutationResult, error) {
	raw, err := os.ReadFile(filepath.Join(n.dir, "live.json"))
	var s map[string]any
	if err != nil || json.Unmarshal(raw, &s) != nil {
		return nomadclient.MutationResult{}, errors.New("missing durable rejection")
	}
	p, ok := s["pending"].(map[string]any)
	if !ok || p["phase"] != "rejected" || p["cloud_rejection"] == nil {
		return nomadclient.MutationResult{}, errors.New("restore before rejection persisted")
	}
	if n.refuse {
		return nomadclient.MutationResult{}, errors.New("temporary restore read failure")
	}
	r, err := n.multiNomad.RestoreOwnedDrain(ctx, id, op)
	if n.after != nil {
		f := n.after
		n.after = nil
		f()
	}
	if n.loseAck {
		return nomadclient.MutationResult{WriteAttempted: true, Unknown: true}, errors.New("lost acknowledgement")
	}
	return r, err
}

func TestRejectedCloudDeleteRestoresAndSurvivesRestart(t *testing.T) {
	for _, mode := range []string{"normal", "lost-restore-ack", "temporary-restore-read", "changed-group-guard"} {
		t.Run(mode, func(t *testing.T) {
			cfg, base, n, e := groupContractFixture(t, 2, 0, 3, false)
			e.Close()
			c := &rejectedCloud{multiCloud: base, reject: true, code: "ResourceInsufficient.InServiceInstanceBelowMinSize"}
			probe := &rejectedNomad{multiNomad: n, dir: cfg.Dir, loseAck: mode == "lost-restore-ack", refuse: mode == "temporary-restore-read"}
			e = openMulti(t, cfg, c, probe)
			if _, err := e.Scale(1); err != nil {
				t.Fatal(err)
			}
			id := e.state.Pending.InstanceID
			if mode == "changed-group-guard" {
				c.after = func() { base.snap.Group.Enabled = "DISABLED" }
				probe.refuse = true
			}
			step(t, e)
			if mode != "normal" {
				if e.state.Pending == nil || e.state.Pending.Unknown || e.state.Pending.Phase != "rejected" {
					t.Fatal("lost durable rejection state")
				}
				e.Close()
				probe.loseAck = false
				probe.refuse = false
				e = openMulti(t, cfg, c, probe)
				step(t, e)
			}
			if e.state.Pending != nil || e.state.Batch != nil || e.state.ManualReview || e.state.LastBatch.Phase != "failed" || e.state.LastCompletedAt != 0 || n.nodes[id].Eligibility != "eligible" || len(n.writes) != 2 || c.calls != 1 || base.writeCount() != 0 {
				t.Fatalf("failed restoration: %+v writes=%v", e.state, n.writes)
			}
			e.Close()
			base.snap.Group.Enabled = "ENABLED"
			c.reject = false
			e = openMulti(t, cfg, c, probe)
			if changed, err := e.Scale(1); !changed || err != nil {
				t.Fatal("next action blocked", changed, err)
			}
			completeMultiDelete(t, e, base)
			if base.writeCount() != 1 {
				t.Fatal("duplicate cloud removal")
			}
		})
	}
}

func TestRejectedCloudDeleteKeepsPriorSuccessfulChild(t *testing.T) {
	cfg, base, n, e := groupContractFixture(t, 3, 0, 3, false)
	e.Close()
	cfg.StabilizationInterval = time.Millisecond
	c := &rejectedCloud{multiCloud: base, code: "AuthFailure.UnauthorizedOperation"}
	e = openMulti(t, cfg, c, n)
	if _, err := e.Scale(0); err != nil {
		t.Fatal(err)
	}
	completeMultiDelete(t, e, base)
	step(t, e)
	c.reject = true
	step(t, e)
	b := e.state.LastBatch
	if b == nil || b.Phase != "failed" || len(b.Results) != 2 || b.Results[0].ActivityStatus != "SUCCESSFUL" || e.state.Pending != nil || e.state.ManualReview || base.writeCount() != 1 || c.calls != 1 || len(n.writes) != 3 {
		t.Fatalf("lost batch or drained next child: %+v", e.state)
	}
	e.Close()
	e = openMulti(t, cfg, c, n)
	if e.state.LastBatch.Phase != "failed" {
		t.Fatal("failed batch not durable")
	}
}

func TestRejectedCloudUnknownCodeCannotUnlock(t *testing.T) {
	cfg, base, n, e := groupContractFixture(t, 2, 0, 3, false)
	e.Close()
	cfg.StabilizationInterval = time.Millisecond
	c := &rejectedCloud{multiCloud: base, reject: true, code: "InternalError"}
	e = openMulti(t, cfg, c, n)
	if _, err := e.Scale(1); err != nil {
		t.Fatal(err)
	}
	if err := e.reconcileOnce(); !errors.Is(err, ErrUnknown) {
		t.Fatal("unknown write not blocked", err)
	}
	if e.state.Pending == nil || !e.state.Pending.Unknown || len(n.writes) != 1 || c.calls != 1 {
		t.Fatal("ambiguous failure restored node")
	}
}

func TestRejectedCloudFailureReturnWhileRestorePending(t *testing.T) {
	cfg, base, n, e := groupContractFixture(t, 2, 0, 3, false)
	e.Close()
	cfg.StabilizationInterval = time.Millisecond
	c := &rejectedCloud{multiCloud: base, reject: true, code: "AuthFailure.UnauthorizedOperation"}
	probe := &rejectedNomad{multiNomad: n, dir: cfg.Dir, refuse: true}
	e = openMulti(t, cfg, c, probe)
	begin := time.Now()
	changed, err := e.ScaleAndWait(1, 2)
	if !changed || err == nil || errors.Is(err, ErrUnknown) || time.Since(begin) > time.Second || e.state.Pending == nil || e.state.Pending.Phase != "rejected" || e.state.LastCompletedAt != 0 {
		t.Fatal("did not return known failure with restoration pending", changed, err)
	}
	probe.refuse = false
	step(t, e)
	if e.state.Pending != nil {
		t.Fatal("worker failed to finish restoration")
	}
}

func TestRejectedCloudPersistenceBarriers(t *testing.T) {
	for _, when := range []string{"before-rejection-save", "after-restore"} {
		t.Run(when, func(t *testing.T) {
			cfg, base, n, e := groupContractFixture(t, 2, 0, 3, false)
			e.Close()
			c := &rejectedCloud{multiCloud: base, reject: true, code: "AuthFailure.UnauthorizedOperation"}
			probe := &rejectedNomad{multiNomad: n, dir: cfg.Dir}
			e = openMulti(t, cfg, c, probe)
			if _, err := e.Scale(1); err != nil {
				t.Fatal(err)
			}
			var repair func()
			obstruct := func() { repair = obstructSettlementJournal(t, cfg.Dir) }
			if when == "before-rejection-save" {
				c.after = obstruct
			} else {
				probe.after = obstruct
			}
			if err := e.reconcileOnce(); err == nil || e.persistError == nil {
				t.Fatal("persistence error did not freeze writes")
			}
			if repair == nil {
				t.Fatal("checkpoint not reached")
			}
			repair()
			e.Close()
			e = openMulti(t, cfg, c, probe)
			if when == "before-rejection-save" {
				if e.state.Pending == nil || !e.state.Pending.Unknown || len(n.writes) != 1 {
					t.Fatal("invented durable rejection or restored without proof")
				}
				step(t, e)
				if c.calls != 1 || len(n.writes) != 1 {
					t.Fatal("replayed unknown mutation")
				}
			} else {
				step(t, e)
				if e.state.Pending != nil || e.state.ManualReview || len(n.writes) != 2 || c.calls != 1 || e.state.LastBatch.Phase != "failed" {
					t.Fatal("repeated restore or lost failure")
				}
			}
		})
	}
}

func TestRejectedCloudRestorationOwnershipGuards(t *testing.T) {
	for _, mode := range []string{"other-owner", "wrong-instance", "vm-not-running", "unobserved-vm"} {
		t.Run(mode, func(t *testing.T) {
			cfg, base, n, e := groupContractFixture(t, 2, 0, 3, false)
			e.Close()
			c := &rejectedCloud{multiCloud: base, reject: true, code: "AuthFailure.UnauthorizedOperation"}
			probe := &rejectedNomad{multiNomad: n, dir: cfg.Dir, refuse: true}
			e = openMulti(t, cfg, c, probe)
			if _, err := e.Scale(1); err != nil {
				t.Fatal(err)
			}
			id := e.state.Pending.InstanceID
			step(t, e)
			e.Close()
			original := copyJSON(n.nodes[id])
			originalVM := base.snap.VMs[id]
			v := copyJSON(original)
			vm := originalVM
			switch mode {
			case "other-owner":
				v.LastDrain.OperationID = "live-ffffffffffffffffffffffffffffffff"
			case "wrong-instance":
				v.InstanceID = "ins-other"
			case "vm-not-running":
				vm.State = "STOPPED"
			case "unobserved-vm":
				vm = tencentcloud.VM{ID: id}
			}
			n.nodes[id] = v
			base.snap.VMs[id] = vm
			probe.refuse = false
			e = openMulti(t, cfg, c, probe)
			_ = e.reconcileOnce()
			if len(n.writes) != 1 || e.state.Pending == nil || e.state.Pending.Phase != "rejected" || c.calls != 1 {
				t.Fatal("unsafe restore after ownership/liveness changed")
			}
			n.nodes[id] = original
			base.snap.VMs[id] = originalVM
			step(t, e)
			if e.state.Pending != nil || len(n.writes) != 2 {
				t.Fatal("read recovery did not resume known rejection")
			}
		})
	}
}

func TestRejectedCloudJournalRejectsContradictions(t *testing.T) {
	for _, mode := range []string{"no-intent", "request-mismatch", "unknown-code", "activity", "unknown-flag", "wrong-action", "wrong-phase", "restored-before-completion"} {
		t.Run(mode, func(t *testing.T) {
			cfg, base, n, e := groupContractFixture(t, 2, 0, 3, false)
			e.Close()
			c := &rejectedCloud{multiCloud: base, reject: true, code: "AuthFailure.UnauthorizedOperation"}
			probe := &rejectedNomad{multiNomad: n, dir: cfg.Dir, refuse: true}
			e = openMulti(t, cfg, c, probe)
			if _, err := e.Scale(1); err != nil {
				t.Fatal(err)
			}
			step(t, e)
			e.Close()
			p := filepath.Join(cfg.Dir, "live.json")
			raw, err := os.ReadFile(p)
			if err != nil {
				t.Fatal(err)
			}
			var s map[string]any
			if json.Unmarshal(raw, &s) != nil {
				t.Fatal("decode")
			}
			op := s["pending"].(map[string]any)
			r := op["cloud_rejection"].(map[string]any)
			switch mode {
			case "no-intent":
				delete(op, "cloud_intent")
			case "request-mismatch":
				r["request_id"] = "aaaaaaaa-2222-3333-4444-555555555555"
			case "unknown-code":
				r["code"] = "InternalError"
			case "activity":
				op["activity_id"] = "asa-other"
			case "unknown-flag":
				r["unknown"] = true
			case "wrong-action":
				r["action"] = "ModifyDesiredCapacity"
			case "wrong-phase":
				op["phase"] = "draining"
			case "restored-before-completion":
				op["drain_outcome"] = "restored"
			}
			changed, _ := json.Marshal(s)
			if err = os.WriteFile(p, changed, 0600); err != nil {
				t.Fatal(err)
			}
			got, err := Open(cfg, c, probe)
			if err == nil {
				got.Close()
				t.Fatal("accepted contradictory rejection journal")
			}
			after, _ := os.ReadFile(p)
			if string(after) != string(changed) || c.calls != 1 || len(n.writes) != 1 {
				t.Fatal("failed open changed durable evidence or wrote")
			}
		})
	}
}
