package live

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/tencentcloud"
	"testing"
	"time"
)

type unsentCloud struct {
	*multiCloud
	fail  bool
	calls int
	after func()
}

func (c *unsentCloud) unsent(action string) (tencentcloud.WriteResult, error) {
	c.calls++
	var result tencentcloud.WriteResult
	_ = json.Unmarshal([]byte(`{"action":"`+action+`","not_submitted":"credential-gate"}`), &result)
	if c.after != nil {
		f := c.after
		c.after = nil
		f()
	}
	return result, errors.New("SyntheticCredentialUnavailable")
}
func (c *unsentCloud) SetDesired(ctx context.Context, g string, n int64) (tencentcloud.WriteResult, error) {
	if c.fail {
		return c.unsent("ModifyDesiredCapacity")
	}
	return c.multiCloud.SetDesired(ctx, g, n)
}
func (c *unsentCloud) RemoveExact(ctx context.Context, g, id string) (tencentcloud.WriteResult, error) {
	if c.fail {
		return c.unsent("RemoveInstances")
	}
	return c.multiCloud.RemoveExact(ctx, g, id)
}
func TestUnsentWriteGrowAndDeleteRecover(t *testing.T) {
	for _, kind := range []string{"grow", "shrink"} {
		t.Run(kind, func(t *testing.T) {
			cfg, base, n, e := groupContractFixture(t, 2, 0, 4, false)
			e.Close()
			cfg.StabilizationInterval = time.Millisecond
			c := &unsentCloud{multiCloud: base, fail: true}
			e = openMulti(t, cfg, c, n)
			target := int64(3)
			if kind == "shrink" {
				target = 1
			}
			_, err := e.ScaleAndWait(target, 2)
			if err == nil || errors.Is(err, ErrUnknown) || e.state.Pending != nil || e.state.Batch != nil || e.state.LastBatch == nil || e.state.LastBatch.Phase != "failed" || e.state.LastCompletedAt != 0 || base.writeCount() != 0 || c.calls != 1 {
				t.Fatalf("unsent request not settled safely: %v phase=%s", err, e.state.LastEvent)
			}
			if kind == "shrink" && len(n.writes) != 2 {
				t.Fatal("did not restore owned drain")
			}
			e.Close()
			c.fail = false
			e = openMulti(t, cfg, c, n)
			defer e.Close()
			if changed, err := e.Scale(3); !changed || err != nil {
				t.Fatal("next action blocked", changed, err)
			}
		})
	}
}

func TestUnsentWriteDeleteRestoresAndSurvivesRestart(t *testing.T) {
	for _, mode := range []string{"normal", "lost-restore-ack", "temporary-restore-read", "changed-group-guard"} {
		t.Run(mode, func(t *testing.T) {
			cfg, base, n, e := groupContractFixture(t, 2, 0, 3, false)
			e.Close()
			c := &unsentCloud{multiCloud: base, fail: true}
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
			c.fail = false
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

func TestUnsentWriteDeleteKeepsPriorSuccessfulChild(t *testing.T) {
	cfg, base, n, e := groupContractFixture(t, 3, 0, 3, false)
	e.Close()
	cfg.StabilizationInterval = time.Millisecond
	c := &unsentCloud{multiCloud: base}
	e = openMulti(t, cfg, c, n)
	if _, err := e.Scale(0); err != nil {
		t.Fatal(err)
	}
	completeMultiDelete(t, e, base)
	step(t, e)
	c.fail = true
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

func TestUnsentWritePersistenceBarriers(t *testing.T) {
	for _, when := range []string{"before-rejection-save", "after-restore"} {
		t.Run(when, func(t *testing.T) {
			cfg, base, n, e := groupContractFixture(t, 2, 0, 3, false)
			e.Close()
			c := &unsentCloud{multiCloud: base, fail: true}
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

func TestUnsentWriteRestorationOwnershipGuards(t *testing.T) {
	for _, mode := range []string{"other-owner", "wrong-instance", "vm-not-running", "unobserved-vm"} {
		t.Run(mode, func(t *testing.T) {
			cfg, base, n, e := groupContractFixture(t, 2, 0, 3, false)
			e.Close()
			c := &unsentCloud{multiCloud: base, fail: true}
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
