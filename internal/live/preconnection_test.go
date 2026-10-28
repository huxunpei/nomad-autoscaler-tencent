package live

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/huxunpei/nomad-autoscaler-tencent/internal/tencentcloud"
)

type preconnectionCloud struct {
	*multiCloud
	fail  bool
	calls int
}

func (c *preconnectionCloud) notSent(action string) (tencentcloud.WriteResult, error) {
	c.calls++
	return tencentcloud.WriteResult{Action: action, Attempted: true, NotSubmitted: "transport-before-connection"}, &tencentcloud.Error{Action: action, Code: "LocalTransportNotSubmitted"}
}
func (c *preconnectionCloud) SetDesired(ctx context.Context, group string, target int64) (tencentcloud.WriteResult, error) {
	if c.fail {
		return c.notSent("ModifyDesiredCapacity")
	}
	return c.multiCloud.SetDesired(ctx, group, target)
}
func (c *preconnectionCloud) RemoveExact(ctx context.Context, group, instance string) (tencentcloud.WriteResult, error) {
	if c.fail {
		return c.notSent("RemoveInstances")
	}
	return c.multiCloud.RemoveExact(ctx, group, instance)
}

func TestPreconnectionFailureEndsBatchWithoutCloudReplay(t *testing.T) {
	for _, kind := range []string{"grow", "shrink"} {
		t.Run(kind, func(t *testing.T) {
			cfg, base, n, e := groupContractFixture(t, 2, 0, 4, false)
			e.Close()
			cfg.StabilizationInterval = time.Millisecond
			c := &preconnectionCloud{multiCloud: base, fail: true}
			e = openMulti(t, cfg, c, n)
			target := int64(3)
			if kind == "shrink" {
				target = 1
			}
			_, err := e.ScaleAndWait(target, 2)
			if err == nil || errors.Is(err, ErrUnknown) || e.state.Pending != nil || e.state.Batch != nil || e.state.LastBatch == nil || e.state.LastBatch.Phase != "failed" || e.state.LastCompletedAt != 0 || c.calls != 1 || base.writeCount() != 0 {
				t.Fatal("known local failure left an unknown obligation", err, e.state.LastEvent)
			}
			if e.state.LastOperation.CloudRejection.Code != "LocalTransportNotSubmitted" || (kind == "shrink" && len(n.writes) != 2) {
				t.Fatal("provenance lost or owned drain not restored")
			}
			e.Close()
			c.fail = false
			e = openMulti(t, cfg, c, n)
			if changed, err := e.Scale(3); !changed || err != nil {
				t.Fatal("restart/next action blocked", changed, err)
			}
		})
	}
}

func TestPreconnectionRestoreRetriesAfterRestart(t *testing.T) {
	cfg, base, n, e := groupContractFixture(t, 2, 0, 3, false)
	e.Close()
	c := &preconnectionCloud{multiCloud: base, fail: true}
	probe := &rejectedNomad{multiNomad: n, dir: cfg.Dir, refuse: true}
	e = openMulti(t, cfg, c, probe)
	if _, err := e.Scale(1); err != nil {
		t.Fatal(err)
	}
	id := e.state.Pending.InstanceID
	step(t, e)
	if e.state.Pending == nil || e.state.Pending.Unknown || e.state.Pending.Phase != "rejected" {
		t.Fatal("non-submission proof was not durable")
	}
	e.Close()
	probe.refuse = false
	e = openMulti(t, cfg, c, probe)
	step(t, e)
	if e.state.Pending != nil || e.state.Batch != nil || e.state.LastBatch.Phase != "failed" || e.state.LastCompletedAt != 0 || n.nodes[id].Eligibility != "eligible" || c.calls != 1 || base.writeCount() != 0 || e.checkRecoveryAudit() != nil {
		t.Fatal("restart did not restore the owned survivor without a cloud write")
	}
}
