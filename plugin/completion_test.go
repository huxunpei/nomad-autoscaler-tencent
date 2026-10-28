package plugin

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/hashicorp/nomad-autoscaler/plugins/shared"
	"github.com/hashicorp/nomad-autoscaler/sdk"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/live"
)

type completedRoutingEngine struct {
	*routingEngine
	stamp int64
}

func TestFirstStatusWaitsForObservationAndCloseCancelsWait(t *testing.T) {
	for _, closing := range []bool{false, true} {
		name := "observation"
		if closing {
			name = "close"
		}
		t.Run(name, func(t *testing.T) {
			block := make(chan struct{})
			c := &routingCloud{account: "10001", region: "ap-guangzhou", blockGroup: "asg-one", block: block}
			p := routeTarget(t, t.TempDir(), c, func(cfg live.Config, _ live.Cloud, _ live.Nomad) (groupController, error) {
				return &routingEngine{cfg: cfg}, nil
			})
			done := make(chan error, 1)
			go func() {
				s, err := p.Status(map[string]string{"tencent_group_id": "asg-one", shared.PluginConfigKeyGRPCTimeout: "500ms"})
				if err == nil && (s == nil || s.Count != 0 || !s.Ready) {
					err = context.Canceled
				}
				done <- err
			}()
			until := time.Now().Add(time.Second)
			for c.reads.Load() == 0 && time.Now().Before(until) {
				time.Sleep(time.Millisecond)
			}
			if c.reads.Load() == 0 {
				t.Fatal("initial observation never started")
			}
			select {
			case err := <-done:
				t.Fatalf("first Status returned before observation: %v", err)
			case <-time.After(20 * time.Millisecond):
			}
			if closing {
				p.Close()
			} else {
				close(block)
			}
			select {
			case err := <-done:
				if (err != nil) != closing {
					t.Fatal("wrong Status outcome", err)
				}
			case <-time.After(time.Second):
				t.Fatal("Status wait did not end")
			}
		})
	}
}

func (e *completedRoutingEngine) Snapshot() (live.State, bool, error) {
	s, ready, err := e.routingEngine.Snapshot()
	s.LastCompletedAt = e.stamp
	return s, ready, err
}
func TestStatusReportsNanosecondCompletion(t *testing.T) {
	c := &routingCloud{account: "10001", region: "ap-guangzhou"}
	stamp := time.Now().Add(-time.Second).UnixNano()
	p := routeTarget(t, t.TempDir(), c, func(cfg live.Config, _ live.Cloud, _ live.Nomad) (groupController, error) {
		return &completedRoutingEngine{routingEngine: &routingEngine{cfg: cfg}, stamp: stamp}, nil
	})
	s := readyRoute(t, p, map[string]string{"tencent_group_id": "asg-one", shared.PluginConfigKeyGRPCTimeout: "750ms"})
	if s.Meta[sdk.TargetStatusMetaKeyLastEvent] != strconv.FormatInt(stamp, 10) {
		t.Fatal("missing official cooldown timestamp", s.Meta)
	}
}
