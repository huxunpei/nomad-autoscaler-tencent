package plugin

import (
	"strconv"
	"testing"
	"time"

	"github.com/hashicorp/nomad-autoscaler/sdk"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/live"
)

func TestRestartCooldownHolding(t *testing.T) {
	now := time.Date(2026, time.October, 25, 1, 0, 0, 0, time.UTC)
	completed := now.Add(-time.Minute).UnixNano()
	if !restartCooldownHolding(time.Minute, completed, now, now) {
		t.Fatal("expected the window to include its deadline")
	}
	if restartCooldownHolding(time.Minute, completed, now, now.Add(time.Nanosecond)) {
		t.Fatal("expected release immediately after the deadline")
	}
	if restartCooldownHolding(time.Minute, now.UnixNano(), now, now.Add(time.Hour)) {
		t.Fatal("a completion in this process was held")
	}
	if restartCooldownHolding(0, completed, now, now) || restartCooldownHolding(time.Minute, 0, now, now) || restartCooldownHolding(time.Minute, completed, time.Time{}, now) {
		t.Fatal("missing cooldown, completion, or process start was held")
	}
}

func TestRestartCooldownStatusPreservesRouteIdentity(t *testing.T) {
	cloud := &routingCloud{account: "10001", region: "ap-guangzhou"}
	engine := &unknownStatusEngine{routingEngine: &routingEngine{}}
	target := routeTarget(t, t.TempDir(), cloud, func(cfg live.Config, _ live.Cloud, _ live.Nomad) (groupController, error) {
		engine.cfg = cfg
		return engine, nil
	})
	started := target.runtime.startedAt
	stamp := started.Add(-time.Minute).UnixNano()
	engine.state = live.State{Members: map[string]live.Member{"ins-one": {}, "ins-two": {}}, LastCompletedAt: stamp, LastEvent: "expand-completed"}

	holding, err := target.Status(map[string]string{"tencent_group_id": "asg-one", "restart_cooldown": "5m"})
	if err != nil || holding == nil || holding.Ready || holding.Count != 2 || holding.Meta["tencent.restart_cooldown"] != "active" || holding.Meta[sdk.TargetStatusMetaKeyLastEvent] != strconv.FormatInt(stamp, 10) || holding.Meta["tencent.event"] != "expand-completed" {
		t.Fatalf("restart window did not hold a pre-process completion: %+v %v", holding, err)
	}
	for _, cfg := range []map[string]string{
		{"tencent_group_id": "asg-one", "restart_cooldown": "10m"},
		{"tencent_group_id": "asg-one"},
	} {
		status, err := target.Status(cfg)
		if err != nil {
			t.Fatal(err)
		}
		wantReady := cfg["restart_cooldown"] == ""
		if status.Ready != wantReady || status.Count != 2 || status.Meta[sdk.TargetStatusMetaKeyLastEvent] != strconv.FormatInt(stamp, 10) {
			t.Fatalf("config %v changed identity or completion: %+v", cfg, status)
		}
		if wantReady && status.Meta["tencent.restart_cooldown"] != "" {
			t.Fatal("unset restart_cooldown stayed active")
		}
	}
	if cloud.writes.Load() != 0 || engine.calls.Load() != 0 {
		t.Fatal("status hold performed a write")
	}

	engine.state.LastCompletedAt = started.UnixNano()
	current, err := target.Status(map[string]string{"tencent_group_id": "asg-one", "restart_cooldown": "5m"})
	if err != nil || current == nil || !current.Ready || current.Meta["tencent.restart_cooldown"] != "" {
		t.Fatalf("in-process completion was held: %+v %v", current, err)
	}
	engine.state.LastCompletedAt = started.Add(-10 * time.Minute).UnixNano()
	expired, err := target.Status(map[string]string{"tencent_group_id": "asg-one", "restart_cooldown": "5m"})
	if err != nil || expired == nil || !expired.Ready || expired.Meta["tencent.restart_cooldown"] != "" {
		t.Fatalf("expired window stayed held: %+v %v", expired, err)
	}
	engine.state.LastCompletedAt = 0
	unset, err := target.Status(map[string]string{"tencent_group_id": "asg-one", "restart_cooldown": "5m"})
	if err != nil || unset == nil || !unset.Ready || unset.Meta[sdk.TargetStatusMetaKeyLastEvent] != "" {
		t.Fatalf("missing completion invented a hold or timestamp: %+v %v", unset, err)
	}
	if _, err = target.Status(map[string]string{"tencent_group_id": "asg-one", "restart_cooldown": "0s"}); err == nil || err.Error() != "invalid restart_cooldown" {
		t.Fatal(err)
	}
}
