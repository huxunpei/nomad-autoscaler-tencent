package plugin

import (
	"context"
	"testing"
	"time"

	"github.com/huxunpei/nomad-autoscaler-tencent/internal/live"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/tencentcloud"
)

type registrationRouteEngine struct{ *routingEngine }

func (e *registrationRouteEngine) Snapshot() (live.State, bool, error) {
	return live.State{Members: map[string]live.Member{
		"ins-present": {RegistrationState: "registered"},
		"ins-absent":  {RegistrationState: "timed_out"},
	}}, true, nil
}
func TestRegistrationGraceRuntimeConfigurationAndStatus(t *testing.T) {
	c := &routingCloud{account: "10001", region: "ap-chengdu"}
	p := &Target{runtimeDeps: &runtimeDependencies{
		openCloud: func(context.Context, tencentcloud.AuthConfig) (accountCloud, error) { return c, nil },
		openEngine: func(cfg live.Config, _ live.Cloud, _ live.Nomad) (groupController, error) {
			if cfg.RegistrationGracePeriod != 15*time.Minute || cfg.Metrics == nil {
				t.Error("registration config not passed")
			}
			return &registrationRouteEngine{&routingEngine{cfg: cfg}}, nil
		},
	}}
	err := p.SetConfig(map[string]string{"tencent_region": c.region, "tencent_state_dir": t.TempDir(), "tencent_registration_grace_period": "15m"})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	s := readyRoute(t, p, map[string]string{"tencent_group_id": "asg-test"})
	if s.Count != 2 || !s.Ready || s.Meta["tencent.registration_timed_out"] != "1" || s.Meta["tencent.registration_waiting"] != "0" {
		t.Fatal("status lost full capacity or registration diagnostics", s)
	}
}
