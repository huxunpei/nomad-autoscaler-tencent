package live

import (
	"bytes"
	"github.com/hashicorp/go-hclog"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/tencentcloud"
	"strings"
	"testing"
)

func TestGroupDisabledWaitsAndRecoversWithoutError(t *testing.T) {
	_, c, _, e := groupContractFixture(t, 1, 0, 5, false)
	var logs bytes.Buffer
	e.cfg.Logger = hclog.New(&hclog.LoggerOptions{Output: &logs})
	c.snap.Group.Enabled = "DISABLED"
	step(t, e)
	s, ready, err := e.Snapshot()
	if err != nil || ready || len(s.Members) != 1 || !strings.Contains(logs.String(), "group is disabled") {
		t.Fatal(ready, err, logs.String())
	}
	if changed, err := e.Scale(2); changed || err == nil || c.writeCount() != 0 {
		t.Fatal(changed, err)
	}
	c.snap.Group.Enabled = "ENABLED"
	step(t, e)
	_, ready, err = e.Snapshot()
	if err != nil || !ready {
		t.Fatal(ready, err)
	}
}

func TestGroupOtherControllersWarnButPermitScaling(t *testing.T) {
	_, c, _, e := groupContractFixture(t, 1, 0, 5, false)
	var logs bytes.Buffer
	e.cfg.Logger = hclog.New(&hclog.LoggerOptions{Output: &logs})
	c.snap.PolicyCount = 1
	c.snap.Policies = []tencentcloud.Policy{{ID: "asp-test", Type: "SIMPLE"}}
	c.snap.ScheduledActionCount = 2
	c.snap.Group.ScalingMode = "WAKE_UP_STOPPED_SCALING"
	if _, err := e.Scale(2); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(logs.String(), "other capacity controllers") || !strings.Contains(logs.String(), "wake-up-stopped") || c.writeCount() != 1 {
		t.Fatal(logs.String(), c.writeCount())
	}
}

func TestGroupUnsupportedModesRejectBothDirections(t *testing.T) {
	for _, mode := range []string{"SPOT_MIXED", "UNSUPPORTED", "PREPAID", "SPOTPAID"} {
		for _, target := range []int64{0, 2} {
			t.Run(mode+string(rune('0'+target)), func(t *testing.T) {
				_, c, _, e := groupContractFixture(t, 1, 0, 5, false)
				if mode == "SPOT_MIXED" || mode == "UNSUPPORTED" {
					c.snap.Group.AllocationPolicy = mode
				} else {
					c.snap.LaunchConfiguration.ChargeType = mode
				}
				if changed, err := e.Scale(target); changed || err == nil || c.writeCount() != 0 {
					t.Fatal(changed, err, c.writeCount())
				}
			})
		}
	}
}
