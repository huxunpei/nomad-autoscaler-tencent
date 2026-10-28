package live

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/huxunpei/nomad-autoscaler-tencent/internal/tencentcloud"
)

type desiredCloud struct {
	*multiCloud
	dir                        string
	calls                      [][2]int64
	history                    []tencentcloud.Activity
	partial                    int64
	lost, notApplied, conflict bool
}

func (c *desiredCloud) ActivitiesSince(_ context.Context, _ string, since time.Time) ([]tencentcloud.Activity, error) {
	var out []tencentcloud.Activity
	for _, a := range c.history {
		at, _ := time.Parse(time.RFC3339, a.StartedAt)
		if !at.Before(since) {
			out = append(out, a)
		}
	}
	return out, nil
}

func (c *desiredCloud) SetDesiredCAS(ctx context.Context, group string, target, origin int64) (tencentcloud.WriteResult, error) {
	var s State
	raw, err := os.ReadFile(filepath.Join(c.dir, "live.json"))
	if err != nil || json.Unmarshal(raw, &s) != nil || !isDesiredCAS(s.Pending) || s.Pending.CloudIntent.Desired.Origin != origin || s.Pending.Target != target {
		return tencentcloud.WriteResult{}, fmt.Errorf("CAS preceded durable intent")
	}
	c.calls = append(c.calls, [2]int64{target, origin})
	result := tencentcloud.WriteResult{Action: "ModifyDesiredCapacity", Attempted: true, RequestID: testRequest}
	if c.conflict || c.snap.Group.Desired != origin {
		return result, &tencentcloud.Error{Action: result.Action, Code: "InvalidParameterValue.AssertDesiredCapacityFailed", RequestID: testRequest}
	}
	if !c.notApplied {
		actual := target
		status := "SUCCESSFUL"
		if c.partial >= 0 {
			actual, status = c.partial, "PARTIALLY_SUCCESSFUL"
		}
		_, _ = c.multiCloud.SetDesired(ctx, group, actual)
		c.snap.Group.Desired = target
		stamp := time.Now().UTC().Format(time.RFC3339)
		c.history = append(c.history, tencentcloud.Activity{ID: fmt.Sprintf("asa-cas%d", len(c.calls)), GroupID: group, Type: "SCALE_OUT", Status: status, StartedAt: stamp, EndedAt: stamp,
			RequestIDs: []tencentcloud.RequestTrace{{Action: "DescribeAutoScalingActivities", RequestID: testRequest}}})
	}
	if c.lost {
		result.RequestID, result.Unknown = "", true
		return result, fmt.Errorf("synthetic response lost")
	}
	return result, nil
}

func desiredFixture(t *testing.T) (Config, *desiredCloud, *multiNomad, *Engine) {
	t.Helper()
	cfg, base, n, e := groupContractFixture(t, 1, 0, 6, false)
	e.Close()
	base.snap.Group.AllocationPolicy = "LAUNCH_CONFIGURATION"
	c := &desiredCloud{multiCloud: base, dir: cfg.Dir, partial: -1}
	cfg.StabilizationInterval = time.Millisecond
	return cfg, c, n, openMulti(t, cfg, c, n)
}

func settleDesired(t *testing.T, e *Engine) {
	t.Helper()
	for i := 0; i < 5; i++ {
		step(t, e)
	}
}

func TestDesiredCASNormalAndDurableAbsoluteIntent(t *testing.T) {
	_, c, _, e := desiredFixture(t)
	if _, err := e.ScaleAndWait(5, 5); err != nil {
		t.Fatal(err)
	}
	if len(c.calls) != 1 || c.calls[0] != [2]int64{5, 1} || len(e.state.Members) != 5 || e.state.LastCompletedAt == 0 {
		t.Fatal(c.calls, e.state)
	}
}

func TestDesiredCASLostResponseReconcilesAfterRestart(t *testing.T) {
	for _, applied := range []bool{true, false} {
		t.Run(fmt.Sprint(applied), func(t *testing.T) {
			cfg, c, n, e := desiredFixture(t)
			c.lost, c.notApplied = true, !applied
			if _, err := e.Scale(5); err != ErrUnknown {
				t.Fatal(err)
			}
			e.Close()
			c.lost, c.notApplied = false, false
			e = openMulti(t, cfg, c, n)
			settleDesired(t, e)
			want := 1
			if !applied {
				want = 2
			}
			if len(c.calls) != want || e.state.Pending != nil || len(e.state.Members) != 5 || e.state.LastCompletedAt == 0 {
				t.Fatal(c.calls, e.state)
			}
			for _, call := range c.calls {
				if call != [2]int64{5, 1} {
					t.Fatal("baseline changed during replay", call)
				}
			}
		})
	}
}

func TestDesiredCASConflictNeverReplaysAcrossRestart(t *testing.T) {
	cfg, c, n, e := desiredFixture(t)
	c.conflict = true
	if _, err := e.Scale(5); err != ErrUnknown {
		t.Fatal(err)
	}
	e.Close()
	c.conflict = false
	e = openMulti(t, cfg, c, n)
	for i := 0; i < 3; i++ {
		if err := e.reconcileOnce(); err != ErrUnknown {
			t.Fatal(err)
		}
	}
	if len(c.calls) != 1 || e.state.Pending == nil || !e.state.Pending.DesiredConflict {
		t.Fatal(c.calls, e.state)
	}
}

func TestDesiredCASPartialFailureReadyBackoffAndSameTargetResume(t *testing.T) {
	cfg, c, n, e := desiredFixture(t)
	c.partial = 3
	if _, err := e.ScaleAndWait(5, 5); err == nil {
		t.Fatal("partial failure reported success")
	}
	settleDesired(t, e)
	s, ready, err := e.Snapshot()
	if err != nil || !ready || len(s.Members) != 3 || s.Desired != 5 || s.LastCompletedAt != 0 || s.GrowthBackoff == nil {
		t.Fatal(ready, err, s)
	}
	deadline := s.GrowthBackoff.RetryAfter
	e.Close()
	e = openMulti(t, cfg, c, n)
	if !e.state.GrowthBackoff.RetryAfter.Equal(deadline) {
		t.Fatal("restart reset backoff")
	}
	if _, err := e.Scale(5); err == nil || err.Error() != "GrowthRetryBackoffActive" || len(c.calls) != 1 {
		t.Fatal(err, c.calls)
	}
	e.mu.Lock()
	e.state.GrowthBackoff.RetryAfter = time.Now().Add(-time.Second)
	if err := e.save(); err != nil {
		t.Fatal(err)
	}
	e.mu.Unlock()
	c.partial = -1
	if _, err := e.ScaleAndWait(5, 5); err != nil {
		t.Fatal(err)
	}
	if len(c.calls) != 2 || c.calls[1] != [2]int64{5, 5} || e.state.GrowthBackoff != nil || e.state.LastCompletedAt == 0 {
		t.Fatal(c.calls, e.state)
	}
}

func TestDesiredCASTargetChangeResetsBackoff(t *testing.T) {
	_, c, _, e := desiredFixture(t)
	c.partial = 3
	_, _ = e.ScaleAndWait(5, 5)
	settleDesired(t, e)
	c.partial = -1
	if _, err := e.ScaleAndWait(6, 5); err != nil {
		t.Fatal(err)
	}
	if c.calls[1] != [2]int64{6, 5} || e.state.GrowthBackoff != nil {
		t.Fatal(c.calls, e.state.GrowthBackoff)
	}
}

func TestDesiredCASSameTargetLostFailureBacksOffWithoutReplay(t *testing.T) {
	_, c, _, e := desiredFixture(t)
	c.partial = 3
	_, _ = e.ScaleAndWait(5, 5)
	settleDesired(t, e)
	e.mu.Lock()
	e.state.GrowthBackoff.RetryAfter = time.Now().Add(-time.Second)
	if err := e.save(); err != nil {
		t.Fatal(err)
	}
	e.mu.Unlock()
	c.lost = true
	if _, err := e.Scale(5); err != ErrUnknown {
		t.Fatal(err)
	}
	settleDesired(t, e)
	if len(c.calls) != 2 || e.state.Pending != nil || e.state.GrowthBackoff == nil || e.state.GrowthBackoff.Failures != 2 || e.state.LastCompletedAt != 0 {
		t.Fatal("lost same-target failure was replayed instead of backing off", c.calls, e.state)
	}
	if _, err := e.Scale(5); err == nil || err.Error() != "GrowthRetryBackoffActive" || len(c.calls) != 2 {
		t.Fatal(err, c.calls)
	}
}

func TestGrowthBackoffExponentialCap(t *testing.T) {
	s := State{}
	at := time.Now()
	for i, want := range []time.Duration{2, 4, 8, 16, 30, 30} {
		recordGrowthBackoff(&s, 5, at)
		if s.GrowthBackoff.Failures != i+1 || s.GrowthBackoff.RetryAfter.Sub(at) != want*time.Minute {
			t.Fatal(s.GrowthBackoff)
		}
	}
	recordGrowthBackoff(&s, 6, at)
	if s.GrowthBackoff.Failures != 1 {
		t.Fatal("new target inherited failures")
	}
}
