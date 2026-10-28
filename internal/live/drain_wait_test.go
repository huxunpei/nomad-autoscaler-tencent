package live

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"testing/synctest"
	"time"

	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadclient"
)

func TestDrainWaitIsDurableBeforeSDKAndSurvivesRestart(t *testing.T) {
	for _, d := range []time.Duration{0, -time.Second} {
		t.Run(d.String(), func(t *testing.T) {
			cfg := config(t)
			cfg.GroupContract, cfg.DrainDeadline, cfg.StabilizationInterval = true, d, 11*time.Millisecond
			c, raw := newCloud(), newNomad()
			c.addMember()
			n := &sdkProbe{fakeNomad: raw, directory: cfg.Dir, incomplete: true}
			var before State
			raw.beforeWrite = func(phase string) error {
				b, err := os.ReadFile(filepath.Join(cfg.Dir, "live.json"))
				if err != nil || json.Unmarshal(b, &before) != nil || before.Pending == nil || before.Batch == nil || before.Pending.DrainWait == nil || before.Batch.DrainWait == nil {
					return code("DrainWaitNotDurableBeforeMutation")
				}
				return nil
			}
			e, err := Open(cfg, c, n)
			if err != nil {
				t.Fatal(err)
			}
			changed, err := e.Scale(0)
			if !changed || err == nil || before.Pending == nil {
				e.Close()
				t.Fatal("SDK write barrier not exercised", changed, err)
			}
			until, parent := before.Pending.DrainWait.Until, before.Batch.DrainWait.Until
			e.Close()
			// Recovery CLI has no stabilization-interval flag. Its different
			// local interval must not invalidate or regenerate the original bound.
			cfg.StabilizationInterval = 2 * time.Second
			session, err := OpenRecovery(cfg, c, n)
			if err != nil {
				t.Fatal(err)
			}
			if !session.e.state.Pending.DrainWait.Until.Equal(until) {
				t.Fatal("recovery changed the deadline")
			}
			session.Close()
			e, err = Open(cfg, c, n)
			if err != nil {
				t.Fatal(err)
			}
			defer e.Close()
			s, _, _ := e.Snapshot()
			if !s.Pending.DrainWait.Until.Equal(until) || !s.Batch.DrainWait.Until.Equal(parent) || s.Pending.DrainWait.Interval != 11*time.Millisecond || n.preCalls != 1 || c.writeCount() != 0 || raw.writeCount() != 1 {
				t.Fatal("restart extended the bound or replayed a mutation")
			}
		})
	}
}

func TestDrainWaitRejectsMissingInconsistentAndExtendedEvidence(t *testing.T) {
	cfg := config(t)
	cfg.GroupContract, cfg.DrainDeadline = true, 0
	c, raw := newCloud(), newNomad()
	c.addMember()
	n := &sdkProbe{fakeNomad: raw, directory: cfg.Dir, incomplete: true}
	e, err := Open(cfg, c, n)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if _, err := e.Scale(0); err == nil {
		t.Fatal("expected incomplete monitoring")
	}
	s, _, _ := e.Snapshot()
	for _, tc := range []struct {
		name   string
		change func(*State)
	}{
		{"missing-operation", func(s *State) { s.Pending.DrainWait = nil }},
		{"missing-batch", func(s *State) { s.Batch.DrainWait = nil }},
		{"changed-attempts", func(s *State) { s.Pending.DrainWait.Attempts++ }},
		{"invalid-interval", func(s *State) { s.Pending.DrainWait.Interval = 0 }},
		{"child-after-parent", func(s *State) { s.Pending.DrainWait.Until = s.Batch.DrainWait.Until.Add(time.Nanosecond) }},
		{"extended-parent", func(s *State) { s.Batch.DrainWait.Until = s.Batch.DrainWait.Until.Add(time.Nanosecond) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			copy := copyJSON(s)
			tc.change(&copy)
			check := &Engine{cfg: e.cfg, state: copy}
			if check.validateState() == nil {
				t.Fatal("unverifiable wait evidence accepted")
			}
		})
	}
}

type deadlineBlockingSDK struct {
	*sdkProbe
	deadline time.Time
}

func (n *deadlineBlockingSDK) PreScaleIn(ctx context.Context, id nomadclient.Identity, op string, d time.Duration, before func() error) (nomadclient.MutationResult, error) {
	result, err := n.sdkProbe.PreScaleIn(ctx, id, op, d, before)
	if err != nil {
		return result, err
	}
	n.deadline, _ = ctx.Deadline()
	<-ctx.Done()
	result.MonitoringIncomplete = true
	return result, ctx.Err()
}

func TestDrainWaitForegroundExpiryRestoresWithoutCloudDelete(t *testing.T) {
	for _, d := range []time.Duration{0, -time.Second} {
		t.Run(d.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				cfg := config(t)
				cfg.GroupContract = true
				cfg.DrainDeadline = d
				cfg.StabilizationInterval = 5 * time.Second
				c, raw := newCloud(), newNomad()
				c.addMember()
				n := &deadlineBlockingSDK{sdkProbe: &sdkProbe{fakeNomad: raw, directory: cfg.Dir}}
				e, err := Open(cfg, c, n)
				if err != nil {
					t.Fatal(err)
				}
				defer e.Close()
				start := time.Now()
				changed, err := e.ScaleAndWait(0, 15)
				elapsed := time.Since(start)
				if !changed || err == nil || c.writeCount() != 0 {
					t.Fatal("expired wait deleted or reported success", changed, err)
				}
				s, _, _ := e.Snapshot()
				if s.Pending == nil || s.Pending.Phase != "draining" || !s.Pending.DrainWait.Until.Equal(n.deadline) || time.Now().Before(n.deadline) || s.LastCompletedAt != 0 {
					t.Fatal("expiry lost the original drain obligation")
				}
				if (d == 0 && (elapsed <= time.Minute || elapsed >= time.Hour)) || (d < 0 && elapsed != time.Minute) {
					t.Fatal("wrong bounded observation mode", elapsed)
				}
				original := s.Pending.DrainWait.Until
				step(t, e)
				step(t, e)
				s, _, _ = e.Snapshot()
				if s.Pending != nil || s.Batch != nil || s.LastBatch == nil || s.LastBatch.Phase != "failed" || s.LastCompletedAt != 0 || c.writeCount() != 0 || raw.writeCount() != 2 || n.preCalls != 1 || !s.LastOperation.DrainWait.Until.Equal(original) {
					t.Fatal("expiry did not close through audited owned restoration")
				}
				t.Logf("virtual elapsed=%s, raw Nomad deadline=%s; expired wait restored only the original node, cloud writes=0", elapsed, d)
			})
		})
	}
}

func TestDrainWaitBatchExpiryDoesNotDrainNextChildAfterRestart(t *testing.T) {
	_, c, n, old := groupContractFixture(t, 2, 0, 3, false)
	old.Close()
	cfg := config(t)
	cfg.GroupContract = true
	cfg.DrainDeadline = 0
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	cfg.Context = ctx
	e, err := Open(cfg, c, n)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = e.Scale(0); err != nil {
		e.Close()
		t.Fatal(err)
	}
	for i := 0; i < 6; i++ {
		if e.state.Pending != nil && e.state.Pending.Phase == "removing" {
			c.terminal(true)
		}
		step(t, e)
		if e.state.Pending == nil && e.state.Batch != nil && len(e.state.Batch.Results) == 1 {
			break
		}
	}
	s, _, _ := e.Snapshot()
	if s.Pending != nil || s.Batch == nil || len(s.Batch.Results) != 1 || c.writeCount() != 1 || len(n.writes) != 1 {
		e.Close()
		t.Fatal("did not stop between children")
	}
	until := s.Batch.DrainWait.Until
	e.Close()
	if left := time.Until(until); left > 0 {
		time.Sleep(left + time.Millisecond)
	}
	cfg.Context = context.Background()
	e, err = Open(cfg, c, n)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if !e.state.Batch.DrainWait.Until.Equal(until) {
		t.Fatal("batch deadline restarted")
	}
	step(t, e)
	s, _, _ = e.Snapshot()
	if s.Pending != nil || s.Batch != nil || s.LastBatch == nil || s.LastBatch.Phase != "failed" || len(s.LastBatch.Results) != 2 || s.LastCompletedAt != 0 || c.writeCount() != 1 || len(n.writes) != 1 {
		t.Fatal("expired batch drained another node or discarded prior success")
	}
	if s.LastBatch.Results[1].AbortReason != "batch-drain-wait-timeout" || s.LastBatch.Results[1].DrainOutcome != "not-submitted" {
		t.Fatal("missing exact no-write failure evidence")
	}
	t.Log("original batch bound retained; completed child preserved; next child not drained")
}

func TestDrainWaitMinimumSignedDurationBudgetDoesNotOverflow(t *testing.T) {
	for _, d := range []time.Duration{0, -time.Second, time.Duration(-1 << 63)} {
		budget, err := completionBudgetFor(d, 5*time.Second, 1, 0, 15)
		if err != nil || budget != 35*time.Minute+15*time.Second {
			t.Fatal("signed deadline changed controller budget", d, budget, err)
		}
	}
}
