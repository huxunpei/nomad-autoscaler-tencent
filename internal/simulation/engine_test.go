package simulation

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func cfg(t *testing.T, initial int64, fault string) Config {
	t.Helper()
	return Config{Dir: t.TempDir(), GroupID: "asg-sim", Initial: initial, Min: 1, Max: 5, Fault: fault, Protect: true, ReconcileInterval: 5 * time.Millisecond}
}

func engine(t *testing.T, c Config) *Engine {
	t.Helper()
	e, err := Open(c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(e.Close)
	return e
}

func TestOneFiveOneAndIdempotence(t *testing.T) {
	e := engine(t, cfg(t, 1, "none"))
	for _, n := range []int64{5, 1} {
		if changed, err := e.Scale(n); err != nil || !changed {
			t.Fatalf("Scale(%d): %v, %v", n, changed, err)
		}
		s, ready, err := e.Snapshot()
		if err != nil || !ready || int64(len(s.Members)) != n || s.Desired != n {
			t.Fatalf("unexpected state: %+v ready=%v err=%v", s, ready, err)
		}
		if changed, err := e.Scale(n); changed || err != nil {
			t.Fatalf("repeat: %v %v", changed, err)
		}
	}
}

func TestSameAbsoluteTargetAfterKnownGrowFailure(t *testing.T) {
	e := engine(t, cfg(t, 1, "grow-failure"))
	if _, err := e.Scale(5); err == nil {
		t.Fatal("expected injected quota failure")
	}
	s, ready, err := e.Snapshot()
	if err != nil || !ready || len(s.Members) != 1 || s.Desired != 5 {
		t.Fatalf("known failure must remain retryable: %+v", s)
	}
	if changed, err := e.Scale(5); err != nil || !changed {
		t.Fatalf("retry: %v %v", changed, err)
	}
	s, _, _ = e.Snapshot()
	if len(s.Members) != 5 {
		t.Fatal("same target did not converge")
	}
}

func TestPartialFailureRestoresOnlySurvivorAndDeletesSelected(t *testing.T) {
	e := engine(t, cfg(t, 5, "partial-delete"))
	if _, err := e.Scale(3); !errors.Is(err, ErrPartial) {
		t.Fatalf("got %v", err)
	}
	s, ready, err := e.Snapshot()
	if err != nil || !ready || len(s.Members) != 4 || s.Desired != 4 {
		t.Fatalf("partial failure state: %+v", s)
	}
	if _, exists := s.Members["ins-sim-0001"]; exists {
		t.Fatal("confirmed deletion not applied")
	}
	if !s.Members["ins-sim-0002"].Eligible || s.Members["ins-sim-0002"].DrainedBy != "" {
		t.Fatal("running failed node not restored")
	}
	for _, id := range []string{"ins-sim-0003", "ins-sim-0004", "ins-sim-0005"} {
		if !s.Members[id].Eligible {
			t.Fatalf("untouched node changed: %s", id)
		}
	}
}

func TestUnknownIsNotFailureAndWorkerRecoversAfterRestart(t *testing.T) {
	c := cfg(t, 3, "unknown-delete")
	e := engine(t, c)
	if _, err := e.Scale(1); !errors.Is(err, ErrUnknown) {
		t.Fatal(err)
	}
	s, ready, _ := e.Snapshot()
	if ready || s.Pending == nil || s.Members["ins-sim-0001"].Eligible {
		t.Fatal("unknown result must remain pending and drained")
	}
	if _, err := e.Scale(1); !errors.Is(err, ErrBusy) {
		t.Fatalf("must not duplicate removal: %v", err)
	}
	e.Close()
	e2 := engine(t, c)
	// Resolve one node while the other stays unknown. The worker must restore
	// the confirmed failed node and continue waiting without an ownership error.
	if err := e2.Observe(map[string]string{"ins-sim-0001": "failed"}); err != nil {
		t.Fatal(err)
	}
	wait(t, func() bool {
		s, _, err := e2.Snapshot()
		return err == nil && s.Pending != nil && s.Members["ins-sim-0001"].Eligible
	})
	time.Sleep(20 * time.Millisecond)
	if _, _, err := e2.Snapshot(); err != nil {
		t.Fatalf("repeated recovery was not idempotent: %v", err)
	}
	if err := e2.Observe(map[string]string{"ins-sim-0002": "deleted"}); err != nil {
		t.Fatal(err)
	}
	wait(t, func() bool {
		s, ready, err := e2.Snapshot()
		return err == nil && ready && s.Pending == nil && len(s.Members) == 2 && s.Desired == 2
	})
}

func TestProtectionUnjoinedAndActualMembershipCount(t *testing.T) {
	e := engine(t, cfg(t, 3, "none"))
	e.mu.Lock()
	m := e.state.Members["ins-sim-0001"]
	m.Protected = true
	e.state.Members[m.ID] = m
	m = e.state.Members["ins-sim-0002"]
	m.Joined = false
	e.state.Members[m.ID] = m
	e.mu.Unlock()
	if _, err := e.Scale(1); err == nil {
		t.Fatal("must reject insufficient candidates before drain")
	}
	s, ready, err := e.Snapshot()
	if err != nil || !ready || len(s.Members) != 3 || s.Pending != nil {
		t.Fatal("unjoined/protected nodes still count")
	}
	for _, m := range s.Members {
		if !m.Eligible || m.DrainedBy != "" {
			t.Fatal("failed selection must not drain")
		}
	}
	if _, err := e.Scale(2); err != nil {
		t.Fatal(err)
	}
	s, _, _ = e.Snapshot()
	if _, ok := s.Members["ins-sim-0003"]; ok {
		t.Fatal("only selectable node should be removed")
	}
}

func TestStatusIsReadOnlyAndStateDirectoryExclusive(t *testing.T) {
	c := cfg(t, 1, "none")
	e := engine(t, c)
	before, err := os.ReadFile(filepath.Join(c.Dir, "simulation.json"))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, _, err := e.Snapshot(); err != nil {
			t.Fatal(err)
		}
	}
	after, err := os.ReadFile(filepath.Join(c.Dir, "simulation.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("Status wrote state")
	}
	if e2, err := Open(c); err == nil {
		e2.Close()
		t.Fatal("two writers used one state directory")
	}
}

func TestMalformedStateDoesNotResetCapacity(t *testing.T) {
	c := cfg(t, 1, "none")
	if err := os.WriteFile(filepath.Join(c.Dir, "simulation.json"), []byte(`{"version":999}`), 0600); err != nil {
		t.Fatal(err)
	}
	if e, err := Open(c); err == nil {
		e.Close()
		t.Fatal("invalid state must fail closed")
	}
}

func TestPersistenceFailureStopsReadyAndFurtherMutations(t *testing.T) {
	c := cfg(t, 1, "none")
	e := engine(t, c)
	oldPath := filepath.Join(c.Dir, "simulation.json")
	before, err := os.ReadFile(oldPath)
	if err != nil {
		t.Fatal(err)
	}
	// Force CreateTemp to fail deterministically, including when tests run as root.
	e.mu.Lock()
	e.cfg.Dir = filepath.Join(c.Dir, "missing", "directory")
	e.mu.Unlock()
	if _, err := e.Scale(5); err == nil {
		t.Fatal("expected persistence failure")
	}
	if _, ready, err := e.Snapshot(); ready || err == nil {
		t.Fatal("unpersisted memory reported ready")
	}
	if _, err := e.Scale(5); err == nil {
		t.Fatal("subsequent mutation should stay blocked")
	}
	after, err := os.ReadFile(oldPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("old state was corrupted")
	}
}

func TestStaleIDCounterDoesNotOverwriteProtectedMember(t *testing.T) {
	c := cfg(t, 1, "none")
	e := engine(t, c)
	e.mu.Lock()
	e.state.NextID = 0
	m := e.state.Members["ins-sim-0001"]
	m.Protected = true
	m.Joined = false
	e.state.Members[m.ID] = m
	err := e.save()
	e.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	e.Close()
	e2 := engine(t, c)
	if _, err := e2.Scale(2); err != nil {
		t.Fatal(err)
	}
	s, _, _ := e2.Snapshot()
	if !s.Members[m.ID].Protected || s.Members[m.ID].Joined {
		t.Fatal("ID collision reset existing member")
	}
}

func TestRejectContradictoryRestoredStateOnRestart(t *testing.T) {
	c := cfg(t, 2, "unknown-delete")
	e := engine(t, c)
	if _, err := e.Scale(1); !errors.Is(err, ErrUnknown) {
		t.Fatal(err)
	}
	s, _, _ := e.Snapshot()
	e.Close()
	id := s.Pending.Selected[0]
	s.Pending.Results[id] = "failed"
	s.Pending.Restored[id] = true
	b, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(c.Dir, "simulation.json"), b, 0600); err != nil {
		t.Fatal(err)
	}
	if e2, err := Open(c); err == nil {
		e2.Close()
		t.Fatal("contradictory recovery state accepted")
	}
}

func TestConcurrentTargetsStayWithinLimits(t *testing.T) {
	e := engine(t, cfg(t, 1, "none"))
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := e.Scale(int64(1 + i%5))
			if err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	s, ready, err := e.Snapshot()
	if err != nil || !ready || s.Desired != int64(len(s.Members)) || len(s.Members) < 1 || len(s.Members) > 5 {
		t.Fatal("inconsistent concurrent targets")
	}
}

func TestOwnershipChangeDoesNotOverwriteExternalDrain(t *testing.T) {
	e := engine(t, cfg(t, 2, "unknown-delete"))
	if _, err := e.Scale(1); !errors.Is(err, ErrUnknown) {
		t.Fatal(err)
	}
	e.mu.Lock()
	m := e.state.Members["ins-sim-0001"]
	m.DrainedBy = "external"
	e.state.Members[m.ID] = m
	e.mu.Unlock()
	if err := e.Observe(map[string]string{"ins-sim-0001": "failed"}); err != nil {
		t.Fatal(err)
	}
	wait(t, func() bool { _, _, err := e.Snapshot(); return err != nil })
	s, _, _ := e.Snapshot()
	if s.Members[m.ID].DrainedBy != "external" || s.Members[m.ID].Eligible {
		t.Fatal("overwrote external drain")
	}
}

func TestRestartAbortsDrainBeforeDeleteIntent(t *testing.T) {
	c := cfg(t, 2, "none")
	e := engine(t, c)
	e.mu.Lock()
	id := "ins-sim-0001"
	m := e.state.Members[id]
	m.DrainedBy = "sim-interrupted"
	m.Eligible = false
	e.state.Members[id] = m
	e.state.Pending = &Operation{ID: "sim-interrupted", Phase: "draining", Target: 1, Selected: []string{id}, Results: map[string]string{}, Restored: map[string]bool{}}
	err := e.save()
	e.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	e.Close()
	e2 := engine(t, c)
	wait(t, func() bool {
		s, ready, err := e2.Snapshot()
		return err == nil && ready && s.Pending == nil && s.Members[id].Eligible
	})
	s, _, _ := e2.Snapshot()
	if len(s.Members) != 2 {
		t.Fatal("restart continued a pre-delete operation")
	}
}

func wait(t *testing.T, ready func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if ready() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("worker did not converge")
}
