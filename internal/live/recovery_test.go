package live

import (
	"context"
	"encoding/json"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/tencentcloud"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func recoveryFixture(t *testing.T, shrink bool) (Config, *fakeCloud, *fakeNomad) {
	t.Helper()
	cfg := config(t)
	c := newCloud()
	n := newNomad()
	e := openTest(t, cfg, c, n)
	if shrink {
		grow(t, e)
		c.removeUnknown = true
		if _, err := e.Scale(0); err != nil {
			t.Fatal(err)
		}
		n.drained()
		if err := e.reconcileOnce(); err != ErrUnknown {
			t.Fatal(err)
		}
		c.complete()
	} else {
		c.growUnknown = true
		if _, err := e.Scale(1); err != ErrUnknown {
			t.Fatal(err)
		}
	}
	e.Close()
	c.activity.Status = "SUCCESSFUL"
	c.activity.RelatedInstances[0].Status = "SUCCESSFUL"
	c.activity.Type = "SCALE_OUT"
	if shrink {
		c.activity.Type = "REMOVE_INSTANCES"
	}
	c.activity.StartedAt = time.Now().UTC().Format(time.RFC3339)
	c.activity.EndedAt = c.activity.StartedAt
	return cfg, c, n
}
func recoverySession(t *testing.T, cfg Config, c *fakeCloud, n *fakeNomad) *Recovery {
	t.Helper()
	r, err := OpenRecovery(cfg, c, n)
	if err != nil {
		t.Fatal(err)
	}
	r.delay = time.Millisecond
	t.Cleanup(r.Close)
	return r
}
func recoveryPlan(t *testing.T, r *Recovery) RecoveryPlan {
	t.Helper()
	p, err := r.Preview(context.Background(), testActivity, "tester", "TEST-123")
	if err != nil {
		t.Fatal(err)
	}
	return p
}
func TestRecoverySuccessfulOutcomes(t *testing.T) {
	for _, shrink := range []bool{false, true} {
		t.Run(map[bool]string{false: "grow", true: "remove"}[shrink], func(t *testing.T) {
			cfg, c, n := recoveryFixture(t, shrink)
			writes := c.writeCount()
			nw := len(n.writes)
			before, _ := os.ReadFile(filepath.Join(cfg.Dir, "live.json"))
			r := recoverySession(t, cfg, c, n)
			p := recoveryPlan(t, r)
			afterPreview, _ := os.ReadFile(filepath.Join(cfg.Dir, "live.json"))
			if string(before) != string(afterPreview) {
				t.Fatal("preview changed journal")
			}
			result, err := r.Apply(context.Background(), p, p.Digest)
			if err != nil {
				t.Fatal(err)
			}
			if result.Status != "committed" {
				t.Fatal(result)
			}
			if c.writeCount() != writes || len(n.writes) != nw {
				t.Fatal("recovery mutated provider")
			}
			if _, err = r.Apply(context.Background(), p, p.Digest); err == nil {
				t.Fatal("reused plan")
			}
			r.Close()
			e, err := Open(cfg, c, n)
			if err != nil {
				t.Fatal(err)
			}
			defer e.Close()
			s, ready, err := e.Snapshot()
			if err != nil || !ready || s.Pending != nil || s.LastOperation.Unknown || s.RecoveryAudit == nil {
				t.Fatalf("unresolved: %v %v %+v", err, ready, s)
			}
			if c.writeCount() != writes {
				t.Fatal("replayed")
			}
		})
	}
}
func TestRecoveryRejectsUnsafeEvidence(t *testing.T) {
	tests := []struct {
		name   string
		change func(*fakeCloud, *fakeNomad)
	}{
		{"failed", func(c *fakeCloud, n *fakeNomad) { c.activity.Status = "FAILED" }},
		{"running", func(c *fakeCloud, n *fakeNomad) { c.activity.Status = "RUNNING" }},
		{"wrong-group", func(c *fakeCloud, n *fakeNomad) { c.activity.GroupID = "asg-other" }},
		{"old-activity", func(c *fakeCloud, n *fakeNomad) { c.activity.StartedAt = "2020-01-01T00:00:00Z" }},
		{"wrong-type", func(c *fakeCloud, n *fakeNomad) { c.activity.Type = "SCALE_IN" }},
		{"wrong-instance", func(c *fakeCloud, n *fakeNomad) { c.activity.RelatedInstances[0].ID = "ins-other" }},
		{"partial-instance", func(c *fakeCloud, n *fakeNomad) { c.activity.RelatedInstances[0].Status = "FAILED" }},
		{"unregistered", func(c *fakeCloud, n *fakeNomad) { n.missing = true }},
		{"foreign-drain", func(c *fakeCloud, n *fakeNomad) { n.node.Eligibility = "ineligible" }},
		{"lc-drift", func(c *fakeCloud, n *fakeNomad) { c.snap.LaunchConfiguration.ImageID = "img-other" }},
		{"cloud-error", func(c *fakeCloud, n *fakeNomad) { c.observeFailure = true }},
		{"desired-drift", func(c *fakeCloud, n *fakeNomad) { c.snap.Group.Desired = 0 }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg, c, n := recoveryFixture(t, false)
			r := recoverySession(t, cfg, c, n)
			tc.change(c, n)
			if _, err := r.Preview(context.Background(), testActivity, "tester", "TEST-123"); err == nil || strings.Contains(err.Error(), "raw-secret") {
				t.Fatal(err)
			}
		})
	}
}
func TestRecoveryPlanAndLockGuards(t *testing.T) {
	cfg, c, n := recoveryFixture(t, false)
	r := recoverySession(t, cfg, c, n)
	if other, err := Open(cfg, c, n); err == nil {
		other.Close()
		t.Fatal("engine shared recovery lock")
	}
	p := recoveryPlan(t, r)
	if _, err := r.Apply(context.Background(), p, strings.Repeat("0", 64)); err == nil {
		t.Fatal("wrong confirmation")
	}
	changed := p
	changed.Operator = "other"
	if _, err := r.Apply(context.Background(), changed, p.Digest); err == nil {
		t.Fatal("tampered plan")
	}
	expired := p
	expired.CreatedAt = time.Now().Add(-10 * time.Minute)
	expired.ExpiresAt = expired.CreatedAt.Add(5 * time.Minute)
	expired.Digest = planDigest(expired)
	if _, err := r.Apply(context.Background(), expired, expired.Digest); err == nil {
		t.Fatal("expired plan")
	}
	vm := c.snap.VMs[testInstance]
	vm.Protected = true
	c.snap.VMs[testInstance] = vm
	if _, err := r.Apply(context.Background(), p, p.Digest); err == nil {
		t.Fatal("changed evidence")
	}
}
func TestRecoveryStaleJournal(t *testing.T) {
	cfg, c, n := recoveryFixture(t, false)
	r := recoverySession(t, cfg, c, n)
	p := recoveryPlan(t, r)
	r.Close()
	e, err := Open(cfg, c, n)
	if err != nil {
		t.Fatal(err)
	}
	e.Close()
	r = recoverySession(t, cfg, c, n)
	if _, err = r.Apply(context.Background(), p, p.Digest); err == nil {
		t.Fatal("stale journal")
	}
}
func TestRecoveryCrashAuditRepair(t *testing.T) {
	for _, at := range []string{"prepared", "journal", "receipt"} {
		t.Run(at, func(t *testing.T) {
			cfg, c, n := recoveryFixture(t, false)
			r := recoverySession(t, cfg, c, n)
			p := recoveryPlan(t, r)
			r.checkpoint = func(stage string) error {
				if stage == at {
					return code("SimulatedCrash")
				}
				return nil
			}
			if _, err := r.Apply(context.Background(), p, p.Digest); err == nil {
				t.Fatal("no crash")
			}
			r.Close()
			if e, err := Open(cfg, c, n); err == nil {
				e.Close()
				t.Fatal("engine ignored incomplete audit")
			}
			result, err := RepairRecovery(cfg, p.Digest)
			if err != nil {
				t.Fatal(err)
			}
			expected := "committed"
			if at == "prepared" {
				expected = "aborted"
			}
			if result.Status != expected {
				t.Fatal(result)
			}
			e, err := Open(cfg, c, n)
			if err != nil {
				t.Fatal(err)
			}
			s, _, _ := e.Snapshot()
			e.Close()
			if (s.Pending != nil) != (at == "prepared") {
				t.Fatal("repair changed resolution")
			}
		})
	}
}
func TestRecoveryAuditCorruptionBlocksRestart(t *testing.T) {
	cfg, c, n := recoveryFixture(t, false)
	r := recoverySession(t, cfg, c, n)
	p := recoveryPlan(t, r)
	if _, err := r.Apply(context.Background(), p, p.Digest); err != nil {
		t.Fatal(err)
	}
	r.Close()
	b, _ := os.ReadFile(filepath.Join(cfg.Dir, "live.json"))
	var s State
	_ = json.Unmarshal(b, &s)
	path := filepath.Join(cfg.Dir, "recovery-audit", s.RecoveryAudit.ID+".json")
	if err := os.WriteFile(path, []byte("{}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if e, err := Open(cfg, c, n); err == nil {
		e.Close()
		t.Fatal("audit corruption accepted")
	}
}

func TestRecoveryMultiBatchContinuationIsExplicit(t *testing.T) {
	cfg, c, n, e := multiFixture(t, 2, 2)
	c.removeUnknown = true
	if _, err := e.Scale(0); err != nil {
		t.Fatal(err)
	}
	if err := e.reconcileOnce(); err != ErrUnknown {
		t.Fatal(err)
	}
	e.Close()
	c.terminal(true)
	var activity string
	for id, a := range c.activities {
		activity = id
		a.Type = "REMOVE_INSTANCES"
		a.StartedAt = time.Now().UTC().Format(time.RFC3339)
		a.EndedAt = a.StartedAt
		c.activities[id] = a
	}
	r, err := OpenRecovery(cfg, c, n)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	r.delay = time.Millisecond
	p, err := r.Preview(context.Background(), activity, "tester", "TEST-123")
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Decision.Remaining) != 1 || p.Decision.BatchTarget == nil || *p.Decision.BatchTarget != 0 || p.Decision.RestartEffect != "remaining-batch-deletions-resume-on-agent-restart" {
		t.Fatal(p.Decision)
	}
	writes := c.writeCount()
	if _, err = r.Apply(context.Background(), p, p.Digest); err != nil {
		t.Fatal(err)
	}
	if c.writeCount() != writes {
		t.Fatal("CLI advanced deletion")
	}
	r.Close()
	e, err = Open(cfg, c, n)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if c.writeCount() != writes {
		t.Fatal("open replayed unknown write")
	}
	c.removeUnknown = false
	step(t, e)
	s := multiState(t, e)
	if s.Pending == nil || s.Pending.InstanceID != p.Decision.Remaining[0].InstanceID {
		t.Fatal("incorrect batch continuation")
	}
	if c.writeCount() != writes {
		t.Fatal("next member deleted before drain")
	}
}
func TestRecoveryRejectsExistingVMAndWrongRemovalType(t *testing.T) {
	for _, name := range []string{"vm-still-exists", "ordinary-scale-in", "other-activity"} {
		t.Run(name, func(t *testing.T) {
			cfg, c, n := recoveryFixture(t, true)
			r := recoverySession(t, cfg, c, n)
			switch name {
			case "vm-still-exists":
				vm := c.snap.VMs[testInstance]
				vm.ID = testInstance
				vm.Returned = true
				vm.State = "RUNNING"
				vm.ChargeType = "POSTPAID_BY_HOUR"
				c.snap.VMs[testInstance] = vm
			case "ordinary-scale-in":
				c.activity.Type = "SCALE_IN"
			case "other-activity":
				c.activity.ID = "asa-other"
			}
			if _, err := r.Preview(context.Background(), testActivity, "tester", "TEST-123"); err == nil {
				t.Fatal("unsafe remove evidence")
			}
		})
	}
}
func TestRecoveryRepairConflictStaysFrozen(t *testing.T) {
	cfg, c, n := recoveryFixture(t, false)
	r := recoverySession(t, cfg, c, n)
	p := recoveryPlan(t, r)
	r.checkpoint = func(string) error { return code("SimulatedCrash") }
	if _, err := r.Apply(context.Background(), p, p.Digest); err == nil {
		t.Fatal("expected crash")
	}
	r.Close()
	path := filepath.Join(cfg.Dir, "live.json")
	b, _ := os.ReadFile(path)
	if err := os.WriteFile(path, append(b, ' '), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := RepairRecovery(cfg, p.Digest); err == nil {
		t.Fatal("repaired unknown file")
	}
	if _, err := os.Stat(filepath.Join(cfg.Dir, recoveryMarkerName)); err != nil {
		t.Fatal("lost safety fence")
	}
}
func TestRecoveryAuditPathAndPermissions(t *testing.T) {
	for _, name := range []string{"symlink-audit", "public-plan", "plan-replace"} {
		t.Run(name, func(t *testing.T) {
			cfg, c, n := recoveryFixture(t, false)
			r := recoverySession(t, cfg, c, n)
			p := recoveryPlan(t, r)
			switch name {
			case "symlink-audit":
				if err := os.Symlink(t.TempDir(), filepath.Join(cfg.Dir, "recovery-audit")); err != nil {
					t.Fatal(err)
				}
				if _, err := r.Apply(context.Background(), p, p.Digest); err == nil {
					t.Fatal("followed symlink")
				}
			case "public-plan":
				path := filepath.Join(t.TempDir(), "plan.json")
				if err := os.WriteFile(path, jsonBytes(p), 0644); err != nil {
					t.Fatal(err)
				}
				if _, err := ReadRecoveryPlan(path); err == nil {
					t.Fatal("read public plan")
				}
			case "plan-replace":
				path := filepath.Join(t.TempDir(), "plan.json")
				if err := WriteRecoveryPlan(path, p); err != nil {
					t.Fatal(err)
				}
				if err := WriteRecoveryPlan(path, p); err == nil {
					t.Fatal("overwrote plan")
				}
			}
		})
	}
}
func TestRecoveryCancellationAndMetadata(t *testing.T) {
	cfg, c, n := recoveryFixture(t, false)
	r := recoverySession(t, cfg, c, n)
	if _, err := r.Preview(context.Background(), testActivity, "tester\nforged", "TEST-123"); err == nil {
		t.Fatal("log injection")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := r.Preview(ctx, testActivity, "tester", "TEST-123"); err == nil {
		t.Fatal("canceled preview")
	}
}

func TestRecoveryMultiGrowPreservesBaseline(t *testing.T) {
	cfg, c, n, e := multiFixture(t, 1, 2)
	if _, err := e.Scale(2); err != nil {
		t.Fatal(err)
	}
	// Reproduce loss after acknowledged cloud success, before local completion.
	e.mu.Lock()
	if err := e.unknown("expand-write-unknown"); err != ErrUnknown {
		t.Fatal(err)
	}
	e.mu.Unlock()
	e.Close()
	c.activities[testActivity] = tencentcloud.Activity{ID: testActivity, GroupID: testGroup, Type: "SCALE_OUT", StartedAt: time.Now().UTC().Format(time.RFC3339), EndedAt: time.Now().UTC().Format(time.RFC3339), Status: "SUCCESSFUL", RelatedInstances: []tencentcloud.RelatedInstance{{ID: "ins-multi1", Status: "SUCCESSFUL"}}}
	r, err := OpenRecovery(cfg, c, n)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	r.delay = time.Millisecond
	p := recoveryPlan(t, r)
	if _, err = r.Apply(context.Background(), p, p.Digest); err != nil {
		t.Fatal(err)
	}
	r.Close()
	e, err = Open(cfg, c, n)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	s, ready, err := e.Snapshot()
	if err != nil || !ready || s.Batch != nil || s.LastBatch.Phase != "completed" || len(s.Members) != 2 {
		t.Fatal(ready, err, s)
	}
}
func TestRecoveryAuditChainAndMissingReceipt(t *testing.T) {
	cfg, c, n := recoveryFixture(t, false)
	r := recoverySession(t, cfg, c, n)
	p := recoveryPlan(t, r)
	if _, err := r.Apply(context.Background(), p, p.Digest); err != nil {
		t.Fatal(err)
	}
	r.Close()
	e, err := Open(cfg, c, n)
	if err != nil {
		t.Fatal(err)
	}
	c.removeUnknown = true
	if _, err = e.Scale(0); err != nil {
		t.Fatal(err)
	}
	n.drained()
	if err = e.reconcileOnce(); err != ErrUnknown {
		t.Fatal(err)
	}
	e.Close()
	c.complete()
	c.activity.Type = "REMOVE_INSTANCES"
	c.activity.StartedAt = time.Now().UTC().Format(time.RFC3339)
	c.activity.EndedAt = c.activity.StartedAt
	r = recoverySession(t, cfg, c, n)
	p2 := recoveryPlan(t, r)
	if _, err = r.Apply(context.Background(), p2, p2.Digest); err != nil {
		t.Fatal(err)
	}
	r.Close()
	e, err = Open(cfg, c, n)
	if err != nil {
		t.Fatal(err)
	}
	e.Close()
	if err = os.Remove(filepath.Join(cfg.Dir, "recovery-audit", p.Digest+".result.json")); err != nil {
		t.Fatal(err)
	}
	if e, err = Open(cfg, c, n); err == nil {
		e.Close()
		t.Fatal("missing historical receipt")
	}
}
