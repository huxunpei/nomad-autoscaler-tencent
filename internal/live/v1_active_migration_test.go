package live

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func sdkV1ActiveFixture(t *testing.T, mode string) (Config, *fakeCloud, *sdkProbe, State, []byte) {
	t.Helper()
	cfg := config(t)
	c := newCloud()
	if mode != "grow" {
		c.addMember()
	}
	n := &sdkProbe{fakeNomad: newNomad(), directory: cfg.Dir}
	e, err := Open(cfg, c, n)
	if err != nil {
		t.Fatal(err)
	}
	target := int64(0)
	if mode == "grow" {
		target = 1
	}
	if _, err = e.Scale(target); err != nil {
		e.Close()
		t.Fatal(err)
	}
	if mode == "removing" || mode == "post" || mode == "unknown" {
		n.drained()
		c.removeUnknown = mode == "unknown"
		_ = e.reconcileOnce()
		if mode == "post" {
			n.postFail = true
			c.complete()
			for i := 0; i < 6 && e.state.Pending != nil && e.state.Pending.Phase != "post-scale"; i++ {
				_ = e.reconcileOnce()
			}
		}
	}
	s, _, err := e.Snapshot()
	e.Close()
	if (err != nil && !(mode == "unknown" && err == ErrUnknown)) || s.Version != 1 || s.Batch != nil || s.Pending == nil || s.NomadPolicySHA256 == "" {
		t.Fatal("not an actual standalone SDK v1 source", err)
	}
	expected := map[string]string{"grow": "growing", "draining": "draining", "removing": "removing", "post": "post-scale", "unknown": "unknown"}[mode]
	if s.Pending.Phase != expected {
		t.Fatal("wrong v1 source phase", s.Pending.Phase, expected)
	}
	raw, err := os.ReadFile(filepath.Join(cfg.Dir, "live.json"))
	if err != nil {
		t.Fatal(err)
	}
	return cfg, c, n, s, raw
}

func finishSDKV1Operation(t *testing.T, e *Engine, c *fakeCloud, n *sdkProbe) {
	t.Helper()
	n.postFail = false
	if e.state.Pending.Kind == "shrink" {
		n.drained()
	}
	for i := 0; i < 15 && e.state.Pending != nil; i++ {
		if e.state.Pending.Kind == "shrink" && e.state.Pending.Phase != "draining" {
			c.complete()
			if e.state.Pending.Unknown {
				c.mu.Lock()
				c.activity.Type = "REMOVE_INSTANCES"
				c.activity.StartedAt = e.state.Pending.StartedAt.UTC().Format(time.RFC3339)
				c.activity.EndedAt = time.Now().UTC().Format(time.RFC3339)
				c.mu.Unlock()
			}
		}
		if err := e.reconcileOnce(); err != nil {
			t.Fatal("v1 operation continuation", err)
		}
	}
	if e.state.Pending != nil || e.state.Batch != nil || e.state.LastBatch == nil || e.state.LastBatch.Phase != "completed" || e.state.LastCompletedAt <= 0 || e.state.LegacyContract != nil {
		t.Fatal("v1 original operation did not finish")
	}
}

func TestSDKV1ActiveMigrationPreservesSingleOperationAndCompletes(t *testing.T) {
	for _, mode := range []string{"grow", "draining", "removing", "post", "unknown"} {
		t.Run(mode, func(t *testing.T) {
			cfg, c, n, old, raw := sdkV1ActiveFixture(t, mode)
			writes, drains, pre := c.writeCount(), n.writeCount(), n.preCalls
			cfg.GroupContract = true
			e, err := Open(cfg, c, n)
			if err != nil {
				t.Fatal("standalone SDK v1 cannot migrate", err)
			}
			s, _, _ := e.Snapshot()
			if s.Version != 3 || s.LegacyContract == nil || s.LegacyContract.SourceVersion != 1 || s.LegacyContract.MaxCapacity != 1 || s.Batch == nil || s.Batch.ID == old.Pending.ID || s.Batch.StartedAt != old.Pending.StartedAt || s.Batch.Kind != old.Pending.Kind || s.Batch.Target != old.Pending.Target || !reflect.DeepEqual(s.Pending, old.Pending) || s.LastCompletedAt != 0 || s.Sequence != old.Sequence || c.writeCount() != writes || n.writeCount() != drains || n.preCalls != pre {
				e.Close()
				t.Fatal("v1 migration changed original operation or wrote providers")
			}
			batchID := s.Batch.ID
			if s.RecoveryAudit == nil || e.checkRecoveryAudit() != nil {
				e.Close()
				t.Fatal("v1 migration audit missing")
			}
			data, err := os.ReadFile(filepath.Join(cfg.Dir, "live-v1-before-group-contract.backup.json"))
			if err != nil || string(data) != string(raw) {
				e.Close()
				t.Fatal("v1 original backup missing")
			}
			e.Close()
			e, err = Open(cfg, c, n)
			if err != nil {
				t.Fatal("v1 migration reopen", err)
			}
			defer e.Close()
			if e.state.Batch.ID != batchID || !reflect.DeepEqual(e.state.Pending, old.Pending) || c.writeCount() != writes || n.preCalls != pre {
				t.Fatal("v1 restart regenerated bookkeeping or replayed operation")
			}
			finishSDKV1Operation(t, e, c, n)
			if c.writeCount() != 1 || n.preCalls != pre || e.state.LastBatch.ID != batchID || !e.usesGroupCapacityContract() || e.checkRecoveryAudit() != nil {
				t.Fatal("v1 completion replayed work or kept original contract")
			}
			if old.Pending.Kind == "shrink" && n.postCalls < 1 {
				t.Fatal("v1 lost SDK post obligation")
			}
			t.Logf("source=%s op=%s migrated without new provider calls; original request/activity retained; final cloud writes=1", mode, old.Pending.ID)
		})
	}
}

func TestSDKV1ActiveMigrationRejectsPolicyChangeAndUnsafeBackup(t *testing.T) {
	for _, mode := range []string{"policy", "backup"} {
		t.Run(mode, func(t *testing.T) {
			cfg, c, n, _, raw := sdkV1ActiveFixture(t, "draining")
			cfg.GroupContract = true
			if mode == "policy" {
				n.digest = strings.Repeat("b", 64)
			} else {
				if err := os.WriteFile(filepath.Join(cfg.Dir, "live-v1-before-group-contract.backup.json"), []byte("different"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			writes, nomadWrites := c.writeCount(), n.writeCount()
			e, err := Open(cfg, c, n)
			if err == nil {
				e.Close()
				t.Fatal("conflicting v1 evidence accepted")
			}
			if mode == "policy" && err.Error() != "SDKPolicyChangeRequiresMigration" {
				t.Fatal("wrong policy refusal", err)
			}
			if mode == "backup" && err.Error() != "LegacyBackupConflict" {
				t.Fatal("wrong backup refusal", err)
			}
			after, readErr := os.ReadFile(filepath.Join(cfg.Dir, "live.json"))
			if readErr != nil || string(after) != string(raw) || c.writeCount() != writes || n.writeCount() != nomadWrites {
				t.Fatal("rejected v1 migration changed original state/providers")
			}
		})
	}
}

func TestSDKV1ActiveMigrationKeepsOldCapacityAndProtectionUntilAbort(t *testing.T) {
	for _, mode := range []string{"max", "retry", "as-protection"} {
		t.Run(mode, func(t *testing.T) {
			cfg, c, n, _, _ := sdkV1ActiveFixture(t, "draining")
			cfg.GroupContract = true
			e, err := Open(cfg, c, n)
			if err != nil {
				t.Fatal(err)
			}
			defer e.Close()
			switch mode {
			case "max":
				c.snap.Group.Max = 2
			case "retry":
				c.snap.Group.RetryPolicy = "IMMEDIATE_RETRY"
			case "as-protection":
				c.snap.Members[0].Protected = true
				n.drained() // reach the protected pre-delete gate after drain completes
			}
			for i := 0; i < 4 && e.state.Pending != nil; i++ {
				if err := e.reconcileOnce(); err != nil {
					t.Fatal("v1 original contract abort", err)
				}
			}
			if e.state.Pending != nil || e.state.Batch != nil || e.state.LastBatch == nil || e.state.LastBatch.Phase != "failed" || e.state.LastCompletedAt != 0 || e.state.LegacyContract != nil || c.writeCount() != 0 || n.writeCount() != 2 || n.node.Eligibility != "eligible" || e.checkRecoveryAudit() != nil {
				t.Fatal("v1 original bounds/protection relaxed or failed restoration")
			}
		})
	}
}
