package live

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

// Exercise real SDK lifecycle state, including original nonpositive wait bounds.
func TestIdlePolicyHistoricalResultsAndCooldown(t *testing.T) {
	for _, first := range []time.Duration{time.Minute, 0, -time.Second} {
		t.Run(first.String(), func(t *testing.T) {
			cfg := config(t)
			cfg.GroupContract = true
			cfg.DrainDeadline = first
			c := newCloud()
			c.addMember()
			n := &sdkProbe{fakeNomad: newNomad()}
			e := openSDKProbe(t, cfg, c, n)
			if _, err := e.Scale(0); err != nil {
				t.Fatal(err)
			}
			n.drained()
			step(t, e)
			c.complete()
			step(t, e)
			step(t, e)
			before := cloneRetryState(e.state)
			if before.Pending != nil || before.LastBatch == nil || before.LastCompletedAt == 0 {
				t.Fatal("fixture not settled")
			}
			e.Close()
			for i, next := range []time.Duration{10 * time.Minute, 0, -time.Second} {
				digest := strings.Repeat(string(rune('b'+i)), 64)
				unlock, err := LockIdlePolicyState(cfg.Dir)
				if err != nil {
					t.Fatal(err)
				}
				change, err := PrepareIdlePolicyChange(cfg, digest, "", "test", "unit/history", next)
				unlock()
				if err != nil {
					t.Fatal(err)
				}
				var after State
				_ = json.Unmarshal(change.After, &after)
				if !reflect.DeepEqual(before.LastOperation, after.LastOperation) || !reflect.DeepEqual(before.LastBatch, after.LastBatch) || before.LastCompletedAt != after.LastCompletedAt || before.Sequence != after.Sequence || !reflect.DeepEqual(before.RecoveryAudit, after.RecoveryAudit) {
					t.Fatal("historical evidence rewritten")
				}
				dir := filepath.Join(cfg.Dir, "policy-audit")
				_ = os.MkdirAll(dir, 0700)
				_ = os.WriteFile(filepath.Join(dir, change.AuditID+".json"), change.Audit, 0600)
				_ = os.WriteFile(filepath.Join(cfg.Dir, "live.json"), change.After, 0600)
				cfg.DrainDeadline = next
				n.digest = digest
				e = openSDKProbe(t, cfg, c, n)
				e.Close()
			}
			// A replacement fake VM has a fresh Nomad registration.
			n.fakeNomad = newNomad()
			// A future action uses the new policy, while old batch/cooldown history survives.
			e = openSDKProbe(t, cfg, c, n)
			if _, err := e.Scale(1); err != nil {
				t.Fatal(err)
			}
			step(t, e)
			e.Close()
			e = openSDKProbe(t, cfg, c, n)
			if _, err := e.Scale(0); err != nil {
				t.Fatal(err)
			}
			n.drained()
			step(t, e)
			c.complete()
			step(t, e)
			step(t, e)
			e.Close()
			e = openSDKProbe(t, cfg, c, n)
			if e.state.Pending != nil || e.state.LastBatch.DrainWait == nil {
				t.Fatal("new action lost its deadline mode")
			}
			e.Close()
			// Losing or corrupting any audit record blocks restart without rewriting state.
			raw, _ := os.ReadFile(filepath.Join(cfg.Dir, "live.json"))
			path := filepath.Join(cfg.Dir, "policy-audit", e.state.PolicyAuditSHA256+".json")
			_ = os.WriteFile(path, []byte("{}"), 0600)
			if opened, err := Open(cfg, c, n); err == nil {
				opened.Close()
				t.Fatal("broken policy audit accepted")
			}
			got, _ := os.ReadFile(filepath.Join(cfg.Dir, "live.json"))
			if !bytes.Equal(raw, got) {
				t.Fatal("invalid history rewritten")
			}
		})
	}
}

func TestIdlePolicyPendingStateAndMarkerBlocked(t *testing.T) {
	cfg := config(t)
	cfg.GroupContract = true
	c := newCloud()
	c.addMember()
	n := &sdkProbe{fakeNomad: newNomad()}
	e := openSDKProbe(t, cfg, c, n)
	if _, err := e.Scale(0); err != nil {
		t.Fatal(err)
	}
	e.Close()
	unlock, err := LockIdlePolicyState(cfg.Dir)
	if err != nil {
		t.Fatal(err)
	}
	_, err = PrepareIdlePolicyChange(cfg, strings.Repeat("b", 64), "", "test", "unit/pending", time.Minute)
	unlock()
	if err == nil {
		t.Fatal("active operation allowed")
	}
	_ = os.WriteFile(filepath.Join(cfg.Dir, "policy-update-pending.json"), []byte("{}"), 0600)
	if x, err := Open(cfg, c, n); err == nil {
		x.Close()
		t.Fatal("engine ignored pending policy update")
	}
	if x, err := OpenRecovery(cfg, c, n); err == nil {
		x.Close()
		t.Fatal("recovery ignored pending policy update")
	}
}
