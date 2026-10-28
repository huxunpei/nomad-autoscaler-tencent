package live

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestLegacyCustomNonpositiveAdoptionMaintainsTwoDrainContracts(t *testing.T) {
	entries, err := os.ReadDir("testdata/historical-custom-d24948f")
	if err != nil || len(entries) != 13 {
		t.Fatal("missing historical sources")
	}
	for _, d := range []time.Duration{0, -time.Second} {
		for _, entry := range entries {
			t.Run(d.String()+"/"+entry.Name(), func(t *testing.T) {
				cfg, c, h, old, e := legacyHTTPOpen(t, entry.Name(), 5*time.Second, d)
				defer e.Close()
				s, _, err := e.Snapshot()
				if (err != nil && !(err == ErrUnknown && s.Pending != nil && s.Pending.Unknown)) || s.Version != 3 || s.DrainDeadlineNanoseconds != int64(d) || c.writeCount() != 0 || len(h.writes) != 0 || s.LastCompletedAt != 0 || e.checkRecoveryAudit() != nil {
					t.Fatal("adoption changed providers or completion")
				}
				if s.Batch.Kind == "shrink" {
					var b map[string]any
					if json.Unmarshal(jsonBytes(s.Batch), &b) != nil || b["legacy_drain_origin"] == nil || s.Batch.DrainWait == nil {
						t.Fatal("missing explicit split between historical work and new wait")
					}
					if s.Pending != nil && (!e.operationDrainUntil(s.Pending).Equal(s.Pending.StartedAt.Add(time.Duration(old.DrainDeadlineNanoseconds)+time.Minute)) || s.Pending.DrainWait != nil || s.Pending.LegacyOrigin == nil || s.Pending.SDKLifecycle) {
						t.Fatal("historical operation deadline changed")
					}
				} else if s.Batch.DrainWait != nil {
					t.Fatal("grow gained a drain window")
				}
				if old.Batch != nil && !reflect.DeepEqual(s.Batch.Results, old.Batch.Results) {
					t.Fatal("historical successful prefix rewritten")
				}
				backup := filepath.Join(cfg.Dir, "live-v"+string(rune('0'+old.Version))+"-before-group-contract.backup.json")
				raw, err := os.ReadFile(backup)
				if err != nil || sha(raw) != s.LegacyContract.BackupSHA256 {
					t.Fatal("source backup missing")
				}
			})
		}
	}
}

func TestLegacyCustomNonpositiveFutureSDKChildrenAndCompletedReader(t *testing.T) {
	for _, d := range []time.Duration{0, -time.Second} {
		for _, name := range []string{"v1-grow", "v2-grow", "v1-draining", "v1-removing", "v1-delete-unknown", "v2-removing", "v2-delete-unknown", "v2-between-children"} {
			t.Run(d.String()+"/"+name, func(t *testing.T) {
				cfg, c, h, old, e := legacyHTTPOpen(t, name, 5*time.Second, d)
				defer e.Close()
				prefix := []Operation{}
				if old.Batch != nil {
					prefix = copyJSON(old.Batch.Results)
				}
				// An acknowledged custom pre must still wait for its original tasks,
				// including system tasks, despite the adopted ignore-system SDK policy.
				if name == "v1-draining" {
					h.mu.Lock()
					n := h.nodes[testInstance]
					n.Drain = false
					n.HasDrainStrategy = false
					n.LastDrain.Status = "complete"
					h.nodes[testInstance] = n
					h.blockedJob = "system"
					h.mu.Unlock()
					e.EnableReconciliation()
					_ = e.reconcileOnce()
					if c.writeCount() != 0 || len(h.writes) != 0 {
						t.Fatal("new policy relaxed original task contract")
					}
					h.mu.Lock()
					h.blockedJob = ""
					h.mu.Unlock()
				}
				runLegacyToTerminal(t, e, c)
				s, _, _ := e.Snapshot()
				if s.LastBatch == nil || s.LastBatch.Phase != "completed" || s.LastCompletedAt == 0 || s.LegacyContract != nil || e.checkRecoveryAudit() != nil {
					t.Fatal("adopted batch did not complete", s.LastEvent)
				}
				if len(prefix) > 0 && !reflect.DeepEqual(s.LastBatch.Results[:len(prefix)], prefix) {
					t.Fatal("successful old prefix rewritten")
				}
				if old.Pending != nil && old.Pending.Phase == "removing" && old.Version == 1 && c.writeCount() != 0 {
					t.Fatal("known accepted delete replayed")
				}
				adapter := e.nomad
				e.Close()
				cfg.StabilizationInterval = 2 * time.Second
				reader, err := OpenRecovery(cfg, c, adapter)
				if err != nil {
					t.Fatal("completed state not readable", err)
				}
				reader.Close()
				next, err := Open(cfg, c, adapter)
				if err != nil {
					t.Fatal("completed state restart", err)
				}
				defer next.Close()
				if next.state.LastCompletedAt != s.LastCompletedAt || next.checkRecoveryAudit() != nil {
					t.Fatal("completed state changed on read")
				}
			})
		}
	}
}

func TestLegacyCustomNonpositiveExpiredOriginalDrainRestores(t *testing.T) {
	for _, d := range []time.Duration{0, -time.Second} {
		t.Run(d.String(), func(t *testing.T) {
			_, c, h, _, e := legacyHTTPOpen(t, "v1-draining", 3*time.Minute, d)
			defer e.Close()
			runLegacyToTerminal(t, e, c)
			if c.writeCount() != 0 || !reflect.DeepEqual(h.writes, []string{"restore:" + testInstance}) || e.state.LastBatch.Phase != "failed" || e.state.LastCompletedAt != 0 || e.state.LastOperation.AbortReason != "drain-completion-timeout" || e.checkRecoveryAudit() != nil {
				t.Fatal("adoption extended or forced original drain")
			}
		})
	}
}

func TestLegacyCustomNonpositiveFutureWaitSurvivesRestartAndAuditTampering(t *testing.T) {
	for _, d := range []time.Duration{0, -time.Second} {
		t.Run(d.String(), func(t *testing.T) {
			cfg, c, _, old, e := legacyHTTPOpen(t, "v2-between-children", 5*time.Second, d)
			parent := copyJSON(e.state.Batch.DrainWait)
			e.EnableReconciliation()
			if err := e.reconcileOnce(); err != nil {
				t.Fatal(err)
			}
			if e.state.Pending == nil || e.state.Pending.LegacyOrigin != nil || !e.state.Pending.SDKLifecycle || e.state.Pending.DrainWait == nil {
				t.Fatal("future child did not use the adopted SDK wait")
			}
			child := copyJSON(e.state.Pending.DrainWait)
			if child.Attempts != parent.Attempts || child.Interval != parent.Interval || child.Until.After(parent.Until) {
				t.Fatal("future child escaped adopted batch bound")
			}
			adapter := e.nomad
			e.Close()
			cfg.StabilizationInterval = 2 * time.Second
			e, err := Open(cfg, c, adapter)
			if err != nil {
				t.Fatal("restart with another polling interval", err)
			}
			if !reflect.DeepEqual(e.state.Pending.DrainWait, child) || !reflect.DeepEqual(e.state.Batch.DrainWait, parent) {
				e.Close()
				t.Fatal("restart regenerated wait windows")
			}
			runLegacyToTerminal(t, e, c)
			final := copyJSON(e.state)
			if !reflect.DeepEqual(final.LastBatch.Results[:len(old.Batch.Results)], old.Batch.Results) {
				t.Fatal("old prefix changed")
			}
			e.Close()
			path := filepath.Join(cfg.Dir, "live.json")
			for _, mode := range []string{"drop-origin", "origin-source", "origin-deadline", "shorten-window", "prefix-hash", "prefix-count"} {
				t.Run(mode, func(t *testing.T) {
					s := copyJSON(final)
					switch mode {
					case "drop-origin":
						s.LastBatch.LegacyDrainOrigin = nil
					case "origin-source":
						s.LastBatch.LegacyDrainOrigin.JournalSHA256 = sha([]byte("other"))
					case "origin-deadline":
						s.LastBatch.LegacyDrainOrigin.DrainDeadline += time.Second
					case "shorten-window":
						s.LastBatch.DrainWait.Until = s.LastBatch.DrainWait.Until.Add(-time.Nanosecond)
					case "prefix-hash":
						s.LastBatch.LegacyDrainOrigin.PrefixSHA256 = sha([]byte("other"))
					case "prefix-count":
						s.LastBatch.LegacyDrainOrigin.PrefixCount = 0
						s.LastBatch.LegacyDrainOrigin.PrefixSHA256 = legacyPrefixDigest(s.LastBatch, 0)
					}
					if err := os.WriteFile(path, jsonBytes(s), 0600); err != nil {
						t.Fatal(err)
					}
					opened, err := Open(cfg, c, adapter)
					if err == nil {
						opened.Close()
						t.Fatal("completed migration proof tampering accepted")
					}
				})
			}
			if err := os.WriteFile(path, jsonBytes(final), 0600); err != nil {
				t.Fatal(err)
			}
			reader, err := OpenRecovery(cfg, c, adapter)
			if err != nil {
				t.Fatal("untampered completed state unreadable", err)
			}
			reader.Close()
		})
	}
}

func TestLegacyCustomNonpositiveFailedDeleteKeepsFailureAndNextAction(t *testing.T) {
	for _, d := range []time.Duration{0, -time.Second} {
		t.Run(d.String(), func(t *testing.T) {
			_, c, h, old, e := legacyHTTPOpen(t, "v1-failed-restored", 5*time.Second, d)
			defer e.Close()
			e.EnableReconciliation()
			_ = e.reconcileOnce()
			_ = e.reconcileOnce()
			if e.state.Pending == nil || e.state.Pending.DrainOutcome != "restored" || len(h.writes) != 0 || c.writeCount() != 0 || e.state.LastCompletedAt != 0 {
				t.Fatal("restored historical failure changed")
			}
			laterDeletion(t, e, c)
			runLegacyToTerminal(t, e, c)
			if e.state.LastBatch.Phase != "failed" || e.state.LastCompletedAt != 0 || e.state.LastOperation.ActivityID != old.LastOperation.ActivityID || e.checkRecoveryAudit() != nil || c.writeCount() != 0 {
				t.Fatal("failed history fabricated success")
			}
			if ok, err := e.Scale(1); !ok || err != nil {
				t.Fatal("next normal action blocked", err)
			}
		})
	}
}

func TestLegacyCustomNonpositiveInvalidWaitBindingRejected(t *testing.T) {
	old, _, raw := historicalCustomInput(t, "v1-draining", 5*time.Second)
	cfg := Config{GroupID: old.GroupID, Region: old.Region, GroupContract: true, DrainDeadline: 0, StabilizationInterval: 17 * time.Millisecond}
	binding := LegacyCustomBinding{PolicySHA256: sha([]byte("policy")), InstanceMetaKey: "cloud_instance_id"}
	migrated, err := legacyCustomAfter(cfg, old, sha(raw), binding)
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"attempts", "interval", "extension", "shortening"} {
		t.Run(mode, func(t *testing.T) {
			b := copyJSON(*migrated.LegacyContract.Custom)
			switch mode {
			case "attempts":
				b.DrainWait.Attempts++
			case "interval":
				b.DrainWait.Interval = 0
			case "extension":
				b.DrainWait.Until = b.DrainWait.Until.Add(time.Nanosecond)
			case "shortening":
				b.DrainWait.Until = b.DrainWait.Until.Add(-time.Nanosecond)
			}
			if _, err := legacyCustomAfter(cfg, old, sha(raw), b); err == nil {
				t.Fatal("inconsistent adopted wait accepted")
			}
		})
	}
}

func TestLegacyCustomNonpositiveMigrationAbruptExitRetainsBothBounds(t *testing.T) {
	for _, d := range []time.Duration{0, -time.Second} {
		for _, at := range []string{"before-record", "record", "prepared", "journal", "receipt", "cleared"} {
			t.Run(d.String()+"/"+at, func(t *testing.T) {
				old, model, raw := historicalCustomInput(t, "v1-draining", 5*time.Second)
				cfg := Config{Dir: filepath.Join(t.TempDir(), "journal"), GroupID: testGroup, Region: testRegion, GroupContract: true, DrainDeadline: d, ReconcileInterval: time.Hour, DisableReconciliation: true}
				if err := os.Mkdir(cfg.Dir, 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(cfg.Dir, "live.json"), raw, 0600); err != nil {
					t.Fatal(err)
				}
				input := filepath.Join(t.TempDir(), "input.json")
				if err := os.WriteFile(input, jsonBytes(map[string]any{"Directory": cfg.Dir, "At": at, "Deadline": d}), 0600); err != nil {
					t.Fatal(err)
				}
				cmd := exec.Command(os.Args[0], "-test.run=^TestLegacyCustomMigrationCrashProcessHelper$")
				cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + os.Getenv("HOME"), "TMPDIR=" + os.Getenv("TMPDIR"), "NOMAD_CUSTOM_MIGRATION_CRASH_FIXTURE=" + input}
				out, err := cmd.CombinedOutput()
				if err == nil || cmd.ProcessState == nil || cmd.ProcessState.ExitCode() != 59 {
					t.Fatalf("expected exit59: %v %s", err, out)
				}
				c := &multiCloud{fakeCloud: &fakeCloud{snap: model.Snapshot}, activities: model.Activities}
				n := legacyCustomProbe{&sdkProbe{fakeNomad: &fakeNomad{node: model.Nodes[0]}, directory: cfg.Dir}}
				e, err := Open(cfg, c, n)
				if err != nil {
					t.Fatal("interrupted nonpositive adoption", err)
				}
				op, wait := copyJSON(e.state.Pending), copyJSON(e.state.Batch.DrainWait)
				if op.LegacyOrigin == nil || op.SDKLifecycle || op.DrainWait != nil || !e.operationDrainUntil(op).Equal(old.Pending.StartedAt.Add(time.Duration(old.DrainDeadlineNanoseconds)+time.Minute)) || wait == nil || wait.Interval != 5*time.Second || c.writeCount() != 0 || n.writeCount() != 0 || e.checkRecoveryAudit() != nil {
					e.Close()
					t.Fatal("crash changed old origin or new budget")
				}
				e.Close()
				cfg.StabilizationInterval = 37 * time.Millisecond
				reader, err := OpenRecovery(cfg, c, n)
				if err != nil {
					t.Fatal("read-only recovery after crash", err)
				}
				if !reflect.DeepEqual(reader.e.state.Batch.DrainWait, wait) {
					t.Fatal("recovery regenerated wait")
				}
				reader.Close()
			})
		}
	}
}
