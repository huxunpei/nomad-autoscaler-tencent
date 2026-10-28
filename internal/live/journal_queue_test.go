package live

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestJournalRetryRechecksDiskBeforeAuditReplacement(t *testing.T) {
	cfg, c, n, e := retryObservationFault(t)
	defer e.Close()
	var conflict []byte
	e.recoveryCheckpoint = func(stage string) error {
		if stage == "prepared" {
			s := e.state
			s.LastEvent = "external-edit-after-audit-prepare"
			conflict = jsonBytes(s)
			return os.WriteFile(filepath.Join(cfg.Dir, "live.json"), conflict, 0600)
		}
		return nil
	}
	if err := e.reconcileOnce(); err == nil {
		t.Fatal("overwrote journal changed after audit preparation")
	}
	actual, err := os.ReadFile(filepath.Join(cfg.Dir, "live.json"))
	if err != nil || string(actual) != string(conflict) || c.writeCount() != 0 || len(n.writes) != 0 {
		t.Fatal("recovery replaced conflicting journal")
	}
}

func TestJournalRetryQueueWriteAndCleanupFailure(t *testing.T) {
	for _, mode := range []string{"write", "cleanup"} {
		for _, restart := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/restart%v", mode, restart), func(t *testing.T) {
				cfg, c, n, e := retryObservationFault(t)
				defer func() { e.Close() }()
				path := filepath.Join(cfg.Dir, journalRetryQueueName)
				backup := filepath.Join(cfg.Dir, "queue-test-backup")
				if mode == "write" {
					if err := os.Mkdir(path, 0700); err != nil {
						t.Fatal(err)
					}
				} else {
					e.recoveryCheckpoint = func(stage string) error {
						if stage == "cleared" {
							if err := os.Rename(path, backup); err != nil {
								return err
							}
							return os.Mkdir(path, 0700)
						}
						return nil
					}
				}
				if err := e.reconcileOnce(); err == nil {
					t.Fatal("expected queue file failure")
				}
				if _, ready, err := e.Snapshot(); ready || err == nil {
					t.Fatal("published readiness before queue completed")
				}
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
				if mode == "cleanup" {
					if err := os.Rename(backup, path); err != nil {
						t.Fatal(err)
					}
				}
				e.recoveryCheckpoint = nil
				if restart {
					e.Close()
					e = openMulti(t, cfg, c, n)
				}
				step(t, e)
				s, ready, err := e.Snapshot()
				if err != nil || !ready || c.writeCount() != 0 || len(n.writes) != 0 {
					t.Fatal("queue did not recover safely", ready, err)
				}
				// Before queue publication a restart has only the durable observation;
				// no request existed. Otherwise exactly one linked recovery is retained.
				if mode == "cleanup" || !restart {
					if s.RecoveryAudit == nil {
						t.Fatal("missing committed audit")
					}
				}
				if _, err = os.Lstat(path); !os.IsNotExist(err) {
					t.Fatal("queue remains")
				}
			})
		}
	}
}

func TestJournalRetryQueuedUnsubmittedEvidenceSurvivesRestart(t *testing.T) {
	for _, mode := range []string{"accept", "sdk-barrier", "remove"} {
		t.Run(mode, func(t *testing.T) {
			cfg, c, n, e := retryNomadFixture(t)
			defer func() { e.Close() }()
			var restore func()
			fault := func() { restore = blockRetryJournal(t, cfg.Dir) }
			switch mode {
			case "accept":
				n.onSelect = fault
			case "sdk-barrier":
				n.onPre = fault
			case "remove":
				if _, err := e.Scale(1); err != nil {
					t.Fatal(err)
				}
				n.onTasks = fault
			}
			if mode == "remove" {
				if err := e.reconcileOnce(); err == nil {
					t.Fatal("missing save fault")
				}
			} else if _, err := e.Scale(1); err == nil {
				t.Fatal("missing save fault")
			}
			restore()
			e.recoveryCheckpoint = func(stage string) error {
				if stage == "before-record" {
					return code("CrashAfterRetryQueue")
				}
				return nil
			}
			if err := e.reconcileOnce(); err == nil {
				t.Fatal("missing interruption")
			}
			e.recoveryCheckpoint = nil
			e.Close()
			e = openMulti(t, cfg, c, n)
			step(t, e)
			if e.persistError != nil || e.state.ManualReview || (e.state.Pending != nil && e.state.Pending.Unknown) {
				t.Fatal("lost persisted unsubmitted proof")
			}
			if mode == "remove" {
				if c.writeCount() != 1 || len(n.writes) != 1 {
					t.Fatal("replayed completed drain")
				}
			} else if c.writeCount() != 0 || len(n.writes) != 0 || e.state.Pending != nil {
				t.Fatal("unsubmitted accept crossed write boundary")
			}
		})
	}
}

func TestJournalRetryUnsubmittedRemoveHonorsChangedBounds(t *testing.T) {
	cfg, c, n, e := retryNomadFixture(t)
	defer e.Close()
	if _, err := e.Scale(1); err != nil {
		t.Fatal(err)
	}
	var restore func()
	n.onTasks = func() { restore = blockRetryJournal(t, cfg.Dir) }
	if err := e.reconcileOnce(); err == nil {
		t.Fatal("missing save fault")
	}
	restore()
	c.snap.Group.Min = 2
	step(t, e)
	if c.writeCount() != 0 || e.state.Pending != nil || e.state.LastBatch == nil || e.state.LastBatch.Phase != "failed" || e.state.LastCompletedAt != 0 {
		t.Fatal("retried stale destructive bounds")
	}
	id := e.state.LastOperation.InstanceID
	if n.nodes[id].Eligibility != "eligible" {
		t.Fatal("did not restore original drain")
	}
}
