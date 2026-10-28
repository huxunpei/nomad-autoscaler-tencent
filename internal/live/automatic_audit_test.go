package live

import (
	"context"
	"encoding/json"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/tencentcloud"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func automaticPlan(t *testing.T, r *Recovery) RecoveryPlan {
	t.Helper()
	r.automatic = true
	p, err := r.Preview(context.Background(), r.e.state.Pending.ActivityID, "automatic-reconciler", "recorded-activity/"+r.e.state.Pending.ID)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestAutomaticAuditInterruptionRestartsWithoutManualRepair(t *testing.T) {
	for _, at := range []string{"record", "prepared", "journal", "receipt", "cleared"} {
		t.Run(at, func(t *testing.T) {
			cfg, c, n := recoveryFixture(t, true)
			writes, nwrites := c.writeCount(), n.writeCount()
			r := recoverySession(t, cfg, c, n)
			p := automaticPlan(t, r)
			r.checkpoint = func(stage string) error {
				if stage == at {
					return code("SimulatedCrash")
				}
				return nil
			}
			if _, err := r.Apply(context.Background(), p, p.Digest); err == nil {
				t.Fatal("expected interruption")
			}
			r.Close()
			e, err := Open(cfg, c, n)
			if err != nil {
				t.Fatalf("automatic transaction still requires manual repair after %s: %v", at, err)
			}
			defer e.Close()
			if err = e.reconcileOnce(); err != nil {
				t.Fatal(err)
			}
			if err = e.reconcileOnce(); err != nil {
				t.Fatal("fresh observation after recovery", err)
			}
			state, ready, err := e.Snapshot()
			if err != nil || !ready || state.Pending != nil || state.ManualReview || state.RecoveryAudit == nil {
				t.Fatalf("did not recover: ready=%v error=%v state=%+v", ready, err, state)
			}
			if c.writeCount() != writes || n.writeCount() != nwrites {
				t.Fatal("bookkeeping repair replayed an external mutation")
			}
			if _, err = os.Stat(filepath.Join(cfg.Dir, recoveryMarkerName)); !os.IsNotExist(err) {
				t.Fatal("pending marker remains", err)
			}
			if err = e.checkRecoveryAudit(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestAutomaticAuditInterruptionRetriesInSameProcess(t *testing.T) {
	for _, at := range []string{"before-record", "record", "prepared", "journal", "receipt", "cleared"} {
		t.Run(at, func(t *testing.T) {
			cfg, c, n := recoveryFixture(t, true)
			writes, nwrites := c.writeCount(), n.writeCount()
			e, err := Open(cfg, c, n)
			if err != nil {
				t.Fatal(err)
			}
			defer e.Close()
			e.recoveryCheckpoint = func(stage string) error {
				if stage == at {
					return code("SimulatedInterruption")
				}
				return nil
			}
			if err = e.reconcileOnce(); err == nil {
				t.Fatal("interruption not injected")
			}
			if _, ready, err := e.Snapshot(); ready || err == nil {
				t.Fatal("published readiness before transaction completion")
			}
			e.recoveryCheckpoint = nil
			if err = e.reconcileOnce(); err != nil {
				t.Fatal("automatic retry failed", err)
			}
			if err = e.reconcileOnce(); err != nil {
				t.Fatal("fresh observation after recovery", err)
			}
			state, ready, err := e.Snapshot()
			if err != nil || !ready || state.Pending != nil || state.RecoveryAudit == nil || e.automaticAudit != nil {
				t.Fatal("did not automatically resume", ready, err)
			}
			if c.writeCount() != writes || n.writeCount() != nwrites {
				t.Fatal("replayed external mutation")
			}
			if err = e.checkRecoveryAudit(); err != nil {
				t.Fatal(err)
			}
			if changed, err := e.Scale(1); !changed || err != nil {
				t.Fatal("next engine action blocked", changed, err)
			}
		})
	}
}

func TestAutomaticAuditRealFilesystemWriteFailuresRecover(t *testing.T) {
	for _, at := range []string{"record", "marker", "journal", "receipt"} {
		t.Run(at, func(t *testing.T) {
			cfg, c, n := recoveryFixture(t, true)
			e, err := Open(cfg, c, n)
			if err != nil {
				t.Fatal(err)
			}
			defer e.Close()
			writes, nwrites := c.writeCount(), n.writeCount()
			var blocked string
			backup := filepath.Join(cfg.Dir, "test-journal-backup")
			e.recoveryCheckpoint = func(stage string) error {
				switch {
				case at == "record" && stage == "before-record":
					blocked = auditPath(cfg.Dir, e.automaticAudit.Audit)
				case at == "marker" && stage == "record":
					blocked = filepath.Join(cfg.Dir, recoveryMarkerName)
				case at == "journal" && stage == "prepared":
					blocked = filepath.Join(cfg.Dir, "live.json")
					if err := os.Rename(blocked, backup); err != nil {
						return err
					}
				case at == "receipt" && stage == "journal":
					blocked = resultPath(cfg.Dir, e.automaticAudit.Audit)
				default:
					return nil
				}
				return os.Mkdir(blocked, 0700)
			}
			if err = e.reconcileOnce(); err == nil || blocked == "" {
				t.Fatal("write failure not reached", err)
			}
			if e.automaticAudit == nil || e.persistError == nil {
				t.Fatal("failed transaction lost obligation")
			}
			if _, ready, err := e.Snapshot(); ready || err == nil {
				t.Fatal("failed transaction appeared ready")
			}
			if err = os.Remove(blocked); err != nil {
				t.Fatal(err)
			}
			if at == "journal" {
				if err = os.Rename(backup, filepath.Join(cfg.Dir, "live.json")); err != nil {
					t.Fatal(err)
				}
			}
			e.recoveryCheckpoint = nil
			if err = e.reconcileOnce(); err != nil {
				t.Fatal("restored storage did not resume", err)
			}
			if err = e.reconcileOnce(); err != nil {
				t.Fatal("fresh observation after recovery", err)
			}
			state, ready, err := e.Snapshot()
			if err != nil || !ready || state.Pending != nil || state.RecoveryAudit == nil {
				t.Fatal("not recovered", ready, err)
			}
			if c.writeCount() != writes || n.writeCount() != nwrites {
				t.Fatal("duplicated external operation")
			}
		})
	}
}

func TestAutomaticAuditDoesNotAdoptManualTransactionByName(t *testing.T) {
	cfg, c, n := recoveryFixture(t, true)
	r := recoverySession(t, cfg, c, n)
	p, err := r.Preview(context.Background(), testActivity, "automatic-reconciler", "recorded-activity/"+r.e.state.Pending.ID)
	if err != nil {
		t.Fatal(err)
	}
	// Same display identity, explicitly a manual transaction.
	r.checkpoint = func(stage string) error {
		if stage == "journal" {
			return code("Interruption")
		}
		return nil
	}
	if _, err := r.Apply(context.Background(), p, p.Digest); err == nil {
		t.Fatal("no interruption")
	}
	r.Close()
	before, err := os.ReadFile(filepath.Join(cfg.Dir, "live.json"))
	if err != nil {
		t.Fatal(err)
	}
	if e, err := Open(cfg, c, n); err == nil {
		e.Close()
		t.Fatal("manual transaction adopted automatically")
	}
	after, _ := os.ReadFile(filepath.Join(cfg.Dir, "live.json"))
	if string(before) != string(after) {
		t.Fatal("blocked startup changed journal")
	}
	if _, err = RepairRecovery(cfg, p.Digest); err != nil {
		t.Fatal("manual repair regressed", err)
	}
}

func TestAutomaticAuditRejectsConflictingRecoveryEvidence(t *testing.T) {
	for _, mode := range []string{"journal", "record", "receipt", "marker", "scope", "origin"} {
		t.Run(mode, func(t *testing.T) {
			cfg, c, n := recoveryFixture(t, true)
			r := recoverySession(t, cfg, c, n)
			p := automaticPlan(t, r)
			r.checkpoint = func(stage string) error {
				if stage == "journal" {
					return code("Interruption")
				}
				return nil
			}
			if _, err := r.Apply(context.Background(), p, p.Digest); err == nil {
				t.Fatal("no interruption")
			}
			marker := *r.e.automaticAudit
			r.Close()
			switch mode {
			case "journal":
				b, _ := os.ReadFile(filepath.Join(cfg.Dir, "live.json"))
				b = append(b, ' ')
				if err := os.WriteFile(filepath.Join(cfg.Dir, "live.json"), b, 0600); err != nil {
					t.Fatal(err)
				}
			case "record":
				if err := os.WriteFile(auditPath(cfg.Dir, marker.Audit), []byte("{}\n"), 0600); err != nil {
					t.Fatal(err)
				}
			case "receipt":
				if err := os.WriteFile(resultPath(cfg.Dir, marker.Audit), []byte("{}\n"), 0600); err != nil {
					t.Fatal(err)
				}
			case "marker":
				marker.BeforeSHA256 = "invalid"
				if err := os.WriteFile(filepath.Join(cfg.Dir, recoveryMarkerName), jsonBytes(marker), 0600); err != nil {
					t.Fatal(err)
				}
			case "scope":
				cfg.ScaleInProtection = !cfg.ScaleInProtection
			case "origin":
				record, err := readRecord(cfg.Dir, marker.Audit)
				if err != nil {
					t.Fatal(err)
				}
				record.Automatic = false // Version 2 without the automatic origin is invalid.
				data := jsonBytes(record)
				marker.Audit.SHA256 = sha(data)
				if err = os.WriteFile(auditPath(cfg.Dir, marker.Audit), data, 0600); err != nil {
					t.Fatal(err)
				}
				if err = os.WriteFile(filepath.Join(cfg.Dir, recoveryMarkerName), jsonBytes(marker), 0600); err != nil {
					t.Fatal(err)
				}
			}
			before, _ := os.ReadFile(filepath.Join(cfg.Dir, "live.json"))
			writes, nwrites := c.writeCount(), n.writeCount()
			if e, err := Open(cfg, c, n); err == nil {
				e.Close()
				t.Fatal("conflicting audit accepted")
			}
			after, _ := os.ReadFile(filepath.Join(cfg.Dir, "live.json"))
			if string(before) != string(after) || c.writeCount() != writes || n.writeCount() != nwrites {
				t.Fatal("rejected recovery mutated state")
			}
			if _, err := os.Stat(filepath.Join(cfg.Dir, recoveryMarkerName)); err != nil {
				t.Fatal("evidence removed after conflict", err)
			}
		})
	}
}

func TestAutomaticAuditGroupContractCompletionTimeSurvivesRepair(t *testing.T) {
	for _, at := range []string{"journal", "receipt"} {
		t.Run(at, func(t *testing.T) {
			cfg, c, n, e := groupContractFixture(t, 1, 0, 2, false)
			c.removeUnknown = true
			if _, err := e.Scale(0); err != nil {
				t.Fatal(err)
			}
			if err := e.reconcileOnce(); err != ErrUnknown {
				t.Fatal("expected unknown removal", err)
			}
			activity := e.state.Pending.ActivityID
			c.terminal(true)
			a := c.activities[activity]
			a.Type = "REMOVE_INSTANCES"
			a.StartedAt = time.Now().UTC().Format(time.RFC3339)
			a.EndedAt = a.StartedAt
			c.activities[activity] = a
			e.Close()
			r, err := OpenRecovery(cfg, c, n)
			if err != nil {
				t.Fatal(err)
			}
			r.delay = time.Millisecond
			p := automaticPlan(t, r)
			r.checkpoint = func(stage string) error {
				if stage == at {
					return code("Interruption")
				}
				return nil
			}
			if _, err = r.Apply(context.Background(), p, p.Digest); err == nil {
				t.Fatal("no interruption")
			}
			completed := r.e.state.LastCompletedAt
			if completed <= 0 || r.e.state.LastBatch == nil || r.e.state.LastBatch.CompletedAt != completed {
				t.Fatal("no completed batch fact")
			}
			r.Close()
			writes, nwrites := c.writeCount(), len(n.writes)
			e, err = Open(cfg, c, n)
			if err != nil {
				t.Fatal(err)
			}
			defer e.Close()
			for i := 0; i < 2; i++ {
				if err = e.reconcileOnce(); err != nil {
					t.Fatal(err)
				}
			}
			state, ready, err := e.Snapshot()
			if err != nil || !ready || state.Version != 3 || state.LastCompletedAt != completed || state.LastBatch == nil || state.LastBatch.CompletedAt != completed {
				t.Fatal("repair changed completion fact", ready, err)
			}
			if c.writeCount() != writes || len(n.writes) != nwrites {
				t.Fatal("repair repeated external work")
			}
		})
	}
}

type auditCrashInput struct {
	Directory, Phase string
	Cloud            tencentcloud.Snapshot
	Activity         tencentcloud.Activity
}

func TestAutomaticAuditCrashProcessHelper(t *testing.T) {
	path := os.Getenv("NOMAD_AUDIT_CRASH_FIXTURE")
	if path == "" {
		t.Skip("subprocess helper")
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var in auditCrashInput
	if err = json.Unmarshal(b, &in); err != nil {
		t.Fatal(err)
	}
	cfg := config(t)
	cfg.Dir = in.Directory
	c := newCloud()
	c.snap = in.Cloud
	c.activity = in.Activity
	r := recoverySession(t, cfg, c, newNomad())
	p := automaticPlan(t, r)
	r.checkpoint = func(stage string) error {
		if stage == in.Phase {
			os.Exit(43)
		}
		return nil
	}
	if _, err = r.Apply(context.Background(), p, p.Digest); err != nil {
		t.Fatal(err)
	}
	t.Fatal("crash checkpoint not reached")
}

func TestAutomaticAuditAbruptProcessExitThenRestart(t *testing.T) {
	for _, at := range []string{"prepared", "journal", "receipt"} {
		t.Run(at, func(t *testing.T) {
			cfg, c, n := recoveryFixture(t, true)
			fixture := filepath.Join(t.TempDir(), "crash-input.json")
			if err := os.WriteFile(fixture, jsonBytes(auditCrashInput{cfg.Dir, at, c.snap, c.activity}), 0600); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command(os.Args[0], "-test.run=^TestAutomaticAuditCrashProcessHelper$")
			cmd.Env = append(os.Environ(), "NOMAD_AUDIT_CRASH_FIXTURE="+fixture)
			output, err := cmd.CombinedOutput()
			if err == nil || cmd.ProcessState.ExitCode() != 43 {
				t.Fatalf("child did not reach checkpoint: %v %s", err, output)
			}
			writes, nwrites := c.writeCount(), n.writeCount()
			e, err := Open(cfg, c, n)
			if err != nil {
				t.Fatal("restart needs manual repair", err)
			}
			defer e.Close()
			for i := 0; i < 2; i++ {
				if err = e.reconcileOnce(); err != nil {
					t.Fatal(err)
				}
			}
			state, ready, err := e.Snapshot()
			if err != nil || !ready || state.Pending != nil || state.RecoveryAudit == nil {
				t.Fatal("restart did not finish recovery", ready, err)
			}
			if c.writeCount() != writes || n.writeCount() != nwrites {
				t.Fatal("restart repeated provider writes")
			}
		})
	}
}
