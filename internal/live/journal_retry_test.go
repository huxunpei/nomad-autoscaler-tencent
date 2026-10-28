package live

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadclient"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/tencentcloud"
)

func blockRetryJournal(t *testing.T, dir string) func() {
	t.Helper()
	path := filepath.Join(dir, "live.json")
	backup := filepath.Join(dir, "journal-fault-backup")
	if err := os.Rename(path, backup); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	return func() {
		t.Helper()
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(backup, path); err != nil {
			t.Fatal(err)
		}
	}
}

func TestJournalRetryObservationFailureUnfreezesSameProcess(t *testing.T) {
	cfg, c, n, e := groupContractFixture(t, 1, 0, 3, false)
	defer e.Close()
	restore := blockRetryJournal(t, cfg.Dir)
	if err := e.reconcileOnce(); err == nil {
		t.Fatal("expected journal failure")
	}
	if _, ready, err := e.Snapshot(); ready || err == nil {
		t.Fatal("advertised readiness during persistence failure")
	}
	restore()
	step(t, e)
	s, ready, err := e.Snapshot()
	if err != nil || !ready || s.RecoveryAudit == nil || c.writeCount() != 0 || len(n.writes) != 0 {
		t.Fatal("ordinary journal failure remained frozen or replayed write", ready, err)
	}
	if err = e.checkRecoveryAudit(); err != nil {
		t.Fatal(err)
	}
}

type journalRetryCloud struct {
	*multiCloud
	afterGrow func()
}

func (c *journalRetryCloud) SetDesired(ctx context.Context, g string, count int64) (tencentcloud.WriteResult, error) {
	r, err := c.multiCloud.SetDesired(ctx, g, count)
	if c.afterGrow != nil {
		f := c.afterGrow
		c.afterGrow = nil
		f()
	}
	return r, err
}
func TestJournalRetryRetainsAcknowledgedGrowWithoutReplay(t *testing.T) {
	cfg, base, n, e := groupContractFixture(t, 1, 0, 3, false)
	e.Close()
	c := &journalRetryCloud{multiCloud: base}
	e = openMulti(t, cfg, c, n)
	defer e.Close()
	var restore func()
	c.afterGrow = func() { restore = blockRetryJournal(t, cfg.Dir) }
	if changed, err := e.Scale(2); !changed || err == nil {
		t.Fatal("expected acknowledged grow persistence failure", changed, err)
	}
	if base.writeCount() != 1 {
		t.Fatal("grow fixture")
	}
	restore()
	step(t, e)
	step(t, e)
	s, ready, err := e.Snapshot()
	if err != nil || !ready || s.Pending != nil || s.LastCompletedAt == 0 || s.RecoveryAudit == nil || base.writeCount() != 1 {
		t.Fatal("lost acknowledgment or duplicated grow", ready, err)
	}
}

type journalRetryNomad struct {
	*sdkMultiProbe
	onSelect, onPre, onTasks func()
}

func (n *journalRetryNomad) SelectScaleIn(ctx context.Context, ids []nomadclient.Identity, count int) ([]nomadclient.Identity, error) {
	if n.onSelect != nil {
		f := n.onSelect
		n.onSelect = nil
		f()
	}
	return n.sdkMultiProbe.SelectScaleIn(ctx, ids, count)
}
func (n *journalRetryNomad) PreScaleIn(ctx context.Context, id nomadclient.Identity, op string, d time.Duration, before func() error) (nomadclient.MutationResult, error) {
	if n.onPre != nil {
		f := n.onPre
		n.onPre = nil
		f()
	}
	return n.sdkMultiProbe.PreScaleIn(ctx, id, op, d, before)
}
func (n *journalRetryNomad) RequiredTasksStopped(ctx context.Context, id string) (bool, error) {
	if n.onTasks != nil {
		f := n.onTasks
		n.onTasks = nil
		f()
	}
	return n.sdkMultiProbe.RequiredTasksStopped(ctx, id)
}
func retryNomadFixture(t *testing.T) (Config, *multiCloud, *journalRetryNomad, *Engine) {
	t.Helper()
	cfg, c, raw, e := groupContractFixture(t, 2, 0, 3, false)
	e.Close()
	n := &journalRetryNomad{sdkMultiProbe: &sdkMultiProbe{multiNomad: raw, directory: cfg.Dir}}
	e = openMulti(t, cfg, c, n)
	return cfg, c, n, e
}
func TestJournalRetryUnsubmittedAcceptAndSDKBarrier(t *testing.T) {
	for _, at := range []string{"accept", "sdk-barrier"} {
		t.Run(at, func(t *testing.T) {
			cfg, c, n, e := retryNomadFixture(t)
			defer e.Close()
			var restore func()
			fault := func() { restore = blockRetryJournal(t, cfg.Dir) }
			if at == "accept" {
				n.onSelect = fault
			} else {
				n.onPre = fault
			}
			if _, err := e.Scale(1); err == nil {
				t.Fatal("expected unsubmitted intent save failure")
			}
			if c.writeCount() != 0 || len(n.writes) != 0 {
				t.Fatal("mutation crossed failed durable barrier")
			}
			restore()
			step(t, e)
			s, ready, err := e.Snapshot()
			if err != nil || !ready || s.Pending != nil || s.Batch != nil || s.LastCompletedAt != 0 || s.RecoveryAudit == nil {
				t.Fatal("unsubmitted request turned into unknown or stayed frozen", ready, err)
			}
			if changed, err := e.Scale(1); !changed || err != nil {
				t.Fatal("next normal action blocked", changed, err)
			}
			completeMultiDelete(t, e, c)
			if c.writeCount() != 1 || len(n.writes) != 1 {
				t.Fatal("duplicate provider write")
			}
		})
	}
}
func TestJournalRetryUnsubmittedRemoveRechecksBeforeWriting(t *testing.T) {
	cfg, c, n, e := retryNomadFixture(t)
	defer e.Close()
	if _, err := e.Scale(1); err != nil {
		t.Fatal(err)
	}
	var restore func()
	n.onTasks = func() { restore = blockRetryJournal(t, cfg.Dir) }
	if err := e.reconcileOnce(); err == nil || c.writeCount() != 0 {
		t.Fatal("failed pre-delete save wrote cloud")
	}
	restore()
	step(t, e)
	if c.writeCount() != 1 || len(n.writes) != 1 || e.state.Pending == nil || e.state.Pending.Phase != "removing" {
		t.Fatal("did not safely retry original drain")
	}
	c.terminal(true)
	step(t, e)
	step(t, e)
	if e.state.Pending != nil || c.writeCount() != 1 {
		t.Fatal("replayed delete after recovery")
	}
}

func retryObservationFault(t *testing.T) (Config, *multiCloud, *multiNomad, *Engine) {
	t.Helper()
	cfg, c, n, e := groupContractFixture(t, 1, 0, 3, false)
	restore := blockRetryJournal(t, cfg.Dir)
	if err := e.reconcileOnce(); err == nil {
		t.Fatal("missing save fault")
	}
	restore()
	return cfg, c, n, e
}
func TestJournalRetryAuditAllBoundariesRestartOrSameProcess(t *testing.T) {
	for _, at := range []string{"before-record", "record", "prepared", "journal", "receipt", "cleared"} {
		for _, restart := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/restart%v", at, restart), func(t *testing.T) {
				cfg, c, n, e := retryObservationFault(t)
				defer func() { e.Close() }()
				e.recoveryCheckpoint = func(stage string) error {
					if stage == at {
						return code("InterruptedJournalRetry")
					}
					return nil
				}
				if err := e.reconcileOnce(); err == nil {
					t.Fatal("missing audit interruption")
				}
				if _, err := os.Stat(filepath.Join(cfg.Dir, journalRetryQueueName)); err != nil {
					t.Fatal("retry proof was not durable before audit", err)
				}
				e.recoveryCheckpoint = nil
				if restart {
					e.Close()
					e = openMulti(t, cfg, c, n)
				}
				for i := 0; i < 2; i++ {
					step(t, e)
				}
				s, ready, err := e.Snapshot()
				if err != nil || !ready || e.pendingSave != nil || s.RecoveryAudit == nil || c.writeCount() != 0 || len(n.writes) != 0 {
					t.Fatal("did not resume local transaction", ready, err)
				}
				if err = e.checkRecoveryAudit(); err != nil {
					t.Fatal(err)
				}
				if _, err = os.Lstat(filepath.Join(cfg.Dir, journalRetryQueueName)); !os.IsNotExist(err) {
					t.Fatal("retry queue not cleaned")
				}
				record, err := readRecord(cfg.Dir, *s.RecoveryAudit)
				if err != nil || record.Plan.Decision.Action != "retry-journal-save" || record.Evidence[0].Journal == nil {
					t.Fatal("missing original failed save evidence", err)
				}
			})
		}
	}
}

func TestJournalRetryAfterRenameUsesOnlyKnownDiskVersions(t *testing.T) {
	for _, mode := range []string{"observation", "unsubmitted", "completed"} {
		t.Run(mode, func(t *testing.T) {
			cfg, c, n, e := groupContractFixture(t, 1, 0, 3, false)
			defer e.Close()
			if mode == "completed" {
				if _, err := e.Scale(2); err != nil {
					t.Fatal(err)
				}
			}
			fired := false
			e.journalCheckpoint = func(stage string) error {
				eligible := mode == "observation" || (mode == "unsubmitted" && e.state.Pending != nil && e.state.Pending.Phase == "grow-intent") ||
					(mode == "completed" && e.state.Pending == nil && e.state.LastBatch != nil && e.state.LastBatch.Phase == "completed")
				if eligible && !fired {
					fired = true
					return code("SimulatedDirectorySyncFailure")
				}
				return nil
			}
			var err error
			if mode == "unsubmitted" {
				_, err = e.Scale(2)
			} else {
				err = e.reconcileOnce()
			}
			if err == nil || !fired || e.pendingSave == nil {
				t.Fatal("after-rename failure not captured", err)
			}
			captured := e.pendingSave.Attempted.LastCompletedAt
			e.journalCheckpoint = nil
			step(t, e)
			step(t, e)
			s, ready, err := e.Snapshot()
			if err != nil || !ready || s.RecoveryAudit == nil {
				t.Fatal("failed to commit exact known state", ready, err)
			}
			if mode == "unsubmitted" && (c.writeCount() != 0 || s.Pending != nil || s.LastCompletedAt != 0) {
				t.Fatal("submitted rolled-back intent")
			}
			if mode == "completed" && (captured == 0 || s.LastCompletedAt != captured || c.writeCount() != 1) {
				t.Fatal("changed original completion fact")
			}
			if len(n.writes) != 0 {
				t.Fatal("unexpected node write")
			}
			_ = cfg
		})
	}
}

func TestJournalRetryRefusesChangedJournalOrQueue(t *testing.T) {
	for _, mode := range []string{"journal", "queue", "symlink", "manual-marker"} {
		t.Run(mode, func(t *testing.T) {
			cfg, c, n, e := retryObservationFault(t)
			defer e.Close()
			switch mode {
			case "journal":
				s := e.state
				s.LastEvent = "external-change"
				if err := os.WriteFile(filepath.Join(cfg.Dir, "live.json"), jsonBytes(s), 0600); err != nil {
					t.Fatal(err)
				}
			case "queue":
				if err := os.WriteFile(filepath.Join(cfg.Dir, journalRetryQueueName), []byte("{}\n"), 0600); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				if err := os.Symlink(filepath.Join(cfg.Dir, "live.json"), filepath.Join(cfg.Dir, journalRetryQueueName)); err != nil {
					t.Fatal(err)
				}
			case "manual-marker":
				if err := os.WriteFile(filepath.Join(cfg.Dir, recoveryMarkerName), []byte("{}\n"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			before, err := os.ReadFile(filepath.Join(cfg.Dir, "live.json"))
			if err != nil {
				t.Fatal(err)
			}
			if err = e.reconcileOnce(); err == nil {
				t.Fatal("accepted conflicting state")
			}
			after, err := os.ReadFile(filepath.Join(cfg.Dir, "live.json"))
			if err != nil || string(before) != string(after) || c.writeCount() != 0 || len(n.writes) != 0 {
				t.Fatal("conflict changed state or provider")
			}
		})
	}
}

func TestJournalRetryReadOnlyRouteRepairsOnlyLocalState(t *testing.T) {
	cfg, c, n, e := groupContractFixture(t, 1, 0, 3, false)
	e.Close()
	cfg.DisableReconciliation = true
	e = openMulti(t, cfg, c, n)
	defer e.Close()
	restore := blockRetryJournal(t, cfg.Dir)
	if err := e.reconcileOnce(); err == nil {
		t.Fatal("missing failure")
	}
	restore()
	step(t, e)
	if e.reconcileEnabled.Load() || c.writeCount() != 0 || len(n.writes) != 0 || e.persistError != nil {
		t.Fatal("local repair enabled provider writer")
	}
}

func TestJournalRetryAuditRejectsChangedProofAndDelta(t *testing.T) {
	for _, mode := range []string{"observation", "accept", "sdk-barrier", "remove"} {
		t.Run(mode, func(t *testing.T) {
			var e *Engine
			if mode == "observation" {
				_, _, _, e = retryObservationFault(t)
			} else {
				cfg, _, n, current := retryNomadFixture(t)
				e = current
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
						t.Fatal("missing fault")
					}
				} else if _, err := e.Scale(1); err == nil {
					t.Fatal("missing fault")
				}
				restore()
			}
			defer e.Close()
			step(t, e)
			record, err := readRecord(e.cfg.Dir, *e.state.RecoveryAudit)
			if err != nil || record.Plan.Decision.Action != "retry-journal-save" {
				t.Fatal("missing journal retry audit", err)
			}
			for _, mutation := range []string{"manual", "version", "reference", "scope", "before-state", "after-count", "after-time", "unknown-source", "missing-proof", "foreign-field", "bad-hash"} {
				t.Run(mutation, func(t *testing.T) {
					r := copyJSON(record)
					switch mutation {
					case "manual":
						r.Automatic = false
					case "version":
						r.Plan.Version = 1
					case "reference":
						r.Plan.Reference = "external/repair"
					case "scope":
						r.Plan.Scope.GroupID = "asg-other"
					case "before-state":
						r.Before.LastEvent = "changed-before"
					case "after-count":
						r.After.Desired++
					case "after-time":
						r.After.LastCompletedAt++
					case "unknown-source":
						r.Evidence[0].Journal.UnsubmittedSource = "operator-says-not-submitted"
					case "missing-proof":
						r.Evidence[0].Journal = nil
					case "foreign-field":
						r.Evidence[0].Post = &nomadclient.PostScaleInResult{}
					case "bad-hash":
						r.Evidence[0].Journal.AttemptedSHA256 = strings.Repeat("0", 64)
					}
					r.Plan.Evidence = r.Evidence
					r.BeforeSHA256 = sha(jsonBytes(r.Before))
					r.Plan.JournalSHA256 = r.BeforeSHA256
					r.Plan.Digest = planDigest(r.Plan)
					if validRecoveryRecordOrigin(r) {
						t.Fatal("accepted expanded journal recovery")
					}
				})
			}
		})
	}
}

func TestJournalRetryQueueRejectsWrongOriginScopeOrProof(t *testing.T) {
	for _, mutation := range []string{"manual", "version", "origin", "region", "directory", "proof"} {
		t.Run(mutation, func(t *testing.T) {
			cfg, c, n, e := retryObservationFault(t)
			e.recoveryCheckpoint = func(stage string) error {
				if stage == "before-record" {
					return code("QueueReady")
				}
				return nil
			}
			if err := e.reconcileOnce(); err == nil {
				t.Fatal("missing checkpoint")
			}
			e.Close()
			path := filepath.Join(cfg.Dir, journalRetryQueueName)
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			var q journalRetryQueue
			if err = json.Unmarshal(raw, &q); err != nil {
				t.Fatal(err)
			}
			switch mutation {
			case "manual":
				q.Automatic = false
			case "version":
				q.Version = 0
			case "origin":
				q.Origin = "manual"
			case "region":
				q.Scope.Region = "ap-other"
			case "directory":
				q.Scope.Directory = cfg.Dir + "-other"
			case "proof":
				q.Failure.FailedAt = time.Time{}
			}
			bad := jsonBytes(q)
			if err = os.WriteFile(path, bad, 0600); err != nil {
				t.Fatal(err)
			}
			before, err := os.ReadFile(filepath.Join(cfg.Dir, "live.json"))
			if err != nil {
				t.Fatal(err)
			}
			reads := c.observeCalls
			opened, err := Open(cfg, c, n)
			if opened != nil {
				opened.Close()
			}
			if err == nil {
				t.Fatal("accepted incompatible queue")
			}
			after, readErr := os.ReadFile(filepath.Join(cfg.Dir, "live.json"))
			still, _ := os.ReadFile(path)
			if readErr != nil || string(before) != string(after) || string(still) != string(bad) || c.observeCalls != reads || c.writeCount() != 0 || len(n.writes) != 0 {
				t.Fatal("invalid queue changed state or contacted provider")
			}
		})
	}
}
