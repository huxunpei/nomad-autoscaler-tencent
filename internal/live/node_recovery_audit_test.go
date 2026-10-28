package live

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func nodeAuditFixture(t *testing.T, mode string) (Config, Cloud, *multiCloud, *multiNomad, *Engine, string) {
	t.Helper()
	cfg, base, n, e := groupContractFixture(t, 2, 0, 3, false)
	var cloud Cloud = base
	if mode == "rejected" {
		e.Close()
		cloud = &rejectedCloud{multiCloud: base, reject: true, code: "ResourceInsufficient.InServiceInstanceBelowMinSize"}
		e = openMulti(t, cfg, cloud, n)
	}
	var id string
	if mode == "failed-delete" {
		id = submittedFailure(t, e, base)
	} else {
		if _, err := e.Scale(1); err != nil {
			t.Fatal(err)
		}
		id = e.state.Pending.InstanceID
		if mode == "aborting" {
			base.snap.Group.Min = 2
		}
	}
	return cfg, cloud, base, n, e, id
}

func assertNodeAudit(t *testing.T, e *Engine, mode, id string, n *multiNomad) {
	t.Helper()
	if e.state.ManualReview || e.state.LastCompletedAt != 0 || n.nodes[id].Eligibility != "eligible" || len(n.writes) != 2 {
		t.Fatalf("restore changed failure semantics or duplicated mutation: %+v writes=%v", e.state, n.writes)
	}
	if mode == "failed-delete" {
		if e.state.Pending == nil || e.state.Pending.Phase != "failed-delete" || e.state.Pending.DrainOutcome != "restored" {
			t.Fatal("lost asynchronous deletion obligation")
		}
	} else if e.state.Pending != nil || e.state.LastBatch == nil || e.state.LastBatch.Phase != "failed" {
		t.Fatal("did not finish failed batch")
	}
	if err := e.checkRecoveryAudit(); err != nil {
		t.Fatal(err)
	}
	ref := e.state.RecoveryAudit
	for _, action := range []string{"confirm-node-restored", "begin-node-restore"} {
		if ref == nil {
			t.Fatal("missing unified recovery audit", action)
		}
		r, err := readRecord(e.cfg.Dir, *ref)
		if err != nil || r.Version != 3 || !r.Automatic || r.Plan.Decision.Action != action || r.Before.Pending.Phase != mode || len(r.Evidence) != 1 || len(r.Evidence[0].Nodes) != 1 {
			t.Fatalf("invalid restoration audit: %v %+v", err, r)
		}
		if r.Plan.Decision.OperationID != r.Before.Pending.ID || r.Evidence[0].Nodes[0].InstanceID != id {
			t.Fatal("lost operation or node identity")
		}
		ref = r.Before.RecoveryAudit
	}
	if ref != nil {
		t.Fatal("unexpected duplicate committed recovery records")
	}
}

func TestNodeRecoveryAuditTracksKnownFailureRestoration(t *testing.T) {
	for _, mode := range []string{"aborting", "rejected", "failed-delete"} {
		t.Run(mode, func(t *testing.T) {
			cfg, cloud, base, n, e, id := nodeAuditFixture(t, mode)
			defer func() { e.Close() }()
			step(t, e)
			assertNodeAudit(t, e, mode, id, n)
			writes := base.writeCount()
			e.Close()
			e = openMulti(t, cfg, cloud, n)
			step(t, e)
			assertNodeAudit(t, e, mode, id, n)
			if base.writeCount() != writes {
				t.Fatal("replayed cloud write")
			}
		})
	}
}

func TestNodeRecoveryAuditResumesEveryBoundary(t *testing.T) {
	for _, mode := range []string{"aborting", "rejected", "failed-delete"} {
		for _, txn := range []int{1, 2} {
			for _, at := range []string{"before-record", "record", "prepared", "journal", "receipt", "cleared"} {
				for _, restart := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/txn%d/%s/restart%v", mode, txn, at, restart), func(t *testing.T) {
						cfg, cloud, base, n, e, id := nodeAuditFixture(t, mode)
						defer func() { e.Close() }()
						ordinal := 0
						e.recoveryCheckpoint = func(stage string) error {
							if stage == "before-record" {
								ordinal++
							}
							if ordinal == txn && stage == at {
								return code("SimulatedNodeRecoveryCrash")
							}
							return nil
						}
						if err := e.reconcileOnce(); err == nil {
							t.Fatal("expected audit interruption")
						}
						if txn == 1 && len(n.writes) != 1 {
							t.Fatal("restored before start audit committed")
						}
						writes := base.writeCount()
						e.recoveryCheckpoint = nil
						if restart {
							e.Close()
							e = openMulti(t, cfg, cloud, n)
						}
						for i := 0; i < 3; i++ {
							step(t, e)
						}
						assertNodeAudit(t, e, mode, id, n)
						if base.writeCount() != writes {
							t.Fatal("replayed cloud write during audit repair")
						}
						if c, ok := cloud.(*rejectedCloud); ok && c.calls != 1 {
							t.Fatal("replayed rejected cloud write")
						}
					})
				}
			}
		}
	}
}

func TestNodeRecoveryAuditStorageFailureRetries(t *testing.T) {
	for _, mode := range []string{"aborting", "rejected", "failed-delete"} {
		for _, txn := range []int{1, 2} {
			for _, at := range []string{"record", "marker", "journal", "receipt"} {
				t.Run(fmt.Sprintf("%s/txn%d/%s", mode, txn, at), func(t *testing.T) {
					cfg, _, _, n, e, id := nodeAuditFixture(t, mode)
					defer e.Close()
					ordinal := 0
					blocked := ""
					backup := filepath.Join(cfg.Dir, "journal-test-backup")
					e.recoveryCheckpoint = func(stage string) error {
						if stage == "before-record" {
							ordinal++
						}
						if ordinal != txn {
							return nil
						}
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
					if err := e.reconcileOnce(); err == nil || blocked == "" {
						t.Fatal("storage fault not reached", err)
					}
					if e.automaticAudit == nil || e.persistError == nil {
						t.Fatal("lost pending transaction")
					}
					if _, ready, err := e.Snapshot(); ready || err == nil {
						t.Fatal("advertised ready before audit committed")
					}
					if err := os.Remove(blocked); err != nil {
						t.Fatal(err)
					}
					if at == "journal" {
						if err := os.Rename(backup, filepath.Join(cfg.Dir, "live.json")); err != nil {
							t.Fatal(err)
						}
					}
					e.recoveryCheckpoint = nil
					for i := 0; i < 3; i++ {
						step(t, e)
					}
					assertNodeAudit(t, e, mode, id, n)
				})
			}
		}
	}
}

func TestNodeRecoveryAuditRejectsInvalidOriginAndExpandedDelta(t *testing.T) {
	for _, mode := range []string{"aborting", "rejected", "failed-delete"} {
		t.Run(mode, func(t *testing.T) {
			_, _, _, _, e, _ := nodeAuditFixture(t, mode)
			defer e.Close()
			step(t, e)
			ref := e.state.RecoveryAudit
			for _, action := range []string{"confirm-node-restored", "begin-node-restore"} {
				record, err := readRecord(e.cfg.Dir, *ref)
				if err != nil {
					t.Fatal(err)
				}
				for _, mutation := range []string{"manual-origin", "manual-version", "unknown-phase", "wrong-node", "wrong-owner", "not-running", "false-restoration", "capacity", "cooldown", "old-result", "wrong-request", "missing-evidence"} {
					t.Run(action+"/"+mutation, func(t *testing.T) {
						r := copyJSON(record)
						switch mutation {
						case "manual-origin":
							r.Automatic = false
						case "manual-version":
							r.Plan.Version = 1
						case "unknown-phase":
							r.Before.Pending.Phase = "unknown"
							r.Before.Pending.Unknown = true
						case "wrong-node":
							r.Evidence[0].Nodes[0].InstanceID = "ins-other"
						case "wrong-owner":
							r.Evidence[0].Nodes[0].LastDrain.OperationID = "live-11111111111111111111111111111111"
						case "not-running":
							vm := r.Evidence[0].Cloud.VMs[r.Before.Pending.InstanceID]
							vm.State = "STOPPED"
							r.Evidence[0].Cloud.VMs[vm.ID] = vm
						case "false-restoration":
							r.Evidence[0].Nodes[0].Eligibility = "unknown"
						case "capacity":
							r.After.Desired++
						case "cooldown":
							r.After.LastCompletedAt = 42
						case "old-result":
							r.After.LastEvent = "forged-result"
						case "wrong-request":
							r.Plan.Decision.OperationID = "live-11111111111111111111111111111111"
						case "missing-evidence":
							r.Evidence = nil
						}
						r.Plan.Evidence = r.Evidence
						r.BeforeSHA256 = sha(jsonBytes(r.Before))
						r.Plan.JournalSHA256 = r.BeforeSHA256
						r.Plan.Digest = planDigest(r.Plan)
						if validRecoveryRecordOrigin(r) {
							t.Fatal("accepted unsafe automatic audit")
						}
					})
				}
				ref = record.Before.RecoveryAudit
			}
		})
	}
}
