package live

import (
	"context"
	"fmt"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadclient"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/tencentcloud"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func postAuditFixture(t *testing.T, failed bool) (Config, *settlementCloud, *settlementNomad, *Engine, string) {
	t.Helper()
	cfg, c, n, e := settlementFixture(t, 2)
	var id string
	if failed {
		id = submittedFailure(t, e, c.multiCloud)
		laterDeletion(t, e, c)
	} else {
		if _, err := e.Scale(1); err != nil {
			t.Fatal(err)
		}
		id = e.state.Pending.InstanceID
		step(t, e)
		c.terminal(true)
	}
	step(t, e) // first absence observation, no post yet
	if n.purges != 0 || e.state.Pending == nil {
		t.Fatal("invalid pre-post fixture")
	}
	return cfg, c, n, e, id
}

func assertPostAudit(t *testing.T, e *Engine, c *settlementCloud, n *settlementNomad, failed bool) {
	t.Helper()
	if e.state.Pending != nil || e.state.Batch != nil || e.state.ManualReview || e.state.LastBatch == nil || n.purges != 1 || c.writeCount() != 1 {
		t.Fatalf("post did not complete safely: %+v calls=%d purges=%d", e.state, n.postCalls, n.purges)
	}
	want := "completed"
	if failed {
		want = "failed"
	}
	if e.state.LastBatch.Phase != want || (failed && e.state.LastCompletedAt != 0) || (!failed && e.state.LastCompletedAt == 0) {
		t.Fatal("incorrect failure/cooldown result")
	}
	if err := e.checkRecoveryAudit(); err != nil {
		t.Fatal(err)
	}
	ref := e.state.RecoveryAudit
	for _, action := range []string{"confirm-sdk-post", "begin-sdk-post"} {
		if ref == nil {
			t.Fatal("missing post audit", action)
		}
		r, err := readRecord(e.cfg.Dir, *ref)
		if err != nil || r.Version != 3 || !r.Automatic || r.Plan.Decision.Action != action || r.Before.Pending.ID != e.state.LastOperation.ID {
			t.Fatal("invalid post audit", action, err)
		}
		if action == "confirm-sdk-post" && !failed && r.Plan.CreatedAt.UnixNano() != e.state.LastCompletedAt {
			t.Fatal("completion timestamp not bound to audit")
		}
		ref = r.Before.RecoveryAudit
	}
	if ref != nil {
		t.Fatal("duplicate committed post audits")
	}
}

func TestSDKPostAuditRecordsProofAndCompletion(t *testing.T) {
	for _, failed := range []bool{false, true} {
		t.Run(fmt.Sprint(failed), func(t *testing.T) {
			cfg, c, n, e, _ := postAuditFixture(t, failed)
			defer func() { e.Close() }()
			step(t, e)
			assertPostAudit(t, e, c, n, failed)
			completed := e.state.LastCompletedAt
			e.Close()
			e = openMulti(t, cfg, c, n)
			step(t, e)
			assertPostAudit(t, e, c, n, failed)
			if n.postCalls != 1 || e.state.LastCompletedAt != completed {
				t.Fatal("replayed completed post or changed cooldown")
			}
		})
	}
}

func TestFailedPostRechecksExactVMBeforePurge(t *testing.T) {
	_, c, n, e, id := postAuditFixture(t, true)
	defer e.Close()
	n.lostPost = true
	if err := e.reconcileOnce(); err == nil {
		t.Fatal("expected lost post response")
	}
	calls := n.postCalls
	c.snap.VMs[id] = tencentcloud.VM{ID: id, Returned: true, State: "RUNNING", ChargeType: "POSTPAID_BY_HOUR"}
	_ = e.reconcileOnce()
	if n.postCalls != calls || e.state.Pending == nil || e.state.Pending.Phase != "failed-delete-post" || e.state.LastCompletedAt != 0 {
		t.Fatal("post advanced despite fresh CVM existence")
	}
}

func TestSDKPostAuditResumesEveryBoundary(t *testing.T) {
	for _, failed := range []bool{false, true} {
		for _, txn := range []int{1, 2} {
			for _, at := range []string{"before-record", "record", "prepared", "journal", "receipt", "cleared"} {
				for _, restart := range []bool{false, true} {
					t.Run(fmt.Sprintf("failed%v/txn%d/%s/restart%v", failed, txn, at, restart), func(t *testing.T) {
						cfg, c, n, e, _ := postAuditFixture(t, failed)
						defer func() { e.Close() }()
						ordinal := 0
						e.recoveryCheckpoint = func(stage string) error {
							if stage == "before-record" {
								ordinal++
							}
							if ordinal == txn && stage == at {
								return code("SimulatedPostAuditCrash")
							}
							return nil
						}
						if err := e.reconcileOnce(); err == nil {
							t.Fatal("expected audit interruption")
						}
						if txn == 1 && n.purges != 0 {
							t.Fatal("purged before begin audit committed")
						}
						completed := e.state.LastCompletedAt
						if _, ready, err := e.Snapshot(); ready || err == nil {
							t.Fatal("published ready before transaction finished")
						}
						e.recoveryCheckpoint = nil
						if restart {
							e.Close()
							e = openMulti(t, cfg, c, n)
						}
						for i := 0; i < 3; i++ {
							step(t, e)
						}
						assertPostAudit(t, e, c, n, failed)
						if completed > 0 && e.state.LastCompletedAt != completed {
							t.Fatal("recreated committed cooldown time")
						}
					})
				}
			}
		}
	}
}

func TestSDKPostAuditStorageFailuresResume(t *testing.T) {
	for _, failed := range []bool{false, true} {
		for _, txn := range []int{1, 2} {
			for _, at := range []string{"record", "marker", "journal", "receipt"} {
				t.Run(fmt.Sprintf("failed%v/txn%d/%s", failed, txn, at), func(t *testing.T) {
					cfg, c, n, e, _ := postAuditFixture(t, failed)
					defer e.Close()
					ordinal := 0
					blocked := ""
					backup := filepath.Join(cfg.Dir, "post-audit-journal-backup")
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
						t.Fatal("expected actual storage failure", err)
					}
					if _, ready, err := e.Snapshot(); ready || err == nil {
						t.Fatal("published readiness despite storage failure")
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
					assertPostAudit(t, e, c, n, failed)
				})
			}
		}
	}
}

type invalidPostReceipt struct {
	*settlementNomad
	invalid bool
}

func (n *invalidPostReceipt) PostScaleInWithEvidence(ctx context.Context, id nomadclient.Identity, op string, before func() error) (nomadclient.PostScaleInResult, error) {
	r, err := n.settlementNomad.PostScaleInWithEvidence(ctx, id, op, before)
	if n.invalid {
		r.PolicySHA256 = "0000000000000000000000000000000000000000000000000000000000000000"
	}
	return r, err
}
func TestSDKPostAuditRejectsWrongReceiptWithoutReplayingPurge(t *testing.T) {
	cfg, c, n, e, _ := postAuditFixture(t, false)
	e.Close()
	probe := &invalidPostReceipt{settlementNomad: n, invalid: true}
	e = openMulti(t, cfg, c, probe)
	defer e.Close()
	if err := e.reconcileOnce(); err == nil || e.state.Pending == nil || e.state.LastCompletedAt != 0 || n.purges != 1 {
		t.Fatal("invalid receipt completed action")
	}
	probe.invalid = false
	step(t, e)
	assertPostAudit(t, e, c, n, false)
}

func TestSDKPostAuditRejectsExpandedEvidenceAndResult(t *testing.T) {
	for _, failed := range []bool{false, true} {
		t.Run(fmt.Sprint(failed), func(t *testing.T) {
			_, _, _, e, _ := postAuditFixture(t, failed)
			defer e.Close()
			step(t, e)
			record, err := readRecord(e.cfg.Dir, *e.state.RecoveryAudit)
			if err != nil {
				t.Fatal(err)
			}
			for _, mutation := range []string{"manual", "phase", "vm-exists", "capacity", "policy", "operation", "purge-outcome", "missing-receipt", "future-receipt", "completion-time", "failure-result", "wrong-node"} {
				t.Run(mutation, func(t *testing.T) {
					r := copyJSON(record)
					switch mutation {
					case "manual":
						r.Automatic = false
					case "phase":
						r.Before.Pending.Phase = "removing"
					case "vm-exists":
						id := r.Before.Pending.InstanceID
						r.Evidence[0].Cloud.VMs[id] = tencentcloud.VM{ID: id, Returned: true, State: "RUNNING", ChargeType: "POSTPAID_BY_HOUR"}
					case "capacity":
						r.Evidence[0].Cloud.Group.Desired++
					case "policy":
						r.Evidence[0].Post.PolicySHA256 = "invalid"
					case "operation":
						r.Evidence[0].Post.OperationID = "live-11111111111111111111111111111111"
					case "purge-outcome":
						r.Evidence[0].Post.Purge = false
					case "missing-receipt":
						r.Evidence[0].Post = nil
					case "future-receipt":
						r.Evidence[0].Post.ObservedAt = r.Plan.CreatedAt.Add(time.Second)
					case "completion-time":
						r.After.LastCompletedAt++
					case "failure-result":
						r.After.LastBatch.Phase = "forged"
					case "wrong-node":
						r.Evidence[0].Post.NodeID = "22222222-1111-1111-1111-111111111111"
					}
					r.Plan.Evidence = r.Evidence
					r.BeforeSHA256 = sha(jsonBytes(r.Before))
					r.Plan.JournalSHA256 = r.BeforeSHA256
					r.Plan.Digest = planDigest(r.Plan)
					if validRecoveryRecordOrigin(r) {
						t.Fatal("unsafe post audit accepted")
					}
				})
			}
		})
	}
}
