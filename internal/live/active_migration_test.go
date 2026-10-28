package live

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func sdkLegacyActiveFixture(t *testing.T) (Config, *multiCloud, *sdkMultiProbe, State, []byte) {
	t.Helper()
	cfg, c, raw, initial := multiFixture(t, 3, 3)
	initial.Close()
	n := &sdkMultiProbe{multiNomad: raw, directory: cfg.Dir}
	e, err := Open(cfg, c, n)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = e.Scale(1); err != nil {
		t.Fatal(err)
	}
	before := multiState(t, e)
	e.Close()
	rawState, err := os.ReadFile(filepath.Join(cfg.Dir, "live.json"))
	if err != nil {
		t.Fatal(err)
	}
	return cfg, c, n, before, rawState
}
func readLegacyContract(t *testing.T, s State) map[string]any {
	t.Helper()
	var raw map[string]any
	if json.Unmarshal(jsonBytes(s), &raw) != nil {
		t.Fatal("state encoding")
	}
	result, ok := raw["legacy_contract"].(map[string]any)
	if !ok {
		t.Fatal("missing original contract")
	}
	return result
}
func TestSDKLegacyActiveMigrationKeepsOriginalBatchAndProtection(t *testing.T) {
	cfg, c, n, old, raw := sdkLegacyActiveFixture(t)
	cfg.GroupContract = true
	e, err := Open(cfg, c, n)
	if err != nil {
		t.Fatal("old SDK batch cannot migrate", err)
	}
	defer e.Close()
	s := multiState(t, e)
	legacy := readLegacyContract(t, s)
	if s.Version != 3 || s.MaxCapacity != 0 || !reflect.DeepEqual(s.Pending, old.Pending) || !reflect.DeepEqual(s.Batch, old.Batch) || s.Sequence != old.Sequence || s.LastCompletedAt != 0 || legacy["source_version"] != float64(2) || legacy["max_capacity"] != float64(3) || legacy["batch_id"] != old.Batch.ID || legacy["backup_sha256"] != sha(raw) {
		t.Fatal("migration reinterpreted original operation")
	}
	data, err := os.ReadFile(filepath.Join(cfg.Dir, "live-v2-before-group-contract.backup.json"))
	if err != nil || string(data) != string(raw) {
		t.Fatal("original backup missing")
	}
	if s.RecoveryAudit == nil || e.checkRecoveryAudit() != nil {
		t.Fatal("migration audit missing")
	}
	// Old batches always respected AS protection, even when the new policy's
	// optional AS protection switch is false.
	c.snap.Members[0].Protected = true
	if err = e.reconcileOnce(); err != nil {
		t.Fatal(err)
	}
	s = multiState(t, e)
	if c.writeCount() != 0 || s.Pending != nil || s.Batch != nil || s.LastBatch == nil || s.LastBatch.Phase != "failed" || n.nodes[old.Pending.InstanceID].Eligibility != "eligible" || n.selections != 1 {
		t.Fatal("old protected member was deleted or original drain was lost")
	}
}
func TestSDKLegacyActiveMigrationFinishesExactBatchAcrossRestarts(t *testing.T) {
	cfg, c, n, old, _ := sdkLegacyActiveFixture(t)
	cfg.GroupContract = true
	for i := 0; i < 2; i++ {
		e, err := Open(cfg, c, n)
		if err != nil {
			t.Fatal("old SDK batch cannot migrate", err)
		}
		if i == 1 {
			step(t, e)
		}
		completeMultiDelete(t, e, c)
		s := multiState(t, e)
		if i == 0 {
			if s.Batch == nil || len(s.Batch.Results) != 1 {
				t.Fatal("successful prefix lost")
			}
			readLegacyContract(t, s)
		}
		if i == 1 {
			if s.Pending != nil || s.Batch != nil || s.LastBatch == nil || s.LastBatch.ID != old.Batch.ID || s.LastBatch.Target != 1 || s.LastCompletedAt <= 0 || s.LastBatch.CompletedAt != s.LastCompletedAt || n.selections != 1 || c.writeCount() != 2 || e.checkRecoveryAudit() != nil {
				t.Fatal("original batch did not complete exactly once")
			}
			var m map[string]any
			_ = json.Unmarshal(jsonBytes(s), &m)
			if m["legacy_contract"] != nil {
				t.Fatal("old limits remained after batch completion")
			}
		}
		e.Close()
	}
	e, err := Open(cfg, c, n)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	c.snap.Group.Max = 5
	if changed, err := e.Scale(5); !changed || err != nil {
		t.Fatal("next normal action retained old limit", err)
	}
}

func TestSDKLegacyActiveMigrationRetainsOldGuards(t *testing.T) {
	for _, mode := range []string{"min", "max", "retry"} {
		t.Run(mode, func(t *testing.T) {
			cfg, c, n, _, _ := sdkLegacyActiveFixture(t)
			cfg.GroupContract = true
			e, err := Open(cfg, c, n)
			if err != nil {
				t.Fatal(err)
			}
			defer e.Close()
			switch mode {
			case "min":
				c.snap.Group.Min = 1
			case "max":
				c.snap.Group.Max = 4
			case "retry":
				c.snap.Group.RetryPolicy = "IMMEDIATE_RETRY"
			}
			_ = e.reconcileOnce()
			s, _, snapshotErr := e.Snapshot()
			if snapshotErr == nil || snapshotErr.Error() != "ExclusiveGroupGuardRejected" {
				t.Fatal("guard mismatch not exposed", snapshotErr)
			}
			if c.writeCount() != 0 || s.Pending != nil || s.Batch != nil || s.LastBatch == nil || s.LastBatch.Phase != "failed" || s.LastCompletedAt != 0 {
				t.Fatal("changed legacy contract allowed deletion")
			}
		})
	}
}

type changedMigrationSDK struct{ *sdkMultiProbe }

func (n changedMigrationSDK) PolicyDigest() string { return strings.Repeat("d", 64) }

type changedMigrationMapping struct{ *sdkMultiProbe }

func (n changedMigrationMapping) InstanceMetaKey() string { return "changed_instance_id" }
func TestSDKLegacyActiveMigrationRefusesDifferentIdentityWithoutWrites(t *testing.T) {
	for _, mode := range []string{"policy", "mapping"} {
		t.Run(mode, func(t *testing.T) {
			cfg, c, n, _, raw := sdkLegacyActiveFixture(t)
			cfg.GroupContract = true
			var adapter Nomad = changedMigrationSDK{n}
			if mode == "mapping" {
				adapter = changedMigrationMapping{n}
			}
			writes := len(n.writes)
			e, err := Open(cfg, c, adapter)
			if e != nil {
				e.Close()
			}
			if err == nil || err.Error() != "SDKPolicyChangeRequiresMigration" {
				t.Fatal("wrong rejection", err)
			}
			now, err := os.ReadFile(filepath.Join(cfg.Dir, "live.json"))
			if err != nil || string(now) != string(raw) || len(n.writes) != writes || c.writeCount() != 0 {
				t.Fatal("rejected identity changed source")
			}
			if _, err = os.Lstat(filepath.Join(cfg.Dir, "live-v2-before-group-contract.backup.json")); !os.IsNotExist(err) {
				t.Fatal("identity mismatch published backup")
			}
		})
	}
}
func TestSDKLegacyActiveMigrationRejectsMissingOrChangedEvidence(t *testing.T) {
	for _, mode := range []string{"backup-content", "backup-missing", "backup-public", "backup-symlink", "contract-max", "contract-hash", "contract-removed", "audit-removed", "selection"} {
		t.Run(mode, func(t *testing.T) {
			cfg, c, n, _, _ := sdkLegacyActiveFixture(t)
			cfg.GroupContract = true
			e, err := Open(cfg, c, n)
			if err != nil {
				t.Fatal(err)
			}
			state := multiState(t, e)
			e.Close()
			path := filepath.Join(cfg.Dir, "live-v2-before-group-contract.backup.json")
			switch mode {
			case "backup-content":
				err = os.WriteFile(path, []byte("changed"), 0600)
			case "backup-missing":
				err = os.Remove(path)
			case "backup-public":
				err = os.Chmod(path, 0644)
			case "backup-symlink":
				target := filepath.Join(cfg.Dir, "original-backup")
				err = os.Rename(path, target)
				if err == nil {
					err = os.Symlink(target, path)
				}
			default:
				var m map[string]any
				_ = json.Unmarshal(jsonBytes(state), &m)
				switch mode {
				case "contract-max":
					m["legacy_contract"].(map[string]any)["max_capacity"] = float64(4)
				case "contract-hash":
					m["legacy_contract"].(map[string]any)["backup_sha256"] = strings.Repeat("f", 64)
				case "contract-removed":
					delete(m, "legacy_contract")
				case "audit-removed":
					delete(m, "recovery_audit")
				case "selection":
					b := m["batch"].(map[string]any)
					selected := b["selected"].([]any)
					selected[0], selected[1] = selected[1], selected[0]
					p := m["pending"].(map[string]any)
					p["instance_id"] = selected[0].(map[string]any)["instance_id"]
					p["node_id"] = selected[0].(map[string]any)["node_id"]
				}
				err = os.WriteFile(filepath.Join(cfg.Dir, "live.json"), jsonBytes(m), 0600)
			}
			if err != nil {
				t.Fatal(err)
			}
			before, err := os.ReadFile(filepath.Join(cfg.Dir, "live.json"))
			if err != nil {
				t.Fatal(err)
			}
			writes := len(n.writes)
			rejected, err := Open(cfg, c, n)
			if rejected != nil {
				rejected.Close()
			}
			if err == nil {
				t.Fatal("accepted inconsistent migration evidence")
			}
			after, readErr := os.ReadFile(filepath.Join(cfg.Dir, "live.json"))
			if readErr != nil || string(before) != string(after) || len(n.writes) != writes || c.writeCount() != 0 {
				t.Fatal("rejection changed state or provider")
			}
		})
	}
}
func TestSDKLegacyActiveMigrationBetweenChildrenPreservesSuccessfulPrefix(t *testing.T) {
	cfg, c, n, _, _ := sdkLegacyActiveFixture(t)
	old, err := Open(cfg, c, n)
	if err != nil {
		t.Fatal(err)
	}
	completeMultiDelete(t, old, c)
	prefix := multiState(t, old)
	old.Close()
	if prefix.Pending != nil || prefix.Batch == nil || len(prefix.Batch.Results) != 1 {
		t.Fatal("invalid successful prefix fixture")
	}
	cfg.GroupContract = true
	e, err := Open(cfg, c, n)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if !reflect.DeepEqual(multiState(t, e).Batch, prefix.Batch) {
		t.Fatal("migration changed successful prefix")
	}
	step(t, e)
	completeMultiDelete(t, e, c)
	done := multiState(t, e)
	if done.Batch != nil || done.LastBatch == nil || !reflect.DeepEqual(done.LastBatch.Results[0], prefix.Batch.Results[0]) || c.writeCount() != 2 || n.selections != 1 {
		t.Fatal("prefix replayed or selection changed")
	}
}

func TestSDKLegacyActiveMigrationGrowKeepsRegistrationRequirement(t *testing.T) {
	cfg, c, raw, initial := multiFixture(t, 0, 3)
	initial.Close()
	n := &sdkMultiProbe{multiNomad: raw, directory: cfg.Dir}
	old, err := Open(cfg, c, n)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = old.Scale(3); err != nil {
		t.Fatal(err)
	}
	original := multiState(t, old)
	old.Close()
	node := n.nodes["ins-multi2"]
	delete(n.nodes, "ins-multi2")
	cfg.GroupContract = true
	e, err := Open(cfg, c, n)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	_ = e.reconcileOnce()
	state, _, _ := e.Snapshot()
	if state.Pending == nil || state.LastCompletedAt != 0 || state.Pending.ID != original.Pending.ID || c.writeCount() != 1 {
		t.Fatal("old grow completed without original registration requirement")
	}
	n.nodes["ins-multi2"] = node
	step(t, e)
	state = multiState(t, e)
	if state.Pending != nil || state.Batch != nil || state.LastBatch == nil || state.LastBatch.ID != original.Batch.ID || state.LastCompletedAt <= 0 || state.LegacyContract != nil || c.writeCount() != 1 {
		t.Fatal("old grow did not complete once mapping returned")
	}
}
func TestSDKLegacyActiveMigrationRecordedUnknownDeletionResumesPost(t *testing.T) {
	cfg, c, n, _, _ := sdkLegacyActiveFixture(t)
	old, err := Open(cfg, c, n)
	if err != nil {
		t.Fatal(err)
	}
	c.removeUnknown = true
	if err = old.reconcileOnce(); err == nil {
		t.Fatal("did not create uncertain write")
	}
	pending := copyJSON(*old.state.Pending)
	if !pending.Unknown || pending.ActivityID == "" {
		t.Fatal("invalid unknown fixture")
	}
	old.Close()
	c.terminal(true)
	a := c.activities[pending.ActivityID]
	a.Type = "REMOVE_INSTANCES"
	a.StartedAt = pending.StartedAt.UTC().Format(time.RFC3339)
	a.EndedAt = time.Now().UTC().Format(time.RFC3339)
	c.activities[a.ID] = a
	cfg.GroupContract = true
	e, err := Open(cfg, c, n)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if c.writeCount() != 1 || e.state.Pending.ID != pending.ID || !e.state.Pending.Unknown {
		t.Fatal("migration replayed unknown deletion")
	}
	step(t, e)
	if e.state.Pending == nil || e.state.Pending.Phase != "post-scale" {
		t.Fatal("known activity did not preserve original post obligation")
	}
	step(t, e)
	if e.state.Batch == nil || len(e.state.Batch.Results) != 1 || e.state.LastCompletedAt != 0 || e.state.LegacyContract == nil {
		t.Fatal("unknown result lost original batch")
	}
	c.removeUnknown = false
	step(t, e)
	completeMultiDelete(t, e, c)
	if e.state.Batch != nil || e.state.Pending != nil || e.state.LegacyContract != nil || e.state.LastCompletedAt <= 0 || n.selections != 1 || c.writeCount() != 2 {
		t.Fatal("unknown recovery repeated writes or did not finish original batch")
	}
	if err = e.checkRecoveryAudit(); err != nil {
		t.Fatal(err)
	}
}

// Do not infer a cloud deletion from an uncertain SDK drain that has no cloud
// write evidence. Migration preserves that unknown result without replaying it.
func TestSDKLegacyActiveMigrationUncertainDrainRemainsUnchanged(t *testing.T) {
	cfg, c, n, old, raw := sdkLegacyActiveFixture(t)
	var source State
	if err := json.Unmarshal(raw, &source); err != nil {
		t.Fatal(err)
	}
	source.Pending.Phase = "unknown"
	source.Pending.Unknown = true
	source.Pending.NodeModifyIndex = 0
	source.LastEvent = "drain-write-unknown"
	if err := os.WriteFile(filepath.Join(cfg.Dir, "live.json"), jsonBytes(source), 0600); err != nil {
		t.Fatal(err)
	}
	writes := len(n.writes)
	cfg.GroupContract = true
	e, err := Open(cfg, c, n)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	_ = e.reconcileOnce()
	if e.state.Pending == nil || !e.state.Pending.Unknown || e.state.Pending.ID != old.Pending.ID || e.state.Pending.CloudIntent != nil || e.state.Pending.ActivityID != "" || c.writeCount() != 0 || len(n.writes) != writes || e.state.LastCompletedAt != 0 {
		t.Fatal("uncertain SDK drain was reinterpreted or replayed")
	}
	// This is preservation evidence, not automatic drain-outcome recovery.
}
