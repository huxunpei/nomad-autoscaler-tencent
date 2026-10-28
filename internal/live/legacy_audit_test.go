package live

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

// Produce an actual old automatic transaction, not a renamed manual record.
func legacyAuditFixture(t *testing.T, at string) (Config, *multiCloud, *sdkMultiProbe, State) {
	return legacyAuditFixtureMode(t, at, true)
}

func legacyAuditFixtureMode(t *testing.T, at string, automatic bool) (Config, *multiCloud, *sdkMultiProbe, State) {
	t.Helper()
	cfg, c, n, _, _ := sdkLegacyActiveFixture(t)
	e, err := Open(cfg, c, n)
	if err != nil {
		t.Fatal(err)
	}
	c.removeUnknown = true
	if err = e.reconcileOnce(); err != ErrUnknown {
		t.Fatal("missing unknown cloud result", err)
	}
	before := multiStateAllowUnknown(t, e)
	e.Close()
	c.terminal(true)
	a := c.activities[before.Pending.ActivityID]
	a.Type = "REMOVE_INSTANCES"
	a.StartedAt = before.Pending.StartedAt.UTC().Format(time.RFC3339)
	a.EndedAt = time.Now().UTC().Format(time.RFC3339)
	c.activities[a.ID] = a
	r, err := OpenRecovery(cfg, c, n)
	if err != nil {
		t.Fatal(err)
	}
	r.delay = time.Millisecond
	r.automatic = automatic
	p, err := r.Preview(context.Background(), a.ID, "automatic-reconciler", "recorded-activity/"+before.Pending.ID)
	if err != nil {
		r.Close()
		t.Fatal(err)
	}
	r.checkpoint = func(stage string) error {
		if stage == at {
			return code("LegacyAuditInterrupted")
		}
		return nil
	}
	if _, err = r.Apply(context.Background(), p, p.Digest); err == nil {
		r.Close()
		t.Fatal("interruption not reached")
	}
	r.Close()
	return cfg, c, n, before
}

type legacyAuditChangedPolicy struct{ *sdkMultiProbe }

func (n legacyAuditChangedPolicy) PolicyDigest() string { return strings.Repeat("d", 64) }

type legacyAuditChangedMapping struct{ *sdkMultiProbe }

func (n legacyAuditChangedMapping) InstanceMetaKey() string { return "other_instance_id" }

func legacyAuditFiles(t *testing.T, dir string) map[string]string {
	t.Helper()
	out := map[string]string{}
	if err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.Mode().IsRegular() {
			b, e := os.ReadFile(path)
			if e != nil {
				return e
			}
			out[path] = sha(b)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestLegacyAutomaticAuditRejectsDifferentContractWithoutMutation(t *testing.T) {
	for _, mode := range []string{"group", "region", "drain", "protection", "policy", "mapping", "missing-sdk", "manual-origin", "old-capacity"} {
		t.Run(mode, func(t *testing.T) {
			cfg, c, n, _ := legacyAuditFixtureMode(t, "prepared", mode != "manual-origin")
			before := legacyAuditFiles(t, cfg.Dir)
			writes, drains := c.writeCount(), len(n.writes)
			cfg.GroupContract = true
			var adapter Nomad = n
			switch mode {
			case "group":
				cfg.GroupID = "asg-another"
			case "region":
				cfg.Region = "ap-shanghai"
			case "drain":
				cfg.DrainDeadline += time.Second
			case "protection":
				cfg.ScaleInProtection = !cfg.ScaleInProtection
			case "policy":
				adapter = legacyAuditChangedPolicy{n}
			case "mapping":
				adapter = legacyAuditChangedMapping{n}
			case "missing-sdk":
				adapter = n.multiNomad
			case "old-capacity":
				cfg.GroupContract = false
				cfg.MaxCapacity = 4
			}
			e, err := Open(cfg, c, adapter)
			if err == nil {
				e.Close()
				t.Fatal("different contract adopted the old transaction")
			}
			if !reflect.DeepEqual(before, legacyAuditFiles(t, cfg.Dir)) || c.writeCount() != writes || len(n.writes) != drains {
				t.Fatal("rejected recovery changed evidence or external state")
			}
		})
	}
}

func TestLegacyAutomaticAuditReceiptFailureRetainsRecoverableMarker(t *testing.T) {
	cfg, c, n, _ := legacyAuditFixture(t, "journal")
	markerBytes, err := os.ReadFile(filepath.Join(cfg.Dir, recoveryMarkerName))
	var marker recoveryMarker
	if err != nil || decodeStrict(markerBytes, &marker) != nil {
		t.Fatal("missing marker", err)
	}
	blocked := resultPath(cfg.Dir, marker.Audit)
	if err = os.Mkdir(blocked, 0700); err != nil {
		t.Fatal(err)
	}
	before := legacyAuditFiles(t, cfg.Dir)
	cfg.GroupContract = true
	if e, err := Open(cfg, c, n); err == nil {
		e.Close()
		t.Fatal("unsafe receipt accepted")
	}
	if !reflect.DeepEqual(before, legacyAuditFiles(t, cfg.Dir)) {
		t.Fatal("failed bookkeeping changed evidence")
	}
	if err = os.Remove(blocked); err != nil {
		t.Fatal(err)
	}
	e, err := Open(cfg, c, n)
	if err != nil {
		t.Fatal("repaired storage cannot migrate", err)
	}
	defer e.Close()
	if e.state.Version != 3 || e.state.LegacyContract == nil || e.checkRecoveryAudit() != nil || c.writeCount() != 1 || len(n.writes) != 1 {
		t.Fatal("repair lost contract or replayed external writes")
	}
}

func multiStateAllowUnknown(t *testing.T, e *Engine) State {
	t.Helper()
	s, _, err := e.Snapshot()
	if err != ErrUnknown {
		t.Fatal("unknown state missing", err)
	}
	return s
}

func TestLegacyAutomaticAuditMigratesAfterBookkeeping(t *testing.T) {
	for _, at := range []string{"before-record", "record", "prepared", "journal", "receipt", "cleared"} {
		t.Run(at, func(t *testing.T) {
			cfg, c, n, old := legacyAuditFixture(t, at)
			writes, drains := c.writeCount(), len(n.writes)
			cfg.GroupContract = true
			e, err := Open(cfg, c, n)
			if err != nil {
				t.Fatal("legacy automatic audit blocks migration", err)
			}
			defer e.Close()
			for i := 0; i < 5; i++ {
				if e.state.Batch != nil && len(e.state.Batch.Results) == 1 && e.state.Pending != nil {
					break
				}
				if err = e.reconcileOnce(); err != nil {
					t.Fatal(err)
				}
			}
			s := multiState(t, e)
			if s.Version != 3 || s.LastCompletedAt != 0 || s.Batch == nil || s.Batch.ID != old.Batch.ID || len(s.Batch.Results) != 1 || s.Pending == nil || s.Pending.InstanceID != old.Batch.Selected[1].InstanceID || n.selections != 1 || c.writeCount() != writes || len(n.writes) != drains+1 || e.checkRecoveryAudit() != nil {
				t.Fatalf("lost original contract or replayed deletion: writes=%d drains=%d state=%+v", c.writeCount(), len(n.writes), s)
			}
			if _, err = os.Lstat(filepath.Join(cfg.Dir, recoveryMarkerName)); !os.IsNotExist(err) {
				t.Fatal("old audit marker remains", err)
			}
			c.removeUnknown = false
			completeMultiDelete(t, e, c)
			s = multiState(t, e)
			if s.Batch != nil || s.Pending != nil || s.LastBatch == nil || s.LastBatch.ID != old.Batch.ID || s.LastCompletedAt <= 0 || c.writeCount() != writes+1 || n.selections != 1 {
				t.Fatal("original batch failed to finish")
			}
		})
	}
}
