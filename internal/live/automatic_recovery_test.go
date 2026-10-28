package live

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRecordedUnknownRemovalAutomaticallyReconciles(t *testing.T) {
	cfg, c, n := recoveryFixture(t, true)
	writes := c.writeCount()
	nomadWrites := n.writeCount()
	e, err := Open(cfg, c, n)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(e.Close)
	if err = e.reconcileOnce(); err != nil {
		t.Fatal(err)
	}
	s, _, err := e.Snapshot()
	if err != nil || s.Pending != nil || s.ManualReview || s.RecoveryAudit == nil {
		t.Fatalf("known successful removal still requires manual recovery: %v pending=%v", err, s.Pending)
	}
	if c.writeCount() != writes || n.writeCount() != nomadWrites {
		t.Fatal("replayed provider mutation during automatic recovery")
	}
	record, err := readRecord(cfg.Dir, *s.RecoveryAudit)
	if err != nil {
		t.Fatal(err)
	}
	if record.Plan.Operator != "automatic-reconciler" {
		t.Fatal("automatic action not identified in audit")
	}
	if _, err = os.Stat(filepath.Join(cfg.Dir, recoveryMarkerName)); !os.IsNotExist(err) {
		t.Fatal("unfinished recovery transaction")
	}
	e.Close()
	e, err = Open(cfg, c, n)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(e.Close)
	s, ready, err := e.Snapshot()
	if err != nil || !ready || s.Pending != nil {
		t.Fatal("recovered state did not survive restart", ready, err)
	}
}

func TestUnknownRemovalDoesNotAutoResolveWithoutAuthoritativeEvidence(t *testing.T) {
	for _, mode := range []string{"still-running", "wrong-instance", "wrong-activity-type", "missing-activity-time"} {
		t.Run(mode, func(t *testing.T) {
			cfg, c, n := recoveryFixture(t, true)
			switch mode {
			case "still-running":
				c.activity.Status = "RUNNING"
			case "wrong-instance":
				c.activity.RelatedInstances[0].ID = "ins-unrelated"
			case "wrong-activity-type":
				c.activity.Type = "SCALE_OUT"
			case "missing-activity-time":
				c.activity.EndedAt = ""
			}
			writes := c.writeCount()
			e, err := Open(cfg, c, n)
			if err != nil {
				t.Fatal(err)
			}
			defer e.Close()
			_ = e.reconcileOnce()
			s, ready, err := e.Snapshot()
			if err != ErrUnknown || ready || s.Pending == nil || !s.Pending.Unknown || s.RecoveryAudit != nil || c.writeCount() != writes {
				t.Fatal("unsafe automatic recovery", ready, err)
			}
		})
	}
}
