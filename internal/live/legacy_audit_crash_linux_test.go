package live

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadclient"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/tencentcloud"
)

type legacyAuditCrashInput struct {
	Config     Config
	At         string
	Cloud      tencentcloud.Snapshot
	Activities map[string]tencentcloud.Activity
	Nodes      map[string]nomadclient.Node
}

func TestLegacyAuditCrashProcessHelper(t *testing.T) {
	path := os.Getenv("NOMAD_LEGACY_AUDIT_CRASH_FIXTURE")
	if path == "" {
		t.Skip("subprocess helper")
	}
	raw, err := os.ReadFile(path)
	var in legacyAuditCrashInput
	if err != nil || json.Unmarshal(raw, &in) != nil {
		t.Fatal("invalid crash input", err)
	}
	c := &multiCloud{fakeCloud: &fakeCloud{snap: in.Cloud}, activities: in.Activities}
	n := &sdkMultiProbe{multiNomad: &multiNomad{nodes: in.Nodes}, directory: in.Config.Dir}
	r, err := OpenRecovery(in.Config, c, n)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	r.delay = time.Millisecond
	r.automatic = true
	op := r.e.state.Pending
	p, err := r.Preview(context.Background(), op.ActivityID, "automatic-reconciler", "recorded-activity/"+op.ID)
	if err != nil {
		t.Fatal(err)
	}
	r.checkpoint = func(at string) error {
		if at == in.At {
			if c.writeCount() != 0 || len(n.writes) != 0 {
				t.Fatal("audit wrote to provider")
			}
			os.Exit(56)
		}
		return nil
	}
	if _, err = r.Apply(context.Background(), p, p.Digest); err != nil {
		t.Fatal(err)
	}
	t.Fatal("crash checkpoint not reached")
}

func TestLegacyAutomaticAuditAbruptExitMigratesBeforeContinuing(t *testing.T) {
	for _, at := range []string{"before-record", "record", "prepared", "journal", "receipt", "cleared"} {
		t.Run(at, func(t *testing.T) {
			cfg, c, n, old := legacyAuditFixture(t, "before-record")
			in := legacyAuditCrashInput{cfg, at, c.snap, c.activities, n.nodes}
			path := filepath.Join(t.TempDir(), "crash.json")
			data, err := json.Marshal(in)
			if err != nil {
				t.Fatal(err)
			}
			if err = os.WriteFile(path, data, 0600); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command(os.Args[0], "-test.run=^TestLegacyAuditCrashProcessHelper$")
			cmd.Env = append(os.Environ(), "NOMAD_LEGACY_AUDIT_CRASH_FIXTURE="+path)
			output, err := cmd.CombinedOutput()
			if err == nil || cmd.ProcessState.ExitCode() != 56 {
				t.Fatalf("crash not reached: %v %s", err, output)
			}
			cfg.GroupContract = true
			e, err := Open(cfg, c, n)
			if err != nil {
				t.Fatal("abrupt exit blocks migration", err)
			}
			defer e.Close()
			if c.writeCount() != 1 || len(n.writes) != 1 || n.selections != 1 {
				t.Fatal("startup replayed external mutation")
			}
			for i := 0; i < 5; i++ {
				if e.state.Batch != nil && len(e.state.Batch.Results) == 1 && e.state.Pending != nil {
					break
				}
				step(t, e)
			}
			if e.state.Batch == nil || e.state.Batch.ID != old.Batch.ID || len(e.state.Batch.Results) != 1 || e.state.LastCompletedAt != 0 || e.state.LegacyContract == nil || e.checkRecoveryAudit() != nil || c.writeCount() != 1 {
				t.Fatal("migration lost successful prefix or original contract")
			}
			c.removeUnknown = false
			completeMultiDelete(t, e, c)
			if e.state.Batch != nil || e.state.Pending != nil || e.state.LastBatch == nil || e.state.LastBatch.ID != old.Batch.ID || e.state.LastCompletedAt <= 0 || n.selections != 1 || c.writeCount() != 2 {
				t.Fatal("original batch did not finish once")
			}
		})
	}
}
