package live

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestV1ActiveMigrationCrashProcessHelper(t *testing.T) {
	input := os.Getenv("NOMAD_V1_ACTIVE_MIGRATION_CRASH_FIXTURE")
	if input == "" {
		t.Skip("subprocess only")
	}
	raw, err := os.ReadFile(input)
	var in activeMigrationCrashInput
	if err != nil || json.Unmarshal(raw, &in) != nil {
		t.Fatal("bad v1 migration crash fixture")
	}
	cfg := Config{Dir: in.Directory, GroupID: testGroup, Region: testRegion, GroupContract: true, DrainDeadline: time.Minute, ReconcileInterval: time.Hour}
	c := newCloud()
	n := &sdkProbe{fakeNomad: newNomad(), directory: cfg.Dir}
	e := &Engine{cfg: cfg, cloud: c, nomad: n, ctx: context.Background()}
	if err = e.lockDir(); err != nil {
		t.Fatal(err)
	}
	defer e.release()
	if _, err = e.load(); err != nil {
		t.Fatal(err)
	}
	e.recoveryCheckpoint = func(stage string) error {
		if stage == in.At {
			if c.writeCount() != 0 || n.writeCount() != 0 {
				t.Fatal("migration changed providers")
			}
			os.Exit(59)
		}
		return nil
	}
	if err = e.upgradeGroupContract(); err != nil {
		t.Fatal(err)
	}
	t.Fatal("v1 migration missed crash checkpoint")
}

func TestSDKV1MigrationAbruptExitResumesOwnAuditAndOriginalOperation(t *testing.T) {
	for _, at := range []string{"before-record", "record", "prepared", "journal", "receipt", "cleared"} {
		t.Run(at, func(t *testing.T) {
			cfg, c, n, old, raw := sdkV1ActiveFixture(t, "draining")
			cfg.GroupContract = true
			input := filepath.Join(t.TempDir(), "input.json")
			if err := os.WriteFile(input, jsonBytes(activeMigrationCrashInput{cfg.Dir, at}), 0600); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command(os.Args[0], "-test.run=^TestV1ActiveMigrationCrashProcessHelper$")
			cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + os.Getenv("HOME"), "TMPDIR=" + os.Getenv("TMPDIR"), "NOMAD_V1_ACTIVE_MIGRATION_CRASH_FIXTURE=" + input}
			out, err := cmd.CombinedOutput()
			if err == nil || cmd.ProcessState == nil || cmd.ProcessState.ExitCode() != 59 {
				t.Fatalf("v1 migration child missed exit59: %v %s", err, out)
			}
			writes, nomadWrites := c.writeCount(), n.writeCount()
			e, err := Open(cfg, c, n)
			if err != nil {
				t.Fatal("v1 audit restart", err)
			}
			defer e.Close()
			if e.state.Version != 3 || e.state.Batch == nil || e.state.LegacyContract == nil || e.state.LastCompletedAt != 0 || !reflect.DeepEqual(e.state.Pending, old.Pending) || c.writeCount() != writes || n.writeCount() != nomadWrites || e.checkRecoveryAudit() != nil {
				t.Fatal("v1 audit restart changed original work")
			}
			backup, err := os.ReadFile(filepath.Join(cfg.Dir, "live-v1-before-group-contract.backup.json"))
			if err != nil || string(backup) != string(raw) {
				t.Fatal("v1 crash lost source bytes")
			}
			if _, err := os.Lstat(filepath.Join(cfg.Dir, recoveryMarkerName)); !os.IsNotExist(err) {
				t.Fatal("v1 own marker not cleared")
			}
			finishSDKV1Operation(t, e, c, n)
			if c.writeCount() != 1 || n.preCalls != 1 || n.postCalls != 1 || e.checkRecoveryAudit() != nil {
				t.Fatal("v1 crash restart replayed or dropped SDK work")
			}
		})
	}
}
