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

type activeMigrationCrashInput struct{ Directory, At string }

func TestActiveMigrationCrashProcessHelper(t *testing.T) {
	path := os.Getenv("NOMAD_ACTIVE_MIGRATION_CRASH_FIXTURE")
	if path == "" {
		t.Skip("subprocess helper")
	}
	raw, err := os.ReadFile(path)
	var in activeMigrationCrashInput
	if err != nil || json.Unmarshal(raw, &in) != nil {
		t.Fatal("bad migration crash fixture")
	}
	cfg := Config{Dir: in.Directory, GroupContract: true, GroupID: testGroup, Region: testRegion, ReconcileInterval: time.Hour, DrainDeadline: time.Minute}
	c := newCloud()
	n := &sdkMultiProbe{multiNomad: &multiNomad{}, directory: cfg.Dir}
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
			if c.writeCount() != 0 || len(n.writes) != 0 {
				t.Fatal("migration wrote provider")
			}
			os.Exit(51)
		}
		return nil
	}
	if err = e.upgradeGroupContract(); err != nil {
		t.Fatal(err)
	}
	t.Fatal("migration did not reach abrupt exit")
}
func TestSDKLegacyActiveMigrationAbruptExitResumesOwnTransaction(t *testing.T) {
	for _, at := range []string{"before-record", "record", "prepared", "journal", "receipt", "cleared"} {
		t.Run(at, func(t *testing.T) {
			cfg, c, n, old, raw := sdkLegacyActiveFixture(t)
			cfg.GroupContract = true
			input := filepath.Join(t.TempDir(), "input.json")
			if err := os.WriteFile(input, jsonBytes(activeMigrationCrashInput{cfg.Dir, at}), 0600); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command(os.Args[0], "-test.run=^TestActiveMigrationCrashProcessHelper$")
			cmd.Env = append(os.Environ(), "NOMAD_ACTIVE_MIGRATION_CRASH_FIXTURE="+input)
			output, err := cmd.CombinedOutput()
			if err == nil || cmd.ProcessState.ExitCode() != 51 {
				t.Fatalf("migration child missed crash checkpoint: %v %s", err, output)
			}
			writes := len(n.writes)
			e, err := Open(cfg, c, n)
			if err != nil {
				t.Fatal("migration restart", err)
			}
			s := multiState(t, e)
			if s.Version != 3 || s.LastCompletedAt != 0 || !reflect.DeepEqual(s.Batch, old.Batch) || !reflect.DeepEqual(s.Pending, old.Pending) || s.RecoveryAudit == nil || e.checkRecoveryAudit() != nil || len(n.writes) != writes || c.writeCount() != 0 {
				t.Fatal("migration restart changed original work")
			}
			backup, err := os.ReadFile(filepath.Join(cfg.Dir, "live-v2-before-group-contract.backup.json"))
			if err != nil || string(backup) != string(raw) {
				t.Fatal("migration lost source bytes")
			}
			if _, err = os.Lstat(filepath.Join(cfg.Dir, recoveryMarkerName)); !os.IsNotExist(err) {
				t.Fatal("migration marker retained")
			}
			completeMultiDelete(t, e, c)
			e.Close()
			e, err = Open(cfg, c, n)
			if err != nil {
				t.Fatal(err)
			}
			defer e.Close()
			step(t, e)
			completeMultiDelete(t, e, c)
			s = multiState(t, e)
			if s.Batch != nil || s.Pending != nil || s.LastBatch == nil || s.LastBatch.ID != old.Batch.ID || s.LastCompletedAt <= 0 || c.writeCount() != 2 || n.selections != 1 {
				t.Fatal("crash restart did not complete original batch")
			}
		})
	}
}
