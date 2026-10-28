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

type legacyCustomProbe struct{ *sdkProbe }

func (n legacyCustomProbe) InstanceMetaKey() string { return "cloud_instance_id" }

func TestLegacyCustomMigrationCrashProcessHelper(t *testing.T) {
	input := os.Getenv("NOMAD_CUSTOM_MIGRATION_CRASH_FIXTURE")
	if input == "" {
		t.Skip("subprocess only")
	}
	raw, err := os.ReadFile(input)
	var in struct {
		activeMigrationCrashInput
		Deadline *time.Duration
	}
	if err != nil || json.Unmarshal(raw, &in) != nil {
		t.Fatal("bad custom migration fixture")
	}
	cfg := Config{Dir: in.Directory, GroupID: testGroup, Region: testRegion, GroupContract: true, DrainDeadline: 15 * time.Minute, ReconcileInterval: time.Hour}
	if in.Deadline != nil {
		cfg.DrainDeadline = *in.Deadline
	}
	c := newCloud()
	n := legacyCustomProbe{&sdkProbe{fakeNomad: newNomad(), directory: cfg.Dir}}
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
				t.Fatal("migration wrote providers")
			}
			os.Exit(59)
		}
		return nil
	}
	if err = e.upgradeGroupContract(); err != nil {
		t.Fatal(err)
	}
	t.Fatal("custom migration missed checkpoint")
}
func TestLegacyCustomMigrationAbruptExitRetainsOriginalFalseSDKLifecycle(t *testing.T) {
	for _, at := range []string{"before-record", "record", "prepared", "journal", "receipt", "cleared"} {
		t.Run(at, func(t *testing.T) {
			old, model, raw := historicalCustomInput(t, "v1-draining", 5*time.Second)
			cfg := Config{Dir: filepath.Join(t.TempDir(), "journal"), GroupID: testGroup, Region: testRegion, GroupContract: true, DrainDeadline: 15 * time.Minute, ReconcileInterval: time.Hour, DisableReconciliation: true}
			if err := os.Mkdir(cfg.Dir, 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(cfg.Dir, "live.json"), raw, 0600); err != nil {
				t.Fatal(err)
			}
			input := filepath.Join(t.TempDir(), "input.json")
			if err := os.WriteFile(input, jsonBytes(activeMigrationCrashInput{cfg.Dir, at}), 0600); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command(os.Args[0], "-test.run=^TestLegacyCustomMigrationCrashProcessHelper$")
			cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + os.Getenv("HOME"), "TMPDIR=" + os.Getenv("TMPDIR"), "NOMAD_CUSTOM_MIGRATION_CRASH_FIXTURE=" + input}
			out, err := cmd.CombinedOutput()
			if err == nil || cmd.ProcessState == nil || cmd.ProcessState.ExitCode() != 59 {
				t.Fatalf("custom migration missed exit59: %v %s", err, out)
			}
			c := &multiCloud{fakeCloud: &fakeCloud{snap: model.Snapshot}, activities: model.Activities}
			n := legacyCustomProbe{&sdkProbe{fakeNomad: &fakeNomad{node: model.Nodes[0]}, directory: cfg.Dir}}
			e, err := Open(cfg, c, n)
			if err != nil {
				t.Fatal("custom migration crash restart", err)
			}
			defer e.Close()
			op := e.state.Pending
			if op == nil || op.LegacyOrigin == nil || op.SDKLifecycle || op.ID != old.Pending.ID || !op.StartedAt.Equal(old.Pending.StartedAt) || c.writeCount() != 0 || n.writeCount() != 0 || n.preCalls != 0 || e.checkRecoveryAudit() != nil {
				t.Fatal("crash replayed or relabeled original drain")
			}
			backup, err := os.ReadFile(filepath.Join(cfg.Dir, "live-v1-before-group-contract.backup.json"))
			if err != nil || !reflect.DeepEqual(backup, raw) {
				t.Fatal("original backup changed")
			}
			if _, err = os.Lstat(filepath.Join(cfg.Dir, recoveryMarkerName)); !os.IsNotExist(err) {
				t.Fatal("own automatic marker not cleared")
			}
			e.Close()
			r, err := OpenRecovery(cfg, c, n)
			if err != nil {
				t.Fatal("recovery reader after crash", err)
			}
			r.Close()
		})
	}
}
