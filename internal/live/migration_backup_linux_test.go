package live

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"reflect"
	"syscall"
	"testing"
	"time"
)

type legacyBackupLimitInput struct {
	Directory     string
	GroupContract bool
	MaxCapacity   int64
}

func TestLegacyBackupLimitProcessHelper(t *testing.T) {
	path := os.Getenv("NOMAD_LEGACY_BACKUP_LIMIT_FIXTURE")
	if path == "" {
		t.Skip("subprocess helper")
	}
	raw, err := os.ReadFile(path)
	var in legacyBackupLimitInput
	if err != nil || json.Unmarshal(raw, &in) != nil {
		t.Fatal("invalid limit fixture", err)
	}
	if err = os.WriteFile(filepath.Join(in.Directory, "limit-ready"), []byte("ok"), 0600); err != nil {
		t.Fatal(err)
	}
	var limit syscall.Rlimit
	if err = syscall.Getrlimit(syscall.RLIMIT_FSIZE, &limit); err != nil {
		t.Fatal(err)
	}
	limit.Cur = 128
	signal.Ignore(syscall.SIGXFSZ)
	if err = syscall.Setrlimit(syscall.RLIMIT_FSIZE, &limit); err != nil {
		t.Fatal(err)
	}
	cfg := Config{Dir: in.Directory, GroupID: testGroup, Region: testRegion, GroupContract: in.GroupContract, MaxCapacity: in.MaxCapacity, ReconcileInterval: time.Hour, DrainDeadline: time.Minute}
	e, err := Open(cfg, newCloud(), newNomad())
	if e != nil {
		e.Close()
	}
	if err == nil || err.Error() != "LegacyBackupFailed" {
		t.Fatal("did not reach limited backup write", err)
	}
}

func TestLegacyBackupPartialWriteNeverPublishesFinalName(t *testing.T) {
	for _, mode := range []string{"v1-v2", "v1-v3", "v2-v3"} {
		t.Run(mode, func(t *testing.T) {
			cfg := config(t)
			c, n := newCloud(), newNomad()
			if mode == "v2-v3" {
				cfg.MaxCapacity = 2
				c.snap.Group.Max = 2
			}
			e := openTest(t, cfg, c, n)
			e.Close()
			before, err := os.ReadFile(filepath.Join(cfg.Dir, "live.json"))
			if err != nil || len(before) <= 128 {
				t.Fatal("invalid source fixture", err)
			}
			group := mode != "v1-v2"
			backup := "live-v1.backup.json"
			if group {
				version := 1
				if mode == "v2-v3" {
					version = 2
				}
				backup = fmt.Sprintf("live-v%d-before-group-contract.backup.json", version)
			}
			fixture := filepath.Join(t.TempDir(), "limit-input.json")
			if err = os.WriteFile(fixture, jsonBytes(legacyBackupLimitInput{cfg.Dir, group, 2}), 0600); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command(os.Args[0], "-test.run=^TestLegacyBackupLimitProcessHelper$")
			cmd.Env = append(os.Environ(), "NOMAD_LEGACY_BACKUP_LIMIT_FIXTURE="+fixture)
			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("backup fault helper failed: %v %s", err, out)
			}
			if _, err = os.Stat(filepath.Join(cfg.Dir, "limit-ready")); err != nil {
				t.Fatal("write limit was not applied")
			}
			after, err := os.ReadFile(filepath.Join(cfg.Dir, "live.json"))
			if err != nil || string(before) != string(after) {
				t.Fatal("backup failure modified source state")
			}
			info, err := os.Lstat(filepath.Join(cfg.Dir, backup))
			if err == nil {
				t.Fatal("partial backup was published at final name", info.Size())
			}
			if !os.IsNotExist(err) {
				t.Fatal(err)
			}
			// The soft limit affected only the child. Retrying a valid idle
			// migration now needs no manual cleanup of a partial backup.
			cfg.GroupContract = group
			if !group {
				cfg.MaxCapacity = 2
				c.snap.Group.Max = 2
			}
			upgraded, err := Open(cfg, c, n)
			if err != nil {
				t.Fatal("retry after backup write failure", err)
			}
			upgraded.Close()
			saved, err := os.ReadFile(filepath.Join(cfg.Dir, backup))
			if err != nil || string(saved) != string(before) {
				t.Fatal("retry lost exact source bytes", err)
			}
			if c.writeCount() != 0 || n.writeCount() != 0 {
				t.Fatal("migration submitted provider mutation")
			}

		})
	}
}

func legacyMigrationFixture(t *testing.T, version int) (Config, *fakeCloud, *fakeNomad, string, []byte) {
	t.Helper()
	cfg := config(t)
	c, n := newCloud(), newNomad()
	if version == 2 {
		cfg.MaxCapacity = 2
		c.snap.Group.Max = 2
	}
	e := openTest(t, cfg, c, n)
	e.Close()
	raw, err := os.ReadFile(filepath.Join(cfg.Dir, "live.json"))
	if err != nil {
		t.Fatal(err)
	}
	return cfg, c, n, fmt.Sprintf("live-v%d-before-group-contract.backup.json", version), raw
}

func TestLegacyBackupReuseAndConflictPreserveSource(t *testing.T) {
	for _, version := range []int{1, 2} {
		for _, mode := range []string{"exact", "readonly", "different", "public", "directory", "symlink", "fifo"} {
			t.Run(fmt.Sprintf("v%d/%s", version, mode), func(t *testing.T) {
				cfg, c, n, name, before := legacyMigrationFixture(t, version)
				path := filepath.Join(cfg.Dir, name)
				switch mode {
				case "directory":
					if err := os.Mkdir(path, 0700); err != nil {
						t.Fatal(err)
					}
				case "symlink":
					target := filepath.Join(cfg.Dir, "backup-target")
					if err := os.WriteFile(target, before, 0600); err != nil {
						t.Fatal(err)
					}
					if err := os.Symlink(target, path); err != nil {
						t.Fatal(err)
					}
				case "fifo":
					if err := syscall.Mkfifo(path, 0600); err != nil {
						t.Fatal(err)
					}
				default:
					data := before
					if mode == "different" {
						data = []byte("different")
					}
					if err := os.WriteFile(path, data, 0600); err != nil {
						t.Fatal(err)
					}
					if mode == "public" {
						if err := os.Chmod(path, 0644); err != nil {
							t.Fatal(err)
						}
					}
					if mode == "readonly" {
						if err := os.Chmod(path, 0400); err != nil {
							t.Fatal(err)
						}
					}
				}
				reads, writes := c.observeCalls, c.writeCount()
				cfg.GroupContract = true
				migrated, err := Open(cfg, c, n)
				if migrated != nil {
					migrated.Close()
				}
				if mode == "exact" || mode == "readonly" {
					if err != nil {
						t.Fatal("valid exact backup was not reusable", err)
					}
					data, err := os.ReadFile(path)
					if err != nil || string(data) != string(before) {
						t.Fatal("replaced exact backup")
					}
					if mode == "readonly" {
						info, _ := os.Stat(path)
						if info.Mode().Perm() != 0400 {
							t.Fatal("changed backup permissions")
						}
					}
				} else {
					if err == nil || err.Error() != "LegacyBackupConflict" {
						t.Fatal("accepted unsafe or conflicting backup", err)
					}
					data, err := os.ReadFile(filepath.Join(cfg.Dir, "live.json"))
					if err != nil || string(data) != string(before) || c.observeCalls != reads {
						t.Fatal("rejected migration changed or observed source", err)
					}
				}
				if c.writeCount() != writes || n.writeCount() != 0 {
					t.Fatal("backup handling mutated provider")
				}
			})
		}
	}
}

func TestLegacyMigrationRefusesSourceChangedAfterLoad(t *testing.T) {
	cfg, _, _, name, before := legacyMigrationFixture(t, 2)
	cfg.GroupContract = true
	e := &Engine{cfg: cfg}
	if err := e.lockDir(); err != nil {
		t.Fatal(err)
	}
	defer e.release()
	if exists, err := e.load(); err != nil || !exists {
		t.Fatal(err)
	}
	var changed State
	if err := json.Unmarshal(before, &changed); err != nil {
		t.Fatal(err)
	}
	changed.LastEvent = "external-after-load"
	raw := jsonBytes(changed)
	if err := os.WriteFile(filepath.Join(cfg.Dir, "live.json"), raw, 0600); err != nil {
		t.Fatal(err)
	}
	if err := e.upgradeGroupContract(); err == nil || err.Error() != "LegacyJournalConflict" {
		t.Fatal("migrated a stale loaded snapshot", err)
	}
	after, err := os.ReadFile(filepath.Join(cfg.Dir, "live.json"))
	if err != nil || string(after) != string(raw) || e.state.Version != 2 {
		t.Fatal("source conflict was overwritten")
	}
	if _, err = os.Lstat(filepath.Join(cfg.Dir, name)); !os.IsNotExist(err) {
		t.Fatal("published stale-source backup")
	}
}

func TestLegacyMigrationPreservesCompletedAuditAndMissingTime(t *testing.T) {
	cfg, c, n := recoveryFixture(t, true)
	r := recoverySession(t, cfg, c, n)
	plan, err := r.Preview(context.Background(), r.e.state.Pending.ActivityID, "operator", "legacy-idle-migration")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = r.Apply(context.Background(), plan, plan.Digest); err != nil {
		t.Fatal(err)
	}
	old := copyJSON(r.e.state)
	r.Close()
	before, err := os.ReadFile(filepath.Join(cfg.Dir, "live.json"))
	if err != nil {
		t.Fatal(err)
	}
	writes, nwrites := c.writeCount(), n.writeCount()
	cfg.GroupContract = true
	migrated, err := Open(cfg, c, n)
	if err != nil {
		t.Fatal(err)
	}
	defer migrated.Close()
	current, _, err := migrated.Snapshot()
	if err != nil || current.Version != 3 || current.MaxCapacity != 0 || current.Sequence != old.Sequence || !reflect.DeepEqual(current.LastOperation, old.LastOperation) || !reflect.DeepEqual(current.RecoveryAudit, old.RecoveryAudit) || current.LastCompletedAt != 0 {
		t.Fatal("rewrote old history or invented completion time", err)
	}
	if err = migrated.checkRecoveryAudit(); err != nil {
		t.Fatal(err)
	}
	backup, err := os.ReadFile(filepath.Join(cfg.Dir, "live-v1-before-group-contract.backup.json"))
	if err != nil || string(backup) != string(before) {
		t.Fatal("audit source backup mismatch")
	}
	if c.writeCount() != writes || n.writeCount() != nwrites {
		t.Fatal("migration repeated an old write")
	}
}
