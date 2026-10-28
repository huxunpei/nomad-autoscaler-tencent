package live

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func TestStartupStorageFIFOProcessHelper(t *testing.T) {
	dir := os.Getenv("NOMAD_STARTUP_FIFO_FIXTURE")
	if dir == "" {
		t.Skip("subprocess helper")
	}
	cfg := Config{Dir: dir, GroupID: testGroup, Region: testRegion, ReconcileInterval: time.Hour, DrainDeadline: time.Minute}
	e, err := Open(cfg, newCloud(), newNomad())
	if e != nil {
		e.Close()
	}
	if err == nil || err.Error() != "UnsafeStateFile" {
		t.Fatal("did not reject FIFO journal", err)
	}
}

func TestStartupStorageFIFOIsRejectedWithoutBlocking(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Mkfifo(filepath.Join(dir, "live.json"), 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestStartupStorageFIFOProcessHelper$")
	cmd.Env = append(os.Environ(), "NOMAD_STARTUP_FIFO_FIXTURE="+dir)
	out, err := cmd.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatal("startup blocked opening FIFO journal")
	}
	if err != nil {
		t.Fatalf("FIFO rejection failed: %v %s", err, out)
	}
}

func TestStartupStorageSymlinkIsTerminalAndUntouched(t *testing.T) {
	for _, name := range []string{"live.json", ".lock"} {
		t.Run(name, func(t *testing.T) {
			cfg := config(t)
			if err := os.Mkdir(cfg.Dir, 0700); err != nil {
				t.Fatal(err)
			}
			target := filepath.Join(cfg.Dir, "outside-target")
			if err := os.WriteFile(target, []byte("unchanged"), 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(target, filepath.Join(cfg.Dir, name)); err != nil {
				t.Fatal(err)
			}
			c := newCloud()
			e, err := Open(cfg, c, newNomad())
			if e != nil {
				e.Close()
			}
			want := "UnsafeStateFile"
			if name == ".lock" {
				want = "UnsafeStateLock"
			}
			raw, _ := os.ReadFile(target)
			if err == nil || err.Error() != want || string(raw) != "unchanged" || c.observeCalls != 0 || c.writeCount() != 0 {
				t.Fatal("unsafe state did not fail before provider access", err)
			}
		})
	}
}
