package live

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadclient"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/tencentcloud"
)

type failedGrowthCrashInput struct {
	Config  Config
	At      string
	Cloud   tencentcloud.Snapshot
	History []tencentcloud.Activity
	Nodes   map[string]nomadclient.Node
}

func TestFailedGrowthCrashProcessHelper(t *testing.T) {
	path := os.Getenv("NOMAD_FAILED_GROWTH_CRASH_FIXTURE")
	if path == "" {
		t.Skip("subprocess helper")
	}
	raw, err := os.ReadFile(path)
	var in failedGrowthCrashInput
	if err != nil || json.Unmarshal(raw, &in) != nil {
		t.Fatal("invalid crash input", err)
	}
	c := &failedGrowthCloud{multiCloud: &multiCloud{fakeCloud: &fakeCloud{snap: in.Cloud}}, history: in.History}
	n := &multiNomad{nodes: in.Nodes, load: map[string]int{}}
	e, err := Open(in.Config, c, n)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	e.recoveryCheckpoint = func(stage string) error {
		if stage == in.At {
			if c.writeCount() != 0 || len(n.writes) != 0 {
				t.Fatal("audit wrote to provider")
			}
			os.Exit(57)
		}
		return nil
	}
	if err = e.reconcileOnce(); err != nil {
		t.Fatal(err)
	}
	t.Fatal("crash checkpoint not reached")
}

func TestFailedGrowthAuditsAbruptExitResumeWithoutProviderReplay(t *testing.T) {
	for _, kind := range []string{"classification", "settlement"} {
		for _, at := range []string{"before-record", "record", "prepared", "journal", "receipt", "cleared"} {
			t.Run(kind+"/"+at, func(t *testing.T) {
				cfg, c, n, e := failedGrowthFixture(t, false)
				id := e.state.Batch.ID
				if kind == "settlement" {
					step(t, e)
					c.snap = copyJSON(c.complete)
				}
				e.Close()
				path := filepath.Join(t.TempDir(), "crash.json")
				data, err := json.Marshal(failedGrowthCrashInput{cfg, at, c.snap, c.history, n.nodes})
				if err != nil {
					t.Fatal(err)
				}
				if err = os.WriteFile(path, data, 0600); err != nil {
					t.Fatal(err)
				}
				cmd := exec.Command(os.Args[0], "-test.run=^TestFailedGrowthCrashProcessHelper$")
				cmd.Env = append(os.Environ(), "NOMAD_FAILED_GROWTH_CRASH_FIXTURE="+path)
				output, err := cmd.CombinedOutput()
				if err == nil || cmd.ProcessState.ExitCode() != 57 {
					t.Fatalf("crash not reached: %v %s", err, output)
				}
				e, err = Open(cfg, c, n)
				if err != nil {
					t.Fatal("cannot restart interrupted grow audit", err)
				}
				defer e.Close()
				step(t, e)
				if kind == "classification" && (e.state.Pending == nil || e.state.Pending.Phase != "failed-grow") {
					t.Fatal("failure not classified")
				}
				if kind == "settlement" && (e.state.Pending != nil || e.state.Batch != nil || e.state.LastBatch == nil || e.state.LastBatch.ID != id || e.state.LastBatch.Phase != "failed") {
					t.Fatal("settlement not resumed")
				}
				if e.state.LastCompletedAt != 0 || c.writeCount() != 1 || len(n.writes) != 0 || e.checkRecoveryAudit() != nil {
					t.Fatal("restart replayed provider or lost audit")
				}
				if _, err = os.Lstat(filepath.Join(cfg.Dir, recoveryMarkerName)); !os.IsNotExist(err) {
					t.Fatal("pending audit marker remains", err)
				}
			})
		}
	}
}
