package live

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadclient"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/tencentcloud"
)

type journalCrashInput struct {
	Directory, At string
	Unsubmitted   bool
	Snapshot      tencentcloud.Snapshot
	Nodes         map[string]nomadclient.Node
}

func TestJournalRetryCrashProcessHelper(t *testing.T) {
	path := os.Getenv("NOMAD_JOURNAL_RETRY_CRASH_FIXTURE")
	if path == "" {
		t.Skip("subprocess helper")
	}
	var in journalCrashInput
	b, err := os.ReadFile(path)
	if err != nil || json.Unmarshal(b, &in) != nil {
		t.Fatal("invalid crash fixture", err)
	}
	cfg := Config{Dir: in.Directory, GroupID: testGroup, Region: testRegion, GroupContract: true, ReconcileInterval: time.Hour, DrainDeadline: time.Minute}
	c := &multiCloud{fakeCloud: &fakeCloud{snap: in.Snapshot}, activities: map[string]tencentcloud.Activity{}}
	n := &multiNomad{nodes: in.Nodes, load: map[string]int{}}
	e, err := Open(cfg, c, n)
	if err != nil {
		t.Fatal(err)
	}
	if in.Unsubmitted {
		e.journalCheckpoint = func(string) error {
			if e.state.Pending != nil && e.state.Pending.Phase == "grow-intent" {
				return code("SimulatedPreCallSyncFailure")
			}
			return nil
		}
		if _, err = e.Scale(2); err == nil {
			t.Fatal("missing initial save failure")
		}
		e.journalCheckpoint = nil
	} else {
		restore := blockRetryJournal(t, cfg.Dir)
		if err = e.reconcileOnce(); err == nil {
			t.Fatal("missing initial save failure")
		}
		restore()
	}
	e.recoveryCheckpoint = func(stage string) error {
		if stage == in.At {
			if c.writeCount() != 0 || len(n.writes) != 0 {
				t.Fatal("child mutated providers")
			}
			os.Exit(49)
		}
		return nil
	}
	if err = e.reconcileOnce(); err != nil {
		t.Fatal(err)
	}
	t.Fatal("crash checkpoint was not reached")
}

func TestJournalRetryAbruptExitResumesQueuedProof(t *testing.T) {
	for _, unsubmitted := range []bool{false, true} {
		for _, at := range []string{"before-record", "record", "prepared", "journal", "receipt", "cleared"} {
			t.Run(fmt.Sprintf("unsubmitted%v/%s", unsubmitted, at), func(t *testing.T) {
				cfg, c, n, e := groupContractFixture(t, 1, 0, 3, false)
				e.Close()
				fixture := filepath.Join(t.TempDir(), "input.json")
				in := journalCrashInput{cfg.Dir, at, unsubmitted, c.snap, n.nodes}
				if err := os.WriteFile(fixture, jsonBytes(in), 0600); err != nil {
					t.Fatal(err)
				}
				cmd := exec.Command(os.Args[0], "-test.run=^TestJournalRetryCrashProcessHelper$")
				cmd.Env = append(os.Environ(), "NOMAD_JOURNAL_RETRY_CRASH_FIXTURE="+fixture)
				output, err := cmd.CombinedOutput()
				if err == nil || cmd.ProcessState.ExitCode() != 49 {
					t.Fatalf("child did not reach direct exit: %v %s", err, output)
				}
				e = openMulti(t, cfg, c, n)
				defer e.Close()
				step(t, e)
				s, ready, err := e.Snapshot()
				if err != nil || !ready || s.Pending != nil || s.Batch != nil || s.LastCompletedAt != 0 || s.RecoveryAudit == nil || c.writeCount() != 0 || len(n.writes) != 0 {
					t.Fatal("restart did not resume queued local proof", ready, err)
				}
				if _, err = os.Lstat(filepath.Join(cfg.Dir, journalRetryQueueName)); !os.IsNotExist(err) {
					t.Fatal("queue not cleared")
				}
				if err = e.checkRecoveryAudit(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}
