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

type postCrashInput struct {
	Directory, At, External string
	Transaction             int
	Snapshot                tencentcloud.Snapshot
	Activities              map[string]tencentcloud.Activity
	History                 []tencentcloud.Activity
	Nodes                   map[string]nomadclient.Node
}
type postCrashExternal struct {
	Purged                                     map[string]bool
	Purges, PostCalls, CloudWrites, NodeWrites int
}

func TestSDKPostAuditCrashProcessHelper(t *testing.T) {
	path := os.Getenv("NOMAD_POST_AUDIT_CRASH_FIXTURE")
	if path == "" {
		t.Skip("subprocess helper")
	}
	var in postCrashInput
	b, err := os.ReadFile(path)
	if err != nil || json.Unmarshal(b, &in) != nil {
		t.Fatal("invalid crash fixture", err)
	}
	cfg := Config{Dir: in.Directory, GroupID: testGroup, Region: testRegion, ReconcileInterval: time.Hour, DrainDeadline: time.Minute, GroupContract: true}
	c := &settlementCloud{multiCloud: &multiCloud{fakeCloud: &fakeCloud{snap: in.Snapshot}, activities: in.Activities}, history: in.History}
	n := &settlementNomad{multiNomad: &multiNomad{nodes: in.Nodes, load: map[string]int{}}, dir: in.Directory}
	e, err := Open(cfg, c, n)
	if err != nil {
		t.Fatal(err)
	}
	ordinal := 0
	e.recoveryCheckpoint = func(stage string) error {
		if stage == "before-record" {
			ordinal++
		}
		if ordinal == in.Transaction && stage == in.At {
			// Preserve the synthetic external service state across the child death.
			// The journal is left exactly as the real transaction wrote it.
			out := postCrashExternal{n.purged, n.purges, n.postCalls, c.writeCount(), len(n.writes)}
			if err := os.WriteFile(in.External, jsonBytes(out), 0600); err != nil {
				return err
			}
			os.Exit(47) // deliberately bypass Close and every deferred cleanup
		}
		return nil
	}
	if err = e.reconcileOnce(); err != nil {
		t.Fatal(err)
	}
	t.Fatal("crash checkpoint was not reached")
}

func TestSDKPostAuditAbruptProcessExitResumes(t *testing.T) {
	for _, failed := range []bool{false, true} {
		for _, txn := range []int{1, 2} {
			for _, at := range []string{"prepared", "journal", "receipt"} {
				t.Run(fmt.Sprintf("failed%v/txn%d/%s", failed, txn, at), func(t *testing.T) {
					cfg, c, n, e, _ := postAuditFixture(t, failed)
					e.Close()
					fixture := filepath.Join(t.TempDir(), "input.json")
					external := filepath.Join(t.TempDir(), "external.json")
					in := postCrashInput{cfg.Dir, at, external, txn, c.snap, c.activities, c.history, n.nodes}
					if err := os.WriteFile(fixture, jsonBytes(in), 0600); err != nil {
						t.Fatal(err)
					}
					cmd := exec.Command(os.Args[0], "-test.run=^TestSDKPostAuditCrashProcessHelper$")
					cmd.Env = append(os.Environ(), "NOMAD_POST_AUDIT_CRASH_FIXTURE="+fixture)
					output, err := cmd.CombinedOutput()
					if err == nil || cmd.ProcessState.ExitCode() != 47 {
						t.Fatalf("child failed to reach abrupt exit: %v %s", err, output)
					}
					var out postCrashExternal
					raw, err := os.ReadFile(external)
					if err != nil || json.Unmarshal(raw, &out) != nil {
						t.Fatal("missing external state", err)
					}
					if out.CloudWrites != 0 || out.NodeWrites != 0 {
						t.Fatal("child replayed cloud or drain writes")
					}
					n.purged, n.purges, n.postCalls = out.Purged, out.Purges, out.PostCalls
					var saved State
					raw, err = os.ReadFile(filepath.Join(cfg.Dir, "live.json"))
					if err != nil || json.Unmarshal(raw, &saved) != nil {
						t.Fatal("missing journal", err)
					}
					e = openMulti(t, cfg, c, n)
					defer e.Close()
					for i := 0; i < 3; i++ {
						step(t, e)
					}
					assertPostAudit(t, e, c, n, failed)
					if saved.LastCompletedAt > 0 && e.state.LastCompletedAt != saved.LastCompletedAt {
						t.Fatal("restart changed committed completion time")
					}
				})
			}
		}
	}
}
