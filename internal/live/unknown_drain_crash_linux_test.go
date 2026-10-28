package live

import (
	"context"
	"encoding/json"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadclient"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadsdk"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/targetconfig"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/tencentcloud"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

type unknownDrainCrashInput struct {
	Directory, At string
	Snapshot      tencentcloud.Snapshot
	Nodes         map[string]nomadclient.Node
}

func TestUnknownSDKDrainCrashProcessHelper(t *testing.T) {
	path := os.Getenv("NOMAD_UNKNOWN_DRAIN_CRASH_FIXTURE")
	if path == "" {
		t.Skip("subprocess helper")
	}
	raw, err := os.ReadFile(path)
	var in unknownDrainCrashInput
	if err != nil || json.Unmarshal(raw, &in) != nil {
		t.Fatal("bad unknown drain crash fixture")
	}
	cfg := Config{Dir: in.Directory, GroupID: testGroup, Region: testRegion, GroupContract: true, ReconcileInterval: time.Hour, DrainDeadline: time.Minute}
	c := &multiCloud{fakeCloud: &fakeCloud{snap: in.Snapshot}, activities: map[string]tencentcloud.Activity{}}
	n := &sdkMultiProbe{multiNomad: &multiNomad{nodes: in.Nodes, load: map[string]int{}}, directory: cfg.Dir}
	e, err := Open(cfg, c, n)
	if err != nil {
		t.Fatal(err)
	}
	e.recoveryCheckpoint = func(stage string) error {
		if stage == in.At {
			if c.writeCount() != 0 || len(n.writes) != 0 {
				t.Fatal("restored before classification committed")
			}
			os.Exit(53)
		}
		return nil
	}
	if err = e.reconcileOnce(); err != nil {
		t.Fatal(err)
	}
	t.Fatal("unknown drain classification checkpoint not reached")
}
func TestUnknownSDKDrainAbruptExitResumesClassificationAndRestoration(t *testing.T) {
	for _, at := range []string{"before-record", "record", "prepared", "journal", "receipt", "cleared"} {
		t.Run(at, func(t *testing.T) {
			cfg, c, n, e := unknownDrainFixture(t)
			id := e.state.Pending.InstanceID
			intent := *e.state.Pending.NomadDrainIntent
			e.Close()
			in := unknownDrainCrashInput{cfg.Dir, at, c.snap, n.nodes}
			input := filepath.Join(t.TempDir(), "input.json")
			if err := os.WriteFile(input, jsonBytes(in), 0600); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command(os.Args[0], "-test.run=^TestUnknownSDKDrainCrashProcessHelper$")
			cmd.Env = append(os.Environ(), "NOMAD_UNKNOWN_DRAIN_CRASH_FIXTURE="+input)
			output, err := cmd.CombinedOutput()
			if err == nil || cmd.ProcessState.ExitCode() != 53 {
				t.Fatalf("child did not reach classification exit: %v %s", err, output)
			}
			e, err = Open(cfg, c, n)
			if err != nil {
				t.Fatal(err)
			}
			defer e.Close()
			step(t, e)
			if e.state.Pending != nil || e.state.Batch != nil || e.state.LastBatch == nil || e.state.LastBatch.Phase != "failed" || e.state.LastCompletedAt != 0 || len(n.writes) != 2 || c.writeCount() != 0 || n.nodes[id].Eligibility != "eligible" || e.state.LastOperation.NomadDrainIntent == nil || !e.state.LastOperation.NomadDrainIntent.At.Equal(intent.At) {
				t.Fatal("classification restart lost origin or repeated mutation")
			}
			if err = e.checkRecoveryAudit(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

type unknownDrainSDKExitInput struct {
	Directory, URL string
	Snapshot       tencentcloud.Snapshot
}
type exitBeforeSDKReturn struct{ *nomadsdk.Adapter }

func (n exitBeforeSDKReturn) PreScaleIn(ctx context.Context, id nomadclient.Identity, op string, d time.Duration, before func() error) (nomadclient.MutationResult, error) {
	result, err := n.Adapter.PreScaleIn(ctx, id, op, d, before)
	if result.WriteAttempted && result.Unknown && result.NodeModifyIndex == 0 {
		os.Exit(55)
	}
	return result, err
}
func TestUnknownSDKDrainExitBeforeReturnProcessHelper(t *testing.T) {
	path := os.Getenv("NOMAD_UNKNOWN_DRAIN_SDK_EXIT_FIXTURE")
	if path == "" {
		t.Skip("subprocess helper")
	}
	raw, err := os.ReadFile(path)
	var in unknownDrainSDKExitInput
	if err != nil || json.Unmarshal(raw, &in) != nil {
		t.Fatal("bad SDK exit fixture")
	}
	cfg := Config{Dir: in.Directory, GroupID: testGroup, Region: testRegion, GroupContract: true, ReconcileInterval: time.Hour, DrainDeadline: time.Second}
	c := &fakeCloud{snap: in.Snapshot}
	client, err := nomadclient.New(nomadclient.Config{Address: in.URL, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	scoped, err := client.WithScope(testGroup, testRegion, "cloud_instance_id")
	if err != nil {
		t.Fatal(err)
	}
	policy, err := (targetconfig.Agent{}).Policy(map[string]string{"tencent_group_id": testGroup, "node_drain_deadline": "1s"})
	if err != nil {
		t.Fatal(err)
	}
	e, err := Open(cfg, c, exitBeforeSDKReturn{nomadsdk.New(scoped, policy)})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	_, err = e.Scale(0)
	t.Fatal("SDK exit was not reached", err)
}
