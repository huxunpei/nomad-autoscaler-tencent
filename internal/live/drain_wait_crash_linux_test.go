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

type drainWaitCrashInput struct {
	Directory string
	Deadline  time.Duration
}

type drainWaitCrashRemote struct {
	Node  nomadclient.Node
	Cloud tencentcloud.Snapshot
}

func TestDrainWaitCrashProcessHelper(t *testing.T) {
	input := os.Getenv("NOMAD_DRAIN_WAIT_CRASH_FIXTURE")
	if input == "" {
		t.Skip("subprocess only")
	}
	raw, err := os.ReadFile(input)
	var in drainWaitCrashInput
	if err != nil || json.Unmarshal(raw, &in) != nil {
		t.Fatal("invalid crash input")
	}
	cfg := config(t)
	cfg.Dir, cfg.GroupContract, cfg.DrainDeadline = in.Directory, true, in.Deadline
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	cfg.Context = ctx
	c, rawNomad := newCloud(), newNomad()
	c.addMember()
	n := &sdkProbe{fakeNomad: rawNomad, directory: cfg.Dir, incomplete: true}
	e, err := Open(cfg, c, n)
	if err != nil {
		t.Fatal(err)
	}
	changed, err := e.Scale(0)
	if !changed || err == nil || e.state.Pending == nil || e.state.Pending.Phase != "draining" || e.state.Pending.DrainWait == nil || e.state.Batch.DrainWait == nil || c.writeCount() != 0 || rawNomad.writeCount() != 1 {
		t.Fatal("durable drain checkpoint not reached")
	}
	// Model remote state survives the controller. The journal is real and is
	// deliberately left without Close, like abrupt termination after drain ACK.
	remote := drainWaitCrashRemote{Node: rawNomad.node, Cloud: c.snap}
	if err := os.WriteFile(input+".remote", jsonBytes(remote), 0600); err != nil {
		t.Fatal(err)
	}
	os.Exit(58)
}

func TestDrainWaitAbruptExitKeepsOriginalExpiryAndRestores(t *testing.T) {
	for _, d := range []time.Duration{0, -time.Second} {
		t.Run(d.String(), func(t *testing.T) {
			cfg := config(t)
			cfg.GroupContract, cfg.DrainDeadline = true, d
			input := filepath.Join(t.TempDir(), "input.json")
			if err := os.WriteFile(input, jsonBytes(drainWaitCrashInput{cfg.Dir, d}), 0600); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command(os.Args[0], "-test.run=^TestDrainWaitCrashProcessHelper$")
			cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + os.Getenv("HOME"), "TMPDIR=" + os.Getenv("TMPDIR"), "NOMAD_DRAIN_WAIT_CRASH_FIXTURE=" + input}
			output, err := cmd.CombinedOutput()
			if err == nil || cmd.ProcessState == nil || cmd.ProcessState.ExitCode() != 58 {
				t.Fatalf("child did not reach expected abrupt exit: %v %s", err, output)
			}
			var saved State
			b, err := os.ReadFile(filepath.Join(cfg.Dir, "live.json"))
			if err != nil || json.Unmarshal(b, &saved) != nil || saved.Pending == nil || saved.Batch == nil || saved.Pending.DrainWait == nil {
				t.Fatal("missing crash journal")
			}
			until, parent := saved.Pending.DrainWait.Until, saved.Batch.DrainWait.Until
			var remote drainWaitCrashRemote
			b, err = os.ReadFile(input + ".remote")
			if err != nil || json.Unmarshal(b, &remote) != nil {
				t.Fatal("missing model remote snapshot")
			}
			c := &fakeCloud{snap: remote.Cloud}
			rawNomad := &fakeNomad{node: remote.Node}
			n := &sdkProbe{fakeNomad: rawNomad, directory: cfg.Dir}
			if left := time.Until(until); left > 0 {
				time.Sleep(left + time.Millisecond)
			}
			e, err := Open(cfg, c, n)
			if err != nil {
				t.Fatal(err)
			}
			defer e.Close()
			if !e.state.Pending.DrainWait.Until.Equal(until) || !e.state.Batch.DrainWait.Until.Equal(parent) {
				t.Fatal("restart extended the expired wait")
			}
			step(t, e)
			step(t, e)
			s, _, err := e.Snapshot()
			if err != nil || s.Pending != nil || s.Batch != nil || s.LastBatch == nil || s.LastBatch.Phase != "failed" || s.LastCompletedAt != 0 || c.writeCount() != 0 || rawNomad.writeCount() != 1 || n.preCalls != 0 || rawNomad.node.Eligibility != "eligible" || !s.LastOperation.DrainWait.Until.Equal(until) {
				t.Fatal("abrupt restart replayed drain, deleted, or failed to restore")
			}
			if err := e.checkRecoveryAudit(); err != nil {
				t.Fatal(err)
			}
			t.Log("real child exit58; original expiry retained; model survivor restored with audit; cloud writes=0")
		})
	}
}
