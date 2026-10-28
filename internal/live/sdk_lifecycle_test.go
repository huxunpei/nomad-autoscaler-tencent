package live

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadclient"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type sdkProbe struct {
	*fakeNomad
	directory                           string
	preCalls, postCalls, selectCalls    int
	failSelection, incomplete, postFail bool
	digest                              string
}

func (n *sdkProbe) PolicyDigest() string {
	if n.digest != "" {
		return n.digest
	}
	return strings.Repeat("a", 64)
}
func (n *sdkProbe) SelectScaleIn(_ context.Context, ids []nomadclient.Identity, num int) ([]nomadclient.Identity, error) {
	n.selectCalls++
	if n.failSelection {
		return nil, errors.New("selector unavailable")
	}
	return ids[:num], nil
}
func (n *sdkProbe) barrier(phase string, before func() error) error {
	if before == nil {
		return errors.New("missing barrier")
	}
	if err := before(); err != nil {
		return err
	}
	b, err := os.ReadFile(filepath.Join(n.directory, "live.json"))
	if err != nil {
		return err
	}
	var s State
	if json.Unmarshal(b, &s) != nil || s.Pending == nil || s.Pending.Phase != phase {
		return errors.New("missing durable phase")
	}
	return nil
}
func (n *sdkProbe) PreScaleIn(ctx context.Context, id nomadclient.Identity, op string, d time.Duration, before func() error) (nomadclient.MutationResult, error) {
	n.preCalls++
	if err := n.barrier("drain-intent", before); err != nil {
		return nomadclient.MutationResult{}, err
	}
	result, err := n.fakeNomad.StartOwnedDrain(ctx, id, op, d)
	if n.incomplete {
		result.MonitoringIncomplete = true
		return result, errors.New("SDK monitoring interrupted")
	}
	return result, err
}
func (n *sdkProbe) PostScaleIn(_ context.Context, _ nomadclient.Identity, _ string, before func() error) error {
	n.postCalls++
	if err := n.barrier("post-scale", before); err != nil {
		return err
	}
	if n.postFail {
		return errors.New("temporary post failure")
	}
	return nil
}
func (n *sdkProbe) RequiredTasksStopped(context.Context, string) (bool, error) { return true, nil }
func openSDKProbe(t *testing.T, cfg Config, c Cloud, n *sdkProbe) *Engine {
	t.Helper()
	n.directory = cfg.Dir
	e, err := Open(cfg, c, n)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(e.Close)
	return e
}
func TestSDKLifecycleAcknowledgedDrainResumesAndPostRetriesAfterRestart(t *testing.T) {
	cfg := config(t)
	c := newCloud()
	c.addMember()
	n := &sdkProbe{fakeNomad: newNomad(), incomplete: true, postFail: true}
	e := openSDKProbe(t, cfg, c, n)
	accepted, err := e.Scale(0)
	if !accepted || err == nil {
		t.Fatal("expected acknowledged incomplete monitor")
	}
	s, _, snapErr := e.Snapshot()
	if snapErr != nil || s.Pending == nil || s.Pending.Phase != "draining" || s.Pending.Unknown || n.preCalls != 1 || n.selectCalls != 1 {
		t.Fatalf("SDK acknowledgement lost: %v %+v", snapErr, s.Pending)
	}
	n.drained()
	step(t, e)
	c.complete()
	step(t, e)
	_ = e.reconcileOnce()
	s, _, _ = e.Snapshot()
	if s.Pending == nil || s.Pending.Phase != "post-scale" || s.LastOperation != nil || n.postCalls != 1 {
		t.Fatal("operation finished before post-scale", s.Pending, n.postCalls)
	}
	writes := c.writeCount()
	e.Close()
	n.postFail = false
	e = openSDKProbe(t, cfg, c, n)
	step(t, e)
	s, _, err = e.Snapshot()
	if err != nil || s.Pending != nil || s.LastOperation == nil || n.postCalls != 2 || n.preCalls != 1 || c.writeCount() != writes {
		t.Fatal("restart repeated mutation or skipped post", err)
	}
}
func TestSDKSelectionFailureStopsEvenSingleMemberScaleIn(t *testing.T) {
	cfg := config(t)
	c := newCloud()
	c.addMember()
	n := &sdkProbe{fakeNomad: newNomad(), failSelection: true}
	e := openSDKProbe(t, cfg, c, n)
	if _, err := e.Scale(0); err == nil {
		t.Fatal("single-node path skipped SDK selection")
	}
	if n.writeCount() != 0 || c.writeCount() != 0 {
		t.Fatal("selection failure wrote state to provider")
	}
}
func TestSDKPolicyChangeCannotReinterpretPendingDrain(t *testing.T) {
	cfg := config(t)
	c := newCloud()
	c.addMember()
	n := &sdkProbe{fakeNomad: newNomad()}
	e := openSDKProbe(t, cfg, c, n)
	if _, err := e.Scale(0); err != nil {
		t.Fatal(err)
	}
	e.Close()
	before, _ := os.ReadFile(filepath.Join(cfg.Dir, "live.json"))
	n.digest = strings.Repeat("b", 64)
	reopened, err := Open(cfg, c, n)
	if err == nil {
		reopened.Close()
		t.Fatal("new SDK policy reinterpreted pending drain")
	}
	after, _ := os.ReadFile(filepath.Join(cfg.Dir, "live.json"))
	if string(before) != string(after) {
		t.Fatal("rejected change modified journal")
	}
}
func TestSDKRecordedUnknownRecoveryRetainsPostScaleWork(t *testing.T) {
	cfg, c, raw := recoveryFixture(t, true)
	// This existing legacy fixture represents a cloud deletion already sent by
	// the SDK lifecycle. Persist its marker before opening the upgraded engine.
	p := filepath.Join(cfg.Dir, "live.json")
	b, _ := os.ReadFile(p)
	var state map[string]any
	if json.Unmarshal(b, &state) != nil {
		t.Fatal("fixture")
	}
	state["nomad_policy_sha256"] = strings.Repeat("a", 64)
	state["pending"].(map[string]any)["sdk_lifecycle"] = true
	b, _ = json.Marshal(state)
	if err := os.WriteFile(p, b, 0600); err != nil {
		t.Fatal(err)
	}
	n := &sdkProbe{fakeNomad: raw}
	e := openSDKProbe(t, cfg, c, n)
	step(t, e)
	s, _, err := e.Snapshot()
	if err != nil || s.Pending == nil || s.Pending.Phase != "post-scale" || s.RecoveryAudit == nil {
		t.Fatal("recovery discarded SDK post work", err, s.Pending)
	}
	step(t, e)
	s, _, err = e.Snapshot()
	if err != nil || s.Pending != nil || n.postCalls != 1 {
		t.Fatal("recovery post did not finish", err)
	}
}

type sdkMultiProbe struct {
	*multiNomad
	directory   string
	selections  int
	beforeDrain func(State) error
}

func (n *sdkMultiProbe) PolicyDigest() string { return strings.Repeat("c", 64) }
func (n *sdkMultiProbe) SelectScaleIn(_ context.Context, ids []nomadclient.Identity, num int) ([]nomadclient.Identity, error) {
	n.selections++
	return append([]nomadclient.Identity(nil), ids[:num]...), nil
}
func (n *sdkMultiProbe) PreScaleIn(ctx context.Context, id nomadclient.Identity, op string, d time.Duration, before func() error) (nomadclient.MutationResult, error) {
	if err := before(); err != nil {
		return nomadclient.MutationResult{}, err
	}
	b, err := os.ReadFile(filepath.Join(n.directory, "live.json"))
	var state State
	if err != nil || json.Unmarshal(b, &state) != nil {
		return nomadclient.MutationResult{}, errors.New("missing journal")
	}
	if n.beforeDrain != nil {
		if err := n.beforeDrain(state); err != nil {
			return nomadclient.MutationResult{}, err
		}
	}
	return n.multiNomad.StartOwnedDrain(ctx, id, op, d)
}
func (n *sdkMultiProbe) PostScaleIn(_ context.Context, _ nomadclient.Identity, _ string, before func() error) error {
	return before()
}
func (n *sdkMultiProbe) RequiredTasksStopped(context.Context, string) (bool, error) { return true, nil }
func TestSDKBatchPersistsExactSelectionAndDoesNotReselectAfterRestart(t *testing.T) {
	cfg, c, raw, old := multiFixture(t, 3, 3)
	old.Close()
	raw.load[raw.nodes["ins-multi0"].NodeID] = 999 // legacy count sorting would choose others
	n := &sdkMultiProbe{multiNomad: raw, directory: cfg.Dir}
	n.beforeDrain = func(s State) error {
		if s.Batch == nil || len(s.Batch.Selected) != 2 || s.Batch.Selected[0].InstanceID != "ins-multi0" || s.Batch.Selected[1].InstanceID != "ins-multi1" || s.Pending == nil || s.Pending.Phase != "drain-intent" {
			return errors.New("SDK selection not durable")
		}
		return nil
	}
	e, err := Open(cfg, c, n)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if _, err = e.Scale(1); err != nil {
		t.Fatal(err)
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
	s, _, err := e.Snapshot()
	if err != nil || s.Pending != nil || s.Batch != nil || s.LastBatch == nil || s.LastBatch.Target != 1 || n.selections != 1 {
		t.Fatal("SDK batch did not survive restart", err, n.selections)
	}
	if _, ok := s.Members["ins-multi2"]; !ok {
		t.Fatal("deleted a node outside original SDK selection")
	}
}

func TestSDKPreWriteFailureLeavesRestartableBatchJournal(t *testing.T) {
	for _, partial := range []bool{false, true} {
		t.Run(fmt.Sprint("partial-", partial), func(t *testing.T) {
			cfg, c, raw, old := multiFixture(t, 3, 3)
			old.Close()
			n := &sdkMultiProbe{multiNomad: raw, directory: cfg.Dir}
			e, err := Open(cfg, c, n)
			if err != nil {
				t.Fatal(err)
			}
			defer e.Close()
			if partial {
				if _, err = e.Scale(1); err != nil {
					t.Fatal(err)
				}
				completeMultiDelete(t, e, c)
			}
			n.beforeDrain = func(State) error { return errors.New("pre-write check failed") }
			if partial {
				err = e.reconcileOnce()
			} else {
				_, err = e.Scale(1)
			}
			if err == nil {
				t.Fatal("pre-write failure ignored")
			}
			before := len(n.writes)
			cloudWrites := c.writeCount()
			s, _, snapErr := e.Snapshot()
			if snapErr != nil || s.Pending != nil {
				t.Fatal("no-write failure became unknown", snapErr)
			}
			if !partial && s.Batch != nil {
				t.Fatal("empty batch persisted without an operation")
			}
			if partial && (s.Batch == nil || len(s.Batch.Results) != 1) {
				t.Fatal("successful batch prefix discarded")
			}
			e.Close()
			n.beforeDrain = nil
			e, err = Open(cfg, c, n)
			if err != nil {
				t.Fatal("journal cannot restart", err)
			}
			defer e.Close()
			if len(n.writes) != before || c.writeCount() != cloudWrites {
				t.Fatal("Open replayed writes")
			}
			if partial {
				step(t, e)
				completeMultiDelete(t, e, c)
			} else {
				if _, err = e.Scale(1); err != nil {
					t.Fatal("next evaluation failed", err)
				}
			}
		})
	}
}
