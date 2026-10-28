package live

import (
	"context"
	"encoding/json"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadclient"
	"reflect"
	"testing"
	"time"
)

type unknownDrainSDK struct {
	*sdkMultiProbe
	preCount, loseAt int
	loseRestore      bool
}

func (n *unknownDrainSDK) PreScaleIn(ctx context.Context, id nomadclient.Identity, op string, d time.Duration, before func() error) (nomadclient.MutationResult, error) {
	n.preCount++
	result, err := n.sdkMultiProbe.PreScaleIn(ctx, id, op, d, before)
	if err != nil {
		return result, err
	}
	node := n.nodes[id.InstanceID]
	var raw map[string]any
	_ = json.Unmarshal(jsonBytes(node), &raw)
	raw["modify_index"] = float64(17)
	_ = json.Unmarshal(jsonBytes(raw), &node)
	n.nodes[id.InstanceID] = node
	if n.loseAt > 0 && n.preCount != n.loseAt {
		return result, nil
	}
	return nomadclient.MutationResult{WriteAttempted: true, Unknown: true}, code("SimulatedSDKDrainResponseLost")
}
func unknownDrainFixture(t *testing.T) (Config, *multiCloud, *unknownDrainSDK, *Engine) {
	t.Helper()
	cfg, c, n, old := groupContractFixture(t, 2, 0, 3, false)
	old.Close()
	adapter := &unknownDrainSDK{sdkMultiProbe: &sdkMultiProbe{multiNomad: n, directory: cfg.Dir}}
	e, err := Open(cfg, c, adapter)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(e.Close)
	if changed, err := e.Scale(1); !changed || err != ErrUnknown {
		t.Fatal("did not create an uncertain SDK drain", changed, err)
	}
	return cfg, c, adapter, e
}
func TestUnknownSDKDrainReadbackRestoresOnlyOriginalNodeAndFailsBatch(t *testing.T) {
	cfg, c, n, e := unknownDrainFixture(t)
	id := e.state.Pending.InstanceID
	op := e.state.Pending.ID
	if err := e.reconcileOnce(); err != nil {
		t.Fatal(err)
	}
	if e.state.Pending != nil || e.state.Batch != nil || e.state.LastBatch == nil || e.state.LastBatch.Phase != "failed" || e.state.LastCompletedAt != 0 || e.state.LastOperation == nil || e.state.LastOperation.ID != op || e.state.LastOperation.DrainOutcome != "restored" || n.nodes[id].Eligibility != "eligible" || c.writeCount() != 0 || len(n.writes) != 2 || n.selections != 1 {
		t.Fatal("unknown SDK drain was not reconciled as failed restoration")
	}
	if err := e.checkRecoveryAudit(); err != nil {
		t.Fatal(err)
	}
	e.Close()
	e, err := Open(cfg, c, n)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	step(t, e)
	if c.writeCount() != 0 || len(n.writes) != 2 || e.state.LastCompletedAt != 0 {
		t.Fatal("restart repeated an old action")
	}
	if changed, err := e.Scale(1); !changed || err != ErrUnknown || n.selections != 2 {
		t.Fatal("next original-scope action did not enter SDK", changed, err)
	}
}

func (n *unknownDrainSDK) RestoreOwnedDrain(ctx context.Context, id nomadclient.Identity, op string) (nomadclient.MutationResult, error) {
	result, err := n.multiNomad.RestoreOwnedDrain(ctx, id, op)
	if err != nil {
		return result, err
	}
	node := n.nodes[id.InstanceID]
	node.ModifyIndex++
	n.nodes[id.InstanceID] = node
	if n.loseRestore {
		n.loseRestore = false
		return nomadclient.MutationResult{WriteAttempted: true, Unknown: true}, code("SimulatedRestoreResponseLost")
	}
	result.NodeModifyIndex = node.ModifyIndex
	result.After = &node
	return result, nil
}
func TestUnknownSDKDrainRejectsMissingOriginAndUnownedObservations(t *testing.T) {
	for _, mode := range []string{"old-no-origin", "cloud-intent", "cloud-request", "other-owner", "other-operation", "wrong-identity", "no-index", "node-down", "vm-stopped", "missing-member", "restored-other-owner", "canceled-ineligible"} {
		t.Run(mode, func(t *testing.T) {
			cfg, c, n, e := unknownDrainFixture(t)
			id := e.state.Pending.InstanceID
			op := e.state.Pending
			switch mode {
			case "old-no-origin":
				op.NomadDrainIntent = nil
			case "cloud-intent":
				op.CloudIntent = &CloudWriteIntent{Action: "RemoveInstances", At: time.Now().UTC()}
			case "cloud-request":
				op.RequestID = testRequest
			default:
				node := n.nodes[id]
				switch mode {
				case "other-owner":
					node.LastDrain.Owner = "other-controller"
				case "other-operation":
					node.LastDrain.OperationID = "other-operation"
				case "wrong-identity":
					node.InstanceID = "ins-other"
				case "no-index":
					node.ModifyIndex = 0
				case "node-down":
					node.Status = "down"
				case "vm-stopped":
					vm := c.snap.VMs[id]
					vm.State = "STOPPED"
					c.snap.VMs[id] = vm
				case "missing-member":
					c.snap.Members = c.snap.Members[1:]
					c.snap.Group.Count--
					c.snap.Group.Desired--
				case "canceled-ineligible":
					node.LastDrain.Status = "canceled"
				case "restored-other-owner":
					node.Eligibility = "eligible"
					node.LastDrain.Owner = "other-controller"
					node.LastDrain.Status = "canceled"
				}
				n.nodes[id] = node
			}
			if err := e.save(); err != nil {
				t.Fatal(err)
			}
			e.Close()
			reopened, err := Open(cfg, c, n)
			if err != nil {
				t.Fatal("negative fixture could not reopen", err)
			}
			defer reopened.Close()
			_ = reopened.reconcileOnce()
			if reopened.state.Pending == nil || !reopened.state.Pending.Unknown || len(n.writes) != 1 || c.writeCount() != 0 || reopened.state.LastCompletedAt != 0 {
				t.Fatal("unproven outcome was adopted or another node was changed")
			}
		})
	}
}
func TestUnknownSDKDrainAlreadyRestoredDoesNotWriteEligibilityAgain(t *testing.T) {
	_, c, n, e := unknownDrainFixture(t)
	defer e.Close()
	id := e.state.Pending.InstanceID
	node := n.nodes[id]
	node.Eligibility = "eligible"
	node.LastDrain.Status = "canceled"
	node.ModifyIndex++
	n.nodes[id] = node
	step(t, e)
	if e.state.Pending != nil || e.state.LastBatch == nil || e.state.LastBatch.Phase != "failed" || len(n.writes) != 1 || c.writeCount() != 0 || e.state.LastCompletedAt != 0 {
		t.Fatal("already restored drain was rewritten")
	}
}
func TestUnknownSDKDrainRestorationAfterChangedGroupGuard(t *testing.T) {
	cfg, c, n, e := unknownDrainFixture(t)
	id := e.state.Pending.InstanceID
	e.Close()
	c.snap.Group.Min = 2
	e, err := Open(cfg, c, n)
	if err != nil {
		t.Fatal("guard prevented own restoration startup", err)
	}
	defer e.Close()
	_ = e.reconcileOnce()
	state, _, _ := e.Snapshot()
	if state.Pending != nil || state.Batch != nil || state.LastBatch == nil || state.LastBatch.Phase != "failed" || n.nodes[id].Eligibility != "eligible" || c.writeCount() != 0 {
		t.Fatal("guard prevented own restoration")
	}
}
func TestUnknownSDKDrainPreservesSuccessfulPrefixAndLostRestoreReadback(t *testing.T) {
	cfg, c, n, old := groupContractFixture(t, 3, 0, 3, false)
	old.Close()
	adapter := &unknownDrainSDK{sdkMultiProbe: &sdkMultiProbe{multiNomad: n, directory: cfg.Dir}, loseAt: 2, loseRestore: true}
	e, err := Open(cfg, c, adapter)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if _, err = e.Scale(1); err != nil {
		t.Fatal(err)
	}
	completeMultiDelete(t, e, c)
	prefix := copyJSON(e.state.Batch.Results[0])
	if err = e.reconcileOnce(); err != ErrUnknown {
		t.Fatal("missing second-child SDK fault", err)
	}
	step(t, e)
	if e.state.Pending == nil || e.state.Pending.Phase != "aborting" || e.state.LastCompletedAt != 0 {
		t.Fatal("lost restore response not pending")
	}
	e.Close()
	e, err = Open(cfg, c, adapter)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	step(t, e)
	if e.state.Pending != nil || e.state.Batch != nil || e.state.LastBatch == nil || len(e.state.LastBatch.Results) != 2 || !reflect.DeepEqual(prefix, e.state.LastBatch.Results[0]) || e.state.LastBatch.Phase != "failed" || e.state.LastCompletedAt != 0 || len(n.writes) != 3 || c.writeCount() != 1 || adapter.selections != 1 {
		t.Fatal("successful prefix lost or provider writes replayed")
	}
	if err = e.checkRecoveryAudit(); err != nil {
		t.Fatal(err)
	}
}

func TestUnknownSDKDrainRejectsInvalidPersistedOrigin(t *testing.T) {
	cfg, _, _, e := unknownDrainFixture(t)
	defer e.Close()
	for _, mode := range []string{"zero-time", "before-operation", "not-sdk", "wrong-kind", "cloud-before-drain", "abort-without-origin"} {
		t.Run(mode, func(t *testing.T) {
			state := copyJSON(e.state)
			op := state.Pending
			switch mode {
			case "zero-time":
				op.NomadDrainIntent.At = time.Time{}
			case "before-operation":
				op.NomadDrainIntent.At = op.StartedAt.Add(-time.Second)
			case "not-sdk":
				op.SDKLifecycle = false
			case "wrong-kind":
				op.Kind = "grow"
			case "cloud-before-drain":
				op.CloudIntent = &CloudWriteIntent{Action: "RemoveInstances", At: op.StartedAt.Add(-time.Second)}
			case "abort-without-origin":
				op.NomadDrainIntent = nil
				op.Phase = "aborting"
				op.Unknown = false
				op.AbortReason = "sdk-drain-response-lost"
				op.NodeModifyIndex = 17
			}
			if err := (&Engine{cfg: cfg, state: state}).validateState(); err == nil {
				t.Fatal("invalid origin accepted")
			}
		})
	}
}

func TestUnknownSDKDrainNewChildOfMigratedLegacyBatch(t *testing.T) {
	cfg, c, raw, initial := multiFixture(t, 3, 3)
	initial.Close()
	n := &unknownDrainSDK{sdkMultiProbe: &sdkMultiProbe{multiNomad: raw, directory: cfg.Dir}, loseAt: 2}
	old, err := Open(cfg, c, n)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = old.Scale(1); err != nil {
		t.Fatal(err)
	}
	original := copyJSON(*old.state.Batch)
	old.Close()
	cfg.GroupContract = true
	e, err := Open(cfg, c, n)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if e.state.LegacyContract == nil {
		t.Fatal("fixture did not retain original contract")
	}
	completeMultiDelete(t, e, c)
	prefix := copyJSON(e.state.Batch.Results[0])
	if err = e.reconcileOnce(); err != ErrUnknown {
		t.Fatal("new child did not lose SDK response", err)
	}
	if e.state.Pending.NomadDrainIntent == nil || e.state.LegacyContract == nil {
		t.Fatal("new child lost explicit source or original contract")
	}
	step(t, e)
	if e.state.Pending != nil || e.state.Batch != nil || e.state.LegacyContract != nil || e.state.LastBatch == nil || e.state.LastBatch.ID != original.ID || e.state.LastBatch.Phase != "failed" || !reflect.DeepEqual(e.state.LastBatch.Results[0], prefix) || e.state.LastCompletedAt != 0 || n.selections != 1 || c.writeCount() != 1 || len(n.writes) != 3 {
		t.Fatal("migrated prefix or failure contract changed")
	}
	e.Close()
	e, err = Open(cfg, c, n)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	step(t, e)
	if err = e.checkRecoveryAudit(); err != nil {
		t.Fatal(err)
	}
	if c.writeCount() != 1 || len(n.writes) != 3 {
		t.Fatal("migration recovery replayed provider writes")
	}
}
