package live

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/tencentcloud"
	"sort"
	"time"
)

type allocationCounter interface {
	ActiveAllocations(context.Context, string) (int, error)
}

func newOperationID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", code("OperationIdentityFailed")
	}
	return "live-" + hex.EncodeToString(b), nil
}
func memberIDs(m map[string]Member) []string {
	out := make([]string, 0, len(m))
	for id := range m {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

func (e *Engine) startBatch(target int64) (bool, error) {
	id, err := newOperationID()
	if err != nil {
		return false, err
	}
	count := int64(len(e.state.Members))
	batch := &Batch{ID: id, Phase: "running", Target: target, InitialCount: count, InitialMembers: memberIDs(e.state.Members), Results: []Operation{}, StartedAt: time.Now().UTC()}
	if e.actionSource != nil {
		source := *e.actionSource
		batch.Source = &source
	}
	opID, err := newOperationID()
	if err != nil {
		return false, err
	}
	op := &Operation{ID: opID, StartedAt: time.Now().UTC(), Target: target}
	if target > count {
		batch.Kind = "grow"
		op.Kind = "grow"
		op.Phase = "grow-intent"
		if _, ok := e.cloud.(desiredCASCloud); ok && e.usesGroupCapacityContract() {
			if err := e.prepareDesiredIntent(op); err != nil {
				return false, err
			}
		}
	} else {
		batch.Kind = "shrink"
		selected, err := e.selectForRemoval(int(count - target))
		if err != nil {
			return false, err
		}
		batch.Selected = selected
		op.Kind = "shrink"
		op.Phase = "drain-intent"
		op.Target = count - 1
		op.InstanceID = batch.Selected[0].InstanceID
		op.NodeID = batch.Selected[0].NodeID
	}
	if err := e.prepareBatchDrainWait(batch); err != nil {
		return false, err
	}
	e.state.Batch = batch
	return e.accept(op)
}

// Success is durably recorded before advancing to a new child operation.
func (e *Engine) advanceBatch() error {
	b := e.state.Batch
	if e.cfg.ScaleInProtection && !e.cfg.GroupContract {
		return e.blocked("scale-in-protection-enabled")
	}
	if b == nil || b.Kind != "shrink" || b.Phase != "running" || e.state.ManualReview {
		return e.blocked("batch-not-advanceable")
	}
	if e.state.Pending != nil {
		return ErrBusy
	}
	if b.DrainWait != nil && !time.Now().Before(b.DrainWait.Until) {
		return e.abortNextChild("batch-drain-wait-timeout")
	}
	if len(b.Results) >= len(b.Selected) {
		return e.blocked("batch-result-count-mismatch")
	}
	expected := b.InitialCount - int64(len(b.Results))
	snap := e.state.Cloud
	if snap != nil && snap.Group.Enabled != "ENABLED" {
		return e.abortNextChild("batch-observation-rejected")
	}
	if snap == nil || snap.Group.Desired != expected || snap.Group.Count != expected || snap.Group.Activity != "NOT_IN_ACTIVITY" || !e.batchMembershipMatches(*snap, false) {
		return e.abortNextChild("batch-membership-drift")
	}
	if e.cfg.GroupContract && (expected-1 < snap.Group.Min || expected-1 > snap.Group.Max || serviceMembers(*snap)-1 < snap.Group.Min) {
		return e.abortNextChild("batch-group-limits-changed")
	}
	next := b.Selected[len(b.Results)]
	m, ok := e.state.Members[next.InstanceID]
	if !ok || !e.removalCandidate(m) || m.NodeID != next.NodeID {
		return e.abortNextChild("batch-next-member-not-selectable")
	}
	id, err := newOperationID()
	if err != nil {
		return err
	}
	_, err = e.accept(&Operation{ID: id, Kind: "shrink", Phase: "drain-intent", Target: expected - 1, InstanceID: next.InstanceID, NodeID: next.NodeID, StartedAt: time.Now().UTC()})
	return err
}

// The batch may remove only its fixed selection; external additions/replacements
// cannot silently become part of the operation, including between child writes.
func (e *Engine) batchMembershipMatches(s tencentcloud.Snapshot, removePending bool) bool {
	b := e.state.Batch
	if b == nil {
		return true
	}
	if b.Kind != "shrink" {
		return false
	}
	expected := map[string]bool{}
	for _, id := range b.InitialMembers {
		expected[id] = true
	}
	for _, r := range b.Results {
		if r.ActivityStatus != "SUCCESSFUL" || r.InstanceResult != "SUCCESSFUL" {
			return false
		}
		delete(expected, r.InstanceID)
	}
	if removePending && e.state.Pending != nil {
		delete(expected, e.state.Pending.InstanceID)
	}
	if len(expected) != len(s.Members) {
		return false
	}
	for _, m := range s.Members {
		if !expected[m.ID] {
			return false
		}
	}
	return true
}

func (e *Engine) reconcileBatchGrow(op *Operation) error {
	ctx, cancel := e.callContext()
	defer cancel()
	if handled, err := e.observeGrowthFailure(ctx); handled || err != nil {
		return err
	}
	b := e.state.Batch
	if b.Kind != "grow" || b.Target != op.Target {
		return e.blocked("batch-growth-mismatch")
	}
	if e.state.Desired != op.Target || int64(len(e.state.Members)) != op.Target || e.state.Cloud.Group.Count != op.Target || e.state.Cloud.Group.Activity != "NOT_IN_ACTIVITY" {
		return nil
	}
	initial := map[string]bool{}
	for _, id := range b.InitialMembers {
		initial[id] = true
		if _, ok := e.state.Members[id]; !ok {
			return e.blocked("growth-baseline-member-disappeared")
		}
	}
	added := map[string]string{}
	for _, m := range e.state.Cloud.Members {
		added[m.ID] = m.AddedAt
	}
	for id, m := range e.state.Members {
		if (e.usesGroupCapacityContract() && !runningCloudMember(m)) || (!e.usesGroupCapacityContract() && !stableMember(m)) {
			return nil
		}
		if !initial[id] {
			at, err := time.Parse(time.RFC3339, added[id])
			if (!e.usesGroupCapacityContract() && m.Protected) || err != nil || at.Before(op.StartedAt.Truncate(time.Second)) {
				return e.blocked("expanded-member-provenance-unconfirmed")
			}
		}
	}
	return e.finish("expansion-complete")
}
