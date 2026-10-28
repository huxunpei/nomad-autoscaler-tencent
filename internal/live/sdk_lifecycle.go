package live

import (
	"context"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadclient"
	"sort"
	"time"
)

type sdkLifecycle interface {
	SelectScaleIn(context.Context, []nomadclient.Identity, int) ([]nomadclient.Identity, error)
	PreScaleIn(context.Context, nomadclient.Identity, string, time.Duration, func() error) (nomadclient.MutationResult, error)
	PostScaleIn(context.Context, nomadclient.Identity, string, func() error) error
	RequiredTasksStopped(context.Context, string) (bool, error)
	PolicyDigest() string
}

func (e *Engine) bindSDKPolicy() error {
	digest, key := "", ""
	if mapping, ok := e.nomad.(interface{ InstanceMetaKey() string }); ok {
		key = mapping.InstanceMetaKey()
	}
	if adapter, ok := e.nomad.(sdkLifecycle); ok {
		digest = adapter.PolicyDigest()
		if !digestPattern.MatchString(digest) {
			return code("InvalidSDKPolicyIdentity")
		}
	}
	if e.state.NomadPolicySHA256 == digest {
		if e.state.NomadInstanceIDMetaKey != key {
			return code("SDKPolicyChangeRequiresMigration")
		}
		return nil
	}
	// Existing active operations retain their original implementation and policy.
	// Do not reinterpret old drains or remove a recorded post-scale obligation.
	if e.state.Pending != nil || e.state.Batch != nil || e.state.ManualReview || e.state.NomadPolicySHA256 != "" {
		return code("SDKPolicyChangeRequiresMigration")
	}
	e.state.NomadPolicySHA256 = digest
	e.state.NomadInstanceIDMetaKey = key
	return nil
}

func (e *Engine) selectForRemoval(num int) ([]Candidate, error) {
	ctx, cancel := e.callContext()
	defer cancel()
	candidates := []nomadclient.Identity{}
	eligible := map[string]nomadclient.Identity{}
	for _, iid := range memberIDs(e.state.Members) {
		m := e.state.Members[iid]
		if !e.removalCandidate(m) {
			continue
		}
		id := nomadclient.Identity{InstanceID: iid, NodeID: m.NodeID, GroupID: e.cfg.GroupID, Region: e.cfg.Region}
		candidates = append(candidates, id)
		eligible[id.NodeID] = id
	}
	if num < 1 || len(candidates) < num {
		return nil, code("InsufficientUnprotectedMembers")
	}
	if adapter, ok := e.nomad.(sdkLifecycle); ok {
		selected, err := adapter.SelectScaleIn(ctx, candidates, num)
		if err != nil || len(selected) != num {
			return nil, code("SDKSelectionUnavailable")
		}
		out := make([]Candidate, 0, num)
		seen := map[string]bool{}
		for _, id := range selected {
			if id.Validate() != nil || eligible[id.NodeID] != id || seen[id.NodeID] {
				return nil, code("SDKSelectionInvalid")
			}
			seen[id.NodeID] = true
			out = append(out, Candidate{Identity: id})
		}
		return out, nil
	}
	// Retained only for old engine callers and historical replay tests. The
	// product's live entrypoint supplies sdkLifecycle and never uses this ranking.
	counter, ok := e.nomad.(allocationCounter)
	if !ok {
		if len(candidates) == 1 && num == 1 {
			return []Candidate{{Identity: candidates[0]}}, nil
		}
		return nil, code("AllocationSelectionUnavailable")
	}
	out := make([]Candidate, 0, len(candidates))
	for _, id := range candidates {
		count, err := counter.ActiveAllocations(ctx, id.NodeID)
		if err != nil || count < 0 {
			return nil, code("AllocationSelectionUnavailable")
		}
		out = append(out, Candidate{Identity: id, Allocations: count})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Allocations != out[j].Allocations {
			return out[i].Allocations < out[j].Allocations
		}
		return out[i].InstanceID < out[j].InstanceID
	})
	return out[:num], nil
}

func (e *Engine) durableSDKBarrier(op *Operation, phase string) func() error {
	return func() error {
		if e.workContext().Err() != nil || e.persistError != nil || e.state.Pending != op || op.Phase != phase || !(op.SDKLifecycle || ((phase == "post-scale" || phase == "failed-delete-post") && requiresSDKPost(op))) {
			return code("SDKIntentChanged")
		}
		return e.save()
	}
}

func (e *Engine) postScale(ctx context.Context, op *Operation) error {
	if !requiresSDKPost(op) {
		return e.finish("deletion-complete")
	}
	adapter, ok := e.nomad.(sdkLifecycle)
	if !ok {
		return e.blocked("sdk-post-adapter-unavailable")
	}
	snap := e.state.Cloud
	if snap == nil {
		return e.blocked("sdk-post-cloud-unavailable")
	}
	vm, observed := snap.VMs[op.InstanceID]
	_, exists := e.state.Members[op.InstanceID]
	if exists || !observed || vm.Returned || snap.Group.Count != op.Target || snap.Group.Desired != op.Target || int64(len(snap.Members)) != op.Target || snap.Group.Activity != "NOT_IN_ACTIVITY" || !e.batchMembershipMatches(*snap, true) {
		return e.blocked("sdk-post-cloud-not-settled")
	}
	op.Phase = "post-scale"
	e.state.LastEvent = op.Phase
	if err := e.save(); err != nil {
		return err
	}
	if e.cfg.GroupContract {
		return e.auditedSDKPost(ctx, adapter, *snap)
	}
	if err := adapter.PostScaleIn(ctx, e.identity(op), op.ID, e.durableSDKBarrier(op, "post-scale")); err != nil {
		if e.persistError != nil {
			return e.persistError
		}
		e.state.LastEvent = "sdk-post-incomplete"
		if err = e.save(); err != nil {
			return err
		}
		return code("SDKPostIncomplete")
	}
	return e.finish("deletion-complete")
}
