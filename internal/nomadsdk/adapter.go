// Package nomadsdk connects official scaling helpers to cloud-validated
// membership and caller-owned durable intents. It never invokes a cloud API.
package nomadsdk

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/url"
	"os"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/nomad-autoscaler/sdk/helper/scaleutils"
	"github.com/hashicorp/nomad/api"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadclient"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/targetconfig"
)

const owner = "nomad-autoscaler-tencent"
const scopeClass = "__tencent_verified_group__"

func failure(s string) error { return errors.New(s) }

type Adapter struct {
	*nomadclient.Client
	policy targetconfig.Policy
}

func New(c *nomadclient.Client, p targetconfig.Policy) *Adapter { return &Adapter{c, p} }
func owned(n nomadclient.Node, op string) bool {
	return n.LastDrain != nil && n.LastDrain.Owner == owner && n.LastDrain.OperationID == op
}

// Validate the entire supplied selection against fresh, unambiguous mappings.
// Only the cloud controller may establish membership; Nomad metadata is an ID,
// never evidence that the instance belongs to a particular cloud group.
func (a *Adapter) validate(ctx context.Context, ids []nomadclient.Identity) error {
	if a.Client == nil || len(ids) == 0 {
		return failure("SDKInvalidScope")
	}
	members := make([]string, 0, len(ids))
	nodes := map[string]nomadclient.Identity{}
	region := ids[0].Region
	for _, id := range ids {
		if id.Validate() != nil || id.GroupID != a.policy.GroupID || id.Region != region {
			return failure("SDKInvalidScope")
		}
		if _, ok := nodes[id.NodeID]; ok {
			return failure("SDKInvalidScope")
		}
		nodes[id.NodeID] = id
		members = append(members, id.InstanceID)
	}
	if err := a.VerifyAllocationVisibility(ctx); err != nil {
		return err
	}
	resolved, err := a.ResolveMembers(ctx, a.policy.GroupID, region, members)
	if err != nil || len(resolved) != len(ids) {
		return failure("SDKMappingUnavailable")
	}
	for _, n := range resolved {
		if nodes[n.NodeID] != n.Identity {
			return failure("SDKMappingChanged")
		}
	}
	return nil
}

func (a *Adapter) helper(ctx context.Context, ids []nomadclient.Identity, mode, op string, before func() error) (*scaleutils.ClusterScaleUtils, *scopedTransport, map[string]string, error) {
	st := &scopedTransport{ctx: ctx, client: a.Client, policy: a.policy, mode: mode, op: op, before: before, allowed: map[string]nomadclient.Identity{}, selfAlloc: os.Getenv("NOMAD_ALLOC_ID")}
	for _, id := range ids {
		st.allowed[id.NodeID] = id
	}
	if st.selfAlloc != "" && !uuid.MatchString(st.selfAlloc) {
		return nil, st, nil, failure("SDKSelfIdentityUnavailable")
	}
	cfg := a.SDKConfig(st.wrap)
	origin, err := url.Parse(cfg.Address)
	if err != nil {
		return nil, st, nil, failure("SDKConnectionInvalid")
	}
	st.origin = origin.Scheme + "://" + origin.Host
	// The SDK logs raw upstream errors and job data. The controller logs only
	// its sanitized events and IDs; never attach the product logger here.
	helper, err := scaleutils.NewClusterScaleUtils(cfg, hclog.NewNullLogger())
	if err != nil || (st.selfAlloc != "" && !st.selfVerified.Load()) {
		return nil, st, nil, failure("SDKSelfIdentityUnavailable")
	}
	helper.ClusterNodeIDLookupFunc = func(n *api.Node) (string, error) {
		if n == nil {
			return "", failure("SDKMappingChanged")
		}
		want, ok := st.allowed[n.ID]
		if !ok || n.Meta[a.policy.InstanceIDMetaKey] != want.InstanceID {
			return "", failure("SDKMappingChanged")
		}
		return want.InstanceID, nil
	}
	options := a.policy.SDKConfig()
	// Do not adopt nodes made ineligible by another owner.
	options[scaleutils.XNodeFilterOptionIgnoreIneligible] = "false"
	st.unfiltered = a.policy.NodeClass == "" && a.policy.Datacenter == "" && a.policy.NodePool == ""
	if st.unfiltered {
		options["node_class"] = scopeClass
	}
	return helper, st, options, nil
}

func (a *Adapter) SelectScaleIn(ctx context.Context, ids []nomadclient.Identity, num int) ([]nomadclient.Identity, error) {
	if num < 1 || num > len(ids) {
		return nil, failure("SDKInvalidSelectionCount")
	}
	if err := a.validate(ctx, ids); err != nil {
		return nil, err
	}
	h, st, cfg, err := a.helper(ctx, ids, "select", "", nil)
	if err != nil {
		return nil, err
	}
	nodes, err := h.IdentifyScaleInNodes(cfg, num)
	if err != nil || len(nodes) != num || st.readFailed.Load() {
		return nil, failure("SDKSelectionUnavailable")
	}
	mapped, err := h.IdentifyScaleInRemoteIDs(nodes)
	if err != nil || len(mapped) != num || st.readFailed.Load() {
		return nil, failure("SDKMappingUnavailable")
	}
	out := make([]nomadclient.Identity, 0, num)
	seen := map[string]bool{}
	for _, n := range mapped {
		want, ok := st.allowed[n.NomadNodeID]
		if !ok || seen[n.NomadNodeID] || n.RemoteResourceID != want.InstanceID {
			return nil, failure("SDKMappingChanged")
		}
		seen[n.NomadNodeID] = true
		out = append(out, want)
	}
	return out, nil
}

// The caller has already journaled this exact member. RunPreScaleInTasks sees
// only that fixed member, so a restart or second SDK selection cannot widen it.
func (a *Adapter) PreScaleIn(ctx context.Context, want nomadclient.Identity, op string, deadline time.Duration, before func() error) (out nomadclient.MutationResult, err error) {
	if nomadclient.ValidateOperationID(op) != nil || before == nil || deadline != a.policy.DrainDeadline {
		return out, failure("SDKInvalidDrainIntent")
	}
	if err = a.validate(ctx, []nomadclient.Identity{want}); err != nil {
		return out, err
	}
	n, err := a.ReadNode(ctx, want.NodeID)
	if err != nil || nomadclient.CheckIdentity(n, want) != nil || n.Status != "ready" || n.Eligibility != "eligible" || n.Drain || n.HasDrainStrategy || owned(n, op) {
		return out, failure("SDKDrainPreconditionFailed")
	}
	out.Before = &n
	h, st, cfg, err := a.helper(ctx, []nomadclient.Identity{want}, "pre", op, before)
	if err != nil {
		return out, err
	}
	ids, preErr := h.RunPreScaleInTasks(ctx, cfg, 1)
	out.WriteAttempted = st.writeAttempted.Load()
	out.NodeModifyIndex = st.modifyIndex.Load()
	out.Unknown = out.WriteAttempted && out.NodeModifyIndex == 0
	if preErr != nil || len(ids) != 1 || ids[0].NomadNodeID != want.NodeID || ids[0].RemoteResourceID != want.InstanceID || out.NodeModifyIndex == 0 {
		out.MonitoringIncomplete = out.WriteAttempted && !out.Unknown
		return out, failure("SDKDrainIncomplete")
	}
	// SDK 0.5.0 finishes on terminal allocations, before poststop tasks finish.
	// Retain the SDK drain and wait for actual required task completion as well.
	for {
		stopped, readErr := a.TasksStopped(ctx, want.NodeID, a.policy.IgnoreSystemJobs)
		if readErr != nil {
			out.MonitoringIncomplete = true
			return out, failure("SDKTaskObservationFailed")
		}
		if stopped {
			break
		}
		timer := time.NewTimer(50 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			out.MonitoringIncomplete = true
			return out, failure("SDKTaskWaitIncomplete")
		case <-timer.C:
		}
	}
	after, err := a.ReadNode(ctx, want.NodeID)
	if err != nil || nomadclient.CheckIdentity(after, want) != nil || !owned(after, op) || after.Eligibility != "ineligible" || after.Drain || after.HasDrainStrategy || after.LastDrain.Status != "complete" {
		out.MonitoringIncomplete = true
		return out, failure("SDKDrainReadbackIncomplete")
	}
	out.After = &after
	return out, nil
}

// Only call after the cloud controller has confirmed absence and persisted
// post-scale. A lost purge response is resumed by reading the exact node ID.
func (a *Adapter) PostScaleIn(ctx context.Context, want nomadclient.Identity, op string, before func() error) error {
	_, err := a.PostScaleInWithEvidence(ctx, want, op, before)
	return err
}

func (a *Adapter) PostScaleInWithEvidence(ctx context.Context, want nomadclient.Identity, op string, before func() error) (nomadclient.PostScaleInResult, error) {
	var empty nomadclient.PostScaleInResult
	if want.Validate() != nil || want.GroupID != a.policy.GroupID || nomadclient.ValidateOperationID(op) != nil {
		return empty, failure("SDKInvalidPostIntent")
	}
	result := nomadclient.PostScaleInResult{Identity: want, OperationID: op, PolicySHA256: a.PolicyDigest(), Purge: a.policy.Purge}
	if a.policy.Purge {
		n, err := a.ReadNode(ctx, want.NodeID)
		var apiErr *nomadclient.Error
		if errors.As(err, &apiErr) && apiErr.HTTPStatus == 404 {
			result.Outcome = "already-absent"
			result.ObservedAt = time.Now().UTC()
			return result, nil
		}
		if err != nil || nomadclient.CheckIdentity(n, want) != nil || !owned(n, op) || before == nil {
			return empty, failure("SDKPurgePreconditionFailed")
		}
	}
	h, _, cfg, err := a.helper(ctx, []nomadclient.Identity{want}, "post", op, before)
	if err != nil {
		return empty, err
	}
	if err = h.RunPostScaleInTasks(ctx, cfg, []scaleutils.NodeResourceID{{NomadNodeID: want.NodeID, RemoteResourceID: want.InstanceID}}); err != nil {
		return empty, failure("SDKPostIncomplete")
	}
	if a.policy.Purge {
		_, err = a.ReadNode(ctx, want.NodeID)
		var apiErr *nomadclient.Error
		if !errors.As(err, &apiErr) || apiErr.HTTPStatus != 404 {
			return empty, failure("SDKPurgeReadbackIncomplete")
		}
		result.Outcome = "purged"
	} else {
		result.Outcome = "purge-disabled"
	}
	result.ObservedAt = time.Now().UTC()
	return result, nil
}

// RequiredTasksStopped preserves the same policy on restart, before deletion.
func (a *Adapter) RequiredTasksStopped(ctx context.Context, nodeID string) (bool, error) {
	return a.TasksStopped(ctx, nodeID, a.policy.IgnoreSystemJobs)
}

// PolicyDigest binds pending work to the non-secret SDK policy and adapter
// contract. Credentials and connection addresses are deliberately excluded.
func (a *Adapter) PolicyDigest() string {
	b, _ := json.Marshal(struct {
		Contract string
		Policy   targetconfig.Policy
	}{"nomad-sdk-0.5.0-v1", a.policy})
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func (a *Adapter) InstanceMetaKey() string { return a.policy.InstanceIDMetaKey }
