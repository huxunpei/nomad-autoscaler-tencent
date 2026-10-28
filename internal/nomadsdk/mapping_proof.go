package nomadsdk

import (
	"context"
	"errors"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadclient"
	"time"
)

// A source journal already fixes NodeID/InstanceID. Check the destination key
// against those identities, without requiring the old metadata to remain set.
// Reads are not atomic; every subsequent mutation retains normal fresh checks.
func (a *Adapter) ProveInstanceMapping(ctx context.Context, source string, ids []nomadclient.Identity) (nomadclient.InstanceMappingProof, error) {
	out := nomadclient.InstanceMappingProof{SourceKey: source, DestinationKey: a.policy.InstanceIDMetaKey, Nodes: []nomadclient.InstanceMappingObservation{}}
	if a.Client == nil || source != "cloud_instance_id" {
		return out, failure("SDKInvalidScope")
	}
	expected := map[string]nomadclient.Identity{}
	instances := map[string]string{}
	for _, id := range ids {
		if id.Validate() != nil || id.GroupID != a.policy.GroupID || expected[id.NodeID].NodeID != "" || instances[id.InstanceID] != "" {
			return out, failure("SDKInvalidScope")
		}
		expected[id.NodeID] = id
		instances[id.InstanceID] = id.NodeID
	}
	if len(ids) == 0 {
		out.ObservedAt = time.Now().UTC()
		return out, nil
	}
	nodes, err := a.ListNodes(ctx)
	if err != nil {
		return out, err
	}
	listed := map[string]nomadclient.Node{}
	found := map[string]bool{}
	for _, n := range nodes {
		if _, ok := expected[n.NodeID]; ok {
			listed[n.NodeID] = n
		}
		if target, ok := instances[n.InstanceID]; ok {
			if target != n.NodeID || found[n.InstanceID] {
				return out, failure("SDKMappingChanged")
			}
			found[n.InstanceID] = true
		}
	}
	for _, id := range ids {
		prior, present := listed[id.NodeID]
		fresh, err := a.ReadNode(ctx, id.NodeID)
		if !present {
			var apiErr *nomadclient.Error
			if !errors.As(err, &apiErr) || apiErr.HTTPStatus != 404 {
				return out, failure("SDKMappingUnavailable")
			}
			out.Nodes = append(out.Nodes, nomadclient.InstanceMappingObservation{Identity: id, Missing: true})
			continue
		}
		if err != nil || nomadclient.CheckIdentity(prior, id) != nil || nomadclient.CheckIdentity(fresh, id) != nil || prior.ModifyIndex == 0 || fresh.ModifyIndex != prior.ModifyIndex {
			return out, failure("SDKMappingChanged")
		}
		out.Nodes = append(out.Nodes, nomadclient.InstanceMappingObservation{Identity: id, ModifyIndex: fresh.ModifyIndex})
	}
	out.ObservedAt = time.Now().UTC()
	return out, nil
}
