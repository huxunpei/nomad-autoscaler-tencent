package live

import (
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadclient"
	"sort"
	"strings"
	"time"
)

// Only operands which can still participate in this work need a mapping proof.
// Completed historical prefixes are immutable history, never new candidates.
func legacyMappingOperands(s State) ([]nomadclient.Identity, error) {
	byNode := map[string]nomadclient.Identity{}
	byInstance := map[string]string{}
	add := func(id nomadclient.Identity) bool {
		if id.Validate() != nil || id.GroupID != s.GroupID || id.Region != s.Region {
			return false
		}
		if old, ok := byNode[id.NodeID]; ok && old != id {
			return false
		}
		if old, ok := byInstance[id.InstanceID]; ok && old != id.NodeID {
			return false
		}
		byNode[id.NodeID] = id
		byInstance[id.InstanceID] = id.NodeID
		return true
	}
	for _, m := range s.Members {
		if m.Joined && !add(nomadclient.Identity{NodeID: m.NodeID, InstanceID: m.ID, GroupID: s.GroupID, Region: s.Region}) {
			return nil, code("InvalidLegacyMappingOperands")
		}
	}
	if b := s.Batch; b != nil {
		for i := len(b.Results); i < len(b.Selected); i++ {
			if !add(b.Selected[i].Identity) {
				return nil, code("InvalidLegacyMappingOperands")
			}
		}
	}
	op := s.Pending
	if op == nil && s.ManualReview {
		op = s.LastOperation
	}
	if op != nil && op.NodeID != "" {
		if !add(nomadclient.Identity{NodeID: op.NodeID, InstanceID: op.InstanceID, GroupID: s.GroupID, Region: s.Region}) {
			return nil, code("InvalidLegacyMappingOperands")
		}
	}
	out := make([]nomadclient.Identity, 0, len(byNode))
	for _, id := range byNode {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].NodeID < out[j].NodeID })
	return out, nil
}

func validLegacyMappingProof(p *nomadclient.InstanceMappingProof, key, group, region string, expected []nomadclient.Identity) bool {
	if p == nil || p.SourceKey != "cloud_instance_id" || p.DestinationKey != key || key == "cloud_instance_id" || strings.TrimSpace(key) == "" || strings.ContainsAny(key, "\x00\r\n") || p.ObservedAt.IsZero() {
		return false
	}
	if expected != nil && len(p.Nodes) != len(expected) {
		return false
	}
	last := ""
	instances := map[string]bool{}
	for i, n := range p.Nodes {
		id := n.Identity
		if id.Validate() != nil || id.GroupID != group || id.Region != region || id.NodeID <= last || instances[id.InstanceID] || (n.Missing && n.ModifyIndex != 0) || (!n.Missing && n.ModifyIndex == 0) || (expected != nil && expected[i] != id) {
			return false
		}
		last = id.NodeID
		instances[id.InstanceID] = true
	}
	return true
}
func legacyMappingBindingMatches(before State, binding LegacyCustomBinding) bool {
	if binding.InstanceMetaKey == "cloud_instance_id" {
		return binding.MappingProof == nil
	}
	ids, err := legacyMappingOperands(before)
	return err == nil && validLegacyMappingProof(binding.MappingProof, binding.InstanceMetaKey, before.GroupID, before.Region, ids)
}
func mappingProofFresh(p *nomadclient.InstanceMappingProof, at time.Time) bool {
	return p == nil || (!p.ObservedAt.After(at) && at.Sub(p.ObservedAt) <= 30*time.Second)
}
