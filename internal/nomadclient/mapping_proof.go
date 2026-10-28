package nomadclient

import "time"

// InstanceMappingProof contains only safe IDs and observed node revisions.
// It proves continuity with journaled identities, not cloud membership.
type InstanceMappingObservation struct {
	Identity    Identity `json:"identity"`
	Missing     bool     `json:"missing,omitempty"`
	ModifyIndex uint64   `json:"modify_index,omitempty"`
}
type InstanceMappingProof struct {
	SourceKey      string                       `json:"source_key"`
	DestinationKey string                       `json:"destination_key"`
	ObservedAt     time.Time                    `json:"observed_at"`
	Nodes          []InstanceMappingObservation `json:"nodes"`
}
