// Package tencentcloud provides bounded AS/CVM observations and mutations.
// Only confirmed throttling rejections are retried within the call timeout.
// It does not decide whether cloud deletion or Nomad recovery is safe.
// Callers must persist intents before mutations and reconcile unknown outcomes.
package tencentcloud

import (
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/telemetry"
	"time"
)

type Config struct {
	Metrics                            *telemetry.Collector
	Region, SecretID, SecretKey, Token string
	Timeout                            time.Duration
}
type Group struct {
	ScalingMode         string `json:"scaling_mode,omitempty"`
	AllocationPolicy    string `json:"instance_allocation_policy,omitempty"`
	ID                  string `json:"id"`
	Min                 int64  `json:"min"`
	Max                 int64  `json:"max"`
	Desired             int64  `json:"desired"`
	Count               int64  `json:"count"`
	Enabled             string `json:"enabled"`
	Activity            string `json:"activity"`
	Status              string `json:"status"`
	LaunchConfiguration string `json:"launch_configuration"`
	RetryPolicy         string `json:"retry_policy"`
}
type Member struct {
	ID           string `json:"id"`
	CreationType string `json:"creation_type"`
	Lifecycle    string `json:"lifecycle"`
	AddedAt      string `json:"added_at"`
	Protected    bool   `json:"protected"`
}
type VM struct {
	ID         string `json:"id"`
	State      string `json:"state"`
	ChargeType string `json:"charge_type"`
	Returned   bool   `json:"returned"`
	Protected  bool   `json:"protected"`
}
type LaunchConfiguration struct {
	ID               string   `json:"id"`
	Status           string   `json:"status"`
	InstanceType     string   `json:"instance_type"`
	ChargeType       string   `json:"charge_type"`
	ImageID          string   `json:"image_id"`
	InstanceTypes    []string `json:"instance_types"`
	SecurityGroupIDs []string `json:"security_group_ids"`
	VersionNumber    int64    `json:"version_number"`
}
type Policy struct {
	ID   string `json:"id"`
	Type string `json:"type"`
}
type RequestTrace struct {
	Action    string `json:"action"`
	RequestID string `json:"request_id"`
}
type Snapshot struct {
	ScheduledActionCount        int                 `json:"scheduled_action_count,omitempty"`
	ScheduledActionsUnavailable bool                `json:"scheduled_actions_unavailable,omitempty"`
	At                          time.Time           `json:"at"`
	Region                      string              `json:"region"`
	Group                       Group               `json:"group"`
	Members                     []Member            `json:"members"`
	VMs                         map[string]VM       `json:"vms"`
	LaunchConfiguration         LaunchConfiguration `json:"launch_configuration"`
	Policies                    []Policy            `json:"policies"`
	PolicyCount                 int                 `json:"policy_count"`
	RequestIDs                  []RequestTrace      `json:"request_ids"`
}
type RelatedInstance struct {
	ID     string `json:"id"`
	Status string `json:"status"`
}
type Activity struct {
	FailureCodes     []string          `json:"failure_codes,omitempty"`
	Type             string            `json:"type,omitempty"`
	StartedAt        string            `json:"started_at,omitempty"`
	EndedAt          string            `json:"ended_at,omitempty"`
	ID               string            `json:"id"`
	GroupID          string            `json:"group_id"`
	Status           string            `json:"status"`
	RelatedInstances []RelatedInstance `json:"related_instances"`
	RequestIDs       []RequestTrace    `json:"request_ids"`
}
type WriteResult struct {
	NotSubmitted string `json:"not_submitted,omitempty"`
	Action       string `json:"action"`
	RequestID    string `json:"request_id,omitempty"`
	ActivityID   string `json:"activity_id,omitempty"`
	Attempted    bool   `json:"attempted"`
	Unknown      bool   `json:"unknown"`
}

// Error contains no raw SDK errors, response bodies, credentials or request URLs.
type Error struct {
	Code      string `json:"code"`
	Action    string `json:"action,omitempty"`
	RequestID string `json:"request_id,omitempty"`
	Unknown   bool   `json:"unknown"`
}

func (e *Error) Error() string { return e.Code }
