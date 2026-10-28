// Package live implements an experimental, single-writer, bounded-capacity
// Tencent/Nomad workflow. It is deliberately not a general-purpose controller.
package live

import (
	"context"
	"errors"
	"github.com/hashicorp/go-hclog"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/telemetry"
	"time"

	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadclient"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/tencentcloud"
)

var (
	ErrBusy     = errors.New("LiveOperationPending")
	ErrUnknown  = errors.New("LiveWriteOutcomeUnknown")
	ErrNotReady = errors.New("LiveTargetNotReady")
	ErrClosed   = errors.New("LiveEngineClosed")
)

const MaxSupportedCapacity int64 = 100

type Config struct {
	RegistrationGracePeriod          time.Duration
	Metrics                          *telemetry.Collector
	StabilizationInterval            time.Duration
	Logger                           hclog.Logger
	GroupContract                    bool
	Context                          context.Context
	DisableReconciliation            bool
	MaxCapacity                      int64
	ScaleInProtection                bool
	Dir, GroupID, Region             string
	ReconcileInterval, DrainDeadline time.Duration
}

type Cloud interface {
	Observe(context.Context, string, string) (tencentcloud.Snapshot, error)
	SetDesired(context.Context, string, int64) (tencentcloud.WriteResult, error)
	RemoveExact(context.Context, string, string) (tencentcloud.WriteResult, error)
	Activity(context.Context, string, string) (tencentcloud.Activity, error)
}

type Nomad interface {
	ResolveMembers(context.Context, string, string, []string) ([]nomadclient.Node, error)
	ReadNode(context.Context, string) (nomadclient.Node, error)
	TasksStopped(context.Context, string, bool) (bool, error)
	StartOwnedDrain(context.Context, nomadclient.Identity, string, time.Duration) (nomadclient.MutationResult, error)
	RestoreOwnedDrain(context.Context, nomadclient.Identity, string) (nomadclient.MutationResult, error)
}

type Member struct {
	EverJoined           bool   `json:"ever_joined,omitempty"`
	RegistrationState    string `json:"registration_state,omitempty"`
	RegistrationDeadline string `json:"registration_deadline,omitempty"`
	ID                   string `json:"id"`
	Running              bool   `json:"running"`
	Joined               bool   `json:"joined"`
	Protected            bool   `json:"protected"`
	Eligible             bool   `json:"eligible"`
	NodeID               string `json:"node_id,omitempty"`
	DrainedBy            string `json:"drained_by,omitempty"`
	CreationType         string `json:"creation_type"`
	Lifecycle            string `json:"lifecycle"`
	ChargeType           string `json:"charge_type,omitempty"`
}

// DeleteSettlement records a later exact removal, not an inferred parent/retry
// relationship. Original failure diagnostics remain on Operation.
type DeleteSettlement struct {
	FailedActivity  tencentcloud.Activity `json:"failed_activity"`
	RemovalActivity tencentcloud.Activity `json:"removal_activity"`
	FirstObservedAt time.Time             `json:"first_observed_at"`
	ObservedAt      time.Time             `json:"observed_at"`
}

// CloudWriteIntent is recorded before the provider mutation. It distinguishes
// an uncertain cloud write from an uncertain Nomad drain after a restart.
type CloudWriteIntent struct {
	Desired  *DesiredIntent  `json:"desired,omitempty"`
	ScaleOut *ScaleOutIntent `json:"scale_out,omitempty"`
	Action   string          `json:"action"`
	At       time.Time       `json:"at"`
}

type DesiredIntent struct {
	Origin             int64    `json:"origin"`
	PreviousActivities []string `json:"previous_activities,omitempty"`
}

type GrowthBackoff struct {
	Target     int64     `json:"target"`
	Failures   int       `json:"failures"`
	RetryAfter time.Time `json:"retry_after"`
}

type ScaleOutIntent struct {
	Base             int64  `json:"base"`
	Number           int64  `json:"number"`
	AllocationPolicy string `json:"allocation_policy"`
}

// ActivityDiscovery records a uniquely observed effect, not an API request or
// retry-parent correlation: DescribeAutoScalingActivities exposes neither.
type ActivityDiscovery struct {
	History    []tencentcloud.Activity `json:"history,omitempty"`
	Activity   tencentcloud.Activity   `json:"activity"`
	ObservedAt time.Time               `json:"observed_at"`
}

// DrainWaitWindow is a controller observation bound, separate from the raw
// Nomad DrainSpec. It is saved before a nonpositive-deadline drain can write.
type DrainWaitWindow struct {
	Until    time.Time     `json:"until"`
	Attempts int           `json:"attempts"`
	Interval time.Duration `json:"poll_interval_nanoseconds"`
}

type Operation struct {
	DesiredAccepted    bool                   `json:"desired_accepted,omitempty"`
	DesiredConflict    bool                   `json:"desired_conflict,omitempty"`
	CloudNotSubmitted  bool                   `json:"cloud_not_submitted,omitempty"`
	LegacyOrigin       *LegacyOperationOrigin `json:"legacy_origin,omitempty"`
	DrainWait          *DrainWaitWindow       `json:"drain_wait,omitempty"`
	GrowthFailure      *ActivityDiscovery     `json:"growth_failure,omitempty"`
	NomadDrainIntent   *NomadDrainIntent      `json:"nomad_drain_intent,omitempty"`
	CloudRejection     *tencentcloud.Error    `json:"cloud_rejection,omitempty"`
	CloudIntent        *CloudWriteIntent      `json:"cloud_intent,omitempty"`
	ActivityDiscovery  *ActivityDiscovery     `json:"activity_discovery,omitempty"`
	DeleteSettlement   *DeleteSettlement      `json:"delete_settlement,omitempty"`
	AbortReason        string                 `json:"abort_reason,omitempty"`
	DrainOutcome       string                 `json:"drain_outcome,omitempty"`
	SDKLifecycle       bool                   `json:"sdk_lifecycle,omitempty"`
	ID                 string                 `json:"id"`
	Kind               string                 `json:"kind"`
	Phase              string                 `json:"phase"`
	Target             int64                  `json:"target"`
	InstanceID         string                 `json:"instance_id,omitempty"`
	NodeID             string                 `json:"node_id,omitempty"`
	RequestID          string                 `json:"request_id,omitempty"`
	ActivityID         string                 `json:"activity_id,omitempty"`
	NodeModifyIndex    uint64                 `json:"node_modify_index,omitempty"`
	Unknown            bool                   `json:"unknown"`
	StartedAt          time.Time              `json:"started_at"`
	AbsentObservations int                    `json:"absent_observations"`
	ActivityStatus     string                 `json:"activity_status,omitempty"`
	InstanceResult     string                 `json:"instance_result,omitempty"`
}

type Candidate struct {
	nomadclient.Identity
	Allocations int `json:"allocations"`
}

type Batch struct {
	LegacyDrainOrigin *LegacyBatchDrainOrigin `json:"legacy_drain_origin,omitempty"`
	DrainWait         *DrainWaitWindow        `json:"drain_wait,omitempty"`
	Source            *ActionSource           `json:"source,omitempty"`
	CompletedAt       int64                   `json:"completed_at,omitempty"`
	ID                string                  `json:"id"`
	Kind              string                  `json:"kind"`
	Phase             string                  `json:"phase"`
	Target            int64                   `json:"target"`
	InitialCount      int64                   `json:"initial_count"`
	InitialMembers    []string                `json:"initial_members"`
	Selected          []Candidate             `json:"selected,omitempty"`
	Results           []Operation             `json:"results"`
	StartedAt         time.Time               `json:"started_at"`
}

type State struct {
	GrowthBackoff          *GrowthBackoff        `json:"growth_backoff,omitempty"`
	PolicyAuditSHA256      string                `json:"policy_audit_sha256,omitempty"`
	PolicyHistory          *PolicyHistoryContext `json:"policy_history,omitempty"`
	LegacyContract         *LegacyContract       `json:"legacy_contract,omitempty"`
	LastCompletedAt        int64                 `json:"last_completed_at,omitempty"`
	NomadInstanceIDMetaKey string                `json:"nomad_instance_id_meta_key,omitempty"`
	NomadPolicySHA256      string                `json:"nomad_policy_sha256,omitempty"`
	RecoveryAudit          *RecoveryAuditRef     `json:"recovery_audit,omitempty"`
	MaxCapacity            int64                 `json:"max_capacity,omitempty"`
	Batch                  *Batch                `json:"batch,omitempty"`
	LastBatch              *Batch                `json:"last_batch,omitempty"`

	Version                  int    `json:"version"`
	GroupID                  string `json:"group_id"`
	Region                   string `json:"region"`
	DrainDeadlineNanoseconds int64  `json:"drain_deadline_nanoseconds"`
	Desired                  int64  `json:"desired"`
	// Members includes every AS member, even if it has not joined Nomad.
	Members             map[string]Member                 `json:"members"`
	Pending             *Operation                        `json:"pending,omitempty"`
	LastOperation       *Operation                        `json:"last_operation,omitempty"`
	LastEvent           string                            `json:"last_event,omitempty"`
	ErrorCode           string                            `json:"error_code,omitempty"`
	ManualReview        bool                              `json:"manual_review"`
	Sequence            uint64                            `json:"sequence"`
	ObservedAt          time.Time                         `json:"observed_at"`
	LaunchConfiguration *tencentcloud.LaunchConfiguration `json:"launch_configuration,omitempty"`
	Cloud               *tencentcloud.Snapshot            `json:"cloud,omitempty"`
}
