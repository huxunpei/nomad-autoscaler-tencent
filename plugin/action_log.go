package plugin

import (
	"regexp"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/nomad-autoscaler/sdk"
)

var agentPolicyIDPattern = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

// The standard Agent attaches this ID to ScalingAction.Meta. It is not a
// target config field, group ID, filename hash, or persistent controller key.
// Keep the logger request-scoped so concurrent groups cannot share identities.
func scaleActionLogger(logger hclog.Logger, action sdk.ScalingAction, group string) hclog.Logger {
	if logger == nil {
		logger = hclog.NewNullLogger()
	}
	logger = logger.With("group", group, "requested_count", action.Count)
	if id, ok := action.Meta["nomad_policy_id"].(string); ok && agentPolicyIDPattern.MatchString(id) {
		return logger.With("policy_id", id, "policy_id_available", true)
	}
	// Direct SDK callers may not supply an ID. Do not alter scaling semantics,
	// invent an identity, or print arbitrary metadata in that case.
	return logger.With("policy_id_available", false)
}
