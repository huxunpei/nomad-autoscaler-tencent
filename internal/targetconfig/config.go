// Package targetconfig parses the user-facing Agent and policy contract.
// It does not fetch credentials, modify caller maps, or perform cloud actions.
package targetconfig

import (
	"errors"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/telemetry"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/hashicorp/nomad-autoscaler/plugins/shared"
	"github.com/hashicorp/nomad-autoscaler/sdk"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadclient"
)

type Agent struct {
	PolicyDir                           string
	RegistrationGracePeriod             time.Duration
	Region, AuthMethod, CredentialsFile string
	StateDir                            string
	MetricsAddress                      string
	RetryAttempts                       int
	ScaleInProtection                   bool
}

const DefaultStateDir = "/var/lib/nomad-autoscaler/tencent-as"

type Policy struct {
	GroupID, NodeClass, Datacenter, NodePool, Selector, InstanceIDMetaKey string
	DrainDeadline                                                         time.Duration
	IgnoreSystemJobs, Purge, ScaleInProtection                            bool
}

var regionPattern = regexp.MustCompile(`^[a-z][a-z0-9]*(?:-[a-z0-9]+)+$`)
var groupPattern = regexp.MustCompile(`^asg-[A-Za-z0-9]+$`)

func invalid(key string) error { return errors.New("invalid " + key) }

// RestartCooldown is the optional policy-target duplicate of the Autoscaler
// cooldown. Zero means the key is absent and restart behavior is unchanged.
// It is intentionally not part of Policy: route identity, SDK options and the
// policy digest must not change when an operator sets or edits it.
func RestartCooldown(values map[string]string) (time.Duration, error) {
	raw, ok := values["restart_cooldown"]
	if !ok {
		return 0, nil
	}
	duration, err := time.ParseDuration(raw)
	if err != nil || duration <= 0 {
		return 0, invalid("restart_cooldown")
	}
	return duration, nil
}
func boolean(values map[string]string, key string, fallback bool) (bool, error) {
	v, ok := values[key]
	if !ok {
		return fallback, nil
	}
	b, e := strconv.ParseBool(v)
	if e != nil {
		return false, invalid(key)
	}
	return b, nil
}

func ParseAgent(values map[string]string) (Agent, error) {
	a := Agent{Region: values["tencent_region"], AuthMethod: "instance_identity", RetryAttempts: 15, StateDir: DefaultStateDir, RegistrationGracePeriod: 30 * time.Minute}
	for k := range values {
		switch k {
		case "policy_dir", "tencent_registration_grace_period", "tencent_metrics_listen_address", "tencent_region", "tencent_auth_method", "tencent_credentials_file", "tencent_state_dir", "retry_attempts", "scale_in_protection":
		default:
			if !nomadclient.IsConfigKey(k) {
				return Agent{}, errors.New("unknown or misplaced Agent option")
			}
		}
	}
	a.PolicyDir = values["policy_dir"]
	if a.PolicyDir != "" && (!filepath.IsAbs(a.PolicyDir) || strings.ContainsAny(a.PolicyDir, "\x00\r\n")) {
		return Agent{}, invalid("policy_dir")
	}
	if value, ok := values["tencent_registration_grace_period"]; ok {
		duration, err := time.ParseDuration(value)
		if err != nil || duration <= 0 {
			return Agent{}, invalid("tencent_registration_grace_period")
		}
		a.RegistrationGracePeriod = duration
	}
	a.MetricsAddress = values["tencent_metrics_listen_address"]
	if !telemetry.ValidAddress(a.MetricsAddress) {
		return Agent{}, invalid("tencent_metrics_listen_address")
	}
	if len(a.Region) > 64 || !regionPattern.MatchString(a.Region) {
		return Agent{}, invalid("tencent_region")
	}
	if value, ok := values["tencent_state_dir"]; ok {
		if !filepath.IsAbs(value) || filepath.Clean(value) == "/" || strings.ContainsAny(value, "\x00\r\n") {
			return Agent{}, invalid("tencent_state_dir")
		}
		a.StateDir = filepath.Clean(value)
	}
	if v, ok := values["tencent_auth_method"]; ok {
		a.AuthMethod = v
	}
	if a.AuthMethod != "instance_identity" && a.AuthMethod != "api_key" {
		return Agent{}, invalid("tencent_auth_method")
	}
	if v, ok := values["retry_attempts"]; ok {
		n, e := strconv.Atoi(v)
		if e != nil || n < 1 {
			return Agent{}, invalid("retry_attempts")
		}
		a.RetryAttempts = n
	}
	var err error
	a.ScaleInProtection, err = boolean(values, "scale_in_protection", false)
	if err != nil {
		return Agent{}, err
	}
	a.CredentialsFile = values["tencent_credentials_file"]
	if a.CredentialsFile != "" && (a.AuthMethod != "api_key" || !filepath.IsAbs(a.CredentialsFile) || strings.ContainsAny(a.CredentialsFile, "\x00\r\n")) {
		return Agent{}, invalid("tencent_credentials_file")
	}
	return a, nil
}

func (a Agent) Policy(values map[string]string) (Policy, error) {
	p := Policy{GroupID: values["tencent_group_id"], NodeClass: values["node_class"], Datacenter: values["datacenter"], NodePool: values["node_pool"], Selector: sdk.TargetNodeSelectorStrategyLeastBusy, InstanceIDMetaKey: "cloud_instance_id", DrainDeadline: 15 * time.Minute, ScaleInProtection: a.ScaleInProtection}
	for k := range values {
		switch k {
		case "tencent_group_id", "node_class", "datacenter", "node_pool", "node_selector_strategy", "node_drain_deadline", "node_drain_ignore_system_jobs", "node_purge", "instance_id_meta_key", "scale_in_protection":
		case "dry-run":
			// The Agent recognizes exactly "true". Keep its execution control
			// out of persistent routing and the cloud/SDK policy.
			if values[k] != "true" && values[k] != "false" {
				return Policy{}, invalid("dry-run")
			}
		case "restart_cooldown":
			// Accepted per policy, then applied only while answering Status.
			// Persisting it would make a cooldown edit look like a new SDK policy.
			if _, err := RestartCooldown(values); err != nil {
				return Policy{}, err
			}
		case shared.PluginConfigKeyGRPCTimeout:
			// The standard Agent injects this transport setting. It is not
			// part of the persistent policy or cloud/SDK configuration.
			d, err := time.ParseDuration(values[k])
			if err != nil || d <= 0 {
				return Policy{}, invalid("Agent transport timeout")
			}
		default:
			return Policy{}, errors.New("unknown or misplaced policy option")
		}
	}
	if len(p.GroupID) > 64 || !groupPattern.MatchString(p.GroupID) {
		return Policy{}, invalid("tencent_group_id")
	}
	for _, v := range []string{p.NodeClass, p.Datacenter, p.NodePool} {
		if strings.ContainsAny(v, "\x00\r\n") {
			return Policy{}, errors.New("invalid node filter")
		}
	}
	if v, ok := values["node_selector_strategy"]; ok {
		p.Selector = v
	}
	switch p.Selector {
	case sdk.TargetNodeSelectorStrategyLeastBusy, sdk.TargetNodeSelectorStrategyNewestCreateIndex, sdk.TargetNodeSelectorStrategyOldestCreateIndex, sdk.TargetNodeSelectorStrategyEmpty, sdk.TargetNodeSelectorStrategyEmptyIgnoreSystemJobs:
	default:
		return Policy{}, invalid("node_selector_strategy")
	}
	if v, ok := values["instance_id_meta_key"]; ok {
		p.InstanceIDMetaKey = v
	}
	if strings.TrimSpace(p.InstanceIDMetaKey) == "" || strings.ContainsAny(p.InstanceIDMetaKey, "\x00\r\n") {
		return Policy{}, invalid("instance_id_meta_key")
	}
	var err error
	if v, ok := values["node_drain_deadline"]; ok {
		p.DrainDeadline, err = time.ParseDuration(v)
		if err != nil {
			return Policy{}, invalid("node_drain_deadline")
		}
	}
	if p.IgnoreSystemJobs, err = boolean(values, "node_drain_ignore_system_jobs", false); err != nil {
		return Policy{}, err
	}
	if p.Purge, err = boolean(values, "node_purge", false); err != nil {
		return Policy{}, err
	}
	if p.ScaleInProtection, err = boolean(values, "scale_in_protection", a.ScaleInProtection); err != nil {
		return Policy{}, err
	}
	return p, nil
}

// SDKConfig returns a fresh map. An empty filter is omitted so the adapter can
// constrain an otherwise unfiltered pool to verified cloud group membership.
func (p Policy) SDKConfig() map[string]string {
	out := map[string]string{"node_selector_strategy": p.Selector, "node_drain_deadline": p.DrainDeadline.String(), "node_drain_ignore_system_jobs": strconv.FormatBool(p.IgnoreSystemJobs), "node_purge": strconv.FormatBool(p.Purge)}
	for k, v := range map[string]string{"node_class": p.NodeClass, "datacenter": p.Datacenter, "node_pool": p.NodePool} {
		if v != "" {
			out[k] = v
		}
	}
	return out
}
