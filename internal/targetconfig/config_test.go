package targetconfig

import (
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestSpecAgentAndPolicyDefaults(t *testing.T) {
	a, err := ParseAgent(map[string]string{"tencent_region": "ap-guangzhou"})
	if err != nil {
		t.Fatal(err)
	}
	if a.Region != "ap-guangzhou" || a.AuthMethod != "instance_identity" || a.RetryAttempts != 15 || a.ScaleInProtection || a.CredentialsFile != "" || a.StateDir != DefaultStateDir {
		t.Fatalf("wrong Agent defaults: %+v", a)
	}
	p, err := a.Policy(map[string]string{"tencent_group_id": "asg-example"})
	if err != nil {
		t.Fatal(err)
	}
	if p.GroupID != "asg-example" || p.NodeClass != "" || p.Datacenter != "" || p.NodePool != "" || p.Selector != "least_busy" || p.DrainDeadline != 15*time.Minute || p.IgnoreSystemJobs || p.Purge || p.InstanceIDMetaKey != "cloud_instance_id" || p.ScaleInProtection {
		t.Fatalf("wrong policy defaults: %+v", p)
	}
}

func TestPolicyOverridesAgentProtectionAndPreservesSDKOptions(t *testing.T) {
	for _, agent := range []bool{false, true} {
		for _, policy := range []string{"", "true", "false"} {
			cfg := map[string]string{"tencent_region": "ap-guangzhou", "scale_in_protection": "false"}
			if agent {
				cfg["scale_in_protection"] = "true"
			}
			a, err := ParseAgent(cfg)
			if err != nil {
				t.Fatal(err)
			}
			input := map[string]string{"tencent_group_id": "asg-example", "node_class": "game-server", "datacenter": "dc1", "node_pool": "pool1", "node_selector_strategy": "empty_ignore_system", "node_drain_deadline": "10m", "node_drain_ignore_system_jobs": "true", "node_purge": "true", "instance_id_meta_key": "cvm_id"}
			if policy != "" {
				input["scale_in_protection"] = policy
			}
			original := map[string]string{}
			for k, v := range input {
				original[k] = v
			}
			p, err := a.Policy(input)
			if err != nil {
				t.Fatal(err)
			}
			want := agent
			if policy != "" {
				want = policy == "true"
			}
			if p.ScaleInProtection != want || p.DrainDeadline != 10*time.Minute || p.InstanceIDMetaKey != "cvm_id" {
				t.Fatal(p)
			}
			sdk := p.SDKConfig()
			if sdk["node_class"] != "game-server" || sdk["datacenter"] != "dc1" || sdk["node_pool"] != "pool1" || sdk["node_purge"] != "true" || sdk["node_drain_ignore_system_jobs"] != "true" || sdk["node_selector_strategy"] != "empty_ignore_system" {
				t.Fatal(sdk)
			}
			sdk["node_class"] = "changed"
			if !reflect.DeepEqual(input, original) || p.NodeClass != "game-server" {
				t.Fatal("input policy map mutated")
			}
		}
	}
}

func TestConfigRejectsInvalidOrMisplacedParametersWithoutValues(t *testing.T) {
	agentCases := []map[string]string{
		{}, {"tencent_region": "invalid region"}, {"tencent_auth_method": "automatic"}, {"retry_attempts": "0"}, {"retry_attempts": "-1"}, {"retry_attempts": "overflow"},
		{"tencent_state_dir": ""}, {"tencent_state_dir": "/"}, {"tencent_state_dir": "relative"}, {"tencent_state_dir": "/var/lib/private\npath"},
		{"scale_in_protection": "yes"}, {"tencent_group_id": "asg-example"}, {"node_drain_deadline": "1m"}, {"tencent_secret_key": "PRIVATE_VALUE"}, {"tencent_credentials_file": "relative"},
	}
	for i, c := range agentCases {
		if i > 0 {
			if _, ok := c["tencent_region"]; !ok {
				c["tencent_region"] = "ap-guangzhou"
			}
		}
		_, err := ParseAgent(c)
		if err == nil || strings.Contains(err.Error(), "PRIVATE_VALUE") {
			t.Fatalf("Agent case %d: %v", i, err)
		}
	}
	a, err := ParseAgent(map[string]string{"tencent_region": "ap-guangzhou"})
	if err != nil {
		t.Fatal(err)
	}
	cases := []map[string]string{
		{}, {"tencent_group_id": "wrong"}, {"node_purge": "yes"}, {"node_drain_ignore_system_jobs": "bad"}, {"scale_in_protection": "no"}, {"node_drain_deadline": "bad"}, {"node_selector_strategy": "private-unknown-selector"}, {"instance_id_meta_key": ""},
		{"tencent_region": "ap-other"}, {"retry_attempts": "10"}, {"tencent_auth_method": "api_key"}, {"tencent_credentials_file": "/tmp/private"}, {"nomad_token": "PRIVATE_VALUE"}, {"secret_key": "PRIVATE_VALUE"}, {"tencent_secret_key_env": "PRIVATE_VALUE"}, {"node_pruge": "true"},
	}
	for i, c := range cases {
		if i > 0 {
			if _, ok := c["tencent_group_id"]; !ok {
				c["tencent_group_id"] = "asg-example"
			}
		}
		_, err := a.Policy(c)
		if err == nil || strings.Contains(err.Error(), "PRIVATE_VALUE") || strings.Contains(err.Error(), "private-unknown-selector") {
			t.Fatalf("policy case %d: %v", i, err)
		}
	}
}

func TestExplicitAPIKeySourceAndSDKDeadlineSemantics(t *testing.T) {
	a, err := ParseAgent(map[string]string{"tencent_region": "ap-guangzhou", "tencent_auth_method": "api_key", "tencent_credentials_file": "/run/secrets/tencent.json", "retry_attempts": "20"})
	if err != nil {
		t.Fatal(err)
	}
	if a.AuthMethod != "api_key" || a.CredentialsFile != "/run/secrets/tencent.json" || a.RetryAttempts != 20 {
		t.Fatal(a)
	}
	for _, d := range []string{"0s", "-1s", "10m"} {
		p, err := a.Policy(map[string]string{"tencent_group_id": "asg-example", "node_drain_deadline": d})
		if err != nil || p.SDKConfig()["node_drain_deadline"] == "" {
			t.Fatal(d, err)
		}
	}
	if _, err = ParseAgent(map[string]string{"tencent_region": "ap-guangzhou", "tencent_credentials_file": "/run/secrets/tencent.json"}); err == nil {
		t.Fatal("instance identity must not silently accept an unused API key source")
	}
}

func TestPolicyAcceptsAgentDryRunWithoutChangingPersistentOptions(t *testing.T) {
	a := Agent{}
	base, err := a.Policy(map[string]string{"tencent_group_id": "asg-example"})
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{"true", "false"} {
		t.Run(value, func(t *testing.T) {
			input := map[string]string{"tencent_group_id": "asg-example", "dry-run": value}
			p, err := a.Policy(input)
			if err != nil || !reflect.DeepEqual(p, base) || !reflect.DeepEqual(p.SDKConfig(), base.SDKConfig()) || input["dry-run"] != value {
				t.Fatalf("Agent dry-run changed persistent policy or was rejected: %v", err)
			}
		})
	}
	for _, value := range []string{"", "TRUE", "1", "yes", " true", "PRIVATE_VALUE\n"} {
		if _, err := a.Policy(map[string]string{"tencent_group_id": "asg-example", "dry-run": value}); err == nil || strings.Contains(err.Error(), "PRIVATE_VALUE") {
			t.Fatal("noncanonical dry-run accepted or value disclosed")
		}
	}
	if _, err := ParseAgent(map[string]string{"tencent_region": "ap-guangzhou", "dry-run": "true"}); err == nil {
		t.Fatal("policy-only dry-run accepted in Agent target config")
	}
}

func TestRestartCooldownIsOptionalAndNotPersistent(t *testing.T) {
	a, err := ParseAgent(map[string]string{"tencent_region": "ap-guangzhou"})
	if err != nil {
		t.Fatal(err)
	}
	base, err := a.Policy(map[string]string{"tencent_group_id": "asg-example"})
	if err != nil {
		t.Fatal(err)
	}
	input := map[string]string{"tencent_group_id": "asg-example", "restart_cooldown": "5m"}
	p, err := a.Policy(input)
	if err != nil || !reflect.DeepEqual(p, base) || !reflect.DeepEqual(p.SDKConfig(), base.SDKConfig()) || input["restart_cooldown"] != "5m" {
		t.Fatalf("restart_cooldown changed the persistent policy or was rejected: %+v %v", p, err)
	}
	got, err := RestartCooldown(input)
	if err != nil || got != 5*time.Minute {
		t.Fatal(got, err)
	}
	if d, err := RestartCooldown(map[string]string{"tencent_group_id": "asg-example"}); err != nil || d != 0 {
		t.Fatal(d, err)
	}
	for _, value := range []string{"", "0", "0s", "-1s", "5", "PRIVATE_VALUE"} {
		_, err := a.Policy(map[string]string{"tencent_group_id": "asg-example", "restart_cooldown": value})
		if err == nil || strings.Contains(err.Error(), "PRIVATE_VALUE") || err.Error() != "invalid restart_cooldown" {
			t.Fatalf("value %q: %v", value, err)
		}
	}
	if _, err := ParseAgent(map[string]string{"tencent_region": "ap-guangzhou", "restart_cooldown": "5m"}); err == nil {
		t.Fatal("policy cooldown duplicate accepted in Agent target config")
	}
}
