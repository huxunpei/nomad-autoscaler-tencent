package plugin

import (
	"strings"
	"testing"
)

func TestLiveRequiresExactGroupAndExplicitCredentials(t *testing.T) {
	t.Setenv("TENCENT_AS_SECRET_ID", "")
	t.Setenv("TENCENT_AS_SECRET_KEY", "")
	p := &Target{}
	cfg := map[string]string{"tencent_backend": "live-experimental", "tencent_group_id": "asg-example", "tencent_confirm_exclusive_group": "asg-other"}
	if err := p.SetConfig(cfg); err == nil || !strings.Contains(err.Error(), "exclusive group") {
		t.Fatal("exclusive group confirmation not enforced")
	}
	cfg["tencent_confirm_exclusive_group"] = "asg-example"
	if err := p.SetConfig(cfg); err == nil || !strings.Contains(err.Error(), "explicit TENCENT_AS_SECRET_ID") {
		t.Fatal("explicit credential boundary not enforced")
	}
}

func TestLiveRejectsEmbeddedSecretsAndPublicHTTPBeforeNetwork(t *testing.T) {
	t.Setenv("TENCENT_AS_SECRET_ID", "test-only-id")
	t.Setenv("TENCENT_AS_SECRET_KEY", "test-only-key")
	cfg := map[string]string{"tencent_backend": "live-experimental", "tencent_group_id": "asg-example", "tencent_confirm_exclusive_group": "asg-example", "tencent_region": "ap-chengdu", "nomad_address": "http://example.com:4646", "tencent_secret_key": "DO_NOT_ECHO"}
	p := &Target{}
	err := p.SetConfig(cfg)
	if err == nil || !strings.Contains(err.Error(), "forbidden") || strings.Contains(err.Error(), "DO_NOT_ECHO") {
		t.Fatal("embedded secret was accepted or exposed")
	}
	delete(cfg, "tencent_secret_key")
	if err := p.SetConfig(cfg); err == nil || !strings.Contains(err.Error(), "PlainHTTPRequiresLoopback") {
		t.Fatal("public plain HTTP should be rejected before cloud initialization")
	}
}

func TestLiveRejectsInvalidCapacityLimitBeforeNetwork(t *testing.T) {
	t.Setenv("TENCENT_AS_SECRET_ID", "test-only-id")
	t.Setenv("TENCENT_AS_SECRET_KEY", "test-only-key")
	for _, limit := range []string{"0", "-1", "101", "bad"} {
		cfg := map[string]string{"tencent_backend": "live-experimental", "tencent_group_id": "asg-example", "tencent_confirm_exclusive_group": "asg-example", "tencent_region": "ap-chengdu", "nomad_address": "http://127.0.0.1:4646", "tencent_capacity_limit": limit}
		p := &Target{}
		if err := p.SetConfig(cfg); err == nil || !strings.Contains(err.Error(), "capacity_limit") {
			t.Fatal(limit, err)
		}
	}
}

func TestLiveAcceptsInheritedNomadTokenAndValidatesTLSBeforeCloud(t *testing.T) {
	t.Setenv("TENCENT_AS_SECRET_ID", "synthetic-id")
	t.Setenv("TENCENT_AS_SECRET_KEY", "synthetic-key")
	cfg := map[string]string{"tencent_backend": "live-experimental", "tencent_group_id": "asg-example", "tencent_confirm_exclusive_group": "asg-example", "tencent_region": "ap-chengdu", "nomad_address": "https://127.0.0.1:4646", "nomad_token": "synthetic-inherited-token", "nomad_ca-cert": t.TempDir() + "/absent-ca.pem"}
	p := &Target{}
	// A missing CA must be detected locally, before live.Open can observe cloud.
	if err := p.SetConfig(cfg); err == nil || err.Error() != "InvalidTLSCA" || strings.Contains(err.Error(), "synthetic-inherited-token") {
		t.Fatalf("inherited Nomad config was rejected or not validated: %v", err)
	}
	if p.live != nil || p.nomad != nil {
		t.Fatal("failed setup retained a live controller")
	}
}
