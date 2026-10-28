package targetconfig

import "testing"

func TestMetricsConfigurationScopeAndDefault(t *testing.T) {
	a, err := ParseAgent(map[string]string{"tencent_region": "ap-chengdu"})
	if err != nil || a.MetricsAddress != "" {
		t.Fatal("metrics must default off")
	}
	for _, addr := range []string{"", "127.0.0.1:9091", "[::1]:9091"} {
		a, err = ParseAgent(map[string]string{"tencent_region": "ap-chengdu", "tencent_metrics_listen_address": addr})
		if err != nil || a.MetricsAddress != addr {
			t.Fatal("valid address rejected")
		}
	}
	for _, addr := range []string{"0.0.0.0:9091", "[::]:9091", "localhost:9091", "127.0.0.1:65536"} {
		if _, err = ParseAgent(map[string]string{"tencent_region": "ap-chengdu", "tencent_metrics_listen_address": addr}); err == nil {
			t.Fatal("unsafe address accepted")
		}
	}
	if _, err = a.Policy(map[string]string{"tencent_group_id": "asg-test", "tencent_metrics_listen_address": "127.0.0.1:9091"}); err == nil {
		t.Fatal("Agent-only setting accepted in policy")
	}
}
