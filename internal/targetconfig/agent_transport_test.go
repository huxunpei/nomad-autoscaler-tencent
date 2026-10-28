package targetconfig

import (
	"github.com/hashicorp/nomad-autoscaler/plugins/shared"
	"testing"
)

func TestPolicyAcceptsOnlyKnownAgentTransportKey(t *testing.T) {
	a := Agent{}
	plain := map[string]string{"tencent_group_id": "asg-one"}
	want, err := a.Policy(plain)
	if err != nil {
		t.Fatal(err)
	}
	decorated := map[string]string{"tencent_group_id": "asg-one", shared.PluginConfigKeyGRPCTimeout: "750ms"}
	got, err := a.Policy(decorated)
	if err != nil || got != want {
		t.Fatal("official transport key changed canonical policy", err)
	}
	if decorated[shared.PluginConfigKeyGRPCTimeout] != "750ms" {
		t.Fatal("mutated input")
	}
	for _, v := range []string{"bad", "0s", "-1s"} {
		decorated[shared.PluginConfigKeyGRPCTimeout] = v
		if _, err = a.Policy(decorated); err == nil {
			t.Fatal("invalid transport timeout accepted")
		}
	}
	if _, err = a.Policy(map[string]string{"tencent_group_id": "asg-one", "_unknown_internal_key": "x"}); err == nil {
		t.Fatal("unknown transport key accepted")
	}
}
