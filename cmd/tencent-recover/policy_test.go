package main

import (
	"bytes"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/live"
	"testing"
)

func TestIdlePolicyCLINeverConstructsProviderClients(t *testing.T) {
	factory := func(string, string) (live.RecoveryCloud, live.RecoveryNomad, func(), error) {
		t.Fatal("offline command constructed provider clients")
		return nil, nil, nil, nil
	}
	for _, args := range [][]string{{"policy-preview", "--help"}, {"policy-apply", "--help"}, {"policy-preview"}, {"policy-apply", "--plan", "/nonexistent-policy-plan", "--confirm", "bad"}, {"policy-preview", "--plan", "unused", "--policy", "/nonexistent-policy-options", "--state-dir", "/nonexistent", "--operator", "test", "--reference", "unit/cli"}} {
		var out bytes.Buffer
		code := run(args, &out, factory)
		if len(args) == 2 && code != 0 {
			t.Fatal(out.String())
		}
		if len(args) != 2 && code == 0 {
			t.Fatal("invalid input accepted")
		}
	}
}
