package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"

	"github.com/huxunpei/nomad-autoscaler-tencent/plugin"
)

// Offline configuration maintenance never constructs the cloud/Nomad clients.
func runPolicy(args []string, out io.Writer) int {
	emit := func(v any) int {
		if json.NewEncoder(out).Encode(v) != nil {
			return 1
		}
		return 0
	}
	fail := func(err error) int { emit(map[string]string{"error": err.Error()}); return 1 }
	fs := flag.NewFlagSet(args[0], flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dir := fs.String("state-dir", "", "exact stopped group directory containing route.json and live.json")
	values := fs.String("policy", "", "private JSON map of complete policy target options; no credentials")
	plan := fs.String("plan", "", "new private plan for preview, existing plan for apply")
	actor := fs.String("operator", "", "operator label")
	ref := fs.String("reference", "", "change reference")
	confirm := fs.String("confirm", "", "exact preview digest")
	err := fs.Parse(args[1:])
	if err == flag.ErrHelp {
		fs.SetOutput(out)
		fs.PrintDefaults()
		return 0
	}
	if err != nil || fs.NArg() != 0 || *plan == "" {
		return fail(fmt.Errorf("InvalidPolicyUpdateArguments"))
	}
	if args[0] == "policy-preview" {
		if *dir == "" || *values == "" || *actor == "" || *ref == "" || *confirm != "" {
			return fail(fmt.Errorf("InvalidPolicyPreviewArguments"))
		}
		v, err := plugin.ReadIdlePolicyValues(*values)
		if err != nil {
			return fail(err)
		}
		p, err := plugin.PreviewIdlePolicyUpdate(*dir, v, *actor, *ref)
		if err != nil {
			return fail(err)
		}
		if err = plugin.WriteIdlePolicyPlan(*plan, p); err != nil {
			return fail(err)
		}
		var old, next any
		_ = json.Unmarshal(p.BeforeRoute, &old)
		_ = json.Unmarshal(p.AfterRoute, &next)
		return emit(map[string]any{"digest": p.Digest, "before": old, "after": next, "expires_at": p.ExpiresAt, "provider_writes": 0})
	}
	if *confirm == "" || *dir != "" || *values != "" || *actor != "" || *ref != "" {
		return fail(fmt.Errorf("InvalidPolicyApplyArguments"))
	}
	p, err := plugin.ReadIdlePolicyPlan(*plan)
	if err != nil {
		return fail(err)
	}
	if err = plugin.ApplyIdlePolicyUpdate(p, *confirm); err != nil {
		return fail(err)
	}
	return emit(map[string]string{"status": "committed", "digest": p.Digest, "audit": p.Change.AuditID})
}
