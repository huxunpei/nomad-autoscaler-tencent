package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"os"
	"time"

	"github.com/huxunpei/nomad-autoscaler-tencent/internal/live"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadclient"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/tencentcloud"
	"github.com/huxunpei/nomad-autoscaler-tencent/plugin"
)

type launchClients func(context.Context, string, string, string) (plugin.LaunchConfigurationCloud, live.RecoveryNomad, func(), error)

func actualLaunchClients(ctx context.Context, region, address, auth string) (plugin.LaunchConfigurationCloud, live.RecoveryNomad, func(), error) {
	values := map[string]string{}
	if address != "" {
		values["nomad_address"] = address
	}
	cfg, err := nomadclient.ConfigFromMap(values, os.Getenv)
	if err != nil {
		return nil, nil, nil, err
	}
	n, err := nomadclient.New(cfg)
	if err != nil {
		return nil, nil, nil, err
	}
	c, err := tencentcloud.NewAuthenticated(ctx, tencentcloud.AuthConfig{Region: region, Method: auth, Timeout: 10 * time.Second})
	if err != nil {
		n.Close()
		return nil, nil, nil, err
	}
	return c, n, func() { c.Close(); n.Close() }, nil
}
func runLaunch(args []string, out io.Writer, factory launchClients) int {
	emit := func(v any) int {
		if json.NewEncoder(out).Encode(v) != nil {
			return 1
		}
		return 0
	}
	fail := func(err error) int { emit(map[string]string{"error": err.Error()}); return 1 }
	fs := flag.NewFlagSet(args[0], flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dir := fs.String("state-dir", "", "exact stopped v3 group directory containing route.json and live.json")
	region := fs.String("region", "", "region; must match the verified account and route")
	auth := fs.String("auth-method", "instance_identity", "instance_identity or api_key; keys only in scoped environment")
	address := fs.String("nomad-address", "", "Nomad endpoint; defaults to NOMAD_ADDR")
	path := fs.String("plan", "", "new private plan for preview, existing plan for apply")
	actor := fs.String("operator", "", "operator label")
	ref := fs.String("reference", "", "change reference")
	confirm := fs.String("confirm", "", "exact preview digest")
	timeout := fs.Duration("timeout", 90*time.Second, "bounded read timeout, 1s to 5m")
	err := fs.Parse(args[1:])
	if err == flag.ErrHelp {
		fs.SetOutput(out)
		fs.PrintDefaults()
		return 0
	}
	if err != nil || fs.NArg() != 0 || *dir == "" || *region == "" || *path == "" || *timeout < time.Second || *timeout > 5*time.Minute || (*auth != "api_key" && *auth != "instance_identity") {
		return fail(errors.New("InvalidLaunchConfigurationArguments"))
	}
	preview := args[0] == "lc-preview"
	if (preview && (*actor == "" || *ref == "" || *confirm != "")) || (!preview && (*confirm == "" || *actor != "" || *ref != "")) {
		return fail(errors.New("InvalidLaunchConfigurationArguments"))
	}
	var plan live.RecoveryPlan
	if !preview {
		plan, err = live.ReadRecoveryPlan(*path)
		if err != nil {
			return fail(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	c, n, closeClients, err := factory(ctx, *region, *address, *auth)
	if err != nil {
		return fail(errors.New("RecoveryClientConfigurationFailed"))
	}
	defer closeClients()
	var result any
	err = plugin.WithLaunchConfigurationMaintenance(ctx, *dir, c, n, func(r *live.Recovery, routeHash string) error {
		if preview {
			plan, err = r.PreviewLaunchConfiguration(ctx, *actor, *ref, routeHash)
			if err != nil {
				return err
			}
			if err = live.WriteRecoveryPlan(*path, plan); err != nil {
				return err
			}
			// The private plan contains exact state hash, stable read evidence and actor.
			result = map[string]any{"digest": plan.Digest, "expires_at": plan.ExpiresAt, "before": plan.BeforeLaunchConfiguration, "after": plan.Evidence[1].Cloud.LaunchConfiguration, "provider_writes": 0, "plan": *path}
		} else {
			result, err = r.ApplyLaunchConfiguration(ctx, plan, *confirm, routeHash)
		}
		return err
	})
	if err != nil {
		return fail(err)
	}
	return emit(result)
}
