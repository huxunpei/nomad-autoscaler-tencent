// tencent-recover reconciles a frozen successful operation. It never scales or
// changes Nomad eligibility, and requires the same single-writer journal lock.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"syscall"
	"time"

	"github.com/huxunpei/nomad-autoscaler-tencent/internal/live"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadclient"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/tencentcloud"
)

type clients func(string, string) (live.RecoveryCloud, live.RecoveryNomad, func(), error)

func actualClients(region, address string) (live.RecoveryCloud, live.RecoveryNomad, func(), error) {
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
	c, err := tencentcloud.New(tencentcloud.Config{Region: region, SecretID: os.Getenv("TENCENT_AS_SECRET_ID"), SecretKey: os.Getenv("TENCENT_AS_SECRET_KEY"), Token: os.Getenv("TENCENT_AS_TOKEN"), Timeout: 10 * time.Second})
	if err != nil {
		n.Close()
		return nil, nil, nil, err
	}
	return c, n, n.Close, nil
}
func run(args []string, out io.Writer, newClients clients) int {
	if len(args) > 0 && (args[0] == "lc-preview" || args[0] == "lc-apply") {
		return runLaunch(args, out, actualLaunchClients)
	}
	if len(args) > 0 && (args[0] == "policy-preview" || args[0] == "policy-apply") {
		return runPolicy(args, out)
	}
	emit := func(v any) int {
		if json.NewEncoder(out).Encode(v) != nil {
			return 1
		}
		return 0
	}
	fail := func(s string) int { emit(map[string]string{"error": s}); return 1 }
	if len(args) == 1 && (args[0] == "--help" || args[0] == "-h" || args[0] == "help") {
		return emit(map[string]string{"usage": "tencent-recover <preview|apply|repair|policy-preview|policy-apply|lc-preview|lc-apply> [flags]", "help": "Use <command> --help for flags; credentials are accepted only through the scoped process environment."})
	}
	if len(args) == 0 {
		return fail("ExpectedPreviewApplyOrRepair")
	}
	command := args[0]
	if command != "preview" && command != "apply" && command != "repair" {
		return fail("ExpectedPreviewApplyOrRepair")
	}
	fs := flag.NewFlagSet("tencent-recover", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dir := fs.String("state-dir", "", "existing private live state directory")
	group := fs.String("group", "", "exact ASG ID")
	region := fs.String("region", "", "Tencent region")
	maximum := fs.Int64("max-capacity", 1, "must match legacy plugin configuration")
	groupContract := fs.Bool("group-contract", false, "use group-limit journal contract (v3)")
	deadline := fs.Duration("drain-deadline", time.Minute, "must match plugin configuration")
	protection := fs.Bool("scale-in-protection", false, "must match plugin configuration")
	address := fs.String("nomad-address", "", "Nomad API endpoint; defaults to NOMAD_ADDR or http://127.0.0.1:4646")
	activity := fs.String("activity", "", "operator selected exact successful ActivityId")
	operator := fs.String("operator", "", "operator label; no credentials")
	reference := fs.String("reference", "", "change/ticket reference; no credentials")
	planPath := fs.String("plan", "", "private plan JSON file")
	confirm := fs.String("confirm", "", "exact preview SHA256")
	timeout := fs.Duration("timeout", 90*time.Second, "overall bounded read timeout")
	parseErr := fs.Parse(args[1:])
	if parseErr == flag.ErrHelp {
		fs.SetOutput(out)
		fmt.Fprintf(out, "Usage: tencent-recover %s [flags]\n", command)
		fs.PrintDefaults()
		return 0
	}
	if parseErr != nil || fs.NArg() != 0 || *dir == "" || *group == "" || *region == "" || *timeout < time.Second || *timeout > 5*time.Minute {
		return fail("InvalidArguments")
	}
	cfg := live.Config{GroupContract: *groupContract, Dir: *dir, GroupID: *group, Region: *region, MaxCapacity: *maximum, DrainDeadline: *deadline, ScaleInProtection: *protection, ReconcileInterval: time.Second}
	if command == "repair" {
		if *confirm == "" || *activity != "" || *planPath != "" || *operator != "" || *reference != "" {
			return fail("InvalidRepairArguments")
		}
		result, err := live.RepairRecovery(cfg, *confirm)
		if err != nil {
			return fail(err.Error())
		}
		return emit(result)
	}
	if *planPath == "" {
		return fail("PlanPathRequired")
	}
	var p live.RecoveryPlan
	if command == "apply" {
		if *confirm == "" || *activity != "" || *operator != "" || *reference != "" {
			return fail("InvalidApplyArguments")
		}
		var err error
		p, err = live.ReadRecoveryPlan(*planPath)
		if err != nil {
			return fail(err.Error())
		}
	} else if *activity == "" || *operator == "" || *reference == "" || *confirm != "" {
		return fail("InvalidPreviewArguments")
	}
	c, n, closeClients, err := newClients(*region, *address)
	if err != nil {
		return fail("RecoveryClientConfigurationFailed")
	}
	defer closeClients()
	r, err := live.OpenRecovery(cfg, c, n)
	if err != nil {
		return fail(err.Error())
	}
	defer r.Close()
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	if command == "preview" {
		p, err = r.Preview(ctx, *activity, *operator, *reference)
		if err != nil {
			return fail(err.Error())
		}
		if err = live.WriteRecoveryPlan(*planPath, p); err != nil {
			return fail(err.Error())
		}
		return emit(p)
	}
	result, err := r.Apply(ctx, p, *confirm)
	if err != nil {
		return fail(err.Error())
	}
	return emit(result)
}
func main() {
	syscall.Umask(0077)
	_ = syscall.Setrlimit(syscall.RLIMIT_CORE, &syscall.Rlimit{})
	os.Exit(run(os.Args[1:], os.Stdout, actualClients))
}
