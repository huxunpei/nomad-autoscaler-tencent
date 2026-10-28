// tencent-contract is a guarded 0 -> 1 -> 0 cloud API experiment, not a Nomad test.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"os"
	"os/signal"
	"regexp"
	"syscall"
	"time"

	as "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/as/v20180419"
	"github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common"
	"github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common/profile"
	cvm "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/cvm/v20170312"
)

func runCLI(ctx context.Context, args []string, getenv func(string) string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("tencent-contract", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	o := options{}
	flags.StringVar(&o.Region, "region", "", "Tencent Cloud region")
	flags.StringVar(&o.GroupID, "group-id", "", "test group ID")
	flags.BoolVar(&o.Execute, "execute", false, "execute the guarded 0->1->0 experiment")
	flags.StringVar(&o.ConfirmGroup, "confirm-group", "", "confirm exclusive test group; must exactly match group-id")
	flags.StringVar(&o.EvidenceDir, "evidence-dir", "", "absolute parent path for durable non-secret evidence")
	flags.DurationVar(&o.PollInterval, "poll-interval", 10*time.Second, "observation interval")
	flags.DurationVar(&o.TotalTimeout, "total-timeout", 15*time.Minute, "overall deadline, at most 30m")
	flags.DurationVar(&o.RequestTimeout, "request-timeout", 30*time.Second, "per API request deadline")
	failure := func(err error) int { _ = json.NewEncoder(stderr).Encode(sanitize("CLI", err)); return 1 }
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			_, _ = io.WriteString(stdout, "Usage: tencent-contract -region REGION -group-id ASG_ID [--execute --confirm-group ASG_ID --evidence-dir /absolute/path]\nDefault is read-only preview. Execute requires an exclusive empty Min=0 Max=1 Desired=0 group and creates then removes one VM.\nCredentials: explicit TENCENT_AS_SECRET_ID / TENCENT_AS_SECRET_KEY / optional TENCENT_AS_TOKEN only.\n")
			return 0
		}
		return failure(fail("InvalidArguments"))
	}
	if flags.NArg() != 0 || !regexp.MustCompile(`^[a-z][a-z0-9-]{1,63}$`).MatchString(o.Region) || !regexp.MustCompile(`^asg-[A-Za-z0-9]+$`).MatchString(o.GroupID) || o.PollInterval < time.Second || o.PollInterval > time.Minute || o.TotalTimeout < time.Second || o.TotalTimeout > 30*time.Minute || o.RequestTimeout <= 0 || o.RequestTimeout > 2*time.Minute {
		return failure(fail("InvalidArguments"))
	}
	if o.Execute && (o.ConfirmGroup != o.GroupID || o.EvidenceDir == "") {
		return failure(fail("ExecutionConfirmationMissing"))
	}
	id, key := getenv("TENCENT_AS_SECRET_ID"), getenv("TENCENT_AS_SECRET_KEY")
	if id == "" || key == "" {
		return failure(fail("MissingExplicitCredentials"))
	}
	credential := common.NewTokenCredential(id, key, getenv("TENCENT_AS_TOKEN"))
	cp := profile.NewClientProfile()
	cp.Debug, cp.DisableRegionBreaker = false, true
	cp.NetworkFailureMaxRetries, cp.RateLimitExceededMaxRetries = 0, 0
	cp.UnsafeRetryOnConnectionFailure = false
	cp.HttpProfile.ReqTimeout = int((o.RequestTimeout + time.Second - 1) / time.Second)
	asClient, err := as.NewClient(credential, o.Region, cp)
	if err != nil {
		return failure(err)
	}
	cvmClient, err := cvm.NewClient(credential, o.Region, cp)
	if err != nil {
		return failure(err)
	}
	r := runner{cloud: &sdkCloud{as: asClient, cvm: cvmClient, timeout: o.RequestTimeout}, opts: o}
	var files *fileEvidence
	if o.Execute {
		files, err = newFileEvidence(o.EvidenceDir)
		if err != nil {
			return failure(err)
		}
		r.evidence, r.opts.EvidenceDir = files, files.dir
	}
	ctx, cancel := context.WithTimeout(ctx, o.TotalTimeout)
	defer cancel()
	out, err := r.run(ctx)
	if files != nil {
		if saveErr := r.save("outcome", out); saveErr != nil {
			out.Code, out.Completed, out.Error, err = "NeedsManualCheck", false, sanitize("Evidence", saveErr), saveErr
		}
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	if writeErr := encoder.Encode(out); writeErr != nil {
		return failure(fail("OutputFailed"))
	}
	if err != nil {
		return 1
	}
	return 0
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	os.Exit(runCLI(ctx, os.Args[1:], os.Getenv, os.Stdout, os.Stderr))
}
