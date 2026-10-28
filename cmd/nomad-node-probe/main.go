// nomad-node-probe defaults to read-only inspection of one explicit identity.
// It exercises Nomad only; it never creates/deletes cloud instances.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadclient"
)

type report struct {
	Action   string                      `json:"action"`
	Preview  bool                        `json:"preview"`
	At       time.Time                   `json:"at"`
	Node     *nomadclient.Node           `json:"node,omitempty"`
	Mutation *nomadclient.MutationResult `json:"mutation,omitempty"`
	Error    *nomadclient.Error          `json:"error,omitempty"`
}

func safe(err error) *nomadclient.Error {
	if err == nil {
		return nil
	}
	if e, ok := err.(*nomadclient.Error); ok {
		return e
	}
	return &nomadclient.Error{Code: "ProbeFailed"}
}
func save(dir, name string, value any) error {
	b, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	f, err := os.OpenFile(filepath.Join(dir, name), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	if _, err = f.Write(append(b, '\n')); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
func run(args []string) (out report, exit int) {
	out.At = time.Now().UTC()
	fs := flag.NewFlagSet("nomad-node-probe", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	address := fs.String("nomad-address", "http://127.0.0.1:4646", "Nomad root address")
	action := fs.String("action", "read", "read, start-drain or restore")
	execute := fs.Bool("execute", false, "explicitly mutate this exact node")
	node := fs.String("node-id", "", "full Nomad UUID")
	instance := fs.String("expected-instance", "", "expected cloud_instance_id metadata")
	group := fs.String("expected-group", "", "expected tencent_group_id metadata")
	region := fs.String("expected-region", "", "expected tencent_region metadata")
	operation := fs.String("operation-id", "", "unique durable operation identity")
	deadline := fs.Duration("drain-deadline", 2*time.Minute, "finite drain deadline; allocations may be stopped at expiry")
	timeout := fs.Duration("request-timeout", 10*time.Second, "per HTTP request timeout, maximum 30s")
	evidence := fs.String("evidence-dir", "", "new absolute directory required for execute")
	fail := func(code string) (report, int) { out.Error = &nomadclient.Error{Code: code}; return out, 1 }
	if fs.Parse(args) != nil || fs.NArg() != 0 {
		return fail("InvalidArguments")
	}
	out.Action, out.Preview = *action, !*execute
	if *action != "read" && *action != "start-drain" && *action != "restore" {
		return fail("InvalidAction")
	}
	if *execute && *action == "read" {
		return fail("ExecuteRequiresMutationAction")
	}
	want := nomadclient.Identity{NodeID: *node, InstanceID: *instance, GroupID: *group, Region: *region}
	if err := want.Validate(); err != nil {
		out.Error = safe(err)
		return out, 1
	}
	if *action != "read" {
		if err := nomadclient.ValidateOperationID(*operation); err != nil {
			out.Error = safe(err)
			return out, 1
		}
	}
	if *action == "start-drain" && (*deadline <= 0 || *deadline > 24*time.Hour) {
		return fail("InvalidDrainDeadline")
	}
	client, err := nomadclient.New(nomadclient.Config{Address: *address, Token: os.Getenv("NOMAD_TOKEN"), Timeout: *timeout})
	if err != nil {
		out.Error = safe(err)
		return out, 1
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 4*(*timeout))
	defer cancel()
	n, err := client.ReadNode(ctx, *node)
	if err != nil {
		out.Error = safe(err)
		return out, 1
	}
	if err = nomadclient.CheckIdentity(n, want); err != nil {
		out.Error = safe(err)
		return out, 1
	}
	out.Node = &n
	if !*execute {
		return out, 0
	}
	if !filepath.IsAbs(*evidence) {
		return fail("AbsoluteEvidenceDirectoryRequired")
	}
	if err = os.Mkdir(*evidence, 0700); err != nil {
		return fail("NewEvidenceDirectoryRequired")
	}
	// Make the new directory durable before the operation's durable intent.
	parent, err := os.Open(filepath.Dir(*evidence))
	if err != nil {
		return fail("EvidenceCreateFailed")
	}
	err = parent.Sync()
	parent.Close()
	if err != nil {
		return fail("EvidenceCreateFailed")
	}
	intent := struct {
		At                  time.Time            `json:"at"`
		Action              string               `json:"action"`
		Identity            nomadclient.Identity `json:"identity"`
		OperationID         string               `json:"operation_id"`
		DeadlineNanoseconds int64                `json:"deadline_nanoseconds"`
		Notice              string               `json:"notice"`
	}{time.Now().UTC(), *action, want, *operation, int64(*deadline), "One mutation attempt only. No CAS. Keep a single writer; inspect unknown outcomes instead of replaying."}
	if err = save(*evidence, "intent.json", intent); err != nil {
		return fail("EvidenceWriteFailed")
	}
	var result nomadclient.MutationResult
	if *action == "start-drain" {
		result, err = client.StartOwnedDrain(ctx, want, *operation, *deadline)
	} else {
		result, err = client.RestoreOwnedDrain(ctx, want, *operation)
	}
	out.Mutation = &result
	out.Error = safe(err)
	out.At = time.Now().UTC()
	if saveErr := save(*evidence, "result.json", out); saveErr != nil {
		out.Error = &nomadclient.Error{Code: "ResultEvidenceWriteFailed", Unknown: result.WriteAttempted}
		return out, 1
	}
	if err != nil {
		return out, 1
	}
	return out, 0
}
func main() { out, code := run(os.Args[1:]); _ = json.NewEncoder(os.Stdout).Encode(out); os.Exit(code) }
