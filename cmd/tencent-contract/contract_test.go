package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type fakeEvidence struct {
	kinds  []string
	failAt string
}

func (f *fakeEvidence) Save(kind string, value any) error {
	f.kinds = append(f.kinds, kind)
	if kind == f.failAt {
		return errors.New("private raw filesystem detail")
	}
	return nil
}
func (f *fakeEvidence) has(kind string) bool {
	for _, k := range f.kinds {
		if k == kind {
			return true
		}
	}
	return false
}

type fakeCloud struct {
	t                        *testing.T
	observations             []observation
	index, expands, removals int
	removedID                string
	evidence                 *fakeEvidence
	removeErr                error
	beforeObserve            func()
}

func (f *fakeCloud) Observe(ctx context.Context, groupID, trackedID string) (observation, error) {
	if f.beforeObserve != nil {
		f.beforeObserve()
	}
	if ctx.Err() != nil {
		return observation{}, ctx.Err()
	}
	if f.index >= len(f.observations) {
		return f.observations[len(f.observations)-1], nil
	}
	o := f.observations[f.index]
	f.index++
	return o, nil
}
func (f *fakeCloud) ExpandOne(ctx context.Context, groupID string) (requestResult, error) {
	f.expands++
	if f.evidence == nil || !f.evidence.has("expand-intent") {
		f.t.Fatal("expanded without durable intent")
	}
	return requestResult{Action: "ModifyDesiredCapacity", RequestID: "expand-request"}, nil
}
func (f *fakeCloud) RemoveExact(ctx context.Context, groupID, instanceID string) (requestResult, error) {
	f.removals++
	f.removedID = instanceID
	if f.evidence == nil || !f.evidence.has("remove-intent") {
		f.t.Fatal("removed without durable intent")
	}
	return requestResult{Action: "RemoveInstances", RequestID: "remove-request", ActivityID: "asa-test"}, f.removeErr
}

func emptyObservation() observation {
	return observation{Group: groupState{ID: "asg-test", Min: 0, Max: 1, Desired: 0, Count: 0, Enabled: "ENABLED", Activity: "NOT_IN_ACTIVITY", LaunchConfiguration: "asc-test"}, LaunchConfiguration: launchConfiguration{ID: "asc-test", Status: "NORMAL", ChargeType: "POSTPAID_BY_HOUR", InstanceType: "S5.SMALL2", SecurityGroupIDs: []string{"sg-test"}}, Members: []member{}, Policies: []scalingPolicy{}}
}
func createdObservation() observation {
	o := emptyObservation()
	o.Group.Desired, o.Group.Count = 1, 1
	o.Members = []member{{ID: "ins-only-new", CreationType: "AUTO_CREATION", Lifecycle: "IN_SERVICE", AddedAt: time.Now().UTC().Format(time.RFC3339)}}
	o.VMID, o.VMReturned, o.VMState = "ins-only-new", true, "RUNNING"
	return o
}
func removedObservation() observation { o := emptyObservation(); o.VMID = "ins-only-new"; return o }
func executeRunner(t *testing.T) (*runner, *fakeCloud, *fakeEvidence) {
	e := &fakeEvidence{}
	f := &fakeCloud{t: t, evidence: e, observations: []observation{emptyObservation(), emptyObservation(), createdObservation(), removedObservation(), removedObservation()}}
	r := &runner{cloud: f, evidence: e, opts: options{Execute: true, Region: "ap-chengdu", GroupID: "asg-test", ConfirmGroup: "asg-test", EvidenceDir: "/fake/evidence", PollInterval: time.Nanosecond}}
	return r, f, e
}

func TestPreviewMakesZeroWrites(t *testing.T) {
	r, f, _ := executeRunner(t)
	r.opts.Execute = false
	r.evidence = nil
	out, err := r.run(context.Background())
	if err != nil || out.Mode != "preview" || len(out.Plan) == 0 || f.expands != 0 || f.removals != 0 {
		t.Fatal("preview performed a write or omitted its plan")
	}
}

func TestExecuteDeletesOnlyConfirmedNewIDAndProvesCleanup(t *testing.T) {
	r, f, e := executeRunner(t)
	out, err := r.run(context.Background())
	if err != nil || !out.CleanupProven || !out.Completed || f.expands != 1 || f.removals != 1 || f.removedID != "ins-only-new" || !e.has("cleanup-proof") {
		t.Fatalf("contract failed: outcome=%+v err=%v writes=%d/%d", out, err, f.expands, f.removals)
	}
}

func TestUnknownRemoveResponseIsNeverReplayed(t *testing.T) {
	r, f, _ := executeRunner(t)
	f.removeErr = context.DeadlineExceeded
	out, err := r.run(context.Background())
	if err == nil || out.Completed || !out.CleanupProven || f.removals != 1 || f.expands != 1 {
		t.Fatal("unknown remove response was replayed or reported as unqualified success")
	}
}

func TestBaselineAndPreflightBlocksRejectAllWrites(t *testing.T) {
	for _, name := range []string{"nonempty", "launch-config", "policy"} {
		t.Run(name, func(t *testing.T) {
			r, f, _ := executeRunner(t)
			switch name {
			case "nonempty":
				f.observations[0].Group.Desired = 1
			case "launch-config":
				f.observations[0].LaunchConfiguration.Status = "SECURITY_GROUP_ABNORMAL"
			case "policy":
				f.observations[0].Policies = []scalingPolicy{{ID: "asp-other", Type: "SIMPLE"}}
			}
			out, err := r.run(context.Background())
			if err == nil || len(out.BlockedReasons) == 0 || f.expands != 0 || f.removals != 0 {
				t.Fatal("blocked execute performed writes")
			}
		})
	}
}

func TestNonPostpaidLaunchConfigurationRejectsAllWrites(t *testing.T) {
	for _, chargeType := range []string{"PREPAID", "SPOTPAID", ""} {
		t.Run(chargeType, func(t *testing.T) {
			r, f, _ := executeRunner(t)
			f.observations[0].LaunchConfiguration.ChargeType = chargeType
			out, err := r.run(context.Background())
			if err == nil || f.expands != 0 || f.removals != 0 || len(out.BlockedReasons) != 1 || out.BlockedReasons[0] != "LaunchConfigurationMustBePostpaidByHour" {
				t.Fatal("unsupported billing type allowed a write")
			}
		})
	}
}

func TestLaunchConfigurationChangeStopsBeforeTheNextWrite(t *testing.T) {
	for _, phase := range []struct {
		name            string
		index           int
		expectedExpands int
	}{{"before-expand", 1, 0}, {"before-remove", 2, 1}} {
		for _, field := range []string{"charge-type", "instance-type", "instance-types", "security-groups", "image", "version"} {
			t.Run(phase.name+"/"+field, func(t *testing.T) {
				r, f, _ := executeRunner(t)
				lc := &f.observations[phase.index].LaunchConfiguration
				switch field {
				case "charge-type":
					lc.ChargeType = "PREPAID"
				case "instance-type":
					lc.InstanceType = "S5.MEDIUM4"
				case "instance-types":
					lc.InstanceTypes = []string{"S5.MEDIUM4"}
				case "security-groups":
					lc.SecurityGroupIDs = []string{"sg-replaced"}
				case "image":
					lc.ImageID = "img-replaced"
				case "version":
					version := int64(2)
					lc.VersionNumber = &version
				}
				_, err := r.run(context.Background())
				if err == nil || err.Error() != "LaunchConfigurationChanged" || f.expands != phase.expectedExpands || f.removals != 0 {
					t.Fatal("changed launch configuration allowed a subsequent write")
				}
			})
		}
	}
}

func TestEvidenceFailurePreventsTheNextWrite(t *testing.T) {
	for _, phase := range []string{"observation", "expand-intent", "remove-intent"} {
		t.Run(phase, func(t *testing.T) {
			r, f, e := executeRunner(t)
			e.failAt = phase
			_, err := r.run(context.Background())
			if err == nil || f.removals != 0 || (phase != "remove-intent" && f.expands != 0) {
				t.Fatal("wrote after evidence persistence failed")
			}
		})
	}
}

func TestPreviewExposesLaunchConfigurationAndBlockReason(t *testing.T) {
	r, f, _ := executeRunner(t)
	r.opts.Execute = false
	f.observations[0].LaunchConfiguration.Status = "SECURITY_GROUP_ABNORMAL"
	out, err := r.run(context.Background())
	if err != nil || out.Code != "PreviewBlocked" || out.Baseline.LaunchConfiguration.Status != "SECURITY_GROUP_ABNORMAL" || len(out.BlockedReasons) != 1 || f.expands != 0 || f.removals != 0 {
		t.Fatal("preview omitted blocked preflight")
	}
}

func TestConfirmationIsCheckedBeforeAnyObservation(t *testing.T) {
	r, f, _ := executeRunner(t)
	r.opts.ConfirmGroup = "asg-other"
	_, err := r.run(context.Background())
	if err == nil || f.index != 0 || f.expands != 0 || f.removals != 0 {
		t.Fatal("incorrect confirmation allowed execution")
	}
}

func TestUnfinishedCleanupReturnsFailureOnCancellation(t *testing.T) {
	r, f, _ := executeRunner(t)
	f.observations = f.observations[:3]
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Reach the unfinished cleanup phase before cancelling. A deadline for
	// the whole test can expire before removal on a loaded test machine.
	f.beforeObserve = func() {
		if f.removals == 1 {
			cancel()
		}
	}
	out, err := r.run(ctx)
	if !errors.Is(err, context.Canceled) || out.CleanupProven || out.Completed || f.removals != 1 || f.expands != 1 {
		t.Fatalf("cleanup outcome=%+v error=%v writes=%d/%d observations=%d", out, err, f.expands, f.removals, f.index)
	}
}

func TestContractWaitReturnsFailureForExpiredDeadline(t *testing.T) {
	r, f, _ := executeRunner(t)
	r.opts.PollInterval = time.Hour
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	err := r.wait(ctx)
	var failure *safeFailure
	if !errors.As(err, &failure) || failure.Code != "ObservationDeadlineExceeded" || f.expands != 0 || f.removals != 0 {
		t.Fatal("expired wait did not stop without writes", err)
	}
}

func TestFileEvidenceIsPrivateAndPersistsIntent(t *testing.T) {
	f, err := newFileEvidence(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err = f.Save("expand-intent", map[string]int{"desired": 1}); err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(f.dir, "001-expand-intent.json")
	info, err := os.Stat(p)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatal("evidence file missing or permissions too broad")
	}
	if _, err = newFileEvidence("relative/path"); err == nil {
		t.Fatal("relative evidence path accepted")
	}
}

func TestCLIHelpDoesNotReadCredentialsAndErrorsAreFixed(t *testing.T) {
	var out, errOut bytes.Buffer
	code := runCLI(context.Background(), []string{"-h"}, func(string) string { t.Fatal("help read credentials"); return "" }, &out, &errOut)
	if code != 0 || !strings.Contains(out.String(), "read-only preview") {
		t.Fatal("help failed")
	}
	errOut.Reset()
	code = runCLI(context.Background(), []string{"-secret", "do-not-echo-secret"}, func(string) string { return "" }, &out, &errOut)
	if code == 0 || strings.Contains(errOut.String(), "do-not-echo-secret") {
		t.Fatal("argument error leaked input")
	}
}
