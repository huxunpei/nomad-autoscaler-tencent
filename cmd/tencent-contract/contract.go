package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"time"
)

type groupState struct {
	ID                                     string `json:"id"`
	Min, Max, Desired, Count               int64
	Enabled, Activity, LaunchConfiguration string
}

type member struct {
	ID, CreationType, Lifecycle, AddedAt string
	Protected                            bool
}

type observation struct {
	At                  time.Time           `json:"at"`
	Group               groupState          `json:"group"`
	Members             []member            `json:"members"`
	VMID                string              `json:"cvm_id,omitempty"`
	VMReturned          bool                `json:"cvm_returned"`
	VMState             string              `json:"cvm_state,omitempty"`
	VMProtected         bool                `json:"cvm_delete_protection"`
	Requests            []requestResult     `json:"requests"`
	LaunchConfiguration launchConfiguration `json:"launch_configuration"`
	Policies            []scalingPolicy     `json:"scaling_policies"`
}

type launchConfiguration struct {
	ID, Status, InstanceType, ChargeType, ImageID string
	InstanceTypes, SecurityGroupIDs               []string
	VersionNumber                                 *int64
}

type scalingPolicy struct{ ID, Type string }

type requestResult struct {
	Action     string `json:"action"`
	RequestID  string `json:"request_id,omitempty"`
	ActivityID string `json:"activity_id,omitempty"`
}

type cloud interface {
	Observe(context.Context, string, string) (observation, error)
	ExpandOne(context.Context, string) (requestResult, error)
	RemoveExact(context.Context, string, string) (requestResult, error)
}

type evidence interface{ Save(string, any) error }

type options struct {
	Region, GroupID, ConfirmGroup, EvidenceDir string
	Execute                                    bool
	PollInterval, TotalTimeout, RequestTimeout time.Duration
}

type outcome struct {
	Mode           string       `json:"mode"`
	Region         string       `json:"region"`
	GroupID        string       `json:"group_id"`
	Plan           []string     `json:"plan"`
	EvidenceDir    string       `json:"evidence_dir,omitempty"`
	InstanceID     string       `json:"instance_id,omitempty"`
	CleanupProven  bool         `json:"cleanup_proven"`
	Completed      bool         `json:"completed"`
	Code           string       `json:"code"`
	Error          *safeFailure `json:"error,omitempty"`
	ManualCheck    []string     `json:"manual_check,omitempty"`
	Baseline       *observation `json:"baseline,omitempty"`
	BlockedReasons []string     `json:"blocked_reasons"`
}

type runner struct {
	cloud    cloud
	evidence evidence
	opts     options
}

func fail(code string) error { return &safeFailure{Code: code} }

func (r *runner) save(kind string, value any) error {
	if r.evidence == nil || r.evidence.Save(kind, value) != nil {
		return fail("EvidenceWriteFailed")
	}
	return nil
}

func baseline(o observation, groupID string) bool {
	g := o.Group
	return g.ID == groupID && g.Min == 0 && g.Max == 1 && g.Desired == 0 && g.Count == 0 && len(o.Members) == 0 && g.Enabled == "ENABLED" && g.Activity == "NOT_IN_ACTIVITY"
}

func (r *runner) observe(ctx context.Context, trackedID string) (observation, error) {
	o, err := r.cloud.Observe(ctx, r.opts.GroupID, trackedID)
	if err == nil && r.opts.Execute {
		err = r.save("observation", o)
	}
	return o, err
}

func (r *runner) wait(ctx context.Context) error {
	timer := time.NewTimer(r.opts.PollInterval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return fail("ObservationDeadlineExceeded")
	case <-timer.C:
		return nil
	}
}

func (r *runner) run(ctx context.Context) (out outcome, runErr error) {
	out = outcome{Mode: "preview", Region: r.opts.Region, GroupID: r.opts.GroupID, Code: "PreviewReady", Plan: []string{
		"Require exclusive test group: enabled, idle, Min=0 Max=1 Desired=0 and no members.",
		"Require a NORMAL POSTPAID_BY_HOUR launch configuration; stop on configuration changes.",
		"Persist intent; call ModifyDesiredCapacity(1) once without MinSize/MaxSize.",
		"Observe the sole new AUTO_CREATION VM until IN_SERVICE and RUNNING.",
		"Persist exact ID and removal intent; call RemoveInstances(exact ID) once.",
		"Only observe after uncertain responses; never replay deletion or set DesiredCapacity(0).",
		"Require two observations with N=Desired=0, group idle and exact CVM ID absent.",
	}}
	defer func() {
		if runErr != nil {
			out.Completed = false
			out.Code = "NeedsManualCheck"
			out.Error = sanitize("Contract", runErr)
			out.ManualCheck = []string{"Inspect recorded group desired/member counts and the exact instance ID before any further mutation.", "No automatic resume is supported; never rerun execute while a previous operation is unresolved."}
		}
	}()
	if r.opts.Execute {
		out.Mode, out.EvidenceDir = "execute", r.opts.EvidenceDir
		if r.opts.ConfirmGroup != r.opts.GroupID || r.opts.EvidenceDir == "" || r.evidence == nil {
			return out, fail("ExecutionConfirmationMissing")
		}
	}
	initial, err := r.observe(ctx, "")
	if err != nil {
		return out, err
	}
	out.Baseline = &initial
	out.BlockedReasons = make([]string, 0)
	if !baseline(initial, r.opts.GroupID) {
		out.BlockedReasons = append(out.BlockedReasons, "BaselineMustBeEnabledIdleEmptyMin0Max1Desired0")
	}
	if initial.LaunchConfiguration.ID != initial.Group.LaunchConfiguration || initial.LaunchConfiguration.Status != "NORMAL" {
		out.BlockedReasons = append(out.BlockedReasons, "LaunchConfigurationNotNormal")
	}
	if initial.LaunchConfiguration.ChargeType != "POSTPAID_BY_HOUR" {
		out.BlockedReasons = append(out.BlockedReasons, "LaunchConfigurationMustBePostpaidByHour")
	}
	if len(initial.Policies) != 0 {
		out.BlockedReasons = append(out.BlockedReasons, "OtherScalingPoliciesExist")
	}
	if len(out.BlockedReasons) != 0 {
		if !r.opts.Execute {
			out.Code = "PreviewBlocked"
			return out, nil
		}
		return out, fail("ExecutionPreconditionsBlocked")
	}
	if !baseline(initial, r.opts.GroupID) {
		return out, fail("BaselineMismatch")
	}
	if !r.opts.Execute {
		return out, nil
	}
	// Check again immediately before the first intent. This is not a cloud CAS;
	// the operator must keep other policies and writers away from this test group.
	before, err := r.observe(ctx, "")
	if err != nil {
		return out, err
	}
	if !reflect.DeepEqual(before.LaunchConfiguration, initial.LaunchConfiguration) {
		return out, fail("LaunchConfigurationChanged")
	}
	if !baseline(before, r.opts.GroupID) || before.Group.LaunchConfiguration != initial.Group.LaunchConfiguration || before.LaunchConfiguration.Status != "NORMAL" || len(before.Policies) != 0 {
		return out, fail("BaselineChanged")
	}
	started := time.Now().UTC()
	if err = r.save("expand-intent", map[string]any{"at": started, "region": r.opts.Region, "group_id": r.opts.GroupID, "desired": 1}); err != nil {
		return out, err
	}
	expandResult, expandErr := r.cloud.ExpandOne(ctx, r.opts.GroupID)
	if err = r.save("expand-result", map[string]any{"result": expandResult, "error": sanitizeOptional("ModifyDesiredCapacity", expandErr)}); err != nil {
		return out, err
	}
	var ready observation
	for {
		o, e := r.observe(ctx, "")
		if e != nil {
			return out, e
		}
		if !reflect.DeepEqual(o.LaunchConfiguration, initial.LaunchConfiguration) {
			return out, fail("LaunchConfigurationChanged")
		}
		g := o.Group
		if g.ID != r.opts.GroupID || g.Min != 0 || g.Max != 1 || g.LaunchConfiguration != initial.Group.LaunchConfiguration || len(o.Members) > 1 || len(o.Policies) != 0 {
			return out, fail("UnexpectedGroupChange")
		}
		if len(o.Members) == 1 {
			m := o.Members[0]
			added, parseErr := time.Parse(time.RFC3339, m.AddedAt)
			// AddTime has second precision, so compare against the intent's UTC second.
			if m.CreationType != "AUTO_CREATION" || m.ID == "" || parseErr != nil || added.Before(started.Truncate(time.Second)) {
				return out, fail("InstanceOwnershipUnproven")
			}
			if out.InstanceID != "" && out.InstanceID != m.ID {
				return out, fail("InstanceChanged")
			}
			out.InstanceID = m.ID
			if m.Protected || o.VMProtected {
				return out, fail("InstanceProtected")
			}
			if g.Desired == 1 && g.Count == 1 && g.Activity == "NOT_IN_ACTIVITY" && m.Lifecycle == "IN_SERVICE" && o.VMID == m.ID && o.VMReturned && o.VMState == "RUNNING" {
				ready = o
				break
			}
		}
		if e = r.wait(ctx); e != nil {
			return out, e
		}
	}
	if err = r.save("remove-intent", map[string]any{"at": time.Now().UTC(), "region": r.opts.Region, "group_id": r.opts.GroupID, "instance_ids": []string{out.InstanceID}, "confirmed_observation": ready}); err != nil {
		return out, err
	}
	removeResult, removeErr := r.cloud.RemoveExact(ctx, r.opts.GroupID, out.InstanceID)
	if err = r.save("remove-result", map[string]any{"result": removeResult, "error": sanitizeOptional("RemoveInstances", removeErr)}); err != nil {
		return out, err
	}
	// Even an error may mean the write was accepted. Never replay RemoveInstances.
	stable := 0
	for stable < 2 {
		o, e := r.observe(ctx, out.InstanceID)
		if e != nil {
			return out, e
		}
		if !reflect.DeepEqual(o.LaunchConfiguration, initial.LaunchConfiguration) {
			return out, fail("LaunchConfigurationChanged")
		}
		if o.Group.ID != r.opts.GroupID || o.Group.Min != 0 || o.Group.Max != 1 || o.Group.LaunchConfiguration != initial.Group.LaunchConfiguration {
			return out, fail("UnexpectedGroupChange")
		}
		for _, m := range o.Members {
			if m.ID != out.InstanceID {
				return out, fail("UnexpectedInstanceDuringCleanup")
			}
		}
		if o.Group.Count == 0 && o.Group.Desired == 0 && len(o.Members) == 0 && o.Group.Activity == "NOT_IN_ACTIVITY" && o.VMID == out.InstanceID && !o.VMReturned {
			stable++
		} else {
			stable = 0
		}
		if stable < 2 {
			if e = r.wait(ctx); e != nil {
				return out, e
			}
		}
	}
	out.CleanupProven, out.Completed, out.Code = true, true, "Completed"
	if expandErr != nil || removeErr != nil {
		out.Completed, out.Code = false, "CleanupObservedWriteResponseUncertain"
	}
	if err = r.save("cleanup-proof", out); err != nil {
		out.Completed = false
		return out, err
	}
	if expandErr != nil || removeErr != nil {
		return out, fail("WriteResponseUncertainButCleanupObserved")
	}
	return out, nil
}

// Each unique evidence file is durable before a caller may issue its next write.
// Failures preserve the directory for manual examination, including partial files.
type fileEvidence struct {
	dir string
	seq int
}

func newFileEvidence(base string) (*fileEvidence, error) {
	if !filepath.IsAbs(base) {
		return nil, fail("EvidenceDirectoryMustBeAbsolute")
	}
	if err := os.MkdirAll(base, 0700); err != nil {
		return nil, fail("EvidenceDirectoryUnavailable")
	}
	dir, err := os.MkdirTemp(base, "contract-")
	if err != nil {
		return nil, fail("EvidenceDirectoryUnavailable")
	}
	parent, err := os.Open(base)
	if err != nil {
		return nil, fail("EvidenceDirectoryUnavailable")
	}
	syncErr := parent.Sync()
	closeErr := parent.Close()
	if syncErr != nil || closeErr != nil {
		return nil, fail("EvidenceDirectoryUnavailable")
	}
	return &fileEvidence{dir: dir}, nil
}

func (f *fileEvidence) Save(kind string, value any) error {
	body, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	f.seq++
	file, err := os.OpenFile(filepath.Join(f.dir, fmt.Sprintf("%03d-%s.json", f.seq, kind)), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	if _, err = file.Write(append(body, '\n')); err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	dir, err := os.Open(f.dir)
	if err != nil {
		return err
	}
	err = dir.Sync()
	closeErr = dir.Close()
	if err != nil {
		return err
	}
	return closeErr
}
