package live

import (
	"context"
	"math"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadclient"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/tencentcloud"
)

type completionCloud struct {
	*multiCloud
	hold           atomic.Bool
	started        atomic.Bool
	polls          atomic.Int32
	terminalDelete bool
}

func (c *completionCloud) SetDesired(ctx context.Context, g string, n int64) (tencentcloud.WriteResult, error) {
	out, err := c.multiCloud.SetDesired(ctx, g, n)
	c.started.Store(true)
	return out, err
}
func (c *completionCloud) Observe(ctx context.Context, g, id string) (tencentcloud.Snapshot, error) {
	s, err := c.multiCloud.Observe(ctx, g, id)
	if c.started.Load() {
		c.polls.Add(1)
	}
	if c.started.Load() && c.hold.Load() {
		s.Group.Activity = "IN_ACTIVITY"
	}
	return s, err
}
func (c *completionCloud) Activity(ctx context.Context, g, id string) (tencentcloud.Activity, error) {
	if c.terminalDelete {
		c.terminal(true)
	}
	return c.multiCloud.Activity(ctx, g, id)
}
func completionFixture(t *testing.T) (Config, *completionCloud, *multiNomad, *Engine) {
	cfg, cloud, n, old := groupContractFixture(t, 0, 0, 5, false)
	old.Close()
	cfg.StabilizationInterval = 5 * time.Millisecond
	c := &completionCloud{multiCloud: cloud}
	e := openMulti(t, cfg, c, n)
	return cfg, c, n, e
}
func waitStarted(t *testing.T, c *completionCloud) {
	t.Helper()
	until := time.Now().Add(time.Second)
	for !c.started.Load() && time.Now().Before(until) {
		time.Sleep(time.Millisecond)
	}
	if !c.started.Load() {
		t.Fatal("Scale did not reach cloud")
	}
}
func TestScaleAndWaitRequiresSettledCloudAndPersistsCompletion(t *testing.T) {
	cfg, c, n, e := completionFixture(t)
	done := make(chan error, 1)
	c.hold.Store(true)
	before := time.Now().UnixNano()
	go func() {
		changed, err := e.ScaleAndWait(2, 100)
		if !changed && err == nil {
			err = code("UnexpectedNoOp")
		}
		done <- err
	}()
	waitStarted(t, c)
	select {
	case err := <-done:
		t.Fatalf("returned before cloud settled: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	s, ready, _ := e.Snapshot()
	if ready || s.LastCompletedAt != 0 {
		t.Fatal("in-progress action published completion")
	}
	c.hold.Store(false)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("did not finish after cloud settled")
	}
	s, _, err := e.Snapshot()
	if err != nil || s.LastCompletedAt < before || s.Pending != nil || s.Batch != nil {
		t.Fatal("missing durable completion", err, s.LastCompletedAt)
	}
	stamp := s.LastCompletedAt
	e.Close()
	e = openMulti(t, cfg, c, n)
	s, _, err = e.Snapshot()
	if err != nil || s.LastCompletedAt != stamp {
		t.Fatal("completion lost on restart", err)
	}
	changed, err := e.ScaleAndWait(2, 15)
	s, _, _ = e.Snapshot()
	if changed || err != nil || s.LastCompletedAt != stamp {
		t.Fatal("NoOp changed completion", err)
	}
}
func TestScaleAndWaitHonorsStabilizationPollBudget(t *testing.T) {
	_, c, _, e := completionFixture(t)
	c.hold.Store(true)
	changed, err := e.ScaleAndWait(2, 3)
	if !changed || err == nil || err.Error() != "GroupStabilizationAttemptsExhausted" {
		t.Fatal(changed, err)
	}
	if c.polls.Load() != 3 || c.writeCount() != 1 {
		t.Fatal("wrong poll budget or repeated write", c.polls.Load())
	}
	s, ready, _ := e.Snapshot()
	if ready || s.Pending == nil || s.LastCompletedAt != 0 {
		t.Fatal("timeout discarded operation or claimed success")
	}
	c.hold.Store(false)
	step(t, e)
	s, _, _ = e.Snapshot()
	if s.Pending != nil || s.LastCompletedAt == 0 || c.writeCount() != 1 {
		t.Fatal("later reconciliation did not settle original operation")
	}
}
func TestScaleAndWaitGrowthDoesNotRequireNomadRegistration(t *testing.T) {
	_, c, n, e := completionFixture(t)
	n.nodes = map[string]nomadclient.Node{}
	changed, err := e.ScaleAndWait(2, 3)
	if !changed || err != nil {
		t.Fatal(changed, err)
	}
	s, ready, err := e.Snapshot()
	if err != nil || len(s.Members) != 2 || s.LastCompletedAt == 0 || s.Pending != nil || ready {
		t.Fatal("growth confused cloud completion with Nomad registration", err)
	}
	if c.writeCount() != 1 {
		t.Fatal("duplicate grow")
	}
}
func TestScaleAndWaitCancellationAndInvalidBudget(t *testing.T) {
	_, c, _, e := completionFixture(t)
	for _, attempts := range []int{0, -1, math.MaxInt} {
		if changed, err := e.ScaleAndWait(2, attempts); changed || err == nil || c.writeCount() != 0 {
			t.Fatal("invalid budget wrote cloud", attempts, changed, err)
		}
	}
	c.hold.Store(true)
	done := make(chan error, 1)
	go func() { _, err := e.ScaleAndWait(2, 100); done <- err }()
	waitStarted(t, c)
	e.Close()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("shutdown was success")
		}
	case <-time.After(time.Second):
		t.Fatal("shutdown did not release wait")
	}
}

type completionSDK struct {
	*sdkMultiProbe
	postEntered chan struct{}
	postRelease <-chan struct{}
}

func (n *completionSDK) PostScaleIn(ctx context.Context, id nomadclient.Identity, op string, before func() error) error {
	if err := before(); err != nil {
		return err
	}
	close(n.postEntered)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-n.postRelease:
		return nil
	}
}
func TestScaleAndWaitDoesNotCompleteBeforeSDKPost(t *testing.T) {
	cfg, raw, n, old := groupContractFixture(t, 1, 0, 2, false)
	old.Close()
	cfg.StabilizationInterval = 5 * time.Millisecond
	c := &completionCloud{multiCloud: raw, terminalDelete: true}
	release := make(chan struct{})
	probe := &completionSDK{sdkMultiProbe: &sdkMultiProbe{multiNomad: n, directory: cfg.Dir}, postEntered: make(chan struct{}), postRelease: release}
	e, err := Open(cfg, c, probe)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(e.Close)
	done := make(chan error, 1)
	go func() { _, err := e.ScaleAndWait(0, 15); done <- err }()
	select {
	case <-probe.postEntered:
	case err := <-done:
		t.Fatalf("Scale ended before post: %v", err)
	case <-time.After(time.Second):
		t.Fatal("post not reached")
	}
	s, ready, err := e.Snapshot()
	if err != nil || ready || s.LastCompletedAt != 0 {
		t.Fatal("pending post claimed completion", err)
	}
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Scale stuck after post")
	}
	s, _, _ = e.Snapshot()
	if s.LastCompletedAt == 0 || s.Pending != nil || s.LastBatch == nil || s.LastBatch.Phase != "completed" {
		t.Fatal("missing post completion")
	}
}

type completionPersistenceSDK struct{ *sdkMultiProbe }

func (n *completionPersistenceSDK) PostScaleIn(ctx context.Context, id nomadclient.Identity, op string, before func() error) error {
	if err := before(); err != nil {
		return err
	}
	path := filepath.Join(n.directory, "live.json")
	if err := os.Rename(path, path+".before-completion"); err != nil {
		return err
	}
	return os.Mkdir(path, 0700) // force the final journal replacement to fail
}

func TestScaleAndWaitUsesSeparatePollBudgetForEachDeletion(t *testing.T) {
	cfg, raw, n, old := groupContractFixture(t, 2, 0, 3, false)
	old.Close()
	cfg.StabilizationInterval = time.Millisecond
	cloud := &completionCloud{multiCloud: raw, terminalDelete: true}
	e := openMulti(t, cfg, cloud, n)
	changed, err := e.ScaleAndWait(0, 3)
	s, _, _ := e.Snapshot()
	if !changed || err != nil || s.LastCompletedAt == 0 || s.LastBatch == nil || len(s.LastBatch.Results) != 2 || cloud.writeCount() != 2 {
		t.Fatal("per-write stabilization budget did not settle both exact deletions", changed, err)
	}
}

type completionSlowObservation struct {
	*multiCloud
	enabled atomic.Bool
	starts  chan time.Time
}

func (c *completionSlowObservation) Observe(ctx context.Context, g, id string) (tencentcloud.Snapshot, error) {
	if c.enabled.Load() {
		c.starts <- time.Now()
		timer := time.NewTimer(60 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			return tencentcloud.Snapshot{}, ctx.Err()
		}
	}
	return c.multiCloud.Observe(ctx, g, id)
}
func TestBackgroundPollWaitsIntervalAfterSlowObservation(t *testing.T) {
	cfg, raw, n, old := groupContractFixture(t, 0, 0, 3, false)
	old.Close()
	cfg.ReconcileInterval = 30 * time.Millisecond
	c := &completionSlowObservation{multiCloud: raw, starts: make(chan time.Time, 10)}
	e := openMulti(t, cfg, c, n)
	c.enabled.Store(true)
	next := func() time.Time {
		t.Helper()
		select {
		case at := <-c.starts:
			return at
		case <-time.After(time.Second):
			t.Fatal("no background observation")
			return time.Time{}
		}
	}
	first, second := next(), next()
	e.Close()
	if second.Sub(first) < 85*time.Millisecond {
		t.Fatalf("ticker backlog eliminated the poll interval: %s", second.Sub(first))
	}
}

func TestScaleAndWaitPersistenceFailureDoesNotPublishCompletion(t *testing.T) {
	cfg, raw, n, old := groupContractFixture(t, 1, 0, 2, false)
	old.Close()
	cfg.StabilizationInterval = time.Millisecond
	c := &completionCloud{multiCloud: raw, terminalDelete: true}
	probe := &completionPersistenceSDK{&sdkMultiProbe{multiNomad: n, directory: cfg.Dir}}
	e, err := Open(cfg, c, probe)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	changed, err := e.ScaleAndWait(0, 15)
	s, ready, statusErr := e.Snapshot()
	if !changed || err == nil || statusErr == nil || ready || s.LastCompletedAt != 0 {
		t.Fatal("failed durable commit published completion", changed, err, statusErr, s.LastCompletedAt)
	}
}

func TestCompletionOnlyAfterWholeBatchAndNeverForPartialFailure(t *testing.T) {
	for _, success := range []bool{false, true} {
		t.Run(map[bool]string{false: "failure", true: "success"}[success], func(t *testing.T) {
			_, c, _, e := groupContractFixture(t, 2, 0, 3, false)
			if _, err := e.Scale(0); err != nil {
				t.Fatal(err)
			}
			step(t, e)
			c.terminal(true)
			step(t, e)
			step(t, e)
			s, _, err := e.Snapshot()
			if err != nil || s.LastCompletedAt != 0 || s.Batch == nil || len(s.Batch.Results) != 1 {
				t.Fatal("child completion became batch completion", err)
			}
			step(t, e)
			step(t, e)
			c.terminal(success)
			step(t, e)
			if success {
				step(t, e)
			}
			s, _, err = e.Snapshot()
			if success {
				if err != nil || s.Batch != nil || s.LastBatch == nil || s.LastCompletedAt <= 0 {
					t.Fatal("wrong final completion", err, s.LastCompletedAt)
				}
			} else {
				// The action has failed, but AS may still retry the exact VM.
				// Preserve prior successful children and the cloud reconciliation
				// obligation after restoring the survivor; never publish cooldown.
				if err != nil || s.Batch == nil || s.Pending == nil || s.Pending.Phase != "failed-delete" ||
					s.Pending.DrainOutcome != "restored" || s.ManualReview || s.LastCompletedAt != 0 || len(s.Batch.Results) != 1 {
					t.Fatal("partial failure lost recovery or published completion", err)
				}
				if done, failure := e.completionState(s.Batch.ID); done || failure == nil || failure.Error() != "ScaleActionFailed" {
					t.Fatal("partial batch did not return failure", done, failure)
				}
			}
		})
	}
}

func TestLongPositiveDeadlineBudgetBoundary(t *testing.T) {
	// One child: 3m fixed + (deadline+1m) + one (2m+5s) poll.
	last := time.Duration(math.MaxInt64) - 6*time.Minute - 5*time.Second
	e := &Engine{cfg: Config{DrainDeadline: last, StabilizationInterval: 5 * time.Second}}
	budget, err := e.completionBudget(1, 0, 1)
	if err != nil || budget != time.Duration(math.MaxInt64) {
		t.Fatal("last representable budget rejected", budget, err)
	}
	e.cfg.DrainDeadline = last + time.Nanosecond
	if _, err = e.completionBudget(1, 0, 1); err == nil {
		t.Fatal("unrepresentable total budget accepted")
	}
	e.cfg.DrainDeadline = last
	if _, err = e.completionBudget(2, 0, 1); err == nil {
		t.Fatal("child multiplication overflow accepted")
	}
	if _, err = e.completionBudget(1, 0, 2); err == nil {
		t.Fatal("attempt multiplication overflow accepted")
	}
}
func TestLongPositiveDeadlineOverflowRejectsBeforeMutation(t *testing.T) {
	cfg := config(t)
	cfg.GroupContract = true
	cfg.DrainDeadline = time.Duration(math.MaxInt64) - 2*time.Minute
	c, n := newCloud(), newNomad()
	c.addMember()
	e, err := Open(cfg, c, n)
	if err != nil {
		t.Fatal("representable drain allowance rejected", err)
	}
	defer e.Close()
	changed, err := e.ScaleAndWait(0, 1)
	state, _, _ := e.Snapshot()
	if changed || err == nil || err.Error() != "InvalidCompletionBudget" || c.writeCount() != 0 || n.writeCount() != 0 || state.Pending != nil || state.Batch != nil {
		t.Fatal("overflow produced an intent or business mutation", changed, err)
	}
}
func TestLongPositiveDeadlineAllowanceOverflowRejectedAtOpen(t *testing.T) {
	cfg := config(t)
	cfg.GroupContract = true
	cfg.DrainDeadline = time.Duration(math.MaxInt64)
	e, err := Open(cfg, newCloud(), newNomad())
	if e != nil {
		e.Close()
	}
	if err == nil {
		t.Fatal("drain observation allowance overflow accepted")
	}
	if _, err = os.Stat(cfg.Dir); !os.IsNotExist(err) {
		t.Fatal("invalid duration initialized a journal directory")
	}
}
