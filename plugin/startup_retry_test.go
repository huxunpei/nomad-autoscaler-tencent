package plugin

import (
	"context"
	"errors"
	"github.com/hashicorp/go-hclog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/nomad-autoscaler/plugins/shared"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/live"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/tencentcloud"
)

func startupRetryTarget(t *testing.T, root string, c *routingCloud, open func(live.Config, live.Cloud, live.Nomad) (groupController, error)) *Target {
	t.Helper()
	p := &Target{runtimeDeps: &runtimeDependencies{interval: 5 * time.Millisecond, openCloud: func(context.Context, tencentcloud.AuthConfig) (accountCloud, error) { return c, nil }, openEngine: open}}
	if err := p.SetConfig(map[string]string{"tencent_region": c.region, "tencent_state_dir": root}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	return p
}

func TestStartupRetryRecordedRouteRecoversWithoutStatus(t *testing.T) {
	root := t.TempDir()
	values := map[string]string{"tencent_group_id": "asg-one"}
	first := routeTarget(t, root, &routingCloud{account: "10001", region: "ap-guangzhou"}, func(cfg live.Config, _ live.Cloud, _ live.Nomad) (groupController, error) {
		return &routingEngine{cfg: cfg}, nil
	})
	readyRoute(t, first, values)
	first.Close()
	var calls atomic.Int32
	var wrongMode atomic.Bool
	loaded := make(chan struct{}, 1)
	c := &routingCloud{account: "10001", region: "ap-guangzhou"}
	restart := startupRetryTarget(t, root, c, func(cfg live.Config, _ live.Cloud, _ live.Nomad) (groupController, error) {
		if cfg.DisableReconciliation {
			wrongMode.Store(true)
		}
		if calls.Add(1) == 1 {
			return nil, errors.New("StatePersistenceFailed")
		}
		loaded <- struct{}{}
		return &routingEngine{cfg: cfg}, nil
	})
	select {
	case <-loaded:
	case <-time.After(time.Second):
		t.Fatal("saved route still needs Status to retry startup")
	}
	if calls.Load() != 2 || wrongMode.Load() || c.writes.Load() != 0 {
		t.Fatal("retry lost startup recovery mode or wrote provider")
	}
	if s, err := restart.Status(values); err != nil || !s.Ready {
		t.Fatal("route not ready after automatic retry", err)
	}
}

func TestStartupRetryFreshStatusWaitsForSafeRetry(t *testing.T) {
	var calls atomic.Int32
	var enabled atomic.Bool
	c := &routingCloud{account: "10001", region: "ap-guangzhou"}
	p := startupRetryTarget(t, t.TempDir(), c, func(cfg live.Config, _ live.Cloud, _ live.Nomad) (groupController, error) {
		if !cfg.DisableReconciliation {
			enabled.Store(true)
		}
		if calls.Add(1) == 1 {
			return nil, errors.New("StatePersistenceFailed")
		}
		return &routingEngine{cfg: cfg}, nil
	})
	s, err := p.Status(map[string]string{"tencent_group_id": "asg-one", shared.PluginConfigKeyGRPCTimeout: "500ms"})
	if err != nil || s == nil || !s.Ready || calls.Load() != 2 || enabled.Load() || c.writes.Load() != 0 {
		t.Fatal("first Status did not recover read-only initialization", err)
	}
}

func TestStartupRetryCallerReceivesNewEpochAfterTerminalFailure(t *testing.T) {
	var calls atomic.Int32
	initial := errors.New("InvalidJournal")
	p := startupRetryTarget(t, t.TempDir(), &routingCloud{account: "10001", region: "ap-guangzhou"}, func(cfg live.Config, _ live.Cloud, _ live.Nomad) (groupController, error) {
		if calls.Add(1) == 1 {
			return nil, initial
		}
		return &routingEngine{cfg: cfg}, nil
	})
	values := map[string]string{"tencent_group_id": "asg-one"}
	if _, err := p.Status(values); err != initial {
		t.Fatal("unexpected first failure", err)
	}
	if s, err := p.Status(values); err != nil || s == nil || !s.Ready || calls.Load() != 2 {
		t.Fatal("retry returned stale failed epoch", err)
	}
}

func TestStartupRetryConcurrentStatusSharesOneInitializer(t *testing.T) {
	var calls atomic.Int32
	c := &routingCloud{account: "10001", region: "ap-guangzhou"}
	p := startupRetryTarget(t, t.TempDir(), c, func(cfg live.Config, _ live.Cloud, _ live.Nomad) (groupController, error) {
		if calls.Add(1) <= 3 {
			return nil, errors.New("StateReadFailed")
		}
		return &routingEngine{cfg: cfg}, nil
	})
	errorsOut := make(chan error, 16)
	for i := 0; i < 16; i++ {
		go func() {
			s, err := p.Status(map[string]string{"tencent_group_id": "asg-one", shared.PluginConfigKeyGRPCTimeout: "1s"})
			if err == nil && (s == nil || !s.Ready) {
				err = errors.New("not ready")
			}
			errorsOut <- err
		}()
	}
	for i := 0; i < 16; i++ {
		if err := <-errorsOut; err != nil {
			t.Fatal(err)
		}
	}
	if calls.Load() != 4 || c.writes.Load() != 0 {
		t.Fatal("concurrent callers created duplicate initializers", calls.Load())
	}
}

func TestStartupRetryOtherGroupAndWaitBudgetRemainIndependent(t *testing.T) {
	var allowed atomic.Bool
	loaded := make(chan struct{}, 1)
	c := &routingCloud{account: "10001", region: "ap-guangzhou"}
	p := startupRetryTarget(t, t.TempDir(), c, func(cfg live.Config, _ live.Cloud, _ live.Nomad) (groupController, error) {
		if cfg.GroupID == "asg-bad" {
			if !allowed.Load() {
				return nil, errors.New("StatePersistenceFailed")
			}
			select {
			case loaded <- struct{}{}:
			default:
			}
		}
		return &routingEngine{cfg: cfg}, nil
	})
	if _, err := p.Status(map[string]string{"tencent_group_id": "asg-bad", shared.PluginConfigKeyGRPCTimeout: "10ms"}); err != ErrRouteInitializing {
		t.Fatal("lost caller wait boundary", err)
	}
	if s, err := p.Status(map[string]string{"tencent_group_id": "asg-good"}); err != nil || !s.Ready {
		t.Fatal("bad group blocked healthy group", err)
	}
	allowed.Store(true)
	select {
	case <-loaded:
	case <-time.After(time.Second):
		t.Fatal("wait timeout cancelled background recovery")
	}
	if s, err := p.Status(map[string]string{"tencent_group_id": "asg-bad"}); err != nil || !s.Ready {
		t.Fatal("recovered group not visible", err)
	}
	if c.writes.Load() != 0 {
		t.Fatal("initialization mutated cloud")
	}
}

func TestStartupRetryPermanentErrorsDoNotLoop(t *testing.T) {
	for _, code := range []string{"InvalidJournal", "UnsafeStateFile", "SDKPolicyChangeRequiresMigration", "RecoveryAuditRepairRequired", "JournalRetryQueueInvalid"} {
		t.Run(code, func(t *testing.T) {
			var calls atomic.Int32
			p := startupRetryTarget(t, t.TempDir(), &routingCloud{account: "10001", region: "ap-guangzhou"}, func(live.Config, live.Cloud, live.Nomad) (groupController, error) {
				calls.Add(1)
				return nil, errors.New(code)
			})
			if _, err := p.Status(map[string]string{"tencent_group_id": "asg-one"}); err == nil || err.Error() != code {
				t.Fatal("changed terminal diagnostic", err)
			}
			time.Sleep(20 * time.Millisecond)
			if calls.Load() != 1 {
				t.Fatal("permanent failure retried automatically")
			}
		})
	}
}

func TestStartupRetryCloseCancelsBackoffAndReleasesGroupLock(t *testing.T) {
	entered := make(chan struct{}, 1)
	var calls atomic.Int32
	c := &routingCloud{account: "10001", region: "ap-guangzhou"}
	p := startupRetryTarget(t, t.TempDir(), c, func(live.Config, live.Cloud, live.Nomad) (groupController, error) {
		calls.Add(1)
		select {
		case entered <- struct{}{}:
		default:
		}
		return nil, errors.New("StatePersistenceFailed")
	})
	if _, err := p.Status(map[string]string{"tencent_group_id": "asg-one", shared.PluginConfigKeyGRPCTimeout: "10ms"}); err != ErrRouteInitializing {
		t.Fatal(err)
	}
	<-entered
	groupDir := filepath.Join(p.runtime.root, "asg-one")
	stopped := make(chan struct{})
	go func() { p.Close(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("Close did not cancel retry backoff")
	}
	count := calls.Load()
	time.Sleep(20 * time.Millisecond)
	if calls.Load() != count || !c.closed.Load() {
		t.Fatal("retry survived Close")
	}
	lock, err := lockRouteDir(groupDir)
	if err != nil {
		t.Fatal("failed attempt leaked group lock", err)
	}
	releaseRouteLock(lock)
}

func TestStartupRetryFailedPartialEngineClosesBeforeRetry(t *testing.T) {
	var calls atomic.Int32
	var notClosed atomic.Bool
	previous := &routingEngine{}
	p := startupRetryTarget(t, t.TempDir(), &routingCloud{account: "10001", region: "ap-guangzhou"}, func(cfg live.Config, _ live.Cloud, _ live.Nomad) (groupController, error) {
		if calls.Add(1) == 1 {
			previous.cfg = cfg
			return previous, errors.New("StatePersistenceFailed")
		}
		if !previous.closed.Load() {
			notClosed.Store(true)
		}
		return &routingEngine{cfg: cfg}, nil
	})
	if s, err := p.Status(map[string]string{"tencent_group_id": "asg-one"}); err != nil || !s.Ready || calls.Load() != 2 || notClosed.Load() {
		t.Fatal("partial engine was retained across retry", err)
	}
}

func TestStartupRetryErrorClassificationAndBackoff(t *testing.T) {
	for _, code := range []string{"StatePersistenceFailed", "StateReadFailed", "RouteRecordPersistenceFailed", "GroupInitializationObservationFailed", "JournalRetryQueueCleanupFailed", "LegacyMappingUnavailable"} {
		if !retryableRouteInitialization(errors.New(code)) {
			t.Fatal("lost retryable error", code)
		}
	}
	for _, code := range []string{"GroupControllerAlreadyRunning", "StateDirectoryInUse", "InvalidJournal", "StatePersistenceFailed private-message", "prefix: StateReadFailed", "GroupPolicyChangeRequiresMigration", "RecoveryAuditRepairRequired", "JournalRetryJournalConflict"} {
		if retryableRouteInitialization(errors.New(code)) {
			t.Fatal("promoted unknown/permanent error", code)
		}
	}
	if retryableRouteInitialization(nil) {
		t.Fatal("nil error retried")
	}
	for i, want := range []time.Duration{5 * time.Second, 10 * time.Second, 20 * time.Second, 40 * time.Second, time.Minute, time.Minute} {
		if got := routeInitializationDelay(0, uint64(i)); got != want {
			t.Fatal("wrong bounded backoff", i, got, want)
		}
	}
	if routeInitializationDelay(time.Nanosecond, ^uint64(0)) != time.Minute || routeInitializationDelay(2*time.Hour, 0) != time.Minute {
		t.Fatal("overflow or missing upper bound")
	}
}

type startupFaultCloud struct {
	*routingCloud
	onObserve func()
}

func (c *startupFaultCloud) Observe(ctx context.Context, g, id string) (tencentcloud.Snapshot, error) {
	if c.onObserve != nil {
		f := c.onObserve
		c.onObserve = nil
		f()
	}
	return c.routingCloud.Observe(ctx, g, id)
}

type startupRetryLogWriter struct {
	once      sync.Once
	onFailure func()
}

func (w *startupRetryLogWriter) Write(data []byte) (int, error) {
	if strings.Contains(string(data), "RouteRecordPersistenceFailed") {
		w.once.Do(w.onFailure)
	}
	return len(data), nil
}

func TestStartupRetryActualRouteRecordWriteFailureRecovers(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "accounts", "10001", "ap-guangzhou", "asg-one", "route.json")
	outcome := make(chan error, 2)
	c := &startupFaultCloud{routingCloud: &routingCloud{account: "10001", region: "ap-guangzhou"}}
	c.onObserve = func() { outcome <- os.Mkdir(path, 0700) }
	writer := &startupRetryLogWriter{onFailure: func() { outcome <- os.Remove(path) }}
	p := &Target{logger: hclog.New(&hclog.LoggerOptions{Level: hclog.Warn, Output: writer}), runtimeDeps: &runtimeDependencies{interval: 5 * time.Millisecond, openCloud: func(context.Context, tencentcloud.AuthConfig) (accountCloud, error) { return c, nil }, openEngine: func(cfg live.Config, _ live.Cloud, _ live.Nomad) (groupController, error) {
		return &routingEngine{cfg: cfg}, nil
	}}}
	if err := p.SetConfig(map[string]string{"tencent_region": c.region, "tencent_state_dir": root}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	if s, err := p.Status(map[string]string{"tencent_group_id": "asg-one"}); err != nil || !s.Ready {
		t.Fatal("did not recover actual route record write failure", err)
	}
	for i := 0; i < 2; i++ {
		select {
		case err := <-outcome:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(time.Second):
			t.Fatal("did not reach real filesystem fault")
		}
	}
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 || c.writes.Load() != 0 {
		t.Fatal("record not safely recovered", err)
	}
}

func TestStartupRetryRouteSymlinksAreTerminal(t *testing.T) {
	for _, name := range []string{"route.json", ".route.lock"} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.Chmod(dir, 0700); err != nil {
				t.Fatal(err)
			}
			target := filepath.Join(dir, "outside-target")
			if err := os.WriteFile(target, []byte("unchanged"), 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(target, filepath.Join(dir, name)); err != nil {
				t.Fatal(err)
			}
			var err error
			if name == "route.json" {
				_, _, err = readRouteRecord(dir)
			} else {
				var lock *os.File
				lock, err = lockRouteDir(dir)
				if lock != nil {
					releaseRouteLock(lock)
				}
			}
			want := "UnsafeRouteRecord"
			if name == ".route.lock" {
				want = "UnsafeRouteLock"
			}
			raw, _ := os.ReadFile(target)
			if err == nil || err.Error() != want || retryableRouteInitialization(err) || string(raw) != "unchanged" {
				t.Fatal("unsafe route did not remain terminal", err)
			}
		})
	}
}

func TestStartupRetryLegacyBackupIOAndConflictClassification(t *testing.T) {
	for _, code := range []string{"LegacyMappingUnavailable", "LegacyBackupFailed", "LegacyBackupConflict", "LegacyJournalConflict", "LegacyMigrationEvidenceUnavailable", "LegacyMigrationEvidenceInvalid", "LegacyMigrationAuditMismatch", "LegacyMigrationAuditMissing", "InvalidLegacyContract"} {
		t.Run(code, func(t *testing.T) {
			var calls atomic.Int32
			p := startupRetryTarget(t, t.TempDir(), &routingCloud{account: "10001", region: "ap-guangzhou"}, func(cfg live.Config, _ live.Cloud, _ live.Nomad) (groupController, error) {
				if calls.Add(1) == 1 {
					return nil, errors.New(code)
				}
				return &routingEngine{cfg: cfg}, nil
			})
			status, err := p.Status(map[string]string{"tencent_group_id": "asg-one"})
			if code == "LegacyMappingUnavailable" || code == "LegacyBackupFailed" || code == "LegacyMigrationEvidenceUnavailable" {
				if err != nil || status == nil || !status.Ready || calls.Load() != 2 {
					t.Fatal("did not retry backup IO", err)
				}
			} else {
				if err == nil || err.Error() != code {
					t.Fatal("changed conflict diagnostic", err)
				}
				time.Sleep(20 * time.Millisecond)
				if calls.Load() != 1 {
					t.Fatal("retried conflicting backup/source")
				}
			}
		})
	}
}
