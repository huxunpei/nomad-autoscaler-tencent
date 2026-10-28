package live

import (
	"context"
	"errors"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/tencentcloud"
	"testing"
	"time"
)

func TestObservationOnlyRestartDoesNotResumeWritesUntilScale(t *testing.T) {
	c, n := newCloud(), newNomad()
	c.addMember()
	cfg := config(t)
	first := openTest(t, cfg, c, n)
	if changed, err := first.Scale(0); err != nil || !changed {
		t.Fatalf("initial shrink: %v %v", changed, err)
	}
	n.drained()
	first.Close()
	cfg.DisableReconciliation = true
	observe := openTest(t, cfg, c, n)
	step(t, observe)
	step(t, observe)
	s, ready, err := observe.Snapshot()
	if err != nil || ready || s.Pending == nil || s.Pending.Phase != "draining" || c.writeCount() != 0 || n.writeCount() != 1 {
		t.Fatal("read-only controller resumed pending cloud or Nomad writes")
	}
	if _, err = observe.Scale(-1); err == nil {
		t.Fatal("invalid target accepted")
	}
	step(t, observe)
	if c.writeCount() != 0 {
		t.Fatal("invalid Scale enabled reconciliation")
	}
	if changed, err := observe.Scale(0); changed || !errors.Is(err, ErrBusy) {
		t.Fatalf("expected existing action: %v %v", changed, err)
	}
	step(t, observe)
	if c.writeCount() != 1 || n.writeCount() != 1 {
		t.Fatal("explicit Scale did not resume exact pending operation")
	}
}

type startupBlockingCloud struct {
	*fakeCloud
	entered chan struct{}
}

func (c *startupBlockingCloud) Observe(ctx context.Context, group, tracked string) (tencentcloud.Snapshot, error) {
	close(c.entered)
	<-ctx.Done()
	return tencentcloud.Snapshot{}, ctx.Err()
}
func TestEngineParentContextCancelsStartup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := &startupBlockingCloud{fakeCloud: newCloud(), entered: make(chan struct{})}
	cfg := config(t)
	cfg.Context = ctx
	done := make(chan error, 1)
	go func() {
		e, err := Open(cfg, c, newNomad())
		if e != nil {
			e.Close()
		}
		done <- err
	}()
	select {
	case <-c.entered:
	case <-time.After(time.Second):
		t.Fatal("startup never began observation")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("canceled startup: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("startup ignored cancellation")
	}
	// Cancellation must release the exclusive journal lock.
	cfg.Context = context.Background()
	replacement, err := Open(cfg, newCloud(), newNomad())
	if err != nil {
		t.Fatal(err)
	}
	replacement.Close()
}
