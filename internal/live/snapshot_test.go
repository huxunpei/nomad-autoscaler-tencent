package live

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadclient"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/tencentcloud"
)

type writeGate struct{ entered, release chan struct{} }

func (g *writeGate) wait(ctx context.Context) {
	close(g.entered)
	select {
	case <-g.release:
	case <-ctx.Done():
	}
}

type slowWriteCloud struct {
	*fakeCloud
	gate *writeGate
}

func (f *slowWriteCloud) SetDesired(ctx context.Context, group string, target int64) (tencentcloud.WriteResult, error) {
	f.gate.wait(ctx)
	return f.fakeCloud.SetDesired(ctx, group, target)
}

type slowWriteNomad struct {
	*fakeNomad
	gate *writeGate
}

func (f *slowWriteNomad) StartOwnedDrain(ctx context.Context, id nomadclient.Identity, op string, d time.Duration) (nomadclient.MutationResult, error) {
	f.gate.wait(ctx)
	return f.fakeNomad.StartOwnedDrain(ctx, id, op, d)
}

func TestSnapshotDoesNotWaitForScaleWrite(t *testing.T) {
	for _, kind := range []string{"grow", "drain"} {
		for _, unknown := range []bool{false, true} {
			name := kind
			if unknown {
				name += "-unknown"
			}
			t.Run(name, func(t *testing.T) {
				c, n := newCloud(), newNomad()
				gate := &writeGate{entered: make(chan struct{}), release: make(chan struct{})}
				var cloud Cloud = c
				var nomad Nomad = n
				target, count := int64(1), 0
				if kind == "grow" {
					c.growUnknown = unknown
					cloud = &slowWriteCloud{c, gate}
				} else {
					c.addMember()
					n.drainUnknown = unknown
					nomad = &slowWriteNomad{n, gate}
					target, count = 0, 1
				}
				e, err := Open(config(t), cloud, nomad)
				if err != nil {
					t.Fatal(err)
				}
				var once sync.Once
				unblock := func() { once.Do(func() { close(gate.release) }) }
				defer e.Close()
				defer unblock()
				done := make(chan error, 1)
				go func() {
					changed, err := e.Scale(target)
					if !changed && err == nil {
						err = errors.New("write not accepted")
					}
					done <- err
				}()
				select {
				case <-gate.entered:
				case <-time.After(2 * time.Second):
					t.Fatal("write did not start")
				}
				type result struct {
					s     State
					ready bool
					err   error
				}
				got := make(chan result, 1)
				go func() { s, r, err := e.Snapshot(); got <- result{s, r, err} }()
				select {
				case r := <-got:
					if r.err != nil || r.ready || len(r.s.Members) != count {
						t.Fatalf("unsafe status during write: %+v", r)
					}
				case <-time.After(250 * time.Millisecond):
					t.Fatal("Snapshot waited for a slow Scale write")
				}
				unblock()
				err = <-done
				if unknown && !errors.Is(err, ErrUnknown) || !unknown && err != nil {
					t.Fatalf("Scale error: %v", err)
				}
				s, ready, err := e.Snapshot()
				if ready || s.Pending == nil || s.Pending.Unknown != unknown || unknown && !errors.Is(err, ErrUnknown) || !unknown && err != nil {
					t.Fatalf("write result not published: %+v %v", s, err)
				}
				if c.writeCount()+n.writeCount() != 1 {
					t.Fatal("unexpected mutation count")
				}
			})
		}
	}
}

func TestSnapshotIsolatedFromCallerMutationAndScaleRechecksCloud(t *testing.T) {
	c, n := newCloud(), newNomad()
	e := openTest(t, config(t), c, n)
	grow(t, e)
	step(t, e)
	before, ready, err := e.Snapshot()
	if err != nil || !ready {
		t.Fatalf("initial snapshot: %v %v", ready, err)
	}
	s, _, _ := e.Snapshot()
	delete(s.Members, testInstance)
	s.LastOperation.ID = "caller-modified"
	s.Cloud.Members[0].Protected = true
	delete(s.Cloud.VMs, testInstance)
	s.Cloud.LaunchConfiguration.SecurityGroupIDs[0] = "sg-caller"
	s.LaunchConfiguration.SecurityGroupIDs[0] = "sg-caller"
	after, _, _ := e.Snapshot()
	if !reflect.DeepEqual(before, after) {
		t.Fatal("caller mutation leaked into the cached state")
	}
	// Even a genuinely ready cached observation cannot authorize a write once
	// current protection changes. Scale must still inspect the cloud itself.
	c.mu.Lock()
	c.snap.Members[0].Protected = true
	c.mu.Unlock()
	if changed, err := e.Scale(0); changed || !errors.Is(err, ErrNotReady) {
		t.Fatalf("stale cache authorized scale: %v %v", changed, err)
	}
	if c.writeCount() != 1 || n.writeCount() != 0 {
		t.Fatal("protected node was mutated")
	}
	if _, ready, err := e.Snapshot(); ready || err != nil {
		t.Fatal("new protection state was not published")
	}
	e.Close()
	if _, ready, err := e.Snapshot(); ready || !errors.Is(err, ErrClosed) {
		t.Fatal("closed engine returned a ready cache")
	}
}

func TestKnownPendingDeletionRestartDoesNotRepeatRemove(t *testing.T) {
	c, n := newCloud(), newNomad()
	cfg := config(t)
	e := openTest(t, cfg, c, n)
	grow(t, e)
	startShrink(t, e, n)
	e.Close()
	restarted, err := Open(cfg, c, n)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	step(t, restarted)
	c.complete()
	step(t, restarted)
	step(t, restarted)
	step(t, restarted)
	s, ready, err := restarted.Snapshot()
	if err != nil || !ready || s.Pending != nil || s.LastEvent != "deletion-complete" || s.LastOperation.AbsentObservations != 2 {
		t.Fatalf("restart lost confirmed activity: %+v %v", s, err)
	}
	if c.writeCount() != 2 || n.writeCount() != 1 {
		t.Fatal("restart repeated cloud delete or drain")
	}
}

func TestRestoredFailureRemainsBlockedAcrossRestartAndLaterCloudDeletion(t *testing.T) {
	c, n := newCloud(), newNomad()
	cfg := config(t)
	e := openTest(t, cfg, c, n)
	grow(t, e)
	startShrink(t, e, n)
	c.fail()
	step(t, e)
	e.Close()
	restarted, err := Open(cfg, c, n)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	// External asynchronous deletion can still occur after restoring eligibility.
	// Observe its disappearance, without interpreting it as permission to replay.
	c.complete()
	step(t, restarted)
	s, ready, err := restarted.Snapshot()
	if err != nil || ready || !s.ManualReview || len(s.Members) != 0 || s.Pending != nil {
		t.Fatalf("recovery review gate lost: %+v %v", s, err)
	}
	if changed, err := restarted.Scale(1); changed || !errors.Is(err, ErrNotReady) {
		t.Fatalf("review gate bypassed after restart: %v %v", changed, err)
	}
	if c.writeCount() != 2 || n.writeCount() != 2 {
		t.Fatal("later deletion caused replay or a second restoration")
	}
}
