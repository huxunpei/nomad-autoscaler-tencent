package live

import (
	"context"
	"errors"
	"testing"
)

type taskNomad struct {
	*fakeNomad
	stopped bool
	err     error
	checks  int
}

func (n *taskNomad) TasksStopped(_ context.Context, nodeID string, ignoreSystem bool) (bool, error) {
	n.checks++
	if nodeID != testNode || ignoreSystem {
		return false, errors.New("wrong completion scope")
	}
	return n.stopped, n.err
}

func TestRemovalWaitsForTasksAndRechecksAfterRestart(t *testing.T) {
	for _, mode := range []string{"poststop-running", "read-failure"} {
		t.Run(mode, func(t *testing.T) {
			cfg := config(t)
			cloud := newCloud()
			cloud.addMember()
			n := &taskNomad{fakeNomad: newNomad()}
			if mode == "read-failure" {
				n.err = errors.New("private-upstream-error")
			}
			e, err := Open(cfg, cloud, n)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(e.Close)
			if _, err = e.Scale(0); err != nil {
				t.Fatal(err)
			}
			n.drained()
			if err = e.reconcileOnce(); err != nil {
				t.Fatal(err)
			}
			if cloud.writeCount() != 0 {
				t.Fatal("deleted VM while tasks were running or unobservable")
			}
			e.Close()
			e, err = Open(cfg, cloud, n)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(e.Close)
			if err = e.reconcileOnce(); err != nil {
				t.Fatal(err)
			}
			if cloud.writeCount() != 0 {
				t.Fatal("restart bypassed task completion check")
			}
			n.stopped = true
			n.err = nil
			if err = e.reconcileOnce(); err != nil {
				t.Fatal(err)
			}
			if cloud.writeCount() != 1 || n.checks < 3 {
				t.Fatal("did not delete exactly once after tasks stopped")
			}
		})
	}
}
