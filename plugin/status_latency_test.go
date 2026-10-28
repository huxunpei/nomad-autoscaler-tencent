package plugin

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/nomad-autoscaler/sdk"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/live"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/tencentcloud"
)

// Block the first background observation after Open has published a valid
// single-member state. Keep it blocked until Status has actually returned.
type slowStatusCloud struct {
	statusCloudFake
	calls            atomic.Int32
	entered, release chan struct{}
	fail             bool
}

func (f *slowStatusCloud) Observe(ctx context.Context, group, tracked string) (tencentcloud.Snapshot, error) {
	if f.calls.Add(1) > 1 {
		select {
		case f.entered <- struct{}{}:
		default:
		}
		select {
		case <-f.release:
		case <-ctx.Done():
			return tencentcloud.Snapshot{}, ctx.Err()
		}
		if f.fail {
			return tencentcloud.Snapshot{}, errors.New("private-response-must-not-leak")
		}
	}
	return f.statusCloudFake.Observe(ctx, group, tracked)
}

func TestLiveStatusDoesNotWaitForCloudObservation(t *testing.T) {
	cloud := &slowStatusCloud{entered: make(chan struct{}, 1), release: make(chan struct{}), fail: true}
	nomad := &eventNomadFake{}
	engine, err := live.Open(live.Config{Dir: filepath.Join(t.TempDir(), "live"), GroupID: "asg-status1", Region: "ap-chengdu", ReconcileInterval: 10 * time.Millisecond, DrainDeadline: time.Minute}, cloud, nomad)
	if err != nil {
		t.Fatal(err)
	}
	target := &Target{live: engine, group: "asg-status1", backend: "live-experimental"}
	var once sync.Once
	unblock := func() { once.Do(func() { close(cloud.release) }) }
	defer target.Close()
	defer unblock()
	select {
	case <-cloud.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not enter cloud observation")
	}
	cfg := map[string]string{"tencent_group_id": "asg-status1"}
	type result struct {
		status  *sdk.TargetStatus
		err     error
		elapsed time.Duration
	}
	results := make(chan result, 16)
	for i := 0; i < cap(results); i++ {
		go func() {
			start := time.Now()
			s, err := target.Status(cfg)
			results <- result{s, err, time.Since(start)}
		}()
	}
	deadline := time.NewTimer(250 * time.Millisecond)
	defer deadline.Stop()
	var max time.Duration
	for i := 0; i < cap(results); i++ {
		select {
		case r := <-results:
			if r.err != nil || r.status == nil || r.status.Count != 1 || r.status.Ready {
				t.Fatalf("unsafe cached status: %+v %v", r.status, r.err)
			}
			if r.elapsed > max {
				max = r.elapsed
			}
		case <-deadline.C:
			t.Fatal("Status waited for cloud I/O beyond 250ms (Agent budget is 750ms)")
		}
	}
	t.Logf("16 concurrent Status calls while cloud is blocked: max=%s", max)
	if cloud.calls.Load() != 2 || cloud.writes.Load() != 0 || nomad.writes.Load() != 0 {
		t.Fatal("Status initiated I/O or a mutation")
	}
	unblock()
	// A failed refresh must replace the cache's earlier healthy state.
	until := time.Now().Add(2 * time.Second)
	for time.Now().Before(until) {
		_, err := target.Status(cfg)
		if err != nil {
			if err.Error() != "CloudObservationFailed" {
				t.Fatalf("wrong or unsanitized status error: %v", err)
			}
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("cloud failure did not reach Status")
}
