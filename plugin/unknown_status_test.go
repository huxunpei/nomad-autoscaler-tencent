package plugin

import (
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/huxunpei/nomad-autoscaler-tencent/internal/live"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/tencentcloud"
)

type unknownStatusEngine struct {
	*routingEngine
	state live.State
	err   error
}

func (e *unknownStatusEngine) Snapshot() (live.State, bool, error) {
	// Even an inconsistent implementation must not publish unknown as ready.
	return e.state, true, e.err
}

func TestUnknownStatusPreservesObservationAndFailures(t *testing.T) {
	for _, mode := range []string{"observed", "unmapped", "invalid-mapping", "cloud-read-failed", "invalid-cloud", "guard-failed", "persistence-failed", "closed", "missing-observation", "missing-cloud", "missing-pending", "known-pending"} {
		t.Run(mode, func(t *testing.T) {
			stamp := time.Now().Add(-time.Hour).UnixNano()
			at := time.Now().UTC()
			state := live.State{GroupID: "asg-one", Region: "ap-guangzhou", ObservedAt: at,
				Members: map[string]live.Member{"ins-one": {ID: "ins-one"}, "ins-two": {ID: "ins-two"}},
				Cloud:   &tencentcloud.Snapshot{At: at, Group: tencentcloud.Group{ID: "asg-one", Count: 2}},
				Pending: &live.Operation{Unknown: true}, LastCompletedAt: stamp, LastEvent: "expand-write-unknown"}
			var snapshotErr error = live.ErrUnknown
			wantStatus := false
			switch mode {
			case "observed":
				wantStatus = true
			case "unmapped":
				state.ErrorCode = "NomadMemberMappingUnavailable"
				wantStatus = true
			case "invalid-mapping":
				state.ErrorCode = "NomadMemberMappingInvalid"
				wantStatus = true
			case "cloud-read-failed":
				state.ErrorCode = "CloudObservationFailed"
			case "invalid-cloud":
				state.ErrorCode = "InvalidCloudSnapshot"
			case "guard-failed":
				state.ErrorCode = "LaunchConfigurationChanged"
			case "persistence-failed":
				snapshotErr = errors.New("StatePersistenceFailed")
			case "closed":
				snapshotErr = live.ErrClosed
			case "missing-observation":
				state.ObservedAt = time.Time{}
			case "missing-cloud":
				state.Cloud = nil
			case "missing-pending":
				state.Pending = nil
			case "known-pending":
				state.Pending.Unknown = false
			}
			cloud := &routingCloud{account: "10001", region: "ap-guangzhou"}
			engine := &unknownStatusEngine{routingEngine: &routingEngine{}, state: state, err: snapshotErr}
			target := routeTarget(t, t.TempDir(), cloud, func(cfg live.Config, _ live.Cloud, _ live.Nomad) (groupController, error) {
				engine.cfg = cfg
				return engine, nil
			})
			status, err := target.Status(map[string]string{"tencent_group_id": "asg-one"})
			if !wantStatus {
				if status != nil || err == nil {
					t.Fatalf("unsafe observation accepted: %+v %v", status, err)
				}
			} else {
				if err != nil || status == nil || status.Ready || status.Count != 2 || status.Meta["tencent.write_outcome"] != "unknown" || status.Meta["nomad_autoscaler.last_event"] != strconv.FormatInt(stamp, 10) {
					t.Fatalf("unknown workflow hid real membership or changed completion: %+v %v", status, err)
				}
				wantError := state.ErrorCode
				if wantError == "" {
					wantError = live.ErrUnknown.Error()
				}
				if status.Meta["tencent.error"] != wantError {
					t.Fatal("lost controlled diagnostic", status.Meta)
				}
			}
			if cloud.writes.Load() != 0 || engine.calls.Load() != 0 {
				t.Fatal("Status advanced the workflow")
			}
		})
	}
}
