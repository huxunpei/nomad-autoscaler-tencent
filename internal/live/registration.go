package live

import (
	"context"
	"time"

	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadclient"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/telemetry"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/tencentcloud"
)

type registrationObserver interface {
	ObserveMembers(context.Context, string, string, []string) ([]nomadclient.Node, error)
}

func (e *Engine) registrationEnabled() bool {
	_, available := e.nomad.(registrationObserver)
	// Historical active workflows retain their original strict contract.
	return e.cfg.GroupContract && e.state.LegacyContract == nil && available
}

func (e *Engine) observeRegistrationMembers(ctx context.Context, ids []string) ([]nomadclient.Node, bool, error) {
	if e.registrationEnabled() {
		nodes, err := e.nomad.(registrationObserver).ObserveMembers(ctx, e.cfg.GroupID, e.cfg.Region, ids)
		return nodes, true, err
	}
	nodes, err := e.nomad.ResolveMembers(ctx, e.cfg.GroupID, e.cfg.Region, ids)
	return nodes, false, err
}

func (e *Engine) classifyMissingRegistrations(snap tencentcloud.Snapshot) {
	for _, cloudMember := range snap.Members {
		member := e.state.Members[cloudMember.ID]
		if member.Joined {
			continue
		}
		// Loss of a previously registered node is not an initial-join timeout.
		if member.EverJoined {
			e.observeError = code("NomadMemberMappingUnavailable")
			continue
		}
		added, err := time.Parse(time.RFC3339, cloudMember.AddedAt)
		if err != nil || added.After(snap.At) {
			e.observeError = code("RegistrationTimeInvalid")
			continue
		}
		deadline := added.Add(e.cfg.RegistrationGracePeriod)
		member.RegistrationDeadline = deadline.UTC().Format(time.RFC3339Nano)
		member.RegistrationState = "waiting"
		if !snap.At.Before(deadline) {
			member.RegistrationState = "timed_out"
		}
		e.state.Members[cloudMember.ID] = member
	}
}

// Expiry releases the group readiness gate, never the removal predicate.
// Any cloud activity, unstable member or uncertain observation still blocks.
func (e *Engine) expiredRegistrationMember(member Member) bool {
	return e.registrationEnabled() && member.RegistrationState == "timed_out" &&
		!member.EverJoined && !member.Joined && runningCloudMember(member)
}

func CountRegistration(members map[string]Member) telemetry.RegistrationCounts {
	var out telemetry.RegistrationCounts
	for _, member := range members {
		switch member.RegistrationState {
		case "registered":
			out.Registered++
		case "waiting":
			out.Waiting++
		case "timed_out":
			out.TimedOut++
		case "unknown":
			out.Unknown++
		}
	}
	return out
}

func (e *Engine) publishRegistrationObservation() {
	if !e.registrationEnabled() {
		return
	}
	if e.observeError != nil {
		// Never expose stale absence as a current timeout after a failed read.
		for id, member := range e.state.Members {
			member.EverJoined = member.EverJoined || member.Joined
			member.RegistrationState = "unknown"
			member.RegistrationDeadline = ""
			e.state.Members[id] = member
		}
	}
	counts := CountRegistration(e.state.Members)
	counts.ObservationFailed = e.observeError != nil
	e.cfg.Metrics.SetRegistration(e.cfg.Dir, counts)
	if e.observeError != nil {
		return
	}
	if e.registrationLog == nil {
		e.registrationLog = map[string]string{}
	}
	for _, id := range memberIDs(e.state.Members) {
		member := e.state.Members[id]
		previous := e.registrationLog[id]
		if e.cfg.Logger != nil {
			if member.RegistrationState == "timed_out" && previous != "timed_out" {
				added, _ := time.Parse(time.RFC3339Nano, member.RegistrationDeadline)
				waited := e.state.ObservedAt.Sub(added.Add(-e.cfg.RegistrationGracePeriod))
				e.cfg.Logger.Warn("Nomad registration grace period expired",
					"group", e.cfg.GroupID, "instance", id,
					"waited_seconds", waited.Seconds(), "grace_period", e.cfg.RegistrationGracePeriod.String(),
					"deadline", member.RegistrationDeadline, "action", "retain-and-observe")
			} else if member.RegistrationState == "registered" && previous == "timed_out" {
				e.cfg.Logger.Info("Nomad registration recovered", "group", e.cfg.GroupID, "instance", id, "node", member.NodeID)
			}
		}
		e.registrationLog[id] = member.RegistrationState
	}
	for id := range e.registrationLog {
		if _, exists := e.state.Members[id]; !exists {
			delete(e.registrationLog, id)
		}
	}
}
