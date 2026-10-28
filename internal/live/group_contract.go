package live

import (
	"fmt"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/tencentcloud"
)

// GroupContract journals retain historical targets independently of today's
// Min/Max. Every new mutation checks live bounds; a limit change never rewrites
// an in-flight operation or turns an acknowledged deletion into a new request.
func (e *Engine) groupGuard(s tencentcloud.Snapshot) error {
	g := s.Group
	if g.AllocationPolicy != "" && g.AllocationPolicy != "LAUNCH_CONFIGURATION" {
		return code("UnsupportedGroupAllocationPolicy")
	}
	if s.LaunchConfiguration.ChargeType != "POSTPAID_BY_HOUR" {
		return code("UnsupportedGroupChargeType")
	}
	if g.Status != "NORMAL" || s.LaunchConfiguration.Status != "NORMAL" {
		return code("GroupConfigurationNotReady")
	}
	return nil
}

func (e *Engine) warnGroup(s tencentcloud.Snapshot) {
	if !e.cfg.GroupContract || e.cfg.Logger == nil {
		return
	}
	key := fmt.Sprintf("%s/%d/%d/%t/%s", s.Group.Enabled, s.PolicyCount, s.ScheduledActionCount, s.ScheduledActionsUnavailable, s.Group.ScalingMode)
	if key == e.lastGroupWarning {
		return
	}
	e.lastGroupWarning = key
	if s.Group.Enabled == "DISABLED" {
		e.cfg.Logger.Warn("AS group is disabled; waiting for it to be enabled", "group", e.cfg.GroupID)
	}
	if s.PolicyCount > 0 || s.ScheduledActionCount > 0 {
		e.cfg.Logger.Warn("other capacity controllers are configured; exclusive capacity control is recommended", "group", e.cfg.GroupID, "scaling_policies", s.PolicyCount, "scheduled_actions", s.ScheduledActionCount)
	}
	if s.ScheduledActionsUnavailable {
		e.cfg.Logger.Warn("scheduled action diagnostics unavailable; verify as:DescribeScheduledActions permission", "group", e.cfg.GroupID)
	}
	if s.Group.ScalingMode == "WAKE_UP_STOPPED_SCALING" {
		e.cfg.Logger.Warn("wake-up-stopped scaling mode is outside the supported deployment scope", "group", e.cfg.GroupID)
	}
}
func (e *Engine) clampToGroup(requested int64) int64 {
	g := e.state.Cloud.Group
	effective := requested
	if effective < g.Min {
		effective = g.Min
	}
	if effective > g.Max {
		effective = g.Max
	}
	if effective != requested && e.cfg.Logger != nil {
		e.cfg.Logger.Warn("requested capacity clamped to cloud group limits", "group", e.cfg.GroupID, "old_count", len(e.state.Members), "requested_count", requested, "effective_count", effective, "min", g.Min, "max", g.Max)
	}
	return effective
}
func (e *Engine) deleteProtected(m tencentcloud.Member, vm tencentcloud.VM) bool {
	return vm.Protected || (m.Protected && (!e.cfg.GroupContract || e.state.LegacyContract != nil || e.cfg.ScaleInProtection))
}
func (e *Engine) removalCandidate(m Member) bool {
	if !e.cfg.GroupContract {
		return selectable(m)
	}
	// Protected combines AS and CVM flags for diagnostics. Consult the original
	// cloud fields separately: AS protection is optional, CVM protection is not.
	m.Protected = false
	if !selectable(m) || e.state.Cloud == nil {
		return false
	}
	cm, ok := cloudMember(*e.state.Cloud, m.ID)
	vm, seen := e.state.Cloud.VMs[m.ID]
	return ok && seen && !e.deleteProtected(cm, vm)
}
func serviceMembers(s tencentcloud.Snapshot) int64 {
	var count int64
	for _, m := range s.Members {
		if m.Lifecycle == "IN_SERVICE" {
			count++
		}
	}
	return count
}
func (e *Engine) upgradeGroupContract() error {
	if e.state.Pending != nil || e.state.Batch != nil || e.state.ManualReview {
		return e.migrateActiveSDKState()
	}
	if err := e.backupLegacyState(fmt.Sprintf("live-v%d-before-group-contract.backup.json", e.state.Version)); err != nil {
		return err
	}
	e.state.Version = 3
	e.state.MaxCapacity = 0
	return nil
}
