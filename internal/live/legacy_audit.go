package live

// A v2 automatic transaction may still carry the original fixed-capacity
// scope when a new group-contract controller starts. Repair only its local
// bookkeeping under that exact scope before the normal audited migration.
// This engine deliberately has no cloud/Nomad clients or lock ownership.
func (e *Engine) legacyAutomaticBookkeeper(record recoveryRecord) (*Engine, error) {
	fail := func() (*Engine, error) { return nil, code("RecoveryMarkerMismatch") }
	old := record.Plan.Scope
	if !e.cfg.GroupContract || record.Version != 2 || !record.Automatic || old.GroupContract || old.MaxCapacity < 2 || old.MaxCapacity > MaxSupportedCapacity {
		return fail()
	}
	cfg := e.cfg
	cfg.GroupContract = false
	cfg.MaxCapacity = old.MaxCapacity
	if scope(cfg) != old {
		return fail()
	}
	adapter, ok := e.nomad.(sdkLifecycle)
	if !ok || !digestPattern.MatchString(record.Before.NomadPolicySHA256) || adapter.PolicyDigest() != record.Before.NomadPolicySHA256 {
		return fail()
	}
	key := ""
	if mapping, ok := e.nomad.(interface{ InstanceMetaKey() string }); ok {
		key = mapping.InstanceMetaKey()
	}
	for _, state := range []State{record.Before, record.After} {
		if state.Version != 2 || state.MaxCapacity != old.MaxCapacity || state.NomadPolicySHA256 != record.Before.NomadPolicySHA256 || state.NomadInstanceIDMetaKey != key {
			return fail()
		}
		if _, err := legacyMigrationAfter(e.cfg, state, record.BeforeSHA256); err != nil {
			return fail()
		}
	}
	return &Engine{cfg: cfg}, nil
}
