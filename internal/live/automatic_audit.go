package live

import (
	"errors"
	"os"
	"path/filepath"
)

// Version 1 remains a manual transaction even if an operator chose the same
// display name as the reconciler. Only explicit version 2/3 origins opt into
// automatic bookkeeping. Old completed records remain readable.
func validRecoveryRecordOrigin(record recoveryRecord) bool {
	if record.Version == 4 {
		return validLaunchConfigurationRecord(record)
	}
	if record.Version == 1 {
		return !record.Automatic
	}
	if record.Version == 3 {
		switch record.Plan.Decision.Action {
		case "confirm-grow-failure":
			return validGrowthFailureRecord(record)
		case "finish-grow-failure":
			return validGrowthSettlementRecord(record)
		case "confirm-legacy-custom-drain-observed":
			return validLegacyCustomDrainRecord(record)
		case "confirm-sdk-drain-observed":
			return validUnknownSDKDrainRecord(record)
		case "migrate-legacy-custom-contract":
			return validLegacyCustomRecord(record)
		case "migrate-v2-sdk-contract", "migrate-v1-sdk-operation":
			return validActiveMigrationRecord(record)
		case "begin-node-restore", "confirm-node-restored":
			return validNodeRecoveryRecord(record)
		case "retry-journal-save":
			return validJournalRetryRecord(record)
		case "begin-sdk-post", "confirm-sdk-post":
			return validPostAuditRecord(record)
		default:
			return false
		}
	}
	if record.Version != 2 || !record.Automatic {
		return false
	}
	op := record.Before.Pending
	if op == nil || !op.Unknown || op.Phase != "unknown" || record.Plan.Operator != "automatic-reconciler" || record.Plan.Decision.OperationID != op.ID {
		return false
	}
	if op.ActivityID != "" {
		return record.Plan.Reference == "recorded-activity/"+op.ID && record.Plan.Decision.ActivityID == op.ActivityID
	}
	return validDiscoveredRecoveryRecord(record)
}

// resumeAutomaticRecoveryAudit is called before startup loads/saves or refresh
// can replace the exact journal bytes covered by an incomplete transaction.
// The caller owns the directory lock and, for a running engine, mu.
func (e *Engine) resumeAutomaticRecoveryAudit() error {
	path := filepath.Join(e.cfg.Dir, recoveryMarkerName)
	_, err := os.Lstat(path)
	var marker recoveryMarker
	if errors.Is(err, os.ErrNotExist) {
		if e.automaticAudit == nil {
			return nil
		}
		// The current process may have failed before publishing a marker, or after
		// removing it but before syncing the directory. Retain its exact hashes.
		marker = *e.automaticAudit
		_, recordErr := os.Lstat(auditPath(e.cfg.Dir, marker.Audit))
		if errors.Is(recordErr, os.ErrNotExist) {
			b, readErr := readPrivate(filepath.Join(e.cfg.Dir, "live.json"), stateFileLimit)
			if readErr != nil || sha(b) != marker.BeforeSHA256 {
				return code("RecoveryJournalConflict")
			}
			loaded := &Engine{cfg: e.cfg}
			exists, loadErr := loaded.load()
			if loadErr != nil || !exists {
				return code("RecoveryJournalInvalid")
			}
			if err = loaded.checkRecoveryAudit(); err != nil {
				return err
			}
			if err = syncDir(e.cfg.Dir); err != nil {
				return code("RecoveryAuditRepairRequired")
			}
			e.state = loaded.state
			e.durableJournal = append([]byte(nil), loaded.durableJournal...)
			e.automaticAudit = nil
			e.persistError = nil
			e.ready = false
			return nil
		}
		if recordErr != nil {
			return code("RecoveryAuditInvalid")
		}
	} else {
		if err != nil {
			return code("RecoveryAuditRepairRequired")
		}
		b, readErr := readPrivate(path, 1<<20)
		if readErr != nil || decodeStrict(b, &marker) != nil {
			return code("RecoveryMarkerMismatch")
		}
		if e.automaticAudit != nil && marker != *e.automaticAudit {
			return code("RecoveryMarkerMismatch")
		}
	}
	record, err := readRecord(e.cfg.Dir, marker.Audit)
	if err != nil {
		return err
	}
	if !record.Automatic {
		return code("RecoveryAuditRepairRequired")
	}
	bookkeeper := e
	if scope(e.cfg) != record.Plan.Scope {
		bookkeeper, err = e.legacyAutomaticBookkeeper(record)
		if err != nil {
			return err
		}
	}
	if _, err = bookkeeper.finishRecoveryBookkeeping(marker); err != nil {
		return err
	}
	if bookkeeper != e {
		e.state = bookkeeper.state
		e.durableJournal = append([]byte(nil), bookkeeper.durableJournal...)
	}
	e.automaticAudit = nil
	e.persistError = nil
	e.ready = false
	return nil
}
