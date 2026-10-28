package live

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"syscall"
)

// Migration publishes a backup only after its complete inode is synced. An
// existing exact backup is synced again before use, including its directory:
// a prior failed attempt may have linked it but not completed directory sync.
func (e *Engine) backupLegacyState(name string) error {
	source := filepath.Join(e.cfg.Dir, "live.json")
	old, err := readPrivate(source, stateFileLimit)
	if err != nil {
		return code("LegacyBackupFailed")
	}
	if len(e.durableJournal) == 0 || !bytes.Equal(old, e.durableJournal) {
		return code("LegacyJournalConflict")
	}
	path := filepath.Join(e.cfg.Dir, name)
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if errors.Is(err, os.ErrNotExist) {
		if err = writeOnce(path, old); err != nil {
			return code("LegacyBackupFailed")
		}
	} else {
		if err != nil {
			return code("LegacyBackupConflict")
		}
		f := os.NewFile(uintptr(fd), path)
		info, statErr := f.Stat()
		if statErr != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Size() > stateFileLimit {
			f.Close()
			return code("LegacyBackupConflict")
		}
		prior, readErr := io.ReadAll(io.LimitReader(f, stateFileLimit+1))
		if readErr != nil || !bytes.Equal(prior, old) {
			f.Close()
			return code("LegacyBackupConflict")
		}
		syncErr := f.Sync()
		closeErr := f.Close()
		if syncErr != nil || closeErr != nil || syncDir(e.cfg.Dir) != nil {
			return code("LegacyBackupFailed")
		}
	}
	// The loaded source remains authoritative. Do not promote a snapshot that
	// changed while its backup was being prepared, even if the new JSON is valid.
	current, err := readPrivate(source, stateFileLimit)
	if err != nil || !bytes.Equal(current, old) {
		return code("LegacyJournalConflict")
	}
	return nil
}
