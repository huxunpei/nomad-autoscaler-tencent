package live

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"syscall"
)

const stateFileLimit int64 = 64 << 20

func syncDir(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	err = f.Sync()
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	return err
}

// Create each missing component separately and sync its parent. Existing
// symlink components are refused so two names cannot bypass the process lock.
func privateDir(path string) error {
	if path == string(filepath.Separator) {
		return nil
	}
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		if err = privateDir(filepath.Dir(path)); err != nil {
			return err
		}
		if err = os.Mkdir(path, 0700); err != nil {
			return err
		}
		return syncDir(filepath.Dir(path))
	}
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("UnsafeStateDirectory")
	}
	// Inspect existing parents as well; a symlink in any component is unsafe.
	if err = privateDir(filepath.Dir(path)); err != nil {
		return err
	}
	return nil
}

func (e *Engine) lockDir() error {
	if err := privateDir(e.cfg.Dir); err != nil {
		return errors.New("StateDirectoryFailed")
	}
	info, err := os.Lstat(e.cfg.Dir)
	if err != nil || info.Mode().Perm()&0077 != 0 {
		return errors.New("StateDirectoryNotPrivate")
	}
	path := filepath.Join(e.cfg.Dir, ".lock")
	// O_NOFOLLOW also protects the journal and lock from accidental symlinks.
	fd, err := syscall.Open(path, syscall.O_CREAT|syscall.O_RDWR|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		if errors.Is(err, syscall.ELOOP) {
			return errors.New("UnsafeStateLock")
		}
		return errors.New("StateLockFailed")
	}
	e.lock = os.NewFile(uintptr(fd), path)
	info, err = e.lock.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
		e.lock.Close()
		return errors.New("UnsafeStateLock")
	}
	if err = syscall.Flock(fd, syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		e.lock.Close()
		return errors.New("StateDirectoryInUse")
	}
	if err = syncDir(e.cfg.Dir); err != nil {
		e.release()
		return errors.New("StateDirectorySyncFailed")
	}
	return nil
}

func (e *Engine) release() {
	if e.lock != nil {
		_ = syscall.Flock(int(e.lock.Fd()), syscall.LOCK_UN)
		_ = e.lock.Close()
	}
}

func (e *Engine) load() (bool, error) {
	if err := e.checkPolicyPending(); err != nil {
		return false, err
	}
	path := filepath.Join(e.cfg.Dir, "live.json")
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		if errors.Is(err, syscall.ELOOP) {
			return false, errors.New("UnsafeStateFile")
		}
		return false, errors.New("StateReadFailed")
	}
	f := os.NewFile(uintptr(fd), path)
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Size() > stateFileLimit {
		return false, errors.New("UnsafeStateFile")
	}
	raw, err := io.ReadAll(io.LimitReader(f, stateFileLimit+1))
	if err != nil || int64(len(raw)) > stateFileLimit || decodeStrict(raw, &e.state) != nil {
		return false, errors.New("InvalidJournal")
	}
	if err = e.validateState(); err != nil && !legacyCustomSource(e.cfg, e.state) {
		return false, err
	}
	if err = e.checkLegacyContractBackup(); err != nil {
		return false, err
	}
	if err = e.checkPolicyAudit(); err != nil {
		return false, err
	}
	e.durableJournal = append([]byte(nil), raw...)
	return true, nil
}

func (e *Engine) save() (err error) {
	if e.persistError != nil {
		return e.persistError
	}
	var attempted []byte
	defer func() {
		if err != nil {
			e.captureFailedSave(attempted)
			e.persistError = errors.New("StatePersistenceFailed")
		} else {
			e.durableJournal = append([]byte(nil), attempted...)
			e.logLifecycle()
		}
	}()
	b, err := json.MarshalIndent(e.state, "", "  ")
	if err != nil {
		return errors.New("StatePersistenceFailed")
	}
	attempted = append(b, '\n')
	f, err := os.CreateTemp(e.cfg.Dir, ".live-*")
	if err != nil {
		return errors.New("StatePersistenceFailed")
	}
	defer os.Remove(f.Name())
	attempted = append(b, '\n')
	if _, err = f.Write(attempted); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err == nil {
		err = os.Rename(f.Name(), filepath.Join(e.cfg.Dir, "live.json"))
	}
	if err == nil && e.journalCheckpoint != nil {
		err = e.journalCheckpoint("renamed")
	}
	if err == nil {
		err = syncDir(e.cfg.Dir)
	}
	if err != nil {
		return errors.New("StatePersistenceFailed")
	}
	return nil
}

// Upgrading a live/unknown operation would reinterpret an in-flight contract.
// Require a quiescent v1 journal and retain its exact bytes before publishing v2.
func (e *Engine) upgradeLegacy() error {
	if e.state.Pending != nil || e.state.ManualReview {
		return errors.New("LegacyOperationRequiresCompletion")
	}
	if err := e.backupLegacyState("live-v1.backup.json"); err != nil {
		return err
	}
	e.state.Version = 2
	e.state.MaxCapacity = e.cfg.MaxCapacity
	return nil
}
