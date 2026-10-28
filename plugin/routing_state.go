package plugin

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"syscall"

	"github.com/huxunpei/nomad-autoscaler-tencent/internal/targetconfig"
)

// Credentials and connection settings are intentionally absent. Persisting the
// canonical policy before live.Open makes restart recovery independent of the
// first Status call and preserves the exact SDK semantics of pending work.
type routeRecord struct {
	Version     int                 `json:"version"`
	Account     string              `json:"account_id"`
	Region      string              `json:"region"`
	Policy      targetconfig.Policy `json:"policy"`
	MaxCapacity int64               `json:"max_capacity,omitempty"`
}

func routeSyncDir(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}
func routeDirectoryComponents(path string) error {
	if path == "/" {
		return nil
	}
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		if err = routeDirectoryComponents(filepath.Dir(path)); err != nil {
			return err
		}
		if err = os.Mkdir(path, 0700); err != nil && !os.IsExist(err) {
			return err
		}
		if err = routeSyncDir(filepath.Dir(path)); err != nil {
			return err
		}
		info, err = os.Lstat(path)
	}
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("UnsafeRouteDirectory")
	}
	return routeDirectoryComponents(filepath.Dir(path))
}
func routePrivateDir(path string) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) == "/" {
		return errors.New("InvalidRouteDirectory")
	}
	if routeDirectoryComponents(path) != nil {
		return errors.New("UnsafeRouteDirectory")
	}
	info, err := os.Lstat(path)
	if err != nil || info.Mode().Perm()&0077 != 0 {
		return errors.New("RouteDirectoryNotPrivate")
	}
	return nil
}
func lockRouteDir(dir string) (*os.File, error) {
	if err := routePrivateDir(dir); err != nil {
		return nil, err
	}
	fd, err := syscall.Open(filepath.Join(dir, ".route.lock"), syscall.O_CREAT|syscall.O_RDWR|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		if errors.Is(err, syscall.ELOOP) {
			return nil, errors.New("UnsafeRouteLock")
		}
		return nil, errors.New("RouteLockUnavailable")
	}
	f := os.NewFile(uintptr(fd), "route-lock")
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
		f.Close()
		return nil, errors.New("UnsafeRouteLock")
	}
	if err = syscall.Flock(fd, syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, errors.New("GroupControllerAlreadyRunning")
	}
	if routeSyncDir(dir) != nil {
		releaseRouteLock(f)
		return nil, errors.New("RouteLockSyncFailed")
	}
	return f, nil
}
func releaseRouteLock(f *os.File) { _ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN); _ = f.Close() }
func readRouteRecord(dir string) (routeRecord, bool, error) {
	var record routeRecord
	fd, err := syscall.Open(filepath.Join(dir, "route.json"), syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if errors.Is(err, os.ErrNotExist) {
		return record, false, nil
	}
	if err != nil {
		if errors.Is(err, syscall.ELOOP) {
			return record, false, errors.New("UnsafeRouteRecord")
		}
		return record, false, errors.New("RouteRecordUnreadable")
	}
	f := os.NewFile(uintptr(fd), "route-record")
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Size() > 64<<10 {
		return record, false, errors.New("UnsafeRouteRecord")
	}
	decoder := json.NewDecoder(io.LimitReader(f, (64<<10)+1))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&record) != nil {
		return record, false, errors.New("InvalidRouteRecord")
	}
	var trailing any
	if decoder.Decode(&trailing) != io.EOF {
		return record, false, errors.New("InvalidRouteRecord")
	}
	return record, true, nil
}
func writeRouteRecord(dir string, record routeRecord) error {
	data, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		return errors.New("RouteRecordPersistenceFailed")
	}
	f, err := os.CreateTemp(dir, ".route-*")
	if err != nil {
		return errors.New("RouteRecordPersistenceFailed")
	}
	defer os.Remove(f.Name())
	if _, err = f.Write(append(data, '\n')); err == nil {
		err = f.Sync()
	}
	closed := f.Close()
	if err == nil {
		err = closed
	}
	if err == nil {
		err = os.Rename(f.Name(), filepath.Join(dir, "route.json"))
	}
	if err == nil {
		err = routeSyncDir(dir)
	}
	if err != nil {
		return errors.New("RouteRecordPersistenceFailed")
	}
	return nil
}
