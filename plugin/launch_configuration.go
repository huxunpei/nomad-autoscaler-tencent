package plugin

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"

	"github.com/huxunpei/nomad-autoscaler-tencent/internal/live"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadsdk"
)

type LaunchConfigurationCloud interface {
	live.RecoveryCloud
	AccountID() string
	Region() string
}

// WithLaunchConfigurationMaintenance owns both normal locks. The callback can
// only read providers and commit an audited local acceptance transaction.
func WithLaunchConfigurationMaintenance(ctx context.Context, dir string, c LaunchConfigurationCloud, n live.RecoveryNomad, fn func(*live.Recovery, string) error) error {
	if c == nil || n == nil || fn == nil || !filepath.IsAbs(dir) || filepath.Clean(dir) != dir {
		return errors.New("InvalidLaunchConfigurationDirectory")
	}
	if _, err := os.Lstat(filepath.Join(dir, "route.json")); err != nil {
		return errors.New("PolicyUpdateRouteMissing")
	}
	lock, err := lockRouteDir(dir)
	if err != nil {
		return err
	}
	defer releaseRouteLock(lock)
	raw, err := policyRead(filepath.Join(dir, "route.json"))
	if err != nil {
		return err
	}
	var route routeRecord
	if policyDecode(raw, &route) != nil || !validPolicyRoute(route) || route.Account != c.AccountID() || route.Region != c.Region() || filepath.Base(dir) != route.Policy.GroupID || filepath.Base(filepath.Dir(dir)) != route.Region || filepath.Base(filepath.Dir(filepath.Dir(dir))) != route.Account {
		return errors.New("LaunchConfigurationScopeMismatch")
	}
	r, err := live.OpenRecovery(policyConfig(dir, route), c, n)
	if err != nil {
		return err
	}
	defer r.Close()
	journal, err := policyRead(filepath.Join(dir, "live.json"))
	if err != nil {
		return err
	}
	var state live.State
	if json.Unmarshal(journal, &state) != nil || state.NomadPolicySHA256 != nomadsdk.New(nil, route.Policy).PolicyDigest() || state.NomadInstanceIDMetaKey != route.Policy.InstanceIDMetaKey {
		return errors.New("LaunchConfigurationPolicyMismatch")
	}
	if ctx.Err() != nil {
		return errors.New("RecoveryCanceled")
	}
	return fn(r, policyHash(raw))
}
