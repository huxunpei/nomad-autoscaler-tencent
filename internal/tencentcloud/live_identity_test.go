//go:build liveidentity

package tencentcloud

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/huxunpei/nomad-autoscaler-tencent/internal/telemetry"
)

func TestLiveInstanceIdentity(t *testing.T) {
	if os.Getenv("LIVE_IDENTITY") != "1" {
		t.Skip()
	}
	group, deny := os.Getenv("LIVE_GROUP"), os.Getenv("LIVE_DENY_GROUP")
	denyInstance := os.Getenv("LIVE_DENY_INSTANCE_ID")
	if group == "" || deny == "" || len(denyInstance) != 12 || !resource(denyInstance, "ins") {
		t.Fatal("LIVE_GROUP, LIVE_DENY_GROUP and a preverified nonexistent LIVE_DENY_INSTANCE_ID are required")
	}
	metrics := telemetry.New()
	var jumped atomic.Bool
	var expiry atomic.Int64
	client, err := newAuthenticated(context.Background(), AuthConfig{Metrics: metrics, Region: "ap-guangzhou", Timeout: 20 * time.Second}, authDependencies{now: func() time.Time {
		if !jumped.Load() {
			return time.Now()
		}
		return time.Unix(expiry.Load(), 0).Add(-4 * time.Minute)
	}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	if os.Getenv("LIVE_MODE") == "deny" {
		_, err = client.Observe(ctx, group, "")
		if err == nil {
			t.Fatal("unprivileged identity observed the group")
		}
		t.Log("deny_observe", err)
		return
	}

	snap, err := client.Observe(ctx, group, "")
	if err != nil {
		t.Fatal(err)
	}
	if snap.Group.Count != 0 || snap.Group.Desired != 0 || len(snap.Members) != 0 {
		t.Fatalf("group not empty: desired=%d count=%d members=%d", snap.Group.Desired, snap.Group.Count, len(snap.Members))
	}
	if snap.ScheduledActionsUnavailable {
		t.Fatal("as:DescribeScheduledActions was unavailable to the allowed identity")
	}
	t.Log("account", client.AccountID(), "credential_ttl_sec", client.values.expires-time.Now().Unix())

	roleReads, credReads, identities := metricAttempts(metrics, "InstanceMetadataRole"), metricAttempts(metrics, "InstanceMetadataCredentials"), metricAttempts(metrics, "GetCallerIdentity")
	expiry.Store(client.values.expires)
	jumped.Store(true)
	if _, err = client.Observe(ctx, group, ""); err != nil {
		t.Fatal(err)
	}
	if client.AccountID() == "" {
		t.Fatal("account lost across refresh")
	}
	dRole := metricAttempts(metrics, "InstanceMetadataRole") - roleReads
	dCred := metricAttempts(metrics, "InstanceMetadataCredentials") - credReads
	dID := metricAttempts(metrics, "GetCallerIdentity") - identities
	t.Log("refresh_delta", "role", dRole, "credentials", dCred, "identity", dID)
	if dRole < 1 || dCred < 1 {
		t.Fatal("expiry window did not reread instance metadata")
	}
	if dID < 1 {
		t.Log("same_credential_material_skipped_reauthentication")
	}

	jumped.Store(false)
	other, err := client.Observe(ctx, deny, "")
	if err == nil {
		t.Fatalf("other group visible desired=%d count=%d", other.Group.Desired, other.Group.Count)
	}
	t.Log("other_group_observe", err)
	denied, err := client.RemoveExact(ctx, deny, denyInstance)
	t.Log("other_group_remove", denied.Action, denied.Attempted, denied.Unknown, err)
	var deniedError *Error
	if err == nil || !IsRejectedWrite(denied, err) || !errors.As(err, &deniedError) ||
		(deniedError.Code != "UnauthorizedOperation" && deniedError.Code != "AuthFailure.UnauthorizedOperation") {
		t.Fatal("other group write did not prove a CAM authorization denial")
	}

	out, err := client.SetDesiredCAS(ctx, group, 1, 0)
	t.Log("set_desired_cas", out.Action, out.Attempted, out.RequestID, err)
	if err != nil || out.Action != "ModifyDesiredCapacity" || !requestPattern.MatchString(out.RequestID) {
		t.Fatal("absolute CAS growth was not acknowledged")
	}
	var member string
	deadline := time.Now().Add(8 * time.Minute)
	for time.Now().Before(deadline) {
		snap, err = client.Observe(ctx, group, "")
		if err != nil {
			t.Fatal(err)
		}
		if len(snap.Members) == 1 {
			vm := snap.VMs[snap.Members[0].ID]
			t.Log("member", snap.Members[0].ID, snap.Members[0].Lifecycle, "group_activity", snap.Group.Activity, "vm_returned", vm.Returned, "vm_state", vm.State)
			if vm.Returned && vm.State == "RUNNING" && snap.Members[0].Lifecycle == "IN_SERVICE" &&
				snap.Group.Activity == "NOT_IN_ACTIVITY" && snap.Group.Count == 1 && snap.Group.Desired == 1 {
				member = snap.Members[0].ID
				break
			}
		}
		time.Sleep(15 * time.Second)
	}
	if member == "" {
		t.Fatal("new member did not become a stable AS member with a running described VM")
	}
	removed, err := client.RemoveExact(ctx, group, member)
	t.Log("remove_member", removed.Action, removed.ActivityID, err)
	if err != nil || removed.ActivityID == "" {
		t.Fatal("remove did not return an activity")
	}
	deadline = time.Now().Add(6 * time.Minute)
	for time.Now().Before(deadline) {
		snap, err = client.Observe(ctx, group, member)
		if err != nil {
			t.Fatal(err)
		}
		vm := snap.VMs[member]
		t.Log("after_remove", "members", len(snap.Members), "count", snap.Group.Count, "tracked_returned", vm.Returned, "tracked_state", vm.State)
		if len(snap.Members) == 0 && snap.Group.Count == 0 && snap.Group.Desired == 0 && !vm.Returned {
			return
		}
		time.Sleep(15 * time.Second)
	}
	t.Fatal("removed member was still readable")
}

func metricAttempts(c *telemetry.Collector, action string) uint64 {
	rec := httptest.NewRecorder()
	c.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	prefix := "tencent_as_api_attempts_total{action=\"" + action + "\"} "
	for _, line := range strings.Split(rec.Body.String(), "\n") {
		if strings.HasPrefix(line, prefix) {
			n, _ := strconv.ParseUint(strings.TrimSpace(strings.TrimPrefix(line, prefix)), 10, 64)
			return n
		}
	}
	return 0
}
