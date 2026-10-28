package tencentcloud

import (
	"sort"
	"strings"

	as "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/as/v20180419"
)

// Diagnostic values are a finite vocabulary. Preserve useful documented
// capacity/quota and lifecycle-hook codes, reduce other SDK codes to their family,
// and never copy provider messages or arbitrary code suffixes into state/logs.
// A diagnostic is not evidence of non-submission or permission to retry.
func activityDiagnosticCode(raw string) string {
	switch raw {
	case "云主机售罄":
		// AS can return this localized Code in DetailedStatusMessageSet.
		// Match only the exact observed value; never retain arbitrary messages.
		return "ResourcesSoldOut"
	case "LifecycleHookActionAbandon":
		// Exact AS launch-hook rollback code observed on failed activities.
		return raw
	case "LimitExceeded.InstanceQuota", "LimitExceeded.CvmInstanceQuota",
		"LimitExceeded.SpotQuota", "LimitExceeded.UserSpotQuota",
		"LimitExceeded.VpcSubnetNum",
		"ResourcesSoldOut.AvailableZone", "ResourcesSoldOut.SpecifiedInstanceType",
		"ResourceInsufficient.AvailabilityZoneSoldOut", "ResourceInsufficient.CloudDiskSoldOut",
		"ResourceInsufficient.SpecifiedInstanceType",
		"SpotSoldOut", "DiskSoldOut", "InvalidInstanceType", "InstancesQuotaLimitExceeded":
		return raw
	}
	if safeCode(raw) {
		return strings.SplitN(raw, ".", 2)[0]
	}
	return "Unclassified"
}

func activityFailureCodes(status string, details []*as.DetailedStatusMessage) []string {
	// A successful multi-zone attempt may contain errors from earlier attempts.
	// Only the activity status defines whether this is a failed activity.
	if !oneOf(status, "FAILED", "PARTIALLY_SUCCESSFUL", "CANCELLED") || len(details) == 0 {
		return nil
	}
	seen := map[string]bool{}
	for _, detail := range details {
		raw := ""
		if detail != nil {
			raw = str(detail.Code)
		}
		seen[activityDiagnosticCode(raw)] = true
	}
	out := make([]string, 0, len(seen))
	for code := range seen {
		out = append(out, code)
	}
	sort.Strings(out)
	return out
}

// ValidFailureCodes protects journal/audit readers as well as API observations.
// Old records without diagnostics remain valid; unknown raw values, duplicates
// and noncanonical ordering are refused instead of leaking through logging.
func (a Activity) ValidFailureCodes() bool {
	if len(a.FailureCodes) == 0 {
		return true
	}
	if len(a.FailureCodes) > 64 || !oneOf(a.Status, "FAILED", "PARTIALLY_SUCCESSFUL", "CANCELLED") {
		return false
	}
	previous := ""
	for _, code := range a.FailureCodes {
		if code <= previous || activityDiagnosticCode(code) != code {
			return false
		}
		previous = code
	}
	return true
}
