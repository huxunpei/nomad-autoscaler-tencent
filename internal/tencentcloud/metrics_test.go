package tencentcloud

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/huxunpei/nomad-autoscaler-tencent/internal/telemetry"
)

func metricText(c *telemetry.Collector) string {
	w := httptest.NewRecorder()
	c.ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
	return w.Body.String()
}
func requireMetric(t *testing.T, c *telemetry.Collector, name, action string, count int) {
	t.Helper()
	if !strings.Contains(metricText(c), fmt.Sprintf("tencent_as_%s{action=%q} %d\n", name, action, count)) {
		t.Fatalf("wrong %s for %s", name, action)
	}
}

func TestMetricsActualSDKCountsAttemptsNotLogicalCalls(t *testing.T) {
	for _, action := range []string{"DescribeAutoScalingGroups", "DescribeAutoScalingInstances", "DescribeLaunchConfigurations", "DescribeScalingPolicies", "DescribeInstances", "DescribeAutoScalingActivities", "ModifyDesiredCapacity", "ScaleOutInstances", "RemoveInstances"} {
		t.Run(action, func(t *testing.T) {
			t.Parallel()
			metrics := telemetry.New()
			calls := 0
			c, err := newClient(Config{Region: "ap-chengdu", SecretID: "synthetic-id", SecretKey: "synthetic-key", Timeout: 4 * time.Second, Metrics: metrics}, authHTTPFunc(func(r *http.Request) (*http.Response, error) {
				a := r.Header.Get("X-TC-Action")
				if a == action {
					calls++
					if calls == 1 {
						return authResponse(r, 200, rateBody("RequestLimitExceeded", requestID)), nil
					}
				}
				return authResponse(r, 200, rateSuccess(a)), nil
			}))
			if err != nil {
				t.Fatal(err)
			}
			if _, err = rateInvoke(t, context.Background(), c, action, false); err != nil {
				t.Fatal(err)
			}
			requireMetric(t, metrics, "api_attempts_total", action, 2)
			requireMetric(t, metrics, "api_errors_total", action, 1)
			requireMetric(t, metrics, "api_throttles_total", action, 1)
			if strings.Contains(metricText(metrics), "synthetic") || strings.Contains(metricText(metrics), requestID) {
				t.Fatal("identity or credentials exposed")
			}
		})
	}
}

func TestMetricsWriteUnknownAndInvalidInput(t *testing.T) {
	m := telemetry.New()
	calls := 0
	c, err := newClient(Config{Region: "ap-chengdu", SecretID: "synthetic-id", SecretKey: "synthetic-key", Timeout: 4 * time.Second, Metrics: m}, authHTTPFunc(func(r *http.Request) (*http.Response, error) {
		calls++
		if calls == 1 {
			return authResponse(r, 200, rateBody("RequestLimitExceeded", requestID)), nil
		}
		return nil, fmt.Errorf("PRIVATE_ERROR")
	}))
	if err != nil {
		t.Fatal(err)
	}
	if out, err := c.RemoveExact(context.Background(), "asg-test", "ins-one"); err == nil || !out.Unknown {
		t.Fatal("unknown mutation changed")
	}
	requireMetric(t, m, "api_attempts_total", "RemoveInstances", 2)
	requireMetric(t, m, "api_errors_total", "RemoveInstances", 2)
	requireMetric(t, m, "api_throttles_total", "RemoveInstances", 1)
	if _, err = c.RemoveExact(context.Background(), "INVALID", "ins-one"); err == nil {
		t.Fatal("invalid group accepted")
	}
	requireMetric(t, m, "api_attempts_total", "RemoveInstances", 2)
}

func TestMetricsAuthenticationMetadataAndSTS(t *testing.T) {
	m := telemetry.New()
	roleCalls, credentialCalls, stsCalls := 0, 0, 0
	metadata := &http.Client{Transport: authHTTPFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.String() == metadataRoleURL {
			roleCalls++
			if roleCalls == 1 {
				return authResponse(r, 429, secretMarker), nil
			}
			return authResponse(r, 200, "existing-role"), nil
		}
		credentialCalls++
		if credentialCalls == 1 {
			return authResponse(r, 429, secretMarker), nil
		}
		return authResponse(r, 200, fmt.Sprintf(`{"TmpSecretId":"synthetic-id","TmpSecretKey":"synthetic-key","Token":"synthetic-token","ExpiredTime":%d,"Code":"Success"}`, time.Now().Add(time.Hour).Unix())), nil
	})}
	tr := authHTTPFunc(func(r *http.Request) (*http.Response, error) {
		stsCalls++
		if stsCalls == 1 {
			return authResponse(r, 200, rateBody("RequestLimitExceeded", requestID)), nil
		}
		return authResponse(r, 200, identityBody("10001")), nil
	})
	c, err := newAuthenticated(context.Background(), AuthConfig{Region: "ap-chengdu", Timeout: 5 * time.Second, Metrics: m}, authDependencies{metadataHTTP: metadata, apiTransport: tr, getenv: func(string) string { t.Fatal("unexpected environment fallback"); return "" }})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	for _, a := range []string{"InstanceMetadataRole", "InstanceMetadataCredentials", "GetCallerIdentity"} {
		requireMetric(t, m, "api_attempts_total", a, 2)
		requireMetric(t, m, "api_errors_total", a, 1)
		requireMetric(t, m, "api_throttles_total", a, 1)
	}
}
