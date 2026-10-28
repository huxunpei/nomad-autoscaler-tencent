package tencentcloud

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// These are deliberately synthetic credentials. All requests are intercepted;
// no metadata host, Tencent endpoint, or ambient credential is used by tests.
const authRequestID = "11111111-2222-4333-8444-555555555555"

type authHTTPFunc func(*http.Request) (*http.Response, error)

func (f authHTTPFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func authResponse(r *http.Request, status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body)), Request: r}
}
func authEnvironment(k string) string {
	return map[string]string{"TENCENTCLOUD_SECRET_ID": "synthetic-id-a", "TENCENTCLOUD_SECRET_KEY": "synthetic-key-a", "TENCENTCLOUD_TOKEN": "synthetic-token-a"}[k]
}
func authFile(t *testing.T, path, id string) {
	t.Helper()
	b, _ := json.Marshal(map[string]string{"secret_id": id, "secret_key": "synthetic-file-key", "token": "synthetic-file-token"})
	if err := os.WriteFile(path, b, 0600); err != nil {
		t.Fatal(err)
	}
}
func identityBody(account string) string {
	return fmt.Sprintf(`{"Response":{"AccountId":%q,"UserId":"10001","PrincipalId":"10001","Type":"CAMUser","RequestId":%q}}`, account, authRequestID)
}

func TestAuthenticatedAPIKeyChecksIdentityBeforeAnyCloudWrite(t *testing.T) {
	var actions []string
	transport := authHTTPFunc(func(r *http.Request) (*http.Response, error) {
		actions = append(actions, r.Header.Get("X-TC-Action"))
		if r.URL.Scheme != "https" || r.Header.Get("X-TC-Region") != "ap-guangzhou" || !strings.Contains(r.Header.Get("Authorization"), "Credential=synthetic-id-a/") || r.Header.Get("X-TC-Token") != "synthetic-token-a" {
			t.Error("request did not use the selected credential and region")
		}
		switch r.Header.Get("X-TC-Action") {
		case "GetCallerIdentity":
			if r.URL.Host != "sts.tencentcloudapi.com" {
				t.Error("wrong identity host")
			}
			return authResponse(r, 200, identityBody("10001")), nil
		case "ModifyDesiredCapacity":
			return authResponse(r, 200, `{"Response":{"RequestId":"`+authRequestID+`"}}`), nil
		default:
			t.Error("unexpected action")
			return nil, errors.New("unexpected action")
		}
	})
	c, err := newAuthenticated(context.Background(), AuthConfig{Region: "ap-guangzhou", Method: "api_key"}, authDependencies{getenv: authEnvironment, apiTransport: transport})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if c.AccountID() != "10001" || len(actions) != 1 || actions[0] != "GetCallerIdentity" {
		t.Fatal("startup did not validate canonical account")
	}
	result, err := c.SetDesired(context.Background(), "asg-test", 1)
	if err != nil || !result.Attempted || result.Unknown || len(actions) != 2 {
		t.Fatalf("signed cloud request failed: %v", err)
	}
	c.Close()
	result, err = c.SetDesired(context.Background(), "asg-test", 1)
	if err == nil || result.Attempted || len(actions) != 2 {
		t.Fatal("closed credentials allowed a write")
	}
}

func TestAuthenticatedDefaultIdentityNeverFallsBackToEnvironment(t *testing.T) {
	var environmentReads, cloudCalls int
	metadata := &http.Client{Transport: authHTTPFunc(func(r *http.Request) (*http.Response, error) {
		return authResponse(r, 404, "synthetic-secret-body"), nil
	})}
	c, err := newAuthenticated(context.Background(), AuthConfig{Region: "ap-guangzhou"}, authDependencies{getenv: func(k string) string { environmentReads++; return authEnvironment(k) }, metadataHTTP: metadata, apiTransport: authHTTPFunc(func(*http.Request) (*http.Response, error) { cloudCalls++; return nil, errors.New("unexpected") })})
	if c != nil || err == nil || environmentReads != 0 || cloudCalls != 0 || strings.Contains(err.Error(), "synthetic") {
		t.Fatal("default identity leaked data or fell back to another credential")
	}
}

func TestAuthenticatedCredentialRejectionAndTimeoutFailStartup(t *testing.T) {
	for _, mode := range []string{"rejected", "malformed", "timeout", "redirect", "oversize"} {
		t.Run(mode, func(t *testing.T) {
			var calls atomic.Int32
			tr := authHTTPFunc(func(r *http.Request) (*http.Response, error) {
				calls.Add(1)
				switch mode {
				case "rejected":
					return authResponse(r, 200, `{"Response":{"Error":{"Code":"AuthFailure.SignatureFailure","Message":"synthetic-secret-body"},"RequestId":"`+authRequestID+`"}}`), nil
				case "malformed":
					return authResponse(r, 200, identityBody("../other-account")), nil
				case "timeout":
					<-r.Context().Done()
					return nil, r.Context().Err()
				case "redirect":
					resp := authResponse(r, 302, "")
					resp.Header.Set("Location", "https://attacker.invalid/")
					return resp, nil
				default:
					return authResponse(r, 200, strings.Repeat("x", 2<<20)), nil
				}
			})
			start := time.Now()
			c, err := newAuthenticated(context.Background(), AuthConfig{Region: "ap-guangzhou", Method: "api_key", Timeout: 25 * time.Millisecond}, authDependencies{getenv: authEnvironment, apiTransport: tr})
			if c != nil || err == nil || calls.Load() != 1 || time.Since(start) > time.Second || strings.Contains(err.Error(), "synthetic") {
				t.Fatalf("unsafe startup behavior: %v", err)
			}
		})
	}
}

func TestAuthenticatedFileRotationCannotSwitchAccount(t *testing.T) {
	path := filepath.Join(t.TempDir(), "credential.json")
	authFile(t, path, "synthetic-file-a")
	var identities, writes int
	account := "10001"
	tr := authHTTPFunc(func(r *http.Request) (*http.Response, error) {
		if r.Header.Get("X-TC-Action") == "GetCallerIdentity" {
			identities++
			return authResponse(r, 200, identityBody(account)), nil
		}
		writes++
		return authResponse(r, 200, `{"Response":{"RequestId":"`+authRequestID+`"}}`), nil
	})
	c, err := newAuthenticated(context.Background(), AuthConfig{Region: "ap-guangzhou", Method: "api_key", CredentialsFile: path}, authDependencies{getenv: func(string) string { t.Error("file auth read env"); return "" }, apiTransport: tr})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	authFile(t, path, "synthetic-file-b")
	account = "20002"
	result, err := c.SetDesired(context.Background(), "asg-test", 1)
	if err == nil || result.Attempted || writes != 0 || c.AccountID() != "10001" {
		t.Fatal("rotation switched account or submitted a cloud write")
	}
	account = "10001"
	result, err = c.SetDesired(context.Background(), "asg-test", 1)
	if err != nil || !result.Attempted || writes != 1 || identities != 3 {
		t.Fatalf("same-account recovery failed: %v", err)
	}
	// A broken file must not silently retain an older key or select env credentials.
	if err := os.WriteFile(path, []byte(`{"secret_id":"synthetic-secret-body"}`), 0600); err != nil {
		t.Fatal(err)
	}
	result, err = c.SetDesired(context.Background(), "asg-test", 1)
	if err == nil || result.Attempted || writes != 1 || strings.Contains(err.Error(), "synthetic") {
		t.Fatal("broken file allowed a fallback write")
	}
}

func TestAuthenticatedMetadataRenewalFailureAndRecovery(t *testing.T) {
	var clock atomic.Int64
	clock.Store(time.Date(2026, 10, 18, 0, 0, 0, 0, time.UTC).Unix())
	var mu sync.Mutex
	revision := 1
	fail := false
	reads := 0
	writes := 0
	identities := 0
	metadata := &http.Client{Transport: authHTTPFunc(func(r *http.Request) (*http.Response, error) {
		mu.Lock()
		defer mu.Unlock()
		reads++
		if r.URL.Host != "metadata.tencentyun.com" {
			t.Error("wrong metadata host")
		}
		if r.URL.Path == "/latest/meta-data/cam/security-credentials/" {
			return authResponse(r, 200, "SyntheticRole\n"), nil
		}
		if r.URL.Path != "/latest/meta-data/cam/security-credentials/SyntheticRole" {
			t.Error("wrong role path")
		}
		if fail {
			return authResponse(r, 500, "synthetic-secret-body"), nil
		}
		return authResponse(r, 200, fmt.Sprintf(`{"Code":"Success","TmpSecretId":"synthetic-id-%d","TmpSecretKey":"synthetic-key","Token":"synthetic-token","ExpiredTime":%d}`, revision, clock.Load()+3600)), nil
	})}
	api := authHTTPFunc(func(r *http.Request) (*http.Response, error) {
		mu.Lock()
		defer mu.Unlock()
		if r.Header.Get("X-TC-Action") == "GetCallerIdentity" {
			identities++
			return authResponse(r, 200, identityBody("10001")), nil
		}
		if !strings.Contains(r.Header.Get("Authorization"), fmt.Sprintf("Credential=synthetic-id-%d/", revision)) {
			t.Error("cloud request used stale credentials")
		}
		writes++
		return authResponse(r, 200, `{"Response":{"RequestId":"`+authRequestID+`"}}`), nil
	})
	c, err := newAuthenticated(context.Background(), AuthConfig{Region: "ap-guangzhou"}, authDependencies{getenv: func(string) string { t.Error("identity used env"); return "" }, metadataHTTP: metadata, apiTransport: api, now: func() time.Time { return time.Unix(clock.Load(), 0) }})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err = c.SetDesired(context.Background(), "asg-test", 1); err != nil {
		t.Fatal(err)
	}
	if reads != 2 || identities != 1 {
		t.Fatal("unnecessary refresh before expiry window")
	}
	clock.Add(3400)
	mu.Lock()
	fail = true
	mu.Unlock()
	result, err := c.SetDesired(context.Background(), "asg-test", 1)
	if err == nil || result.Attempted || writes != 1 {
		t.Fatal("failed renewal continued writing")
	}
	mu.Lock()
	fail = false
	revision = 2
	mu.Unlock()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, e := c.SetDesired(context.Background(), "asg-test", 1); e != nil {
				t.Error(e)
			}
		}()
	}
	wg.Wait()
	if identities != 2 || writes != 9 || c.AccountID() != "10001" {
		t.Fatal("renewal was not atomic or did not recover")
	}
}

func TestCredentialFileSecurityAndMalformedContent(t *testing.T) {
	for _, mode := range []string{"permissions", "unknown", "duplicate", "case-duplicate", "trailing", "directory", "expired", "blank", "symlink", "fifo"} {
		t.Run(mode, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "credential.json")
			authFile(t, path, "synthetic-file-a")
			switch mode {
			case "permissions":
				if e := os.Chmod(path, 0644); e != nil {
					t.Fatal(e)
				}
			case "directory":
				path = t.TempDir()
			case "symlink":
				link := filepath.Join(t.TempDir(), "link.json")
				if err := os.Symlink(path, link); err != nil {
					t.Fatal(err)
				}
				path = link
			case "fifo":
				path = filepath.Join(t.TempDir(), "pipe")
				if err := syscall.Mkfifo(path, 0600); err != nil {
					t.Fatal(err)
				}
			case "unknown":
				_ = os.WriteFile(path, []byte(`{"secret_id":"a","secret_key":"b","other":"synthetic-secret-body"}`), 0600)
			case "duplicate":
				_ = os.WriteFile(path, []byte(`{"secret_id":"a","secret_id":"b","secret_key":"c"}`), 0600)
			case "case-duplicate":
				_ = os.WriteFile(path, []byte(`{"secret_id":"a","SECRET_ID":"b","secret_key":"c"}`), 0600)
			case "trailing":
				_ = os.WriteFile(path, []byte(`{"secret_id":"a","secret_key":"b"} {}`), 0600)
			case "expired":
				_ = os.WriteFile(path, []byte(`{"secret_id":"a","secret_key":"b","token":"t","expired_time":1}`), 0600)
			case "blank":
				_ = os.WriteFile(path, []byte(`{"secret_id":" ","secret_key":"b"}`), 0600)
			}
			calls := 0
			c, err := newAuthenticated(context.Background(), AuthConfig{Region: "ap-guangzhou", Method: "api_key", CredentialsFile: path}, authDependencies{apiTransport: authHTTPFunc(func(*http.Request) (*http.Response, error) { calls++; return nil, errors.New("unexpected") })})
			if c != nil || err == nil || calls != 0 || strings.Contains(err.Error(), "synthetic") {
				t.Fatalf("invalid file reached cloud: %v", err)
			}
		})
	}
}

func TestAuthenticatedCloseCancelsRefreshAndQueuedCall(t *testing.T) {
	path := filepath.Join(t.TempDir(), "credential.json")
	authFile(t, path, "synthetic-file-a")
	var identities, writes atomic.Int32
	entered := make(chan struct{})
	tr := authHTTPFunc(func(r *http.Request) (*http.Response, error) {
		if r.Header.Get("X-TC-Action") != "GetCallerIdentity" {
			writes.Add(1)
			return nil, errors.New("unexpected write")
		}
		if identities.Add(1) == 1 {
			return authResponse(r, 200, identityBody("10001")), nil
		}
		close(entered)
		<-r.Context().Done()
		return nil, r.Context().Err()
	})
	c, err := newAuthenticated(context.Background(), AuthConfig{Region: "ap-guangzhou", Method: "api_key", CredentialsFile: path}, authDependencies{apiTransport: tr})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	authFile(t, path, "synthetic-file-b")
	first := make(chan error, 1)
	go func() {
		res, e := c.SetDesired(context.Background(), "asg-test", 1)
		if res.Attempted {
			t.Error("refresh submitted a write")
		}
		first <- e
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("refresh did not start")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	res, err := c.RemoveExact(ctx, "asg-test", "ins-test")
	if err == nil || res.Attempted {
		t.Fatal("queued canceled request was not rejected")
	}
	closed := make(chan struct{})
	go func() { c.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("close blocked behind refresh")
	}
	select {
	case err := <-first:
		if err == nil {
			t.Fatal("canceled refresh succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("refresh was not canceled")
	}
	if writes.Load() != 0 || identities.Load() != 2 {
		t.Fatal("unexpected request replay")
	}
}

func TestAuthenticatedMetadataResponseBoundaries(t *testing.T) {
	for _, mode := range []string{"redirect", "timeout", "multiple-roles", "traversal", "expired", "missing-token", "bad-expiration", "duplicate", "oversize"} {
		t.Run(mode, func(t *testing.T) {
			var calls, cloudCalls atomic.Int32
			metadata := &http.Client{Transport: authHTTPFunc(func(r *http.Request) (*http.Response, error) {
				n := calls.Add(1)
				if mode == "timeout" {
					<-r.Context().Done()
					return nil, r.Context().Err()
				}
				if mode == "redirect" {
					resp := authResponse(r, 302, "")
					resp.Header.Set("Location", "https://attacker.invalid/")
					return resp, nil
				}
				if n == 1 {
					role := "SyntheticRole"
					if mode == "multiple-roles" {
						role = "RoleA\nRoleB"
					}
					if mode == "traversal" {
						role = "../other-role"
					}
					return authResponse(r, 200, role), nil
				}
				expiry := time.Now().Add(time.Hour).Unix()
				body := fmt.Sprintf(`{"Code":"Success","TmpSecretId":"synthetic-id","TmpSecretKey":"synthetic-key","Token":"synthetic-token","ExpiredTime":%d}`, expiry)
				switch mode {
				case "expired":
					body = strings.Replace(body, fmt.Sprint(expiry), "1", 1)
				case "missing-token":
					body = strings.Replace(body, `"Token":"synthetic-token",`, "", 1)
				case "bad-expiration":
					body = strings.TrimSuffix(body, "}") + `,"Expiration":"2000-01-01T00:00:00Z"}`
				case "duplicate":
					body = strings.TrimSuffix(body, "}") + `,"TmpSecretId":"second"}`
				case "oversize":
					body = strings.Repeat("x", credentialLimit+1)
				}
				return authResponse(r, 200, body), nil
			})}
			start := time.Now()
			c, err := newAuthenticated(context.Background(), AuthConfig{Region: "ap-guangzhou", Timeout: 25 * time.Millisecond}, authDependencies{metadataHTTP: metadata, apiTransport: authHTTPFunc(func(*http.Request) (*http.Response, error) { cloudCalls.Add(1); return nil, errors.New("unexpected") })})
			if c != nil || err == nil || cloudCalls.Load() != 0 || calls.Load() > 2 || time.Since(start) > time.Second || strings.Contains(err.Error(), "synthetic") {
				t.Fatalf("invalid metadata escaped validation: %v", err)
			}
		})
	}
}

func TestAuthenticatedIgnoresAmbientKeysUnlessAPIKeySelected(t *testing.T) {
	envReads := 0
	c, err := newAuthenticated(context.Background(), AuthConfig{Region: "ap-guangzhou", Method: "api_key"}, authDependencies{getenv: func(k string) string { envReads++; return "" }, apiTransport: authHTTPFunc(func(*http.Request) (*http.Response, error) {
		t.Error("empty credentials reached cloud")
		return nil, errors.New("unexpected")
	})})
	if c != nil || err == nil || envReads != 3 {
		t.Fatal("missing explicit environment credentials were not rejected")
	}
	for _, cfg := range []AuthConfig{{Region: "ap-guangzhou", Method: "other"}, {Region: "ap-guangzhou", CredentialsFile: "/unused.json"}, {Region: "ap-guangzhou", Method: "api_key", CredentialsFile: "relative.json"}} {
		c, err = newAuthenticated(context.Background(), cfg, authDependencies{getenv: func(string) string { t.Error("invalid source read env"); return "" }})
		if c != nil || err == nil {
			t.Fatal("invalid source selection accepted")
		}
	}
}
