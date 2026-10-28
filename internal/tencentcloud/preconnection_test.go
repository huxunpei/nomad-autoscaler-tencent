package tencentcloud

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	as "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/as/v20180419"
)

func localMutationClient(t *testing.T, endpoint string) *Client {
	t.Helper()
	c, err := New(Config{Region: "ap-test", SecretID: "synthetic-id", SecretKey: "synthetic-key", Timeout: 200 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	p := clientProfile(200 * time.Millisecond)
	p.HttpProfile.Scheme = strings.SplitN(endpoint, "://", 2)[0]
	p.HttpProfile.Endpoint = strings.SplitN(endpoint, "://", 2)[1]
	c.as.(*as.Client).WithProfile(p)
	return c
}

func invokeMutation(c *Client, ctx context.Context, action string) (WriteResult, error) {
	if action == "ScaleOutInstances" {
		return c.ScaleOut(ctx, "asg-test", 3)
	}
	if action == "ModifyDesiredCapacity" {
		return c.SetDesired(ctx, "asg-test", 3)
	}
	return c.RemoveExact(ctx, "asg-test", "ins-test")
}

func TestPreconnectionFailureHasLocalNonSubmissionProof(t *testing.T) {
	for _, mode := range []string{"connect-refused", "tls-rejected"} {
		for _, action := range []string{"ModifyDesiredCapacity", "ScaleOutInstances", "RemoveInstances"} {
			t.Run(mode+"/"+action, func(t *testing.T) {
				var calls atomic.Int64
				endpoint := ""
				if mode == "tls-rejected" {
					s := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						calls.Add(1)
						t.Error("HTTP request reached a server with an untrusted certificate")
					}))
					s.Config.ErrorLog = log.New(io.Discard, "", 0)
					s.StartTLS()
					defer s.Close()
					endpoint = s.URL
				} else {
					l, err := net.Listen("tcp", "127.0.0.1:0")
					if err != nil {
						t.Fatal(err)
					}
					endpoint = "http://" + l.Addr().String()
					_ = l.Close()
				}
				c := localMutationClient(t, endpoint)
				r, err := invokeMutation(c, context.Background(), action)
				if !IsNotSubmittedWrite(r, err) || r.NotSubmitted != "transport-before-connection" || !r.Attempted || r.Unknown || r.RequestID != "" || r.ActivityID != "" || calls.Load() != 0 {
					t.Fatalf("lost reliable preconnection proof: result=%+v error=%v calls=%d", r, err, calls.Load())
				}
			})
		}
	}
}

func TestClientGateCanceledBeforeMutationIsNotUnknown(t *testing.T) {
	for _, action := range []string{"ModifyDesiredCapacity", "ScaleOutInstances", "RemoveInstances"} {
		t.Run(action, func(t *testing.T) {
			c, a, _ := fixture(t)
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			r, err := invokeMutation(c, ctx, action)
			if !IsNotSubmittedWrite(r, err) || r.NotSubmitted != "client-gate" || r.Attempted || r.Unknown || a.desiredCalls != 0 || a.removeCalls != 0 {
				t.Fatalf("canceled local gate lost proof: %+v %v", r, err)
			}
		})
	}
}

func TestPreconnectionProofNeverCoversLostResponse(t *testing.T) {
	for _, action := range []string{"ModifyDesiredCapacity", "ScaleOutInstances", "RemoveInstances"} {
		t.Run(action, func(t *testing.T) {
			var calls atomic.Int64
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				calls.Add(1)
				conn, _, err := w.(http.Hijacker).Hijack()
				if err != nil {
					t.Error(err)
					return
				}
				_ = conn.Close()
			}))
			defer s.Close()
			r, err := invokeMutation(localMutationClient(t, s.URL), context.Background(), action)
			if err == nil || !r.Unknown || IsNotSubmittedWrite(r, err) || r.NotSubmitted != "" || calls.Load() != 1 {
				t.Fatalf("sent request classified as unsent or replayed: %+v %v calls=%d", r, err, calls.Load())
			}
		})
	}
}

func TestPreconnectionProofSurvivesAuthBoundaryButNotOpaqueTransport(t *testing.T) {
	for _, opaque := range []bool{false, true} {
		for _, action := range []string{"ModifyDesiredCapacity", "ScaleOutInstances", "RemoveInstances"} {
			var calls atomic.Int64
			dnsFailure := func() error {
				calls.Add(1)
				return &net.DNSError{Err: secretMarker, Name: "synthetic.invalid", IsNotFound: true}
			}
			native := newSubmissionTransport()
			native.base.Proxy = nil
			native.base.DialContext = func(context.Context, string, string) (net.Conn, error) { return nil, dnsFailure() }
			var tr http.RoundTripper = native
			if opaque {
				tr = authHTTPFunc(func(*http.Request) (*http.Response, error) { return nil, dnsFailure() })
			}
			c, err := newClient(Config{Region: "ap-test", SecretID: "synthetic-id", SecretKey: "synthetic-key", Timeout: time.Second},
				&authBoundTransport{base: tr, hosts: map[string]bool{"as.tencentcloudapi.com": true}, limit: 1 << 20})
			if err != nil {
				t.Fatal(err)
			}
			r, err := invokeMutation(c, context.Background(), action)
			native.CloseIdleConnections()
			if err == nil || IsNotSubmittedWrite(r, err) == opaque || r.Unknown != opaque || calls.Load() != 1 || strings.Contains(err.Error(), secretMarker) {
				t.Fatalf("opaque=%t action=%s proof=%+v err=%v calls=%d", opaque, action, r, err, calls.Load())
			}
		}
	}
}

type partialMutationConn struct {
	net.Conn
	writes *atomic.Int64
}

func (c *partialMutationConn) Write(p []byte) (int, error) {
	c.writes.Add(1)
	if len(p) > 1 {
		p = p[:1]
	}
	n, _ := c.Conn.Write(p)
	return n, errors.New("synthetic-partial-write")
}

func TestPreconnectionProofNeverCoversPartialRequest(t *testing.T) {
	var writes atomic.Int64
	s := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Error("partial request was unexpectedly complete")
	}))
	defer s.Close()
	native := newSubmissionTransport()
	defer native.CloseIdleConnections()
	native.base.Proxy = nil
	native.base.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, err := (&net.Dialer{}).DialContext(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		return &partialMutationConn{Conn: conn, writes: &writes}, nil
	}
	c, err := newClient(Config{Region: "ap-test", SecretID: "synthetic-id", SecretKey: "synthetic-key", Timeout: time.Second}, native)
	if err != nil {
		t.Fatal(err)
	}
	p := clientProfile(time.Second)
	p.HttpProfile.Scheme, p.HttpProfile.Endpoint = "http", strings.TrimPrefix(s.URL, "http://")
	c.as.(*as.Client).WithProfile(p)
	for _, action := range []string{"ModifyDesiredCapacity", "ScaleOutInstances", "RemoveInstances"} {
		before := writes.Load()
		r, err := invokeMutation(c, context.Background(), action)
		if err == nil || !r.Unknown || IsNotSubmittedWrite(r, err) || writes.Load()-before != 1 {
			t.Fatalf("partial write was released or replayed: %+v %v writes=%d", r, err, writes.Load()-before)
		}
	}
}

func TestPreconnectionProofRejectsContradictions(t *testing.T) {
	for _, name := range []string{"request", "activity", "unknown", "not-attempted", "error-action", "error-request", "error-unknown", "error-text", "nil-error", "wrong-source"} {
		t.Run(name, func(t *testing.T) {
			r := WriteResult{Action: "RemoveInstances", Attempted: true, NotSubmitted: "transport-before-connection"}
			e := &Error{Action: r.Action, Code: "LocalTransportNotSubmitted"}
			var err error = e
			switch name {
			case "request":
				r.RequestID = requestID
			case "activity":
				r.ActivityID = "asa-test"
			case "unknown":
				r.Unknown = true
			case "not-attempted":
				r.Attempted = false
			case "error-action":
				e.Action = "ModifyDesiredCapacity"
			case "error-request":
				e.RequestID = requestID
			case "error-unknown":
				e.Unknown = true
			case "error-text":
				err = errors.New("LocalTransportNotSubmitted")
			case "nil-error":
				err = nil
			case "wrong-source":
				r.NotSubmitted = "credential-gate"
			}
			if IsNotSubmittedWrite(r, err) {
				t.Fatal("contradictory source accepted")
			}
		})
	}
}

func TestPreconnectionProofNeverCoversReusedHTTP2Reset(t *testing.T) {
	var calls atomic.Int64
	s := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ProtoMajor != 2 {
			t.Error("test did not negotiate HTTP/2")
		}
		_, _ = io.Copy(io.Discard, r.Body)
		calls.Add(1)
		if r.Header.Get("X-TC-Action") != "" {
			panic(http.ErrAbortHandler)
		}
		w.WriteHeader(http.StatusOK)
	}))
	s.EnableHTTP2 = true
	s.StartTLS()
	defer s.Close()
	native := newSubmissionTransport()
	defer native.CloseIdleConnections()
	pool := x509.NewCertPool()
	pool.AddCert(s.Certificate())
	native.base.TLSClientConfig = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	native.base.Proxy = nil
	httpClient := &http.Client{Transport: native, Timeout: time.Second}
	resp, err := httpClient.Get(s.URL)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	c, err := newClient(Config{Region: "ap-test", SecretID: "synthetic-id", SecretKey: "synthetic-key", Timeout: time.Second}, native)
	if err != nil {
		t.Fatal(err)
	}
	p := clientProfile(time.Second)
	p.HttpProfile.Scheme, p.HttpProfile.Endpoint = "https", strings.TrimPrefix(s.URL, "https://")
	c.as.(*as.Client).WithProfile(p)
	for _, action := range []string{"ModifyDesiredCapacity", "ScaleOutInstances", "RemoveInstances"} {
		var reused atomic.Bool
		ctx := httptrace.WithClientTrace(context.Background(), &httptrace.ClientTrace{
			GotConn: func(info httptrace.GotConnInfo) { reused.Store(info.Reused) },
		})
		before := calls.Load()
		r, err := invokeMutation(c, ctx, action)
		if err == nil || !r.Unknown || IsNotSubmittedWrite(r, err) || !reused.Load() || calls.Load()-before != 1 {
			t.Fatalf("HTTP/2 reset was released or replayed: %+v %v reused=%t calls=%d", r, err, reused.Load(), calls.Load()-before)
		}
	}
}
