package tencentcloud

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/telemetry"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"
	"unicode"
)

const credentialLimit = 64 << 10
const metadataRoleURL = "http://metadata.tencentyun.com/latest/meta-data/cam/security-credentials/"

type credentialValues struct {
	id, key, token string
	expires        int64
}

func (v credentialValues) String() string   { return "[redacted credentials]" }
func (v credentialValues) GoString() string { return v.String() }
func (v credentialValues) validate(now time.Time) error {
	for _, value := range []string{v.id, v.key} {
		if len(value) == 0 || len(value) > 1024 || strings.IndexFunc(value, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) >= 0 {
			return bad("Authenticate", "InvalidCredentialMaterial")
		}
	}
	if len(v.token) > 16384 || strings.IndexFunc(v.token, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) >= 0 || v.expires < 0 {
		return bad("Authenticate", "InvalidCredentialMaterial")
	}
	if v.expires != 0 && (v.token == "" || v.expires <= now.Add(time.Minute).Unix()) {
		return bad("Authenticate", "CredentialExpiredOrExpiring")
	}
	return nil
}

type credentialSource interface {
	read(context.Context) (credentialValues, error)
	cacheable() bool
	close()
}

func newCredentialSource(cfg AuthConfig, deps authDependencies) (credentialSource, error) {
	if cfg.Method == "instance_identity" {
		var h http.Client
		if deps.metadataHTTP != nil {
			h = *deps.metadataHTTP
		} else {
			h.Transport = authTransport(false)
		}
		h.Timeout = cfg.Timeout
		h.CheckRedirect = func(*http.Request, []*http.Request) error { return errors.New("MetadataRedirectDenied") }
		return &instanceCredentials{http: &h, metrics: cfg.Metrics}, nil
	}
	if cfg.CredentialsFile != "" {
		if !filepath.IsAbs(cfg.CredentialsFile) || strings.ContainsAny(cfg.CredentialsFile, "\x00\r\n") {
			return nil, bad("Authenticate", "InvalidCredentialFile")
		}
		return &fileCredentials{path: cfg.CredentialsFile}, nil
	}
	// Snapshot only the explicitly selected environment source. No profile search,
	// ambient SDK provider chain, AssumeRole call or fallback to another source.
	return &environmentCredentials{values: credentialValues{id: deps.getenv("TENCENTCLOUD_SECRET_ID"), key: deps.getenv("TENCENTCLOUD_SECRET_KEY"), token: deps.getenv("TENCENTCLOUD_TOKEN")}}, nil
}

type environmentCredentials struct{ values credentialValues }

func (s *environmentCredentials) read(ctx context.Context) (credentialValues, error) {
	if ctx.Err() != nil {
		return credentialValues{}, bad("Authenticate", "CredentialRequestCanceled")
	}
	return s.values, nil
}
func (*environmentCredentials) cacheable() bool { return true }
func (s *environmentCredentials) close()        { s.values = credentialValues{} }

type fileCredentials struct{ path string }

func (*fileCredentials) cacheable() bool { return false }
func (*fileCredentials) close()          {}
func (s *fileCredentials) read(ctx context.Context) (credentialValues, error) {
	if ctx.Err() != nil {
		return credentialValues{}, bad("Authenticate", "CredentialRequestCanceled")
	}
	// A named pipe must not turn credential retrieval into an unbounded read.
	// Do not follow a substituted final symlink while opening the configured file.
	f, err := os.OpenFile(s.path, os.O_RDONLY|syscall.O_NONBLOCK|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return credentialValues{}, bad("Authenticate", "CredentialFileUnavailable")
	}
	defer f.Close()
	stat, err := f.Stat()
	if err != nil || !stat.Mode().IsRegular() || stat.Mode().Perm()&0077 != 0 || stat.Size() > credentialLimit {
		return credentialValues{}, bad("Authenticate", "UnsafeCredentialFile")
	}
	data, err := io.ReadAll(io.LimitReader(f, credentialLimit+1))
	if err != nil || len(data) > credentialLimit {
		return credentialValues{}, bad("Authenticate", "CredentialFileUnreadable")
	}
	defer clear(data)
	var fields struct {
		ID      string `json:"secret_id"`
		Key     string `json:"secret_key"`
		Token   string `json:"token"`
		Expires int64  `json:"expired_time"`
	}
	if decodeCredentialObject(data, &fields, true) != nil {
		return credentialValues{}, bad("Authenticate", "InvalidCredentialFile")
	}
	return credentialValues{id: fields.ID, key: fields.Key, token: fields.Token, expires: fields.Expires}, nil
}

type instanceCredentials struct {
	http    *http.Client
	metrics *telemetry.Collector
}

func (*instanceCredentials) cacheable() bool { return true }
func (s *instanceCredentials) close()        { s.http.CloseIdleConnections() }

var errMetadataRateLimited = errors.New("MetadataRateLimited")

func (s *instanceCredentials) get(ctx context.Context, endpoint string) ([]byte, error) {
	data, err := retryRateLimited(ctx, s.http.Timeout, func(ctx context.Context) ([]byte, error) {
		data, err := s.getAttempt(ctx, endpoint)
		action := "InstanceMetadataCredentials"
		if endpoint == metadataRoleURL {
			action = "InstanceMetadataRole"
		}
		s.metrics.API(action, err != nil, errors.Is(err, errMetadataRateLimited))
		return data, err
	}, func(_ []byte, err error) bool { return errors.Is(err, errMetadataRateLimited) })
	if err != nil {
		// Neither metadata response bodies nor arbitrary transport errors escape.
		var safe *Error
		if errors.As(err, &safe) && safe != nil {
			return nil, safe
		}
		return nil, bad("Authenticate", "InstanceIdentityUnavailable")
	}
	return data, nil
}

func (s *instanceCredentials) getAttempt(ctx context.Context, endpoint string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, bad("Authenticate", "InvalidMetadataRequest")
	}
	resp, err := s.http.Do(req)
	if err != nil {
		return nil, bad("Authenticate", "InstanceIdentityUnavailable")
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusTooManyRequests {
		return nil, errMetadataRateLimited
	}
	if resp.StatusCode != http.StatusOK {
		return nil, bad("Authenticate", "InstanceIdentityUnavailable")
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, credentialLimit+1))
	if err != nil || len(data) > credentialLimit {
		clear(data)
		return nil, bad("Authenticate", "InvalidMetadataResponse")
	}
	return data, nil
}
func (s *instanceCredentials) read(ctx context.Context) (credentialValues, error) {
	data, err := s.get(ctx, metadataRoleURL)
	if err != nil {
		return credentialValues{}, err
	}
	role := strings.TrimSpace(string(data))
	clear(data)
	if role == "" || len(role) > 1024 || role == "." || role == ".." || strings.ContainsAny(role, "/\\\r\n\x00") || strings.IndexFunc(role, unicode.IsControl) >= 0 {
		return credentialValues{}, bad("Authenticate", "InvalidInstanceRole")
	}
	data, err = s.get(ctx, metadataRoleURL+url.PathEscape(role))
	if err != nil {
		return credentialValues{}, err
	}
	defer clear(data)
	var fields struct {
		ID         string `json:"TmpSecretId"`
		Key        string `json:"TmpSecretKey"`
		Token      string `json:"Token"`
		Expires    int64  `json:"ExpiredTime"`
		Expiration string `json:"Expiration"`
		Code       string `json:"Code"`
	}
	if decodeCredentialObject(data, &fields, false) != nil || fields.Code != "Success" || fields.Token == "" || fields.Expires <= 0 {
		return credentialValues{}, bad("Authenticate", "InvalidInstanceCredential")
	}
	if fields.Expiration != "" {
		expiry, e := time.Parse(time.RFC3339, fields.Expiration)
		if e != nil || expiry.Unix() != fields.Expires {
			return credentialValues{}, bad("Authenticate", "InvalidInstanceExpiration")
		}
	}
	return credentialValues{id: fields.ID, key: fields.Key, token: fields.Token, expires: fields.Expires}, nil
}

// Reject duplicate credential keys and trailing documents rather than letting
// a second field silently choose the identity. Diagnostics never include data.
func decodeCredentialObject(data []byte, out any, strict bool) error {
	d := json.NewDecoder(bytes.NewReader(data))
	start, err := d.Token()
	if err != nil || start != json.Delim('{') {
		return errors.New("InvalidCredentialJSON")
	}
	seen := map[string]bool{}
	for d.More() {
		token, err := d.Token()
		key, ok := token.(string)
		key = strings.ToLower(key)
		if err != nil || !ok || seen[key] {
			return errors.New("InvalidCredentialJSON")
		}
		seen[key] = true
		var raw json.RawMessage
		if d.Decode(&raw) != nil {
			return errors.New("InvalidCredentialJSON")
		}
	}
	if _, err = d.Token(); err != nil {
		return errors.New("InvalidCredentialJSON")
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return errors.New("InvalidCredentialJSON")
	}
	d = json.NewDecoder(bytes.NewReader(data))
	if strict {
		d.DisallowUnknownFields()
	}
	if d.Decode(out) != nil {
		return errors.New("InvalidCredentialJSON")
	}
	return nil
}
