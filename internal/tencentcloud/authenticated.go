package tencentcloud

import (
	"context"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/telemetry"
	"net"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// AuthConfig contains credential source locations, never credential values.
// An empty Method selects the instance's existing identity, without fallback.
type AuthConfig struct {
	Metrics                         *telemetry.Collector
	Region, Method, CredentialsFile string
	Timeout                         time.Duration
}

// Authenticated validates a canonical cloud account before exposing a client.
// Each request uses one immutable, validated credential generation. Rotation
// cannot change the account used to namespace a group's persistent state.
type Authenticated struct {
	cfg       AuthConfig
	source    credentialSource
	transport http.RoundTripper
	now       func() time.Time
	gate      chan struct{}
	values    credentialValues
	client    *Client
	accountID string
	ctx       context.Context
	cancel    context.CancelFunc
	closed    atomic.Bool
	closeOnce sync.Once
}

type authDependencies struct {
	getenv       func(string) string
	metadataHTTP *http.Client
	apiTransport http.RoundTripper
	now          func() time.Time
}

func NewAuthenticated(ctx context.Context, cfg AuthConfig) (*Authenticated, error) {
	return newAuthenticated(ctx, cfg, authDependencies{})
}

func newAuthenticated(ctx context.Context, cfg AuthConfig, deps authDependencies) (*Authenticated, error) {
	if cfg.Timeout == 0 {
		cfg.Timeout = 10 * time.Second
	}
	if cfg.Method == "" {
		cfg.Method = "instance_identity"
	}
	if !regionPattern.MatchString(cfg.Region) || cfg.Timeout < time.Millisecond || cfg.Timeout > 30*time.Second || (cfg.Method != "instance_identity" && cfg.Method != "api_key") || (cfg.Method != "api_key" && cfg.CredentialsFile != "") {
		return nil, bad("Authenticate", "InvalidAuthConfiguration")
	}
	if deps.getenv == nil {
		deps.getenv = os.Getenv
	}
	if deps.now == nil {
		deps.now = time.Now
	}
	source, err := newCredentialSource(cfg, deps)
	if err != nil {
		return nil, err
	}
	if deps.apiTransport == nil {
		deps.apiTransport = newSubmissionTransport()
	}
	life, cancel := context.WithCancel(context.Background())
	c := &Authenticated{cfg: cfg, source: source, transport: deps.apiTransport, now: deps.now, gate: make(chan struct{}, 1), ctx: life, cancel: cancel}
	c.gate <- struct{}{}
	if _, err = c.authClient(ctx); err != nil {
		c.Close()
		return nil, err
	}
	return c, nil
}

func authTransport(proxy bool) *http.Transport {
	tr := &http.Transport{DialContext: (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext, ForceAttemptHTTP2: true, MaxIdleConns: 20, IdleConnTimeout: 30 * time.Second, TLSHandshakeTimeout: 10 * time.Second, ExpectContinueTimeout: time.Second}
	if proxy {
		tr.Proxy = http.ProxyFromEnvironment
	}
	return tr
}

func (c *Authenticated) AccountID() string { return c.accountID }
func (c *Authenticated) Region() string    { return c.cfg.Region }
func (c *Authenticated) String() string    { return "AuthenticatedTencentClient" }
func (c *Authenticated) GoString() string  { return c.String() }

func (c *Authenticated) linkedContext(ctx context.Context) (context.Context, context.CancelFunc) {
	child, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(c.ctx, cancel)
	return child, func() { stop(); cancel() }
}

func (c *Authenticated) authClient(ctx context.Context) (*Client, error) {
	ctx, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
	defer cancel()
	ctx, unlink := c.linkedContext(ctx)
	defer unlink()
	select {
	case <-ctx.Done():
		return nil, bad("Authenticate", "CredentialRequestCanceled")
	case <-c.gate:
	}
	defer func() { c.gate <- struct{}{} }()
	if c.closed.Load() || ctx.Err() != nil {
		return nil, bad("Authenticate", "CredentialRequestCanceled")
	}
	if c.client != nil && c.source.cacheable() && (c.values.expires == 0 || c.values.expires > c.now().Add(5*time.Minute).Unix()) {
		return c.client, nil
	}
	values, err := c.source.read(ctx)
	if err != nil {
		return nil, err
	}
	if err = values.validate(c.now()); err != nil {
		return nil, err
	}
	if c.client != nil && values == c.values {
		return c.client, nil
	}
	account, err := verifyCaller(ctx, c.cfg, values, c.transport)
	if err != nil {
		return nil, err
	}
	if c.accountID != "" && account != c.accountID {
		return nil, bad("Authenticate", "CredentialAccountChanged")
	}
	if err = values.validate(c.now()); err != nil {
		return nil, err
	}
	client, err := newClient(Config{Metrics: c.cfg.Metrics, Region: c.cfg.Region, SecretID: values.id, SecretKey: values.key, Token: values.token, Timeout: c.cfg.Timeout}, &authBoundTransport{base: c.transport, hosts: map[string]bool{"as.tencentcloudapi.com": true, "cvm.tencentcloudapi.com": true}, limit: 8 << 20})
	if err != nil {
		return nil, err
	}
	if c.accountID == "" {
		c.accountID = account
	}
	c.values, c.client = values, client
	return client, nil
}

func (c *Authenticated) Close() {
	c.closeOnce.Do(func() {
		c.closed.Store(true)
		c.cancel()
		<-c.gate
		c.values = credentialValues{}
		c.client = nil
		c.source.close()
		if transport, ok := c.transport.(interface{ CloseIdleConnections() }); ok {
			transport.CloseIdleConnections()
		}
		c.gate <- struct{}{}
	})
}

func (c *Authenticated) Observe(ctx context.Context, group, tracked string) (Snapshot, error) {
	ctx, cancel := c.linkedContext(ctx)
	defer cancel()
	client, err := c.authClient(ctx)
	if err != nil {
		return Snapshot{}, err
	}
	return client.Observe(ctx, group, tracked)
}
func (c *Authenticated) Activity(ctx context.Context, group, activity string) (Activity, error) {
	ctx, cancel := c.linkedContext(ctx)
	defer cancel()
	client, err := c.authClient(ctx)
	if err != nil {
		return Activity{}, err
	}
	return client.Activity(ctx, group, activity)
}
func (c *Authenticated) SetDesired(ctx context.Context, group string, count int64) (WriteResult, error) {
	ctx, cancel := c.linkedContext(ctx)
	defer cancel()
	client, err := c.authClient(ctx)
	if err != nil {
		return WriteResult{Action: "ModifyDesiredCapacity", NotSubmitted: "credential-gate"}, err
	}
	return client.SetDesired(ctx, group, count)
}
func (c *Authenticated) RemoveExact(ctx context.Context, group, instance string) (WriteResult, error) {
	ctx, cancel := c.linkedContext(ctx)
	defer cancel()
	client, err := c.authClient(ctx)
	if err != nil {
		return WriteResult{Action: "RemoveInstances", NotSubmitted: "credential-gate"}, err
	}
	return client.RemoveExact(ctx, group, instance)
}

func (c *Authenticated) ActivitiesSince(ctx context.Context, group string, since time.Time) ([]Activity, error) {
	ctx, cancel := c.linkedContext(ctx)
	defer cancel()
	client, err := c.authClient(ctx)
	if err != nil {
		return nil, err
	}
	return client.ActivitiesSince(ctx, group, since)
}
