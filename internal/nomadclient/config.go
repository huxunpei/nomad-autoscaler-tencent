package nomadclient

import (
	"crypto/tls"
	"crypto/x509"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

var configEnvironment = map[string]string{
	"nomad_address": "NOMAD_ADDR", "nomad_region": "NOMAD_REGION", "nomad_namespace": "NOMAD_NAMESPACE", "nomad_token": "NOMAD_TOKEN",
	"nomad_http-auth": "NOMAD_HTTP_AUTH", "nomad_ca-cert": "NOMAD_CACERT", "nomad_ca-path": "NOMAD_CAPATH",
	"nomad_client-cert": "NOMAD_CLIENT_CERT", "nomad_client-key": "NOMAD_CLIENT_KEY", "nomad_tls-server-name": "NOMAD_TLS_SERVER_NAME",
	"nomad_skip-verify": "NOMAD_SKIP_VERIFY",
}

// IsConfigKey identifies the standard Agent Nomad connection keys. The Agent
// performs inheritance before calling the plugin; the plugin never stores its
// original configuration map, which may contain an inherited ACL token.
func IsConfigKey(key string) bool {
	_, ok := configEnvironment[key]
	return ok || key == "nomad_config_inherit" || key == "nomad_block-query-wait-time"
}

// ConfigFromMap receives the merged Agent plugin configuration. Environment
// fallback also supports the standalone recovery command. An explicitly present
// value, including an empty token, takes precedence over environment fallback.
func ConfigFromMap(values map[string]string, getenv func(string) string) (Config, error) {
	for k := range values {
		if strings.HasPrefix(k, "nomad_") && !IsConfigKey(k) {
			return Config{}, failure("UnknownNomadOption")
		}
	}
	get := func(k string) string {
		if v, ok := values[k]; ok {
			return v
		}
		if getenv != nil {
			return getenv(configEnvironment[k])
		}
		return ""
	}
	cfg := Config{Address: get("nomad_address"), Region: get("nomad_region"), Namespace: get("nomad_namespace"), Token: get("nomad_token"), HTTPAuth: get("nomad_http-auth"), CACert: get("nomad_ca-cert"), CAPath: get("nomad_ca-path"), ClientCert: get("nomad_client-cert"), ClientKey: get("nomad_client-key"), TLSServerName: get("nomad_tls-server-name")}
	if cfg.Address == "" {
		if _, ok := values["nomad_address"]; !ok {
			cfg.Address = "http://127.0.0.1:4646"
		}
	}
	if raw, ok := values["nomad_config_inherit"]; ok {
		if _, err := strconv.ParseBool(raw); err != nil {
			return Config{}, failure("InvalidNomadConfigInherit")
		}
	}
	if raw := get("nomad_skip-verify"); raw != "" {
		var err error
		cfg.SkipVerify, err = strconv.ParseBool(raw)
		if err != nil {
			return Config{}, failure("InvalidNomadSkipVerify")
		}
	}
	if raw, ok := values["nomad_block-query-wait-time"]; ok {
		d, err := time.ParseDuration(raw)
		if err != nil || d < 0 {
			return Config{}, failure("InvalidNomadWaitTime")
		}
		cfg.WaitTime = d
	}
	return cfg, nil
}

func (Config) String() string   { return "NomadConfig(redacted)" }
func (Config) GoString() string { return "NomadConfig(redacted)" }

func validateAddress(address string) (*url.URL, error) {
	u, err := url.Parse(address)
	if err != nil || u == nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil || (u.Path != "" && u.Path != "/") || u.RawPath != "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.Opaque != "" {
		return nil, failure("InvalidAddress")
	}
	if u.Scheme == "http" {
		ip := net.ParseIP(u.Hostname())
		if ip == nil || !ip.IsLoopback() {
			return nil, failure("PlainHTTPRequiresLoopback")
		}
	}
	return u, nil
}

func configureTLS(transport *http.Transport, cfg Config, scheme string) error {
	if (cfg.ClientCert == "") != (cfg.ClientKey == "") {
		return failure("IncompleteTLSClientIdentity")
	}
	if scheme != "https" {
		if cfg.CACert != "" || cfg.CAPath != "" || cfg.ClientCert != "" || cfg.TLSServerName != "" || cfg.SkipVerify {
			return failure("TLSOptionsRequireHTTPS")
		}
		return nil
	}
	tc := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: cfg.TLSServerName, InsecureSkipVerify: cfg.SkipVerify} // Explicit standard Nomad option; verification is on by default.
	if cfg.CACert != "" || cfg.CAPath != "" {
		pool, err := x509.SystemCertPool()
		if err != nil || pool == nil {
			pool = x509.NewCertPool()
		}
		appendCA := func(path string) error {
			b, e := os.ReadFile(path)
			if e != nil || len(b) == 0 || len(b) > maxBody || !pool.AppendCertsFromPEM(b) {
				return failure("InvalidTLSCA")
			}
			return nil
		}
		if cfg.CACert != "" {
			if err = appendCA(cfg.CACert); err != nil {
				return err
			}
		}
		if cfg.CAPath != "" {
			entries, e := os.ReadDir(cfg.CAPath)
			if e != nil {
				return failure("InvalidTLSCAPath")
			}
			count := 0
			for _, entry := range entries {
				if entry.IsDir() {
					continue
				}
				if e = appendCA(filepath.Join(cfg.CAPath, entry.Name())); e != nil {
					return e
				}
				count++
			}
			if count == 0 {
				return failure("InvalidTLSCAPath")
			}
		}
		tc.RootCAs = pool
	}
	if cfg.ClientCert != "" {
		pair, err := tls.LoadX509KeyPair(cfg.ClientCert, cfg.ClientKey)
		if err != nil {
			return failure("InvalidTLSClientIdentity")
		}
		tc.Certificates = []tls.Certificate{pair}
	}
	transport.TLSClientConfig = tc
	return nil
}
