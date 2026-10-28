package nomadclient

import (
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadtest"

	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"log"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// All certificates and credentials in these tests are synthetic and local.
type testPKI struct {
	ca, cert, key string
	server        tls.Certificate
	pool          *x509.CertPool
}

func makePKI(t *testing.T) testPKI {
	t.Helper()
	dir := t.TempDir()
	now := time.Now()
	caPub, caKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	root := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test CA"}, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	caDER, err := x509.CreateCertificate(rand.Reader, root, root, caPub, caKey)
	if err != nil {
		t.Fatal(err)
	}
	root, err = x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatal(err)
	}
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
	caPath := filepath.Join(dir, "ca.pem")
	if err = os.WriteFile(caPath, caPEM, 0600); err != nil {
		t.Fatal(err)
	}
	leaf := func(name string, usage x509.ExtKeyUsage, serial int64) (tls.Certificate, string, string) {
		pub, key, e := ed25519.GenerateKey(rand.Reader)
		if e != nil {
			t.Fatal(e)
		}
		tpl := &x509.Certificate{SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: name}, DNSNames: []string{name}, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{usage}}
		der, e := x509.CreateCertificate(rand.Reader, tpl, root, pub, caKey)
		if e != nil {
			t.Fatal(e)
		}
		keyDER, e := x509.MarshalPKCS8PrivateKey(key)
		if e != nil {
			t.Fatal(e)
		}
		certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
		keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
		certPath, keyPath := filepath.Join(dir, name+".pem"), filepath.Join(dir, name+".key")
		if e = os.WriteFile(certPath, certPEM, 0600); e != nil {
			t.Fatal(e)
		}
		if e = os.WriteFile(keyPath, keyPEM, 0600); e != nil {
			t.Fatal(e)
		}
		pair, e := tls.X509KeyPair(certPEM, keyPEM)
		if e != nil {
			t.Fatal(e)
		}
		return pair, certPath, keyPath
	}
	server, _, _ := leaf("server.nomad.test", x509.ExtKeyUsageServerAuth, 2)
	_, cert, key := leaf("client.nomad.test", x509.ExtKeyUsageClientAuth, 3)
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(caPEM)
	return testPKI{caPath, cert, key, server, pool}
}

func TestInheritedNomadTLSAuthScopeAndAllNamespaceTasks(t *testing.T) {
	pki := makePKI(t)
	var requests atomic.Int32
	s := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.Header.Get("X-Nomad-Token") != "synthetic-token" {
			t.Error("inherited token missing")
		}
		user, pass, ok := r.BasicAuth()
		if !ok || user != "synthetic-user" || pass != "synthetic:password" {
			t.Error("HTTP authentication missing")
		}
		if r.URL.Query().Get("region") != "region-one" {
			t.Error("Nomad region missing")
		}
		if nomadtest.DisabledACL(w, r) {
			return
		}
		if strings.HasSuffix(r.URL.Path, "/allocations") {
			if r.URL.Query().Get("namespace") != "*" {
				t.Error("cluster drain must see all namespaces")
			}
			io.WriteString(w, "[]")
			return
		}
		if r.URL.Query().Get("namespace") != "team-one" {
			t.Error("inherited namespace missing")
		}
		json.NewEncoder(w).Encode(raw(identity))
	}))
	s.Config.ErrorLog = log.New(io.Discard, "", 0)
	s.TLS = &tls.Config{Certificates: []tls.Certificate{pki.server}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: pki.pool, MinVersion: tls.VersionTLS12}
	s.StartTLS()
	defer s.Close()
	values := map[string]string{"nomad_address": s.URL, "nomad_region": "region-one", "nomad_namespace": "team-one", "nomad_token": "synthetic-token", "nomad_http-auth": "synthetic-user:synthetic:password", "nomad_ca-cert": pki.ca, "nomad_client-cert": pki.cert, "nomad_client-key": pki.key, "nomad_tls-server-name": "server.nomad.test"}
	cfg, err := ConfigFromMap(values, func(string) string { return "" })
	if err != nil {
		t.Fatal(err)
	}
	c, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err = c.ReadNode(context.Background(), firstID); err != nil {
		t.Fatal(err)
	}
	if ok, err := c.TasksStopped(context.Background(), firstID, false); err != nil || !ok {
		t.Fatal(ok, err)
	}
	if _, err = c.ActiveAllocations(context.Background(), firstID); err != nil {
		t.Fatal(err)
	}
	if requests.Load() != 7 {
		t.Fatal("requests did not reach authenticated server")
	}
	for _, change := range []map[string]string{{"nomad_tls-server-name": "wrong.nomad.test"}, {"nomad_client-cert": "", "nomad_client-key": ""}, {"nomad_ca-cert": ""}} {
		bad := map[string]string{}
		for k, v := range values {
			bad[k] = v
		}
		for k, v := range change {
			bad[k] = v
		}
		cfg, err = ConfigFromMap(bad, nil)
		if err != nil {
			t.Fatal(err)
		}
		n, err := New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		_, err = n.ReadNode(context.Background(), firstID)
		n.Close()
		if err == nil {
			t.Fatal("invalid TLS identity accepted")
		}
	}
	if requests.Load() != 7 {
		t.Fatal("invalid TLS reached application")
	}
}

func TestConfigPrecedenceValidationAndSanitizedErrors(t *testing.T) {
	env := map[string]string{"NOMAD_ADDR": "http://127.0.0.1:4646", "NOMAD_TOKEN": "synthetic-env-token", "NOMAD_REGION": "env-region", "NOMAD_NAMESPACE": "env-ns"}
	get := func(k string) string { return env[k] }
	cfg, err := ConfigFromMap(nil, get)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Address != env["NOMAD_ADDR"] || cfg.Token != env["NOMAD_TOKEN"] {
		t.Fatal("environment fallback missing")
	}
	cfg, err = ConfigFromMap(map[string]string{"nomad_token": "", "nomad_region": "override"}, get)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Token != "" || cfg.Region != "override" || cfg.Namespace != "env-ns" {
		t.Fatal("configuration precedence was not preserved")
	}
	cfg.Token = "PRIVATE_VALUE"
	cfg.HTTPAuth = "user:PRIVATE_VALUE"
	cfg.ClientKey = "/PRIVATE_VALUE/key"
	encoded, e := json.Marshal(cfg)
	if e != nil {
		t.Fatal(e)
	}
	if strings.Contains(string(encoded), "PRIVATE_VALUE") || strings.Contains(fmt.Sprintf("%+v %#v", cfg, cfg), "PRIVATE_VALUE") {
		t.Fatal("configuration diagnostic exposed credentials")
	}
	cases := []map[string]string{
		{"nomad_token": "PRIVATE_VALUE\n"}, {"nomad_http-auth": "PRIVATE_VALUE\n"}, {"nomad_skip-verify": "invalid"}, {"nomad_config_inherit": "bad"}, {"nomad_block-query-wait-time": "bad"},
		{"nomad_client-cert": "/PRIVATE_VALUE/cert"}, {"nomad_client-key": "/PRIVATE_VALUE/key"},
		{"nomad_address": "https://127.0.0.1:4646", "nomad_ca-cert": "/PRIVATE_VALUE/missing"},
		{"nomad_address": "https://127.0.0.1:4646", "nomad_ca-path": "/PRIVATE_VALUE/missing"},
		{"nomad_address": "http://PRIVATE_VALUE.example:4646"}, {"nomad_address": "https://PRIVATE_VALUE@host"}, {"nomad_tokne": "PRIVATE_VALUE"},
	}
	for i, v := range cases {
		cfg, err := ConfigFromMap(v, get)
		if err == nil {
			var c *Client
			c, err = New(cfg)
			if c != nil {
				c.Close()
			}
		}
		if err == nil || strings.Contains(err.Error(), "PRIVATE_VALUE") {
			t.Fatalf("case %d must fail without echoing values: %v", i, err)
		}
	}
}

func TestNomadCADirectoryAndRedirectIsolation(t *testing.T) {
	pki := makePKI(t)
	var leaked atomic.Int32
	dest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { leaked.Add(1); io.WriteString(w, "{}") }))
	defer dest.Close()
	s := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, dest.URL, http.StatusTemporaryRedirect)
	}))
	s.Config.ErrorLog = log.New(io.Discard, "", 0)
	s.TLS = &tls.Config{Certificates: []tls.Certificate{pki.server}, MinVersion: tls.VersionTLS12}
	s.StartTLS()
	defer s.Close()
	caDir := t.TempDir()
	b, err := os.ReadFile(pki.ca)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(caDir, "ca.pem"), b, 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := ConfigFromMap(map[string]string{"nomad_address": s.URL, "nomad_ca-path": caDir, "nomad_tls-server-name": "server.nomad.test", "nomad_token": "synthetic-token"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	c, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_, err = c.ReadNode(context.Background(), firstID)
	code(t, err, "HTTPFailure")
	if leaked.Load() != 0 {
		t.Fatal("redirect leaked credentials")
	}
}
