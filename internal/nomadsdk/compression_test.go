package nomadsdk

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadclient"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/targetconfig"
)

type gzipResponseWriter struct {
	http.ResponseWriter
	zip *gzip.Writer
}

func (w gzipResponseWriter) Write(p []byte) (int, error) { return w.zip.Write(p) }

// Nomad's API client explicitly advertises gzip. An outer RoundTripper sees
// the compressed bytes before that API client's own decoder. Synthetic JSON
// fixtures must exercise this real protocol behavior as well as plain JSON.
func TestSDKCompressedResponsesUseSameScopedLifecycle(t *testing.T) {
	for _, paged := range []bool{false, true} {
		t.Run(map[bool]string{false: "single-page", true: "paged"}[paged], func(t *testing.T) {
			t.Setenv("NOMAD_ALLOC_ID", "")
			f := &fixture{paged: paged}
			handler := f.serve(t)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
					handler.ServeHTTP(w, r)
					return
				}
				w.Header().Set("Content-Encoding", "gzip")
				z := gzip.NewWriter(w)
				handler.ServeHTTP(gzipResponseWriter{w, z}, r)
				_ = z.Close()
			}))
			defer server.Close()
			c, err := nomadclient.New(nomadclient.Config{Address: server.URL, Token: "synthetic-token", Timeout: time.Second})
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			c, err = c.WithScope("asg-example", "ap-guangzhou", "custom_instance")
			if err != nil {
				t.Fatal(err)
			}
			p, err := (targetconfig.Agent{}).Policy(map[string]string{"tencent_group_id": "asg-example", "instance_id_meta_key": "custom_instance", "node_drain_deadline": "1s", "node_purge": "true"})
			if err != nil {
				t.Fatal(err)
			}
			a := New(c, p)
			got, err := a.SelectScaleIn(context.Background(), []nomadclient.Identity{identity(first), identity(second)}, 1)
			if err != nil || len(got) != 1 || got[0] != identity(second) {
				t.Fatal("gzip selection failed", got, err)
			}
			result, err := a.PreScaleIn(context.Background(), identity(second), operation, time.Second, func() error { f.barrier.Store(true); return nil })
			if err != nil || result.After == nil || result.Unknown || result.NodeModifyIndex == 0 {
				t.Fatal("gzip pre failed", err)
			}
			if err = a.PostScaleIn(context.Background(), identity(second), operation, func() error { return nil }); err != nil {
				t.Fatal("gzip post failed", err)
			}
			if f.drainWrites != 1 || f.purgeWrites != 1 {
				t.Fatal("gzip lifecycle write count")
			}
		})
	}
}
func TestSDKResponseEncodingFailureAndDecodedSizeStayBounded(t *testing.T) {
	for _, mode := range []string{"invalid-gzip", "truncated-gzip", "decoded-too-large", "unknown-encoding"} {
		t.Run(mode, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if mode == "unknown-encoding" {
					w.Header().Set("Content-Encoding", "br")
					_, _ = io.WriteString(w, "[]")
					return
				}
				w.Header().Set("Content-Encoding", "gzip")
				if mode == "invalid-gzip" {
					_, _ = io.WriteString(w, "not-gzip")
					return
				}
				var b bytes.Buffer
				z := gzip.NewWriter(&b)
				data := "[]"
				if mode == "decoded-too-large" {
					data = strings.Repeat("x", maxBody+1)
				}
				_, _ = io.WriteString(z, data)
				_ = z.Close()
				raw := b.Bytes()
				if mode == "truncated-gzip" {
					raw = raw[:len(raw)-5]
				}
				_, _ = w.Write(raw)
			}))
			defer server.Close()
			r, _ := http.NewRequest(http.MethodGet, server.URL+"/v1/nodes", nil)
			r.Header.Set("Accept-Encoding", "gzip")
			s := &scopedTransport{next: http.DefaultTransport}
			_, _, err := s.fetch(r)
			if err == nil {
				t.Fatal("unsafe compressed response accepted", mode)
			}
			if strings.Contains(err.Error(), "not-gzip") || strings.Contains(err.Error(), server.URL) {
				t.Fatal("upstream response leaked")
			}
		})
	}
}
