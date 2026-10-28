package live

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/hashicorp/nomad/api"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadclient"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadsdk"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/targetconfig"
)

// ACLs belong only to a new, isolated Nomad process, never Tencent CAM. Tokens
// live in Go memory; generated laboratory private keys stay in the short-lived
// data directory and are excluded from every report/source archive.
func TestRealNomadACLTLSVisibilityAndLifecycle(t *testing.T) {
	binary := os.Getenv("NOMAD_REAL_BINARY")
	if binary == "" {
		t.Skip("requires separately provisioned real Nomad binary on the Linux test host")
	}
	expected := os.Getenv("NOMAD_REAL_SHA256")
	raw, e := os.ReadFile(binary)
	if e != nil {
		t.Fatal(e)
	}
	hash := sha256.Sum256(raw)
	if !filepath.IsAbs(binary) || hex.EncodeToString(hash[:]) != expected || !digestPattern.MatchString(expected) {
		t.Fatal("real Nomad pin mismatch")
	}
	dir, err := os.MkdirTemp("", "nt-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	report := map[string]any{"started_at": time.Now().UTC(), "nomad_binary_sha256": expected, "scope": "isolated real Nomad ACL and HTTPS mTLS; no cloud APIs", "data_directory": dir}
	secrets := []string{}
	jobs := map[string]string{}
	var admin *api.Client
	pki := newNomadRuntimePKI(t, dir)
	report["ca_sha256"] = pki.caSHA
	address, cmd, done := startRealNomadACLTLS(t, binary, dir, pki, report)
	defer func() {
		if admin != nil {
			for id, ns := range jobs {
				_, _, _ = admin.Jobs().Deregister(id, true, &api.WriteOptions{Namespace: ns})
			}
			deadline := time.Now().Add(15 * time.Second)
			terminal := false
			for time.Now().Before(deadline) {
				rows, _, e := admin.Allocations().List(&api.QueryOptions{Namespace: "*"})
				terminal = e == nil
				for _, row := range rows {
					if _, own := jobs[row.JobID]; !own {
						continue
					}
					a, _, e := admin.Allocations().Info(row.ID, &api.QueryOptions{Namespace: row.Namespace})
					if e != nil {
						terminal = false
						continue
					}
					if a.ClientStatus != "complete" && a.ClientStatus != "failed" && a.ClientStatus != "lost" {
						terminal = false
					}
					for _, task := range a.TaskStates {
						if task == nil || task.State != "dead" {
							terminal = false
						}
					}
				}
				if terminal {
					break
				}
				time.Sleep(100 * time.Millisecond)
			}
			report["owned_tasks_terminal"] = terminal
			if !terminal {
				t.Error("test-owned task cleanup incomplete")
			}
		}
		_ = cmd.Process.Signal(os.Interrupt)
		select {
		case <-done:
		case <-time.After(8 * time.Second):
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
			<-done
		}
		report["nomad_process_terminal"] = cmd.ProcessState != nil
		mounts, _ := os.ReadFile("/proc/self/mountinfo")
		unmounted := []string{}
		for _, line := range strings.Split(string(mounts), "\n") {
			f := strings.Fields(line)
			if len(f) > 5 && strings.HasPrefix(f[4], dir+"/data/alloc/") && strings.Contains(line, " - tmpfs tmpfs ") {
				if e := syscall.Unmount(f[4], 0); e != nil {
					t.Error("owned tmpfs cleanup", e)
				} else {
					unmounted = append(unmounted, f[4])
				}
			}
		}
		report["owned_tmpfs_unmounted"] = unmounted
		logBytes, _ := os.ReadFile(filepath.Join(dir, "nomad.log"))
		lines := strings.Split(string(logBytes), "\n")
		sensitiveLine := regexp.MustCompile(`(?i)token|secret|credential|password|-----BEGIN`)
		for i, line := range lines {
			for _, secret := range secrets {
				line = strings.ReplaceAll(line, secret, "<redacted>")
			}
			if sensitiveLine.MatchString(line) {
				line = "[credential-related agent log line omitted]"
			}
			lines[i] = line
		}
		report["nomad_log"] = strings.Join(lines, "\n")
		conn, e := net.DialTimeout("tcp", strings.TrimPrefix(address, "https://"), 200*time.Millisecond)
		if e == nil {
			_ = conn.Close()
		}
		report["https_listener_closed"] = e != nil
		if e := os.RemoveAll(dir); e != nil {
			t.Error(e)
		}
		_, e = os.Stat(dir)
		report["private_key_and_data_directory_removed"] = os.IsNotExist(e)
		report["test_failed"] = t.Failed()
		report["ended_at"] = time.Now().UTC()
		data := jsonBytes(report)
		for _, secret := range secrets {
			if len(secret) > 0 && strings.Contains(string(data), secret) {
				t.Error("ephemeral ACL token leaked into report")
				return
			}
		}
		if out := os.Getenv("NOMAD_REAL_TLS_REPORT"); out != "" {
			if !filepath.IsAbs(out) {
				t.Error("report path must be absolute")
			} else if e := os.WriteFile(out, data, 0600); e != nil {
				t.Error(e)
			}
		}
	}()
	cfg := nomadclient.Config{Address: address, CACert: pki.ca, ClientCert: pki.clientCert, ClientKey: pki.clientKey, TLSServerName: "server.global.nomad", Timeout: 5 * time.Second}
	transport, err := nomadclient.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer transport.Close()
	adminConfig := transport.SDKConfig(nil)
	adminConfig.Namespace = "default"
	admin, err = api.NewClient(adminConfig)
	if err != nil {
		t.Fatal(err)
	}
	waitRealNomad(t, 20*time.Second, "TLS leader before ACL bootstrap", func() bool { leader, e := admin.Status().Leader(); return e == nil && leader != "" })
	management, _, err := admin.ACLTokens().Bootstrap(nil)
	if err != nil {
		t.Fatal("isolated ACL bootstrap", err)
	}
	secrets = append(secrets, management.SecretID)
	admin.SetSecretID(management.SecretID)
	report["management_accessor_id"] = management.AccessorID
	var node *api.Node
	waitRealNomad(t, 40*time.Second, "ACL-enabled ready raw_exec node", func() bool {
		rows, _, e := admin.Nodes().List(nil)
		if e != nil || len(rows) != 1 {
			return false
		}
		n, _, e := admin.Nodes().Info(rows[0].ID, nil)
		if e != nil || n.Status != "ready" || n.Meta["cloud_instance_id"] != testInstance {
			return false
		}
		d := n.Drivers["raw_exec"]
		if d == nil || !d.Detected || !d.Healthy {
			return false
		}
		node = n
		return true
	})
	report["node_id"] = node.ID
	hiddenNS := "hidden"
	if _, err = admin.Namespaces().Register(&api.Namespace{Name: hiddenNS}, nil); err != nil {
		t.Fatal("create isolated namespace", err)
	}
	rules := map[string]string{
		"full":          `node { policy = "write" } namespace "*" { capabilities = ["read-job"] }`,
		"readonly":      `node { policy = "read" } namespace "*" { capabilities = ["read-job"] }`,
		"partial":       `node { policy = "write" } namespace "default" { capabilities = ["read-job"] }`,
		"wildcard-deny": `node { policy = "write" } namespace "*" { capabilities = ["read-job"] } namespace "hidden" { policy = "deny" }`,
	}
	tokens := map[string]*api.ACLToken{}
	for name, rule := range rules {
		if _, err = admin.ACLPolicies().Upsert(&api.ACLPolicy{Name: "nomad-spec-" + name, Rules: rule}, nil); err != nil {
			t.Fatal("create isolated ACL policy", err)
		}
		tok, _, e := admin.ACLTokens().Create(&api.ACLToken{Name: "nomad-spec-" + name, Type: "client", Policies: []string{"nomad-spec-" + name}}, nil)
		if e != nil {
			t.Fatal("create isolated ACL token", e)
		}
		tokens[name] = tok
		secrets = append(secrets, tok.SecretID)
	}
	clientFor := func(token string, options func(*nomadclient.Config)) *nomadclient.Client {
		t.Helper()
		c := cfg
		c.Token = token
		if options != nil {
			options(&c)
		}
		raw, e := nomadclient.New(c)
		if e != nil {
			t.Fatal(e)
		}
		t.Cleanup(raw.Close)
		scoped, e := raw.WithScope(testGroup, testRegion, "cloud_instance_id")
		if e != nil {
			t.Fatal(e)
		}
		return scoped
	}
	full := clientFor(tokens["full"].SecretID, nil)
	partial := clientFor(tokens["partial"].SecretID, nil)
	readonly := clientFor(tokens["readonly"].SecretID, nil)
	outcomes := map[string]any{}
	noToken := clientFor("", nil)
	_, err = noToken.ReadNode(context.Background(), node.ID)
	outcomes["missing_acl_token"] = nomadRuntimeError(err)
	var aclErr *nomadclient.Error
	if !errors.As(err, &aclErr) || aclErr.HTTPStatus != 403 {
		t.Error("valid mTLS must not bypass ACL")
	}
	for _, tc := range []struct {
		name string
		edit func(*nomadclient.Config)
	}{
		{"missing_client_certificate", func(c *nomadclient.Config) { c.ClientCert = ""; c.ClientKey = "" }},
		{"wrong_server_name", func(c *nomadclient.Config) { c.TLSServerName = "wrong.nomad.invalid" }},
		{"untrusted_server_ca", func(c *nomadclient.Config) { c.CACert = pki.otherCA }},
		{"untrusted_client_certificate", func(c *nomadclient.Config) { c.ClientCert = pki.otherClientCert; c.ClientKey = pki.otherClientKey }},
	} {
		c := clientFor(tokens["full"].SecretID, tc.edit)
		_, e := c.ReadNode(context.Background(), node.ID)
		outcomes[tc.name] = nomadRuntimeError(e)
		if e == nil {
			t.Error("invalid TLS identity accepted", tc.name)
		}
	}
	capath := clientFor(tokens["full"].SecretID, func(c *nomadclient.Config) { c.CACert = ""; c.CAPath = pki.caDir })
	if _, err = capath.ReadNode(context.Background(), node.ID); err != nil {
		t.Fatal("real CA directory trust", err)
	}
	outcomes["ca_directory"] = true
	metadata := map[string]any{}
	for name, tok := range tokens {
		cli := clientFor(tok.SecretID, nil)
		sdkConfig := cli.SDKConfig(nil)
		path := "/v1/acl/policy/nomad-spec-" + name
		req, _ := http.NewRequest(http.MethodGet, address+path, nil)
		req.Header.Set("X-Nomad-Token", tok.SecretID)
		resp, e := sdkConfig.HttpClient.Do(req)
		if e != nil {
			metadata[name] = nomadRuntimeError(e)
			continue
		}
		var data struct {
			Name      string
			RulesJSON json.RawMessage
		}
		_ = json.NewDecoder(resp.Body).Decode(&data)
		_ = resp.Body.Close()
		metadata[name] = map[string]any{"status": resp.StatusCode, "name": data.Name, "server_parsed_rules": data.RulesJSON}
	}
	report["own_policy_metadata"] = metadata
	report["tls_and_missing_token_outcomes"] = outcomes
	visibleID, hiddenID := "nomad-spec-visible", "nomad-spec-hidden"
	jobs[visibleID] = "default"
	jobs[hiddenID] = hiddenNS
	visible := realRecoveryJob(visibleID, node.ID, "batch", false)
	visible.TaskGroups[0].Tasks[0].Config["args"] = []string{"1"}
	hidden := realRecoveryJob(hiddenID, node.ID, "batch", false)
	hidden.Namespace = &hiddenNS
	hidden.TaskGroups[0].Tasks[0].Config["args"] = []string{"120"}
	if _, _, err = admin.Jobs().Register(visible, nil); err != nil {
		t.Fatal(err)
	}
	if _, _, err = admin.Jobs().Register(hidden, &api.WriteOptions{Namespace: hiddenNS}); err != nil {
		t.Fatal(err)
	}
	var hiddenAllocationID string
	waitRealNomad(t, 20*time.Second, "hidden task running", func() bool {
		rows, _, e := admin.Jobs().Allocations(hiddenID, false, &api.QueryOptions{Namespace: hiddenNS})
		if e != nil {
			return false
		}
		for _, a := range rows {
			if a.NodeID == node.ID && a.ClientStatus == "running" {
				hiddenAllocationID = a.ID
				return true
			}
		}
		return false
	})
	waitRealNomad(t, 15*time.Second, "visible task complete", func() bool {
		rows, _, e := admin.Jobs().Allocations(visibleID, false, nil)
		if e != nil {
			return false
		}
		for _, a := range rows {
			if a.ClientStatus == "complete" {
				return true
			}
		}
		return false
	})
	allStopped, allErr := full.TasksStopped(context.Background(), node.ID, false)
	partialStopped, partialErr := partial.TasksStopped(context.Background(), node.ID, false)
	report["namespace_visibility"] = map[string]any{"hidden_running_allocation": hiddenAllocationID, "full_tasks_stopped": allStopped, "full_error": nomadRuntimeError(allErr), "partial_tasks_stopped": partialStopped, "partial_error": nomadRuntimeError(partialErr)}
	if allErr != nil || allStopped {
		t.Error("full token did not see real hidden task", allErr)
	}
	if partialErr == nil && partialStopped {
		t.Error("partial namespace ACL incorrectly proves all node tasks stopped")
	}
	deniedNamespaceClient := clientFor(tokens["wildcard-deny"].SecretID, nil)
	deniedStopped, deniedNSerr := deniedNamespaceClient.TasksStopped(context.Background(), node.ID, false)
	report["wildcard_with_exact_namespace_deny"] = map[string]any{"tasks_stopped": deniedStopped, "error": nomadRuntimeError(deniedNSerr)}
	if deniedNSerr == nil && deniedStopped {
		t.Error("wildcard ACL with namespace deny incorrectly proves all tasks stopped")
	}
	for name, tok := range tokens {
		apiClient, _ := api.NewClient(clientFor(tok.SecretID, nil).SDKConfig(nil))
		rows, _, e := apiClient.Nodes().Allocations(node.ID, &api.QueryOptions{Namespace: "*"})
		ids := []string{}
		namespaces := []string{}
		for _, a := range rows {
			ids = append(ids, a.ID)
			namespaces = append(namespaces, a.Namespace)
		}
		report["node_allocations_"+name] = map[string]any{"count": len(rows), "ids": ids, "namespaces": namespaces, "error": nomadRuntimeError(e)}
	}

	// These tokens have node:write, so an ACL denial at the mutation endpoint
	// cannot conceal a failure to reject incomplete allocation visibility.
	policy, err := (targetconfig.Agent{}).Policy(map[string]string{"tencent_group_id": testGroup, "node_drain_deadline": "5s", "node_purge": "false"})
	if err != nil {
		t.Fatal(err)
	}
	identity := nomadclient.Identity{NodeID: node.ID, InstanceID: testInstance, GroupID: testGroup, Region: testRegion}
	beforeGuard, err := full.ReadNode(context.Background(), node.ID)
	if err != nil {
		t.Fatal(err)
	}
	deniedBeforeDrain := map[string]any{}
	for name, client := range map[string]*nomadclient.Client{"partial": partial, "wildcard-deny": deniedNamespaceClient} {
		adapter := nomadsdk.New(client, policy)
		_, selectErr := adapter.SelectScaleIn(context.Background(), []nomadclient.Identity{identity}, 1)
		callbacks := 0
		result, preErr := adapter.PreScaleIn(context.Background(), identity, "live-abababababababababababababababab", 5*time.Second, func() error { callbacks++; return nil })
		after, e := full.ReadNode(context.Background(), node.ID)
		deniedBeforeDrain[name] = map[string]any{"selection_error": nomadRuntimeError(selectErr), "pre_error": nomadRuntimeError(preErr), "write_attempted": result.WriteAttempted, "intent_callbacks": callbacks, "node_unchanged": e == nil && after.ModifyIndex == beforeGuard.ModifyIndex && after.Eligibility == "eligible" && !after.Drain && !after.HasDrainStrategy}
		if selectErr == nil || preErr == nil || result.WriteAttempted || callbacks != 0 || e != nil || after.ModifyIndex != beforeGuard.ModifyIndex {
			t.Error("partial ACL was not rejected before drain", name)
		}
	}
	report["incomplete_visibility_rejected_before_drain"] = deniedBeforeDrain
	for id, ns := range jobs {
		if _, _, err = admin.Jobs().Deregister(id, true, &api.WriteOptions{Namespace: ns}); err != nil {
			t.Fatal(err)
		}
	}
	waitRealNomad(t, 15*time.Second, "all real tasks stopped before lifecycle", func() bool { yes, e := full.TasksStopped(context.Background(), node.ID, false); return e == nil && yes })
	const op = "live-cccccccccccccccccccccccccccccccc"

	denied, deniedErr := readonly.StartOwnedDrain(context.Background(), identity, "live-dddddddddddddddddddddddddddddddd", 5*time.Second)
	_ = denied
	if !errors.As(deniedErr, &aclErr) || aclErr.HTTPStatus != 403 {
		t.Error("read-only ACL allowed node mutation", nomadRuntimeError(deniedErr))
	}
	unchanged, err := full.ReadNode(context.Background(), node.ID)
	if err != nil || unchanged.Eligibility != "eligible" || unchanged.Drain || unchanged.HasDrainStrategy {
		t.Fatal("denied request altered real node", err)
	}
	report["read_only_write_denied"] = nomadRuntimeError(deniedErr)

	sdk := nomadsdk.New(capath, policy)
	selected, err := sdk.SelectScaleIn(context.Background(), []nomadclient.Identity{identity}, 1)
	if err != nil || len(selected) != 1 || selected[0] != identity {
		t.Fatal("real ACL/mTLS SDK selection", err)
	}
	pre, err := sdk.PreScaleIn(context.Background(), identity, op, 5*time.Second, func() error { return nil })
	if err != nil || pre.NodeModifyIndex == 0 || pre.After == nil {
		t.Fatal("real ACL/mTLS SDK pre", err)
	}
	// Loss of global job visibility must still allow owned node compensation.
	if _, err = admin.ACLPolicies().Upsert(&api.ACLPolicy{Name: "nomad-spec-full", Rules: rules["partial"]}, nil); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	if yes, e := full.TasksStopped(context.Background(), node.ID, false); e == nil || yes {
		t.Error("lost read-job visibility was accepted")
	}
	restored, err := full.RestoreOwnedDrain(context.Background(), identity, op)
	if err != nil || restored.After == nil || restored.After.Eligibility != "eligible" {
		t.Fatal("real ACL/mTLS owned restore", err)
	}
	report["owned_restore_with_incomplete_job_visibility"] = true
	if _, err = admin.ACLPolicies().Upsert(&api.ACLPolicy{Name: "nomad-spec-full", Rules: rules["full"]}, nil); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	if err = sdk.PostScaleIn(context.Background(), identity, op, func() error { return nil }); err != nil {
		t.Fatal("real ACL/mTLS purge-disabled post", err)
	}
	report["sdk_https_acl_pre_post_restore"] = true
	// A policy change must take effect without changing cloud identities or
	// treating a failed permission check as success. Keep the token in memory.
	fullPolicy := "nomad-spec-full"
	if _, err = admin.ACLPolicies().Upsert(&api.ACLPolicy{Name: fullPolicy, Rules: rules["readonly"]}, nil); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	_, err = full.StartOwnedDrain(context.Background(), identity, "live-eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", 5*time.Second)
	report["withdrawn_write_permission"] = nomadRuntimeError(err)
	if !errors.As(err, &aclErr) || aclErr.HTTPStatus != 403 {
		t.Error("withdrawn node write permission not enforced")
	}
	if _, err = admin.ACLPolicies().Upsert(&api.ACLPolicy{Name: fullPolicy, Rules: rules["full"]}, nil); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	result, err := full.StartOwnedDrain(context.Background(), identity, "live-ffffffffffffffffffffffffffffffff", 5*time.Second)
	if err != nil || !result.WriteAttempted {
		t.Fatal("restored ACL permission", err)
	}
	waitRealNomad(t, 5*time.Second, "empty real drain complete", func() bool {
		n, e := full.ReadNode(context.Background(), node.ID)
		return e == nil && n.LastDrain != nil && n.LastDrain.Status == "complete"
	})
	if _, err = full.RestoreOwnedDrain(context.Background(), identity, "live-ffffffffffffffffffffffffffffffff"); err != nil {
		t.Fatal(err)
	}
	report["permission_restored_without_identity_change"] = true
	// The recovery reader gets the same credentials/CA/key/server-name through
	// the documented map/environment parser; no cloud credential is involved.
	env := map[string]string{"NOMAD_ADDR": address, "NOMAD_CACERT": pki.ca, "NOMAD_CLIENT_CERT": pki.clientCert, "NOMAD_CLIENT_KEY": pki.clientKey, "NOMAD_TLS_SERVER_NAME": "server.global.nomad", "NOMAD_TOKEN": tokens["full"].SecretID}
	recoveryConfig, err := nomadclient.ConfigFromMap(nil, func(k string) string { return env[k] })
	if err != nil {
		t.Fatal(err)
	}
	recoveryClient, err := nomadclient.New(recoveryConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer recoveryClient.Close()
	c := newCloud()
	c.addMember()
	journalCfg := Config{Dir: filepath.Join(dir, "journal"), GroupID: testGroup, Region: testRegion, GroupContract: true, DrainDeadline: 5 * time.Second, ReconcileInterval: time.Hour, DisableReconciliation: true}
	engine, err := Open(journalCfg, c, nomadsdk.New(full, policy))
	if err != nil {
		t.Fatal(err)
	}
	engine.Close()
	before, err := os.ReadFile(filepath.Join(journalCfg.Dir, "live.json"))
	if err != nil {
		t.Fatal(err)
	}
	r, err := OpenRecovery(journalCfg, c, recoveryClient)
	if err != nil {
		t.Fatal("real ACL/mTLS recovery reader", err)
	}
	nodes, resolveErr := r.nomad.ResolveMembers(context.Background(), testGroup, testRegion, []string{testInstance})
	r.Close()
	after, _ := os.ReadFile(filepath.Join(journalCfg.Dir, "live.json"))
	if resolveErr != nil || len(nodes) != 1 || nodes[0].NodeID != node.ID || string(before) != string(after) {
		t.Fatal("recovery reader transport/identity mismatch", resolveErr)
	}
	for _, secret := range secrets {
		if strings.Contains(string(after), secret) {
			t.Fatal("ephemeral ACL credential stored in journal")
		}
	}
	report["recovery_reader_verified_same_tls_acl"] = true
	if _, err = admin.ACLTokens().Delete(tokens["full"].AccessorID, nil); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	_, err = full.ReadNode(context.Background(), node.ID)
	report["revoked_token_read"] = nomadRuntimeError(err)
	if !errors.As(err, &aclErr) || aclErr.HTTPStatus != 403 {
		t.Error("revoked token accepted")
	}
	fresh, _, err := admin.ACLTokens().Create(&api.ACLToken{Name: "nomad-spec-replacement", Type: "client", Policies: []string{fullPolicy}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	secrets = append(secrets, fresh.SecretID)
	if _, err = clientFor(fresh.SecretID, nil).ReadNode(context.Background(), node.ID); err != nil {
		t.Fatal("new authorized token after revocation", err)
	}
	report["replacement_token_read_succeeded"] = true
	// Native anonymous token/self has AccessorID=anonymous and ModifyIndex=0.
	// Grant only this short-lived laboratory's read permissions, verify the
	// policy proof, and keep node writes forbidden (no implicit management).
	if _, err = admin.ACLPolicies().Upsert(&api.ACLPolicy{Name: "anonymous", Rules: `node { policy = "read" } namespace "*" { capabilities = ["read-job"] }`}, nil); err != nil {
		t.Fatal("isolated anonymous read policy", err)
	}
	time.Sleep(300 * time.Millisecond)
	yes, anonymousErr := noToken.TasksStopped(context.Background(), node.ID, false)
	report["anonymous_complete_visibility"] = map[string]any{"tasks_stopped": yes, "error": nomadRuntimeError(anonymousErr)}
	if anonymousErr != nil || !yes {
		t.Error("native anonymous full visibility rejected", anonymousErr)
	}
	_, anonymousWriteErr := noToken.StartOwnedDrain(context.Background(), identity, "live-23232323232323232323232323232323", 5*time.Second)
	report["anonymous_node_write_denied"] = nomadRuntimeError(anonymousWriteErr)
	if !errors.As(anonymousWriteErr, &aclErr) || aclErr.HTTPStatus != 403 {
		t.Error("anonymous read policy gained node writes")
	}

	report["passed"] = !t.Failed()
}

func nomadRuntimeError(err error) any {
	if err == nil {
		return nil
	}
	var e *nomadclient.Error
	if errors.As(err, &e) {
		return map[string]any{"code": e.Code, "http_status": e.HTTPStatus, "unknown": e.Unknown}
	}
	return map[string]string{"error_type": fmt.Sprintf("%T", err)}
}

type nomadRuntimePKI struct{ ca, caDir, serverCert, serverKey, clientCert, clientKey, otherCA, otherClientCert, otherClientKey, caSHA string }

func newNomadRuntimePKI(t *testing.T, dir string) nomadRuntimePKI {
	t.Helper()
	signer := func(serial int64) (*x509.Certificate, *ecdsa.PrivateKey, []byte) {
		key, e := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if e != nil {
			t.Fatal(e)
		}
		c := &x509.Certificate{SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: "nomad-isolated-test-ca"}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
		der, e := x509.CreateCertificate(rand.Reader, c, c, &key.PublicKey, key)
		if e != nil {
			t.Fatal(e)
		}
		return c, key, der
	}
	ca, key, der := signer(1)
	other, otherKey, otherDER := signer(2)
	write := func(name string, b []byte) string {
		p := filepath.Join(dir, name)
		if e := os.WriteFile(p, b, 0600); e != nil {
			t.Fatal(e)
		}
		return p
	}
	cert := func(name, cn string, serial int64, issuer *x509.Certificate, issuerKey *ecdsa.PrivateKey, server bool) (string, string) {
		k, e := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if e != nil {
			t.Fatal(e)
		}
		c := &x509.Certificate{SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: cn}, DNSNames: []string{cn}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}
		if server {
			c.IPAddresses = []net.IP{net.ParseIP("127.0.0.1")}
			c.ExtKeyUsage = append(c.ExtKeyUsage, x509.ExtKeyUsageServerAuth)
		}
		d, e := x509.CreateCertificate(rand.Reader, c, issuer, &k.PublicKey, issuerKey)
		if e != nil {
			t.Fatal(e)
		}
		kb, e := x509.MarshalPKCS8PrivateKey(k)
		if e != nil {
			t.Fatal(e)
		}
		return write(name+".pem", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: d})), write(name+".key", pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: kb}))
	}
	p := nomadRuntimePKI{}
	p.ca = write("ca.pem", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
	p.caDir = filepath.Join(dir, "trusted")
	if e := os.Mkdir(p.caDir, 0700); e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(filepath.Join(p.caDir, "ca.pem"), pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0600); e != nil {
		t.Fatal(e)
	}
	h := sha256.Sum256(der)
	p.caSHA = hex.EncodeToString(h[:])
	p.otherCA = write("other-ca.pem", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: otherDER}))
	p.serverCert, p.serverKey = cert("server", "server.global.nomad", 3, ca, key, true)
	p.clientCert, p.clientKey = cert("client", "client.global.nomad", 4, ca, key, false)
	p.otherClientCert, p.otherClientKey = cert("other-client", "client.global.nomad", 5, other, otherKey, false)
	return p
}
func startRealNomadACLTLS(t *testing.T, binary, dir string, p nomadRuntimePKI, report map[string]any, instanceID ...string) (string, *exec.Cmd, <-chan struct{}) {
	t.Helper()
	free := func() int {
		l, e := net.Listen("tcp", "127.0.0.1:0")
		if e != nil {
			t.Fatal(e)
		}
		p := l.Addr().(*net.TCPAddr).Port
		_ = l.Close()
		return p
	}
	h, r, s := free(), free(), free()
	instance := testInstance
	if len(instanceID) == 1 {
		instance = instanceID[0]
	}
	config := fmt.Sprintf(`bind_addr = "127.0.0.1"
name = "nomad-spec-acl-tls"
log_level = "WARN"
ports { http = %d rpc = %d serf = %d }
advertise { http = "127.0.0.1:%d" rpc = "127.0.0.1:%d" serf = "127.0.0.1:%d" }
acl { enabled = true token_ttl = "100ms" policy_ttl = "100ms" }
tls { http = true rpc = false ca_file = %q cert_file = %q key_file = %q verify_https_client = true }
client { network_interface = "lo" meta { cloud_instance_id = %q } }
plugin "raw_exec" { config { enabled = true } }
consul { auto_advertise = false server_auto_join = false client_auto_join = false }
`, h, r, s, h, r, s, p.ca, p.serverCert, p.serverKey, instance)
	path := filepath.Join(dir, "agent.hcl")
	if e := os.WriteFile(path, []byte(config), 0600); e != nil {
		t.Fatal(e)
	}
	log, e := os.OpenFile(filepath.Join(dir, "nomad.log"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		t.Fatal(e)
	}
	cmd := exec.Command(binary, "agent", "-dev", "-config="+path, "-data-dir="+filepath.Join(dir, "data"))
	cmd.Env = []string{"PATH=/usr/bin:/bin", "HOME=" + dir}
	cmd.Stdout = log
	cmd.Stderr = log
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if e = cmd.Start(); e != nil {
		_ = log.Close()
		t.Fatal(e)
	}
	done := make(chan struct{})
	go func() { _ = cmd.Wait(); _ = log.Close(); close(done) }()
	report["nomad_pid"] = cmd.Process.Pid
	report["https_address"] = fmt.Sprintf("https://127.0.0.1:%d", h)
	return fmt.Sprintf("https://127.0.0.1:%d", h), cmd, done
}
