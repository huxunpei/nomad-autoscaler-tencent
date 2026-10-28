package nomadclient

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"regexp"
	"sort"
)

var aclPolicyName = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)
var aclNamespacePattern = regexp.MustCompile(`^[A-Za-z0-9_*-]{1,128}$`)

type visibilityToken struct {
	Type, AccessorID string
	ModifyIndex      uint64
}
type visibilityPolicyStub struct {
	Name        string
	ModifyIndex uint64
}
type visibilityNamespace struct {
	Name, Policy string
	Capabilities []string
}
type visibilityPolicy struct {
	visibilityPolicyStub
	RulesJSON *struct{ Namespaces []*visibilityNamespace }
}

// VerifyAllocationVisibility proves a sufficient all-namespace read-job grant
// using the server's parsed policies. HTTP 200 on node allocations is not proof:
// Nomad silently removes allocations in namespaces hidden by the caller's ACL.
// This conservative check is not an ACL compiler and requires native RulesJSON.
func (c *Client) VerifyAllocationVisibility(ctx context.Context) error {
	_, err := c.allocationVisibilityProof(ctx)
	return err
}

func (c *Client) allocationVisibilityProof(ctx context.Context) (string, error) {
	var token visibilityToken // Never decode or retain SecretID from token/self.
	_, err := c.requestWithACLStatus(ctx, http.MethodGet, "/v1/acl/token/self", nil, nil, &token, true)
	if err != nil {
		var apiErr *Error
		if !errors.As(err, &apiErr) {
			return "", failure("AllocationVisibilityUnavailable")
		}
		if apiErr.Code == "ACLDisabled" {
			return "acl-disabled", nil
		}
		// Anonymous/workload identities may not expose an ACL token object. Native
		// policy listing still authorizes and resolves the calling identity itself.
		if apiErr.HTTPStatus != 403 && apiErr.HTTPStatus != 404 {
			return "", failure("AllocationVisibilityUnavailable")
		}
		token = visibilityToken{Type: "self-unavailable"}
	} else {
		if token.Type == "client" && token.AccessorID == "acls-disabled" && token.ModifyIndex == 0 {
			// Nomad 2.0.7 WhoAmI returns a reserved identity even with ACLs disabled.
			// Confirm through the policy endpoint; never treat an arbitrary HTTP 200,
			// missing token or malformed identity as unrestricted allocation visibility.
			var policies []visibilityPolicyStub
			_, probeErr := c.requestWithACLStatus(ctx, http.MethodGet, "/v1/acl/policies", nil, nil, &policies, true)
			var apiErr *Error
			if errors.As(probeErr, &apiErr) && apiErr.Code == "ACLDisabled" {
				return "acl-disabled", nil
			}
			return "", failure("AllocationVisibilityUnavailable")
		}
		anonymous := token.Type == "client" && token.AccessorID == "anonymous" && token.ModifyIndex == 0
		if !anonymous && ((token.Type != "client" && token.Type != "management") || !uuid.MatchString(token.AccessorID) || token.ModifyIndex == 0) {
			return "", failure("AllocationVisibilityUnavailable")
		}
	}
	if token.Type == "management" {
		return visibilityFingerprint(token, nil)
	}
	var stubs []visibilityPolicyStub
	h, err := c.request(ctx, http.MethodGet, "/v1/acl/policies", nil, nil, &stubs)
	if err != nil || h.Get("X-Nomad-NextToken") != "" || len(stubs) == 0 || len(stubs) > 256 {
		return "", failure("AllocationVisibilityUnavailable")
	}
	sort.Slice(stubs, func(i, j int) bool { return stubs[i].Name < stubs[j].Name })
	policies := make([]visibilityPolicy, 0, len(stubs))
	type grant struct{ read, deny bool }
	grants := map[string]grant{}
	total := 0
	for i, stub := range stubs {
		if !aclPolicyName.MatchString(stub.Name) || stub.ModifyIndex == 0 || (i > 0 && stubs[i-1].Name == stub.Name) {
			return "", failure("AllocationVisibilityUnavailable")
		}
		var policy *visibilityPolicy
		h, err = c.request(ctx, http.MethodGet, "/v1/acl/policy/"+stub.Name, nil, nil, &policy)
		if err != nil || h.Get("X-Nomad-NextToken") != "" || policy == nil || policy.visibilityPolicyStub != stub || policy.RulesJSON == nil {
			return "", failure("AllocationVisibilityUnavailable")
		}
		for _, ns := range policy.RulesJSON.Namespaces {
			total++
			if total > 4096 || ns == nil || !aclNamespacePattern.MatchString(ns.Name) || len(ns.Capabilities) > 256 {
				return "", failure("AllocationVisibilityUnavailable")
			}
			switch ns.Policy {
			case "", "read", "write", "deny":
			default:
				return "", failure("AllocationVisibilityUnavailable")
			}
			g := grants[ns.Name]
			if ns.Policy == "deny" {
				g.deny = true
			}
			for _, capability := range ns.Capabilities {
				if capability == "read-job" {
					g.read = true
				}
				if capability == "deny" {
					g.deny = true
				}
			}
			grants[ns.Name] = g
		}
		policies = append(policies, *policy)
	}
	// Exact namespace names and more-specific globs override '*'. Requiring
	// read-job on every declared pattern is a sufficient, deliberately strict
	// proof even when multiple attached policies merge capabilities for a name.
	if !grants["*"].read {
		return "", failure("IncompleteAllocationVisibility")
	}
	for _, g := range grants {
		if !g.read || g.deny {
			return "", failure("IncompleteAllocationVisibility")
		}
	}
	return visibilityFingerprint(token, policies)
}

func visibilityFingerprint(token visibilityToken, policies []visibilityPolicy) (string, error) {
	b, err := json.Marshal(struct {
		Token    visibilityToken
		Policies []visibilityPolicy
	}{token, policies})
	if err != nil {
		return "", failure("AllocationVisibilityUnavailable")
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}
