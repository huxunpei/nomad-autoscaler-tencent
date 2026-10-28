package live

import (
	"context"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadclient"
	"time"
)

// Preserve each fixture's failure/blocking behavior while giving the engine
// the same explicit receipt contract as the real SDK adapter.
func probePostResult(id nomadclient.Identity, op, digest string) nomadclient.PostScaleInResult {
	return nomadclient.PostScaleInResult{Identity: id, OperationID: op, PolicySHA256: digest, Outcome: "purge-disabled", ObservedAt: time.Now().UTC()}
}
func (n *sdkProbe) PostScaleInWithEvidence(ctx context.Context, id nomadclient.Identity, op string, before func() error) (nomadclient.PostScaleInResult, error) {
	err := n.PostScaleIn(ctx, id, op, before)
	return probePostResult(id, op, n.PolicyDigest()), err
}
func (n *sdkMultiProbe) PostScaleInWithEvidence(ctx context.Context, id nomadclient.Identity, op string, before func() error) (nomadclient.PostScaleInResult, error) {
	err := n.PostScaleIn(ctx, id, op, before)
	return probePostResult(id, op, n.PolicyDigest()), err
}
func (n *abortSDK) PostScaleInWithEvidence(ctx context.Context, id nomadclient.Identity, op string, before func() error) (nomadclient.PostScaleInResult, error) {
	err := n.PostScaleIn(ctx, id, op, before)
	return probePostResult(id, op, n.PolicyDigest()), err
}
func (n *completionSDK) PostScaleInWithEvidence(ctx context.Context, id nomadclient.Identity, op string, before func() error) (nomadclient.PostScaleInResult, error) {
	err := n.PostScaleIn(ctx, id, op, before)
	return probePostResult(id, op, n.PolicyDigest()), err
}
func (n *completionPersistenceSDK) PostScaleInWithEvidence(ctx context.Context, id nomadclient.Identity, op string, before func() error) (nomadclient.PostScaleInResult, error) {
	err := n.PostScaleIn(ctx, id, op, before)
	return probePostResult(id, op, n.PolicyDigest()), err
}
func (n *settlementNomad) PostScaleInWithEvidence(ctx context.Context, id nomadclient.Identity, op string, before func() error) (nomadclient.PostScaleInResult, error) {
	already := n.purged[id.InstanceID]
	err := n.PostScaleIn(ctx, id, op, before)
	if err != nil {
		return nomadclient.PostScaleInResult{}, err
	}
	result := probePostResult(id, op, n.PolicyDigest())
	result.Purge = true
	result.Outcome = "purged"
	if already {
		result.Outcome = "already-absent"
	}
	return result, nil
}
