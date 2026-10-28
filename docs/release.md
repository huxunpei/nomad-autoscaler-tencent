# v0.1.0 release preparation

Personal repository: `huxunpei/nomad-autoscaler-tencent`.
Module: `github.com/huxunpei/nomad-autoscaler-tencent`.
License: MPL-2.0. Maintainer: xunpeihu (`huxunpei`). Fix deadline: one month.
v0.1.0 is the first release. Installation verification of the packaged release has not been performed.

## Validation scope for the release notes

- Unmodified Agent v0.6.0; Nomad 2.0.7; Autoscaler SDK v0.5.0.
- Tencent AS SDK v1.3.173 and common/CVM SDK v1.3.183; Go 1.26.6.
- Real-cloud validation baseline: workload scaling 1→5→1, exact deletion after task cleanup, partial-growth/failed-deletion recovery, instance identity, least privilege and scale-in protection.
- Separate five-minute restart cooldown, real Nomad ACL/mTLS, purge=false/true and lost-acknowledgement recovery passed. Cloud APIs in those targeted tests were synthetic.
- Linux amd64 loading is native; arm64 loading uses QEMU user emulation, not native arm64 hardware.
- The default 30-minute test verified timing, persistence, retention and late registration. Its complete expiry-to-scale-out path was not executed successfully; no additional run is planned for the first release. A separately configured 3-minute case passed the complete path. Do not report a 30-minute end-to-end PASS.
- The public-module binary hashes have not run against real AS. Baseline cloud evidence and the public-module builds are bound by an explicit production-source comparison, not by claiming they are the same binary.
- Ordinary cloud-response loss is covered by synthetic HTTP/official Agent recovery and real AS normal/partial-failure evidence; no additional real-cloud response-loss injection is planned.
- Installation verification of the packaged release has not occurred. The GitHub workflow ran successfully on the candidate source before release.

## Candidate preparation

Use new directories and the pinned Linux toolchain. These commands do not publish:

The example starts from the public module namespace. When rewriting another
module, add `--privacy-policy /absolute/privacy-policy.json` to the exporter and
packager; see [privacy checks and validation schema](building.md#source-privacy-checks).

```sh
python3 -B scripts/prepare_source.py --source . --output /tmp/tencent-source-candidate \
  --module github.com/huxunpei/nomad-autoscaler-tencent --gofmt /absolute/go/bin/gofmt \
  --mode candidate --version 0.1.0
python3 -B /tmp/tencent-source-candidate/scripts/build_review.py \
  --source /tmp/tencent-source-candidate --output /tmp/tencent-build-candidate \
  --go /absolute/go/bin/go --mode candidate --version 0.1.0
python3 -B /tmp/tencent-source-candidate/scripts/package_release.py \
  --source /tmp/tencent-source-candidate --artifacts /tmp/tencent-build-candidate \
  --output /tmp/tencent-package-candidate --validation /absolute/VALIDATION.json --mode candidate
```

Candidate manifests say `RELEASE_CANDIDATE` and `publication_authorized=false`.
They contain LICENSE/NOTICE and no REVIEW-ONLY.md. Archive names use the intended
release version without a `-review` suffix; names do not imply approval or publication.
The validation record must identify this exact source manifest and state the
actual verification layers. Do not run product or cloud tests solely to refresh
packaging metadata; reuse still-valid evidence with explicit source bindings.

## Approved release mode

After the maintainer explicitly approves the exact candidate and confirms the
source may be published, record that decision in a separate JSON file. No script
creates approval or proves the signer's authority. Do not fill this record before
that human decision. Set `source_publication_confirmed` only after the maintainer
has confirmed the source may be published.

The approval schema is:

```json
{
  "format": 1,
  "decision": "approve-release",
  "module": "github.com/huxunpei/nomad-autoscaler-tencent",
  "version": "0.1.0",
  "candidate_manifest_sha256": "<SHA256 of the exact candidate SOURCE-MANIFEST.json>",
  "approved_by": "<approving GitHub account>",
  "approved_at": "<actual UTC time: YYYY-MM-DDTHH:MM:SSZ>",
  "source_publication_confirmed": true
}
```

Keep the record outside exported inputs. Manually run the exporter and builder
with `--mode release --version 0.1.0 --approval /absolute/approval.json`, using
the same candidate inputs, public module and formatter. Then run the packager
with `--mode release --approval /absolute/approval.json`. Bind the validation
record to the new approved source manifest; retain the candidate evidence.
Changed content, wrong version/module or missing/mismatched approval is rejected.
Approved manifests say `RELEASE_APPROVED`; they do not claim a release was uploaded.

All three tools remain local. GitHub creation, push, visibility changes, tag,
Release creation and distribution are separate explicit actions.
Start the GitHub repository private, set the chosen noreply commit email, push
only the clean public snapshot, and verify CI before requesting the visibility
change and Release publication. Never push the private repository history.

## Package contents and installation

Publish the source archive and both Linux architecture archives together. Binary
archives include tencent-as, tencent-recover, LICENSE, NOTICE, third-party notices,
checksums and build/dependency metadata. Install the official Agent separately.

Verify SHA256SUMS. Adapt the disabled examples, select instance identity or
explicit API-key authentication, and configure `restart_cooldown`, `policy_dir`,
permissions, node identity and persistent state as documented. Complete an
installation walkthrough in the target environment before enabling the policy.
