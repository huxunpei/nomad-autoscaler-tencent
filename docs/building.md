# Reproducible source builds and packages

`scripts/build_review.py` keeps its existing name and supports three explicit modes:

| Mode | Required source status | Publication authorization |
| --- | --- | --- |
| review (default) | REVIEW_ONLY | false; retains the review marker |
| candidate | RELEASE_CANDIDATE | false; no review marker |
| release | RELEASE_APPROVED | exact matching human approval record required |

See [release preparation](release.md) for candidate and manually approved release commands.
`--version` must match the candidate/release manifest. Existing, nested or symlink
outputs and changed/missing/extra source files are rejected. The builder never
publishes, modifies Git or forwards cloud credentials.

## Source privacy checks

When exporting from a different module namespace, pass `--privacy-policy` to
`scripts/prepare_source.py`. The JSON file must be outside the exported inputs:

```json
{"format": 1, "deny_literals": ["private.example.invalid", "@corp.example.invalid"]}
```

Rules match case-insensitively. A missing, malformed, empty, symlinked or changing
policy is rejected. The policy and its literal values are not copied into the
source manifest or archive. Keep organization-specific rules outside public
source. A same-module export, including public CI, can run without this file;
the built-in credential, resource, account, private-address and home-path checks
still apply. Tests use explicit synthetic values and receive no blanket exemption.
These bounded checks supplement review; they do not certify publication rights
or detect every possible encoding of sensitive data.

Pass the same `--privacy-policy` to `scripts/package_release.py` when packaging
with additional private rules. Public `VALIDATION.json` uses format 2 and only
the fields accepted by the packager. `installation_verified` describes an
installation check of the packaged release. The `remaining` values are
`source_publication_authorization`, `github_ci`, `installation_verification`
and `publication`; keep project-specific correspondence in separate records.

## Build and package verification

The builder emits four binaries (tencent-as and tencent-recover for Linux amd64
and arm64), their build-info records, SHA256SUMS, BUILD-MANIFEST.json, the input
source manifest, module-cache verification and DEPENDENCIES.json. Builds use
CGO_ENABLED=0, trimpath, buildvcs=false and pgo=off. A supplied revision is a
provenance declaration, not an automatically verified public Git commit.

Review builds collect root license candidates as UNREVIEWED. Candidate/release
builds additionally compare the exact module/toolchain set, go.sum hash and every
indexed root/nested notice against [dependency-licenses.json](dependency-licenses.json)
and the original text in [third-party notices](../THIRD_PARTY_NOTICES.md).
The index checks raw file bytes; embedded Markdown text is compared with line endings normalized.
Only successful content verification records NOTICES_VERIFIED. This status is a
byte/provenance check, not a legal clearance or substitute for publication approval.

`scripts/package_release.py` checks source/build/validation bindings, binary and
build-info hashes, notice inputs and safe archive paths before writing packages.
It uses deterministic archive metadata and normal versioned filenames. It never
turns a candidate into an approved release by renaming it or changing a flag.

For reproducibility, build identical source snapshots in different directories
with the same installed toolchain and compare all four binary hashes. Disclose
shared caches. Equal hashes establish that tested combination, not arbitrary
cross-version/toolchain reproducibility. Compilation, architecture loading,
real-cloud evidence, GitHub CI and installation verification are separate results.
