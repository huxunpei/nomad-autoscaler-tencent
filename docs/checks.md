# Development check entrypoint

Run from an exact, review-only source export on a dedicated Linux host with
Go and its native C compiler installed. Select the Go executable explicitly;
the `go` directive remains the minimum version. Use a new output directory
outside the source tree. No repository visibility, CI service, publication,
cloud identity or resources are changed by this entrypoint.

```sh
python3 -B scripts/check_review.py --source "$PWD" \
  --output /tmp/new-tencent-check-report --go /absolute/go/bin/go
```

The check runs formatting verification without changing files, Python guard
tests, all-package vet/race and module verification. It records each executed
step, return code, real times, log hashes, the source-manifest hash, Go test
parent/subtest event counts and every skip. The Go test timeout is 12 minutes
per package, matching the existing full-suite budget; the outer command is
bounded to 15 minutes. An all-skipped, empty, malformed or
failing test stream is rejected. The source snapshot is verified again after
execution. Failed output remains available and must not be overwritten.

The environment excludes cloud credentials and optional Agent/Nomad runtime
test switches. Caches may be reused. `DEVELOPMENT_CHECKS_PASSED` is development
evidence only: it does not mean runtime, real-cloud or installation verification ran.
Run the pinned official Agent/real Nomad scenarios separately and preserve
their exact inputs and evidence. Existing tests that require those runtimes
are explicitly listed as skipped by this credential-free entrypoint.

Use this command as the check stage of the eventually selected CI platform.
The prepared GitHub workflow uses this entrypoint with read-only repository
permissions and no cloud credentials. The public module and MPL-2.0 license
are recorded in the release guide; this local command does not publish.
After checks, the [review build command](building.md) can produce both Linux
architectures from the same unchanged export. Architecture loading and the
real workload 1-to-5-to-1 test are separate stages.

Candidate and approved release source layouts are also accepted by the check runner. It runs the exporter, builder and packager protection tests; CI prepares candidate-mode inputs and never supplies a publication approval record. See [release modes](release.md).
