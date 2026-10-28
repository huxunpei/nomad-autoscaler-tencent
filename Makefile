GO ?= go
export GOPROXY ?= https://proxy.golang.org,direct

PACKAGES := . ./cmd/... ./internal/... ./plugin/...

.PHONY: test race build cross-build smoke review-artifacts review-checks

test:
	$(GO) test -mod=readonly $(PACKAGES)

race:
	$(GO) test -race -mod=readonly $(PACKAGES)

build:
	mkdir -p bin
	$(GO) build -mod=readonly -trimpath -o bin/tencent-as .
	$(GO) build -mod=readonly -trimpath -o bin/tencent-snapshot ./cmd/tencent-snapshot
	$(GO) build -mod=readonly -trimpath -o bin/tencent-contract ./cmd/tencent-contract
	$(GO) build -mod=readonly -trimpath -o bin/nomad-node-probe ./cmd/nomad-node-probe
	$(GO) build -mod=readonly -trimpath -o bin/tencent-recover ./cmd/tencent-recover

cross-build:
	mkdir -p dist/linux-amd64 dist/linux-arm64
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(GO) build -mod=readonly -trimpath -o dist/linux-amd64/tencent-as .
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 $(GO) build -mod=readonly -trimpath -o dist/linux-arm64/tencent-as .
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(GO) build -mod=readonly -trimpath -o dist/linux-amd64/tencent-recover ./cmd/tencent-recover
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 $(GO) build -mod=readonly -trimpath -o dist/linux-arm64/tencent-recover ./cmd/tencent-recover

smoke: build
	python3 scripts/agent_smoke.py --agent "$(AUTOSCALER)" --plugin bin/tencent-as

# Run from a review-source export; output must be NEW and outside that tree.
review-artifacts:
	@test -n "$(REVIEW_OUTPUT)" || (echo "REVIEW_OUTPUT is required" >&2; exit 1)
	python3 -B scripts/build_review.py --source "$(CURDIR)" --output "$(REVIEW_OUTPUT)" --go "$(GO)"

# Credential-free checks from an exact review-source export; no runtime services.
review-checks:
	@test -n "$(REVIEW_CHECK_OUTPUT)" || (echo "REVIEW_CHECK_OUTPUT is required" >&2; exit 1)
	python3 -B scripts/check_review.py --source "$(CURDIR)" --output "$(REVIEW_CHECK_OUTPUT)" --go "$(GO)"
