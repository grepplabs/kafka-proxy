.DEFAULT_GOAL := build

.PHONY: clean build all tag release

ROOT_DIR       = $(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))

BINARY        ?= kafka-proxy
SOURCES        = $(shell find . -name '*.go' | grep -v /vendor/)
VERSION       ?= $(shell git describe --tags --always --dirty)
GOPKGS         = $(shell go list ./... | grep -v /vendor/)
BUILD_FLAGS   ?=
LDFLAGS       ?= -X github.com/grepplabs/kafka-proxy/config.Version=$(VERSION) -w -s
TAG           ?= "v0.4.3"
REPO		  ?= "grepplabs/kafka-proxy"

PROTOC_GO_VERSION ?= v1.33
PROTOC_GRPC_VERSION ?= v1.2
PROTOC_VERSION ?= 22.2
PROTOC_BIN_DIR := .tools
PROTOC := $(PROTOC_BIN_DIR)/protoc

GOLANGCI_LINT = go run github.com/golangci/golangci-lint/cmd/golangci-lint@v1.63.4

default: build

test.race:
	go test -v -race -count=1 -mod=vendor `go list ./...`

test:
	go test -v -count=1 -mod=vendor `go list ./...`

fmt:
	go fmt $(GOPKGS)

check: lint-code
	go vet $(GOPKGS)

.PHONY: lint-code
lint-code:
	$(GOLANGCI_LINT) run --timeout 5m

.PHONY: lint-fix
lint-fix:
	$(GOLANGCI_LINT) run --fix

.PHONY: build
build: build/$(BINARY)

.PHONY: build/$(BINARY)
build/$(BINARY): $(SOURCES)
	CGO_ENABLED=0 go build -mod=vendor -o build/$(BINARY) $(BUILD_FLAGS) -ldflags "$(LDFLAGS)" .

docker.build:
	docker build --build-arg VERSION=$(VERSION) -t local/kafka-proxy .

docker.build.all:
	docker build --build-arg VERSION=$(VERSION) -t local/kafka-proxy -f Dockerfile.all .

docker.build.multiarch:
	docker buildx build \
		--platform linux/amd64,linux/arm64 \
		--push \
	    --build-arg VERSION=$(VERSION) \
		-t $(REPO):$(TAG) \
		.

tag:
	git tag $(TAG)

release: clean
	git push origin $(TAG)

protoc.plugin.install:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GO_VERSION)
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$(PROTOC_GRPC_VERSION)

protoc.bin.install: protoc.plugin.install
	@{ \
		set -e;\
		mkdir -p $(PROTOC_BIN_DIR) ;\
		cd $(PROTOC_BIN_DIR);\
	    wget https://github.com/protocolbuffers/protobuf/releases/download/v$(PROTOC_VERSION)/protoc-$(PROTOC_VERSION)-linux-x86_64.zip ;\
	    unzip -qoj protoc-$(PROTOC_VERSION)-linux-x86_64.zip bin/protoc ;\
	    rm -f protoc-$(PROTOC_VERSION)-linux-x86_64.zip ;\
	}


dep-check:
ifeq ("$(wildcard $(PROTOC))","")
	$(error Could not find protoc install, please run 'make protoc.install')
endif

protoc.local-auth: dep-check
	$(PROTOC) -I plugin/local-auth/proto/ plugin/local-auth/proto/auth.proto --go_out=paths=source_relative:plugin/local-auth/proto/ --go-grpc_out=paths=source_relative:plugin/local-auth/proto/

protoc.token-provider: dep-check
	$(PROTOC) -I plugin/token-provider/proto/ plugin/token-provider/proto/token-provider.proto --go_out=paths=source_relative:plugin/token-provider/proto/ --go-grpc_out=paths=source_relative:plugin/token-provider/proto/

protoc.token-info: dep-check
	$(PROTOC) -I plugin/token-info/proto/ plugin/token-info/proto/token-info.proto --go_out=paths=source_relative:plugin/token-info/proto/ --go-grpc_out=paths=source_relative:plugin/token-info/proto/

.PHONY: protoc
protoc: protoc.local-auth protoc.token-provider protoc.token-info

plugin.auth-user:
	CGO_ENABLED=0 go build -o build/auth-user $(BUILD_FLAGS) -ldflags "$(LDFLAGS)" cmd/plugin-auth-user/main.go

plugin.auth-ldap:
	CGO_ENABLED=0 go build -o build/auth-ldap $(BUILD_FLAGS) -ldflags "$(LDFLAGS)" cmd/plugin-auth-ldap/main.go

plugin.google-id-provider:
	CGO_ENABLED=0 go build -o build/google-id-provider $(BUILD_FLAGS) -ldflags "$(LDFLAGS)" cmd/plugin-googleid-provider/main.go

plugin.google-id-info:
	CGO_ENABLED=0 go build -o build/google-id-info $(BUILD_FLAGS) -ldflags "$(LDFLAGS)" cmd/plugin-googleid-info/main.go

plugin.unsecured-jwt-info:
	CGO_ENABLED=0 go build -o build/unsecured-jwt-info $(BUILD_FLAGS) -ldflags "$(LDFLAGS)" cmd/plugin-unsecured-jwt-info/main.go

plugin.unsecured-jwt-provider:
	CGO_ENABLED=0 go build -o build/unsecured-jwt-provider $(BUILD_FLAGS) -ldflags "$(LDFLAGS)" cmd/plugin-unsecured-jwt-provider/main.go

plugin.oidc-provider:
	CGO_ENABLED=0 go build -o build/oidc-provider $(BUILD_FLAGS) -ldflags "$(LDFLAGS)" cmd/plugin-oidc-provider/main.go

all: build plugin.auth-user plugin.auth-ldap plugin.google-id-provider plugin.google-id-info plugin.unsecured-jwt-info plugin.unsecured-jwt-provider plugin.oidc-provider

clean:
	rm -rf $(ROOT_DIR)/build
