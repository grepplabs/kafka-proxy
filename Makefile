.DEFAULT_GOAL := build

.PHONY: clean build all tag release

ROOT_DIR       = $(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))

BINARY        ?= kafka-proxy
SOURCES        = $(shell find . -name '*.go' | grep -v /vendor/)
VERSION       ?= $(shell git describe --tags --always --dirty)
GOPKGS         = $(shell go list ./... | grep -v /vendor/)
BUILD_FLAGS   ?=
LDFLAGS       ?= -X github.com/grepplabs/kafka-proxy/config.Version=$(VERSION) -w -s
TAG           ?= "v0.3.0"
GOARCH        ?= amd64
GOOS          ?= linux
TOOLS_DIR   := .tools
PROTOC_VERSION ?= 3.18.1
PROTOC_GO_VERSION ?= v1.27.1
PROTOC_GRPC_VERSION ?= v1.1
PROTOC      := $(TOOLS_DIR)/protoc

default: build

dep-check:
ifeq ("$(wildcard $(PROTOC))","")
	$(error Could not find protoc install, please run 'make protoc')
endif

test.race:
	go test -v -race -count=1 `go list ./...`

test:
	go test -v -count=1 `go list ./...`

fmt:
	go fmt $(GOPKGS)

check:
	golint $(GOPKGS)
	go vet $(GOPKGS)


build: build/$(BINARY)

build/$(BINARY): $(SOURCES)
	GOOS=$(GOOS) GOARCH=$(GOARCH) CGO_ENABLED=0 go build -o build/$(BINARY) $(BUILD_FLAGS) -ldflags "$(LDFLAGS)" .

tag:
	git tag $(TAG)

release: clean
	git push origin $(TAG)
	rm -rf $(ROOT_DIR)/dist
	curl -sL https://git.io/goreleaser | bash

protoc: protoc_plugin
	@{ \
		set -e;\
		mkdir -p $(TOOLS_DIR) ;\
		cd $(TOOLS_DIR);\
	    wget -q https://github.com/protocolbuffers/protobuf/releases/download/v$(PROTOC_VERSION)/protoc-$(PROTOC_VERSION)-linux-x86_64.zip ;\
	    unzip -qoj protoc-$(PROTOC_VERSION)-linux-x86_64.zip bin/protoc ;\
	    rm -f protoc-$(PROTOC_VERSION)-linux-x86_64.zip ;\
	}

protoc_plugin:
	# get go plugin
	go install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GO_VERSION)
	# get protoc plugin
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$(PROTOC_GRPC_VERSION)


protoc.local-auth: dep-check $(GRPC_PLUGIN)
	$(PROTOC) -I plugin/local-auth/proto/ plugin/local-auth/proto/auth.proto --go_out=paths=source_relative:plugin/local-auth/proto/ --go-grpc_out=paths=source_relative:plugin/local-auth/proto/

protoc.token-provider: dep-check $(GRPC_PLUGIN)
	$(PROTOC) -I plugin/token-provider/proto/ plugin/token-provider/proto/token-provider.proto --go_out=paths=source_relative:plugin/token-provider/proto/ --go-grpc_out=paths=source_relative:plugin/token-provider/proto/

protoc.token-info: dep-check $(GRPC_PLUGIN)
	$(PROTOC) -I plugin/token-info/proto/ plugin/token-info/proto/token-info.proto --go_out=paths=source_relative:plugin/token-info/proto/ --go-grpc_out=paths=source_relative:plugin/token-info/proto/

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
