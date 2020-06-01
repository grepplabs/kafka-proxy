.DEFAULT_GOAL := build

.PHONY: clean build build.docker tag all

BINARY        ?= kafka-proxy
SOURCES        = $(shell find . -name '*.go' | grep -v /vendor/)
VERSION       ?= $(shell git describe --tags --always --dirty)
GOPKGS         = $(shell go list ./... | grep -v /vendor/)
BUILD_FLAGS   ?=
LDFLAGS       ?= -X github.com/grepplabs/kafka-proxy/config.Version=$(VERSION) -w -s
TAG           ?= "v0.2.5"
GOARCH        ?= amd64
GOOS          ?= linux

default: build

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

docker.build:
	docker build --build-arg GOOS=$(GOOS) --build-arg  GOARCH=$(GOARCH) -f Dockerfile.build .

tag:
	git tag $(TAG)

release: clean
	git push origin $(TAG)
	rm -rf ./dist
	curl -sL https://git.io/goreleaser | bash

protoc.local-auth:
	protoc -I plugin/local-auth/proto/ plugin/local-auth/proto/auth.proto --go_out=plugins=grpc:plugin/local-auth/proto/

protoc.token-provider:
	protoc -I plugin/token-provider/proto/ plugin/token-provider/proto/token-provider.proto --go_out=plugins=grpc:plugin/token-provider/proto/

protoc.token-info:
	protoc -I plugin/token-info/proto/ plugin/token-info/proto/token-info.proto --go_out=plugins=grpc:plugin/token-info/proto/

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

plugin.opa-provider:
	CGO_ENABLED=0 go build -o build/opa-provider $(BUILD_FLAGS) -ldflags "$(LDFLAGS)" cmd/plugin-opa-provider/main.go

all: build plugin.auth-user plugin.auth-ldap plugin.google-id-provider plugin.google-id-info plugin.unsecured-jwt-info plugin.unsecured-jwt-provider plugin.oidc-provider plugin.opa-provider

clean:
	@rm -rf build
