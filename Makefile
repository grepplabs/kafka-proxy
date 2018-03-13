.DEFAULT_GOAL := build

.PHONY: clean build build.local build.linux build.osx build.docker build.docker-build.linux build.docker-build.osx

BINARY        ?= kafka-proxy
SOURCES        = $(shell find . -name '*.go' | grep -v /vendor/)
VERSION       ?= $(shell git describe --tags --always --dirty)
GOPKGS         = $(shell go list ./... | grep -v /vendor/)
BUILD_FLAGS   ?=
LDFLAGS       ?= -X github.com/grepplabs/kafka-proxy/config.Version=$(VERSION) -w -s
TAG           ?= "v0.0.1"

PLATFORM      ?= $(shell uname -s)
ifeq ($(PLATFORM), Darwin)
    BUILD_DOCKER_BUILD=build.docker-build.osx
else
    BUILD_DOCKER_BUILD=build.docker-build.linux
endif

default: build.local

test:
	go test -v -race `go list ./...`

fmt:
	go fmt $(GOPKGS)

check:
	golint $(GOPKGS)
	go vet $(GOPKGS)


build.local: build/$(BINARY)
build.linux: build/linux/$(BINARY)
build.osx: build/osx/$(BINARY)

build: build/$(BINARY)

build/$(BINARY): $(SOURCES)
	CGO_ENABLED=0 go build -o build/$(BINARY) $(BUILD_FLAGS) -ldflags "$(LDFLAGS)" .

build/linux/$(BINARY): $(SOURCES)
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o build/linux/$(BINARY) $(BUILD_FLAGS) -ldflags "$(LDFLAGS)" .

build/osx/$(BINARY): $(SOURCES)
	GOOS=darwin GOARCH=amd64 CGO_ENABLED=0 go build -o build/osx/$(BINARY) $(BUILD_FLAGS) -ldflags "$(LDFLAGS)" .

build.docker-build: $(BUILD_DOCKER_BUILD)

build.docker-build.linux:
	set -e ;\
    buildContainerName=${BINARY}-buildcontainer ;\
    docker build -t $$buildContainerName --build-arg target=build.linux -f Dockerfile.build . ;\
    buildContainer=$$(docker create $$buildContainerName) ;\
    echo "containerId: $$buildContainer" ;\
    mkdir -p build ;\
    docker cp $$buildContainer:/go/src/github.com/grepplabs/kafka-proxy/build/linux/${BINARY} build/${BINARY} ;\
    docker rm $$buildContainer ;\
    docker rmi $$buildContainerName ;\

build.docker-build.osx:
	set -e ;\
    buildContainerName=${BINARY}-buildcontainer ;\
    docker build -t $$buildContainerName --build-arg target=build.osx -f Dockerfile.build . ;\
    buildContainer=$$(docker create $$buildContainerName) ;\
    echo "containerId: $$buildContainer" ;\
    mkdir -p build ;\
    docker cp $$buildContainer:/go/src/github.com/grepplabs/kafka-proxy/build/osx/${BINARY} build/${BINARY} ;\
    docker rm $$buildContainer ;\
    docker rmi $$buildContainerName ;\

release: clean build.linux build/osx/$(BINARY)
	git tag $(TAG) && git push --tags
	github-release release -u grepplabs -r $(BINARY) --tag $(TAG)
	github-release upload -u grepplabs -r $(BINARY) -t $(TAG) -f build/linux/$(BINARY) -n linux/amd64/$(BINARY)
	github-release upload -u grepplabs -r $(BINARY) -t $(TAG) -f build/osx/$(BINARY) -n darwin/amd64/$(BINARY)
	github-release info -u grepplabs -r $(BINARY)

protoc.local-auth:
	protoc -I plugin/local-auth/proto/ plugin/local-auth/proto/auth.proto --go_out=plugins=grpc:plugin/local-auth/proto/

plugin.auth-user:
	CGO_ENABLED=0 go build -o build/auth-user $(BUILD_FLAGS) -ldflags "$(LDFLAGS)" cmd/plugin-auth-user/main.go

plugin.auth-ldap:
	CGO_ENABLED=0 go build -o build/auth-ldap $(BUILD_FLAGS) -ldflags "$(LDFLAGS)" cmd/plugin-auth-ldap/main.go

clean:
	@rm -rf build
