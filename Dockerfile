FROM --platform=${BUILDPLATFORM} golang:1.23-alpine3.21 AS builder

RUN apk add --no-cache alpine-sdk ca-certificates

ARG TARGETARCH
ARG TARGETOS
ARG VERSION

ENV CGO_ENABLED=0 \
    GO111MODULE=on \
    LDFLAGS="-X github.com/grepplabs/kafka-proxy/config.Version=${VERSION} -w -s"

WORKDIR /go/src/github.com/grepplabs/kafka-proxy
COPY . .

RUN mkdir -p build && \
    GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -mod=vendor -o build/kafka-proxy -ldflags "${LDFLAGS}" .

FROM alpine:3.21

RUN apk add --no-cache ca-certificates libcap && \
    adduser --disabled-password --gecos "" \
            --home "/nonexistent" --shell "/sbin/nologin" \
            --no-create-home kafka-proxy

COPY --from=builder /go/src/github.com/grepplabs/kafka-proxy/build /opt/kafka-proxy/bin
RUN setcap 'cap_net_bind_service=+ep' /opt/kafka-proxy/bin/kafka-proxy

USER kafka-proxy
ENTRYPOINT ["/opt/kafka-proxy/bin/kafka-proxy"]
CMD ["--help"]
