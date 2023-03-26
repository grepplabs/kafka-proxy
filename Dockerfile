FROM --platform=$BUILDPLATFORM golang:1.20-alpine3.17 as builder
RUN apk add alpine-sdk ca-certificates

ARG TARGETOS
ARG TARGETARCH
ARG TARGETVARIANT
ARG VERSION

ENV CGO_ENABLED=0 \
    GO111MODULE=on \
    GOOS=${TARGETOS} \
    GOARCH=${TARGETARCH} \
    GOARM=${TARGETVARIANT} \
    LDFLAGS="-X github.com/grepplabs/kafka-proxy/config.Version=${VERSION} -w -s"

WORKDIR /go/src/github.com/grepplabs/kafka-proxy
COPY . .

RUN mkdir -p build && \
    export GOARM=$( echo "${GOARM}" | cut -c2-) && \
    go build -mod=vendor -o build/kafka-proxy \
    -ldflags "${LDFLAGS}" .

FROM --platform=$BUILDPLATFORM alpine:3.17
RUN apk add --no-cache ca-certificates
RUN adduser \
        --disabled-password \
        --gecos "" \
        --home "/nonexistent" \
        --shell "/sbin/nologin" \
        --no-create-home \
        kafka-proxy

COPY --from=builder /go/src/github.com/grepplabs/kafka-proxy/build /opt/kafka-proxy/bin
USER kafka-proxy
ENTRYPOINT ["/opt/kafka-proxy/bin/kafka-proxy"]
CMD ["--help"]

