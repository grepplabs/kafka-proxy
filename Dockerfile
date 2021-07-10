FROM golang:1.16-alpine3.14 as builder
RUN apk add alpine-sdk ca-certificates

WORKDIR /go/src/github.com/grepplabs/kafka-proxy
COPY . .

ARG MAKE_TARGET=build
ARG GOOS=linux
ARG GOARCH=amd64
RUN make -e GOARCH=${GOARCH} -e GOOS=${GOOS} clean ${MAKE_TARGET}

FROM alpine:3.14
RUN apk add --no-cache ca-certificates

COPY --from=builder /go/src/github.com/grepplabs/kafka-proxy/build /opt/kafka-proxy/bin
ENTRYPOINT ["/opt/kafka-proxy/bin/kafka-proxy"]
CMD ["--help"]
