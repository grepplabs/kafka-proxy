FROM golang:1.12 as builder

ARG GOOS=linux
ARG GOARCH=amd64

WORKDIR /go/src/github.com/grepplabs/kafka-proxy
COPY . .
RUN make -e GOARCH=${GOARCH} -e GOOS=${GOOS} clean build

FROM alpine:3.7

RUN apk add --no-cache ca-certificates

COPY --from=builder /go/src/github.com/grepplabs/kafka-proxy/build/kafka-proxy /kafka-proxy

ENTRYPOINT ["/kafka-proxy"]
CMD ["--help"]
