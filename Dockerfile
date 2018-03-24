FROM alpine:3.7

RUN apk add --no-cache ca-certificates

ADD kafka-proxy /kafka-proxy

ENTRYPOINT ["/kafka-proxy"]
CMD ["--help"]
