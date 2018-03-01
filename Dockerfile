FROM scratch
ADD kafka-proxy /kafka-proxy
ENTRYPOINT ["/kafka-proxy"]
CMD ["--help"]
