## kafka-proxy
**Work in progress**

[![Build Status](https://travis-ci.org/grepplabs/kafka-proxy.svg?branch=master)](https://travis-ci.org/grepplabs/kafka-proxy)


The Kafka Proxy is based on idea of [Cloud SQL Proxy](https://github.com/GoogleCloudPlatform/cloudsql-proxy). 
It allows a service to connect to Kafka brokers without having to deal with SASL/PLAIN authentication and SSL certificates.  

It works by opening tcp sockets on the local machine and proxying connections to the associated Kafka brokers
when the sockets are used. The host and port in [Metadata](http://kafka.apache.org/protocol.html#The_Messages_Metadata)
and [FindCoordinator](http://kafka.apache.org/protocol.html#The_Messages_FindCoordinator)
responses received from the brokers are replaced by local counterparts.
For discovered brokers (not configured as the boostrap servers), local listeners are started on random ports.
The dynamic local listeners feature can be disabled and an additional list of external server mappings can be provided.

The Proxy can terminate TLS traffic and authenticate users locally using SASL/PLAIN. The credentials check method 
is configurable by providing dynamically linked shared object library.   

Kafka API calls can be restricted to prevent some operations e.g. topic deletion.


See:
* [A Guide To The Kafka Protocol](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol)
* [Kafka protocol guide](http://kafka.apache.org/protocol.html)

### Building

    make build.docker-build

### Usage example
	
	build/kafka-proxy server --bootstrap-server-mapping "192.168.99.100:32400,0.0.0.0:32399"
	
	build/kafka-proxy server --bootstrap-server-mapping "192.168.99.100:32400,127.0.0.1:32400" \
	                         --bootstrap-server-mapping "192.168.99.100:32401,127.0.0.1:32401" \
	                         --bootstrap-server-mapping "192.168.99.100:32402,127.0.0.1:32402"
    
    build/kafka-proxy server --bootstrap-server-mapping "kafka-0.grepplabs.com:9093,0.0.0.0:32399" \
                             --tls-enable --tls-insecure-skip-verify \
                             --sasl-enable -sasl-username myuser --sasl-password mysecret

    build/kafka-proxy server --proxy-listener-key-file "server-key.pem"  \
                             --proxy-listener-cert-file "server-cert.pem" \
                             --proxy-listener-ca-chain-cert-file "ca.pem" \
                             --proxy-listener-tls-enable \
                             --proxy-listener-auth-enable \
                             --dynamic-listeners-disable \
                             --forbidden-api-keys 20 \
                             --bootstrap-server-mapping "192.168.99.100:32400,127.0.0.1:32401" \
                             --external-server-mapping "192.168.99.100:32401,127.0.0.1:32402" \
                             --external-server-mapping "192.168.99.100:32402,127.0.0.1:32403"

### What should be done

* [x] Metadata response versions V0,V1,V2,V3,V4 and V5
* [x] Find coordinator response versions V0 and V1
* [X] TLS
* [X] PLAIN/SASL
* [X] Request / reponse deadlines - socket reads/writes
* [X] Health endpoint
* [X] Prometheus metrics
  1. gauge: proxy_opened_connections {broker}
  2. counter: proxy_requests_total {broker, api_key, api_version}
  3. counter: proxy_connections_total {broker}
  4. counter: proxy_requests_bytes {broker}
  5. counter: proxy_responses_bytes {broker}
* [ ] Plugable local authentication
* [ ] Deploying Kafka Proxy as a sidecar container
* [ ] Performance tests and tuning
* [ ] Socket buffer sizing e.g. SO_RCVBUF = 32768, SO_SNDBUF = 131072
* [ ] Kafka connect tests
* [ ] Different Kafka API versions tests
* [ ] Unit and integration tests
* [ ] Rolling upgrade test
* [ ] Graceful shutdown

### Embedded third-party source code 

* [Cloud SQL Proxy](https://github.com/GoogleCloudPlatform/cloudsql-proxy)
* [Sarama](https://github.com/Shopify/sarama)