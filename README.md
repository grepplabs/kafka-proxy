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
                             --tls-enable true --tls-insecure-skip-verify true \
                             --sasl-enable true --sasl-username myuser --sasl-password mysecret

### What should be done

* [x] Metadata response versions V0,V1,V2,V3,V4 and V5
* [x] Find coordinator response versions V0 and V1
* [X] TLS
* [X] PLAIN/SASL
* [ ] Request / reponse deadlines - socket reads/writes  
* [ ] Json logging
* [ ] Panic handler
* [ ] Health check endpoint
* [ ] Prometheus metrics
* [ ] Performance tests and tuning
* [ ] Socket buffer sizing e.g. SO_RCVBUF = 32768, SO_SNDBUF = 131072
* [ ] Kafka connect tests
* [ ] Different Kafka API versions tests
* [ ] Unit and integration tests
* [ ] Deploying Kafka Proxy as a sidecar container
* [ ] Graceful shutdown

### Embedded third-party source code 

* [Cloud SQL Proxy](https://github.com/GoogleCloudPlatform/cloudsql-proxy)
* [Sarama](https://github.com/Shopify/sarama)