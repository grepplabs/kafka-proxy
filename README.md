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

The Proxy can terminate TLS traffic and authenticate users using SASL/PLAIN. The credentials verification method
is configurable and uses golang plugin system over RPC.

Kafka API calls can be restricted to prevent some operations e.g. topic deletion or produce requests.


See:
* [A Guide To The Kafka Protocol](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol)
* [Kafka protocol guide](http://kafka.apache.org/protocol.html)


### Install binary release

1. Download the latest release

   Linux

        curl -Lso kafka-proxy https://github.com/grepplabs/kafka-proxy/releases/download/v0.0.1/linux.amd64.kafka-proxy

   macOS

        curl -Lso kafka-proxy https://github.com/grepplabs/kafka-proxy/releases/download/v0.0.1/darwin.amd64.kafka-proxy

2. Make the kafka-proxy binary executable

    ```
    chmod +x ./kafka-proxy
    ```

3. Move the binary in to your PATH.

    ```
    sudo mv ./kafka-proxy /usr/local/bin/kafka-proxy
    ```

### Building

    make build.docker-build

### Usage example
	
	build/kafka-proxy server --bootstrap-server-mapping "192.168.99.100:32400,0.0.0.0:32399"
	
	build/kafka-proxy server --bootstrap-server-mapping "192.168.99.100:32400,127.0.0.1:32400" \
	                         --bootstrap-server-mapping "192.168.99.100:32401,127.0.0.1:32401" \
	                         --bootstrap-server-mapping "192.168.99.100:32402,127.0.0.1:32402" \
	                         --dynamic-listeners-disable

	build/kafka-proxy server --bootstrap-server-mapping "192.168.99.100:32400,127.0.0.1:32400" \
	                         --external-server-mapping "192.168.99.100:32401,127.0.0.1:32402" \
	                         --external-server-mapping "192.168.99.100:32402,127.0.0.1:32403" \
	                         --forbidden-api-keys 20
    
    build/kafka-proxy server --bootstrap-server-mapping "kafka-0.grepplabs.com:9093,0.0.0.0:32399" \
                             --tls-enable --tls-insecure-skip-verify \
                             --sasl-enable -sasl-username myuser --sasl-password mysecret

### Proxy authentication example

    make clean build plugin.auth-user && build/kafka-proxy server --proxy-listener-key-file "server-key.pem"  \
                             --proxy-listener-cert-file "server-cert.pem" \
                             --proxy-listener-ca-chain-cert-file "ca.pem" \
                             --proxy-listener-tls-enable \
                             --proxy-listener-auth-enable \
                             --proxy-listener-auth-command build/auth-user \
                             --proxy-listener-auth-param "--username=my-test-user" \
                             --proxy-listener-auth-param "--password=my-test-password"

    make clean build plugin.auth-ldap && build/kafka-proxy server \
                             --proxy-listener-auth-enable \
                             --proxy-listener-auth-command build/auth-ldap \
                             --proxy-listener-auth-param "--url=ldaps://ldap.example.com:636" \
                             --proxy-listener-auth-param "--user-dn=cn=users,dc=exemple,dc=com" \
                             --proxy-listener-auth-param "--user-attr=uid" \
                             --bootstrap-server-mapping "192.168.99.100:32400,127.0.0.1:32400"


### Kubernetes sidecar container example

```yaml

---
apiVersion: apps/v1
kind: Deployment
metadata:
   name: myapp
spec:
  replicas: 1
  template:
    metadata:
      labels:
        app: myapp
      annotations:
        prometheus.io/scrape: 'true'
    spec:
      containers:
        - name: kafka-proxy
          image: grepplabs/kafka-proxy:latest
          args:
            - 'server'
            - '--log-format=json'
            - '--bootstrap-server-mapping=kafka-0:9093,127.0.0.1:32400'
            - '--bootstrap-server-mapping=kafka-1:9093,127.0.0.1:32401'
            - '--bootstrap-server-mapping=kafka-2:9093,127.0.0.1:32402'
            - '--tls-enable'
            - '--tls-ca-chain-cert-file=/var/run/secret/kafka-ca-chain-certificate/ca-chain.cert.pem'
            - '--tls-client-cert-file=/var/run/secret/kafka-client-certificate/client.cert.pem'
            - '--tls-client-key-file=/var/run/secret/kafka-client-key/client.key.pem'
            - '--tls-client-key-password=$(TLS_CLIENT_KEY_PASSWORD)'
            - '--sasl-enable'
            - '--sasl-jaas-config-file=/var/run/secret/kafka-client-jaas/jaas.config'
          env:
          - name: TLS_CLIENT_KEY_PASSWORD
            valueFrom:
              secretKeyRef:
                name: tls-client-key-password
                key: password
          volumeMounts:
          - name: "sasl-jaas-config-file"
            mountPath: "/var/run/secret/kafka-client-jaas"
          - name: "tls-ca-chain-certificate"
            mountPath: "/var/run/secret/kafka-ca-chain-certificate"
          - name: "tls-client-cert-file"
            mountPath: "/var/run/secret/kafka-client-certificate"
          - name: "tls-client-key-file"
            mountPath: "/var/run/secret/kafka-client-key"
          ports:
          - name: metrics
            containerPort: 9080
          livenessProbe:
            httpGet:
              path: /health
              port: 9080
            initialDelaySeconds: 5
            periodSeconds: 3
          readinessProbe:
            httpGet:
              path: /health
              port: 9080
            initialDelaySeconds: 5
            periodSeconds: 10
            timeoutSeconds: 5
            successThreshold: 2
            failureThreshold: 5
        - name: myapp
          image: myapp:latest
          ports:
          - containerPort: 8080
            name: metrics
          env:
          - name: BOOTSTRAP_SERVERS
            value: "127.0.0.1:32400,127.0.0.1:32401,127.0.0.1:32402"
      volumes:
      - name: sasl-jaas-config-file
        secret:
          secretName: sasl-jaas-config-file
      - name: tls-ca-chain-certificate
        secret:
          secretName: tls-ca-chain-certificate
      - name: tls-client-cert-file
        secret:
          secretName: tls-client-cert-file
      - name: tls-client-key-file
        secret:
          secretName: tls-client-key-file
```

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
* [X] Pluggable proxy authentication
* [X] Deploying Kafka Proxy as a sidecar container
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