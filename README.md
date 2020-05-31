## kafka-proxy
**Work in progress**

[![Build Status](https://travis-ci.org/grepplabs/kafka-proxy.svg?branch=master)](https://travis-ci.org/grepplabs/kafka-proxy)
[![Docker Hub](https://img.shields.io/badge/docker-latest-blue.svg)](https://hub.docker.com/r/grepplabs/kafka-proxy)
[![Docker Pulls](https://img.shields.io/docker/pulls/grepplabs/kafka-proxy)](https://hub.docker.com/r/grepplabs/kafka-proxy)

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

The proxies can also authenticate each other using a pluggable method which is transparent to other Kafka servers and clients.
Currently the Google ID Token for service accounts is implemented i.e. proxy client requests and sends service account JWT and proxy server receives and validates it against Google JWKS.

Kafka API calls can be restricted to prevent some operations e.g. topic deletion or produce requests.


See:
* [Kafka Proxy with Amazon MKS](https://gist.github.com/everesio/262e11c6e5cebf56f1d5111c8cd7da3f)
* [A Guide To The Kafka Protocol](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol)
* [Kafka protocol guide](http://kafka.apache.org/protocol.html)


### Install binary release

1. Download the latest release

   Linux

        curl -Ls https://github.com/grepplabs/kafka-proxy/releases/download/v0.2.2/kafka-proxy-v0.2.2-linux-amd64.tar.gz | tar xz

   macOS

        curl -Ls https://github.com/grepplabs/kafka-proxy/releases/download/v0.2.2/kafka-proxy-v0.2.2-darwin-amd64.tar.gz | tar xz

2. Move the binary in to your PATH.

    ```
    sudo mv ./kafka-proxy /usr/local/bin/kafka-proxy
    ```

### Building

    make clean build

### Help output

    Run the kafka-proxy server

    Usage:
      kafka-proxy server [flags]

    Flags:
          --auth-gateway-client-command string             Path to authentication plugin binary
          --auth-gateway-client-enable                     Enable gateway client authentication
          --auth-gateway-client-log-level string           Log level of the auth plugin (default "trace")
          --auth-gateway-client-magic uint                 Magic bytes sent in the handshake
          --auth-gateway-client-method string              Authentication method
          --auth-gateway-client-param stringArray          Authentication plugin parameter
          --auth-gateway-client-timeout duration           Authentication timeout (default 10s)
          --auth-gateway-server-command string             Path to authentication plugin binary
          --auth-gateway-server-enable                     Enable proxy server authentication
          --auth-gateway-server-log-level string           Log level of the auth plugin (default "trace")
          --auth-gateway-server-magic uint                 Magic bytes sent in the handshake
          --auth-gateway-server-method string              Authentication method
          --auth-gateway-server-param stringArray          Authentication plugin parameter
          --auth-gateway-server-timeout duration           Authentication timeout (default 10s)
          --auth-local-command string                      Path to authentication plugin binary
          --auth-local-enable                              Enable local SASL/PLAIN authentication performed by listener - SASL handshake will not be passed to kafka brokers
          --auth-local-log-level string                    Log level of the auth plugin (default "trace")
          --auth-local-mechanism string                    SASL mechanism used for local authentication: PLAIN or OAUTHBEARER (default "PLAIN")
          --auth-local-param stringArray                   Authentication plugin parameter
          --auth-local-timeout duration                    Authentication timeout (default 10s)
          --bootstrap-server-mapping stringArray           Mapping of Kafka bootstrap server address to local address (host:port,host:port(,advhost:advport))
          --debug-enable                                   Enable Debug endpoint
          --debug-listen-address string                    Debug listen address (default "0.0.0.0:6060")
          --default-listener-ip string                     Default listener IP (default "127.0.0.1")
          --dial-address-mapping stringArray               Mapping of target broker address to new one (host:port,host:port). The mapping is performed during connection establishment
          --dynamic-listeners-disable                      Disable dynamic listeners.
          --dynamic-sequential-min-port int                If set to non-zero, makes the dynamic listener use a sequential port starting with this value rather than a random port every time.
          --external-server-mapping stringArray            Mapping of Kafka server address to external address (host:port,host:port). A listener for the external address is not started
          --forbidden-api-keys intSlice                    Forbidden Kafka request types. The restriction should prevent some Kafka operations e.g. 20 - DeleteTopics
          --forward-proxy string                           URL of the forward proxy. Supported schemas are socks5 and http
      -h, --help                                           help for server
          --http-disable                                   Disable HTTP endpoints
          --http-health-path string                        Path on which to health endpoint (default "/health")
          --http-listen-address string                     Address that kafka-proxy is listening on (default "0.0.0.0:9080")
          --http-metrics-path string                       Path on which to expose metrics (default "/metrics")
          --kafka-client-id string                         An optional identifier to track the source of requests (default "kafka-proxy")
          --kafka-connection-read-buffer-size int          Size of the operating system's receive buffer associated with the connection. If zero, system default is used
          --kafka-connection-write-buffer-size int         Sets the size of the operating system's transmit buffer associated with the connection. If zero, system default is used
          --kafka-dial-timeout duration                    How long to wait for the initial connection (default 15s)
          --kafka-keep-alive duration                      Keep alive period for an active network connection. If zero, keep-alives are disabled (default 1m0s)
          --kafka-max-open-requests int                    Maximal number of open requests pro tcp connection before sending on it blocks (default 256)
          --kafka-read-timeout duration                    How long to wait for a response (default 30s)
          --kafka-write-timeout duration                   How long to wait for a transmit (default 30s)
          --log-format string                              Log format text or json (default "text")
          --log-level string                               Log level debug, info, warning, error, fatal or panic (default "info")
          --log-level-fieldname string                     Log level fieldname for json format (default "@level")
          --log-msg-fieldname string                       Message fieldname for json format (default "@message")
          --log-time-fieldname string                      Time fieldname for json format (default "@timestamp")
          --proxy-listener-ca-chain-cert-file string       PEM encoded CA's certificate file. If provided, client certificate is required and verified
          --proxy-listener-cert-file string                PEM encoded file with server certificate
          --proxy-listener-cipher-suites stringSlice       List of supported cipher suites
          --proxy-listener-curve-preferences stringSlice   List of curve preferences
          --proxy-listener-keep-alive duration             Keep alive period for an active network connection. If zero, keep-alives are disabled (default 1m0s)
          --proxy-listener-key-file string                 PEM encoded file with private key for the server certificate
          --proxy-listener-key-password string             Password to decrypt rsa private key
          --proxy-listener-read-buffer-size int            Size of the operating system's receive buffer associated with the connection. If zero, system default is used
          --proxy-listener-tls-enable                      Whether or not to use TLS listener
          --proxy-listener-write-buffer-size int           Sets the size of the operating system's transmit buffer associated with the connection. If zero, system default is used
          --proxy-request-buffer-size int                  Request buffer size pro tcp connection (default 4096)
          --proxy-response-buffer-size int                 Response buffer size pro tcp connection (default 4096)
          --sasl-enable                                    Connect using SASL
          --sasl-jaas-config-file string                   Location of JAAS config file with SASL username and password
          --sasl-method string                             SASL method to use (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512 (default "PLAIN")
          --sasl-password string                           SASL user password
          --sasl-plugin-command string                     Path to authentication plugin binary
          --sasl-plugin-enable                             Use plugin for SASL authentication
          --sasl-plugin-log-level string                   Log level of the auth plugin (default "trace")
          --sasl-plugin-mechanism string                   SASL mechanism used for proxy authentication: PLAIN or OAUTHBEARER (default "OAUTHBEARER")
          --sasl-plugin-param stringArray                  Authentication plugin parameter
          --sasl-plugin-timeout duration                   Authentication timeout (default 10s)
          --sasl-username string                           SASL user name
          --tls-ca-chain-cert-file string                  PEM encoded CA's certificate file
          --tls-client-cert-file string                    PEM encoded file with client certificate
          --tls-client-key-file string                     PEM encoded file with private key for the client certificate
          --tls-client-key-password string                 Password to decrypt rsa private key
          --tls-enable                                     Whether or not to use TLS when connecting to the broker
          --tls-insecure-skip-verify                       It controls whether a client verifies the server's certificate chain and host name
          --same-client-cert-enable                        Use only when mutual TLS is enabled on proxy and broker. It controls whether a proxy validates if proxy client certificate matches brokers client cert (tls-client-cert-file)

### Usage example
	
	kafka-proxy server --bootstrap-server-mapping "192.168.99.100:32400,0.0.0.0:32399"
	
	kafka-proxy server --bootstrap-server-mapping "192.168.99.100:32400,127.0.0.1:32400" \
	                   --bootstrap-server-mapping "192.168.99.100:32401,127.0.0.1:32401" \
	                   --bootstrap-server-mapping "192.168.99.100:32402,127.0.0.1:32402" \
	                   --dynamic-listeners-disable

	kafka-proxy server --bootstrap-server-mapping "kafka-0.example.com:9092,0.0.0.0:32401,kafka-0.grepplabs.com:9092" \
	                   --bootstrap-server-mapping "kafka-1.example.com:9092,0.0.0.0:32402,kafka-1.grepplabs.com:9092" \
	                   --bootstrap-server-mapping "kafka-2.example.com:9092,0.0.0.0:32403,kafka-2.grepplabs.com:9092" \
	                   --dynamic-listeners-disable

	kafka-proxy server --bootstrap-server-mapping "192.168.99.100:32400,127.0.0.1:32400" \
	                   --external-server-mapping "192.168.99.100:32401,127.0.0.1:32402" \
	                   --external-server-mapping "192.168.99.100:32402,127.0.0.1:32403" \
	                   --forbidden-api-keys 20
    

    export BOOTSTRAP_SERVER_MAPPING="192.168.99.100:32401,0.0.0.0:32402 192.168.99.100:32402,0.0.0.0:32403" && kafka-proxy server

### SASL authentication initiated by proxy example

SASL authentication is initiated by the proxy. SASL authentication is disabled on the clients and enabled on the Kafka brokers.   

    kafka-proxy server --bootstrap-server-mapping "kafka-0.grepplabs.com:9093,0.0.0.0:32399" \
                       --tls-enable --tls-insecure-skip-verify \
                       --sasl-enable --sasl-username myuser --sasl-password mysecret

    kafka-proxy server --bootstrap-server-mapping "kafka-0.example.com:9092,0.0.0.0:30001" \
                       --bootstrap-server-mapping "kafka-1.example.com:9092,0.0.0.0:30002" \
                       --bootstrap-server-mapping "kafka-1.example.com:9093,0.0.0.0:30003" \
                       --sasl-enable \
                       --sasl-username "alice" \
                       --sasl-password "alice-secret" \
                       --sasl-method "SCRAM-SHA-512" \
                       --log-level debug

    make clean build plugin.unsecured-jwt-provider && build/kafka-proxy server \
                             --sasl-enable \
                             --sasl-plugin-enable \
                             --sasl-plugin-mechanism "OAUTHBEARER" \
                             --sasl-plugin-command build/unsecured-jwt-provider \
                             --sasl-plugin-param "--claim-sub=alice" \
                             --bootstrap-server-mapping "192.168.99.100:32400,127.0.0.1:32400"

### Proxy authentication example

SASL authentication is performed by the proxy. SASL authentication is enabled on the clients and disabled on the Kafka brokers.   

    make clean build plugin.auth-user && build/kafka-proxy server --proxy-listener-key-file "server-key.pem"  \
                             --proxy-listener-cert-file "server-cert.pem" \
                             --proxy-listener-ca-chain-cert-file "ca.pem" \
                             --proxy-listener-tls-enable \
                             --auth-local-enable \
                             --auth-local-command build/auth-user \
                             --auth-local-param "--username=my-test-user" \
                             --auth-local-param "--password=my-test-password"

    make clean build plugin.auth-ldap && build/kafka-proxy server \
                             --auth-local-enable \
                             --auth-local-command build/auth-ldap \
                             --auth-local-param "--url=ldaps://ldap.example.com:636" \
                             --auth-local-param "--user-dn=cn=users,dc=exemple,dc=com" \
                             --auth-local-param "--user-attr=uid" \
                             --bootstrap-server-mapping "192.168.99.100:32400,127.0.0.1:32400"

    make clean build plugin.unsecured-jwt-info && build/kafka-proxy server \
                             --auth-local-enable \
                             --auth-local-command build/unsecured-jwt-info \
                             --auth-local-mechanism "OAUTHBEARER" \
                             --auth-local-param "--claim-sub=alice" \
                             --auth-local-param "--claim-sub=bob" \
                             --bootstrap-server-mapping "192.168.99.100:32400,127.0.0.1:32400"
                             
### Same client certificate check enabled example

Validate that client certificate used by proxy client is exactly the same as client certificate in authentication initiated by proxy 
                       
    kafka-proxy server --bootstrap-server-mapping "kafka-0.grepplabs.com:9093,0.0.0.0:32399" \
       --tls-enable \
       --tls-client-cert-file client.crt \
       --tls-client-key-file client.pem \
       --tls-client-key-password changeit \
       --proxy-listener-tls-enable \
       --proxy-listener-key-file server.pem \
       --proxy-listener-cert-file server.crt \
       --proxy-listener-key-password changeit \
       --proxy-listener-ca-chain-cert-file ca.crt \
       --same-client-cert-enable

### Kafka Gateway example

Authentication between Kafka Proxy Client and Kafka Proxy Server with Google-ID (service account JWT)

    kafka-proxy server --bootstrap-server-mapping "kafka-0.grepplabs.com:9092,127.0.0.1:32500" \
                       --bootstrap-server-mapping "kafka-1.grepplabs.com:9092,127.0.0.1:32501" \
                       --bootstrap-server-mapping "kafka-2.grepplabs.com:9092,127.0.0.1:32502" \
                       --dynamic-listeners-disable \
                       --http-disable \
                       --proxy-listener-tls-enable \
                       --proxy-listener-cert-file=/var/run/secret/server.cert.pem \
                       --proxy-listener-key-file=/var/run/secret/server.key.pem \
                       --auth-gateway-server-enable \
                       --auth-gateway-server-method google-id \
                       --auth-gateway-server-magic 3285573610483682037 \
                       --auth-gateway-server-command google-id-info \
                       --auth-gateway-server-param  "--timeout=10" \
                       --auth-gateway-server-param  "--audience=tcp://kafka-gateway.grepplabs.com" \
                       --auth-gateway-server-param  "--email-regex=^kafka-gateway@my-project.iam.gserviceaccount.com$"

    kafka-proxy server --bootstrap-server-mapping "127.0.0.1:32500,127.0.0.1:32400" \
                       --bootstrap-server-mapping "127.0.0.1:32501,127.0.0.1:32401" \
                       --bootstrap-server-mapping "127.0.0.1:32502,127.0.0.1:32402" \
                       --dynamic-listeners-disable \
                       --http-disable \
                       --tls-enable \
                       --tls-ca-chain-cert-file /var/run/secret/client/ca-chain.cert.pem \
                       --auth-gateway-client-enable \
                       --auth-gateway-client-method google-id \
                       --auth-gateway-client-magic 3285573610483682037 \
                       --auth-gateway-client-command google-id-provider \
                       --auth-gateway-client-param  "--credentials-file=/var/run/secret/client/service-account.json" \
                       --auth-gateway-client-param  "--target-audience=tcp://kafka-gateway.grepplabs.com" \
                       --auth-gateway-client-param  "--timeout=10"

### Connect to Kafka through SOCKS5 Proxy example

Connect through test SOCKS5 Proxy server

```
    kafka-proxy tools socks5-proxy --addr localhost:1080

    kafka-proxy server --bootstrap-server-mapping "kafka-0.grepplabs.com:9092,127.0.0.1:32500" \
                       --bootstrap-server-mapping "kafka-1.grepplabs.com:9092,127.0.0.1:32501" \
                       --bootstrap-server-mapping "kafka-2.grepplabs.com:9092,127.0.0.1:32502"
                       --forward-proxy socks5://localhost:1080
```

```
    kafka-proxy tools socks5-proxy --addr localhost:1080 --username my-proxy-user --password my-proxy-password

    kafka-proxy server --bootstrap-server-mapping "kafka-0.grepplabs.com:9092,127.0.0.1:32500" \
                       --bootstrap-server-mapping "kafka-1.grepplabs.com:9092,127.0.0.1:32501" \
                       --bootstrap-server-mapping "kafka-2.grepplabs.com:9092,127.0.0.1:32502" \
                       --forward-proxy socks5://my-proxy-user:my-proxy-password@localhost:1080
```

### Connect to Kafka through HTTP Proxy example

Connect through test HTTP Proxy server using CONNECT method

```
    kafka-proxy tools http-proxy --addr localhost:3128

    kafka-proxy server --bootstrap-server-mapping "kafka-0.grepplabs.com:9092,127.0.0.1:32500" \
                       --bootstrap-server-mapping "kafka-1.grepplabs.com:9092,127.0.0.1:32501" \
                       --bootstrap-server-mapping "kafka-2.grepplabs.com:9092,127.0.0.1:32502"
                       --forward-proxy http://localhost:3128
```

```
    kafka-proxy tools http-proxy --addr localhost:3128 --username my-proxy-user --password my-proxy-password

    kafka-proxy server --bootstrap-server-mapping "kafka-0.grepplabs.com:9092,127.0.0.1:32500" \
                       --bootstrap-server-mapping "kafka-1.grepplabs.com:9092,127.0.0.1:32501" \
                       --bootstrap-server-mapping "kafka-2.grepplabs.com:9092,127.0.0.1:32502" \
                       --forward-proxy http://my-proxy-user:my-proxy-password@localhost:3128
```

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

### Connect to Kafka running in Kubernetes example (kafka proxy runs in cluster)

```yaml

---
apiVersion: apps/v1beta2
kind: StatefulSet
metadata:
   name: kafka-proxy
spec:
  selector:
    matchLabels:
      app: kafka-proxy
  replicas: 1
  serviceName: kafka-proxy
  template:
    metadata:
      labels:
        app: kafka-proxy
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
            - '--proxy-request-buffer-size=32768'
            - '--proxy-response-buffer-size=32768'
            - '--proxy-listener-read-buffer-size=32768'
            - '--proxy-listener-write-buffer-size=131072'
            - '--kafka-connection-read-buffer-size=131072'
            - '--kafka-connection-write-buffer-size=32768'
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
          - name: kafka-0
            containerPort: 32400
          - name: kafka-1
            containerPort: 32401
          - name: kafka-2
            containerPort: 32402
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
          resources:
            requests:
              memory: 128Mi
              cpu: 1000m
      restartPolicy: Always
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


```bash
kubectl port-forward kafka-proxy-0 32400:32400 32401:32401 32402:32402
```

Use localhost:32400, localhost:32401 and localhost:32402 as bootstrap servers


### Connect to Kafka running in Kubernetes example (kafka proxy runs locally)
####  one node Kafka cluster
kafka.properties

```
broker.id=0
advertised.listeners=PLAINTEXT://kafka-0.kafka-headless.kafka:9092
...
```

```bash
kubectl port-forward -n kafka kafka-0 9092:9092
```

```bash
kafka-proxy server --bootstrap-server-mapping "127.0.0.1:9092,0.0.0.0:19092" --dial-address-mapping "kafka-0.kafka-headless.kafka:9092,0.0.0.0:9092"
```

Use localhost:19092 as bootstrap servers

#### 3 nodes Kafka cluster

[strimzi 0.13.0 CRD](https://strimzi.io/)

```yaml
apiVersion: kafka.strimzi.io/v1beta1
kind: Kafka
metadata:
  name: test-cluster
  namespace: kafka
spec:
  kafka:
    version: 2.3.0
    replicas: 3
    listeners:
      plain: {}
      tls: {}
    config:
      offsets.topic.replication.factor: 3
      transaction.state.log.replication.factor: 3
      transaction.state.log.min.isr: 2
      num.partitions: 60
      default.replication.factor: 3
    storage:
      type: jbod
      volumes:
        - id: 0
          type: persistent-claim
          size: 20Gi
          deleteClaim: true
  zookeeper:
    replicas: 3
    storage:
      type: persistent-claim
      size: 5Gi
      deleteClaim: true
  entityOperator:
    topicOperator: {}
    userOperator: {}
```

```bash
kubectl port-forward -n kafka test-cluster-kafka-0 9092:9092
kubectl port-forward -n kafka test-cluster-kafka-1 9093:9092
kubectl port-forward -n kafka test-cluster-kafka-2 9094:9092

kafka-proxy server --log-level debug \
  --bootstrap-server-mapping "127.0.0.1:9092,0.0.0.0:19092" \
  --bootstrap-server-mapping "127.0.0.1:9093,0.0.0.0:19093" \
  --bootstrap-server-mapping "127.0.0.1:9094,0.0.0.0:19094" \
  --dial-address-mapping "test-cluster-kafka-0.test-cluster-kafka-brokers.kafka.svc.cluster.local:9092,0.0.0.0:9092" \
  --dial-address-mapping "test-cluster-kafka-1.test-cluster-kafka-brokers.kafka.svc.cluster.local:9092,0.0.0.0:9093" \
  --dial-address-mapping "test-cluster-kafka-2.test-cluster-kafka-brokers.kafka.svc.cluster.local:9092,0.0.0.0:9094"
```

Use localhost:19092 as bootstrap servers

### Embedded third-party source code 

* [Cloud SQL Proxy](https://github.com/GoogleCloudPlatform/cloudsql-proxy)
* [Sarama](https://github.com/Shopify/sarama)
