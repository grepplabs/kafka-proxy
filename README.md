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

The proxies can also authenticate each other using a pluggable method which is transparent to other Kafka servers and clients.
Currently the Google ID Token for service accounts is implemented i.e. proxy client requests and sends service account JWT and proxy server receives and validates it against Google JWKS.

Kafka API calls can be restricted to prevent some operations e.g. topic deletion or produce requests.


See:
* [A Guide To The Kafka Protocol](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol)
* [Kafka protocol guide](http://kafka.apache.org/protocol.html)


### Install binary release

1. Download the latest release

   Linux

        curl -Ls https://github.com/grepplabs/kafka-proxy/releases/download/v0.0.8/kafka-proxy_0.0.6_linux_amd64.tar.gz | tar xz

   macOS

        curl -Ls https://github.com/grepplabs/kafka-proxy/releases/download/v0.0.8/kafka-proxy_0.0.6_darwin_amd64.tar.gz | tar xz

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
          --auth-local-param stringArray                   Authentication plugin parameter
          --auth-local-timeout duration                    Authentication timeout (default 10s)
          --bootstrap-server-mapping stringArray           Mapping of Kafka bootstrap server address to local address (host:port,host:port(,advhost:advport))
          --debug-enable                                   Enable Debug endpoint
          --debug-listen-address string                    Debug listen address (default "0.0.0.0:6060")
          --default-listener-ip string                     Default listener IP (default "127.0.0.1")
          --dynamic-listeners-disable                      Disable dynamic listeners.
          --external-server-mapping stringArray            Mapping of Kafka server address to external address (host:port,host:port). A listener for the external address is not started
          --forbidden-api-keys intSlice                    Forbidden Kafka request types. The restriction should prevent some Kafka operations e.g. 20 - DeleteTopics
          --forward-proxy string                           URL of the forward proxy. Supported schemas are http and socks5
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
          --sasl-enable                                    Connect using SASL/PLAIN
          --sasl-jaas-config-file string                   Location of JAAS config file with SASL username and password
          --sasl-password string                           SASL user password
          --sasl-username string                           SASL user name
          --tls-ca-chain-cert-file string                  PEM encoded CA's certificate file
          --tls-client-cert-file string                    PEM encoded file with client certificate
          --tls-client-key-file string                     PEM encoded file with private key for the client certificate
          --tls-client-key-password string                 Password to decrypt rsa private key
          --tls-enable                                     Whether or not to use TLS when connecting to the broker
          --tls-insecure-skip-verify                       It controls whether a client verifies the server's certificate chain and host name



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
    
    kafka-proxy server --bootstrap-server-mapping "kafka-0.grepplabs.com:9093,0.0.0.0:32399" \
                       --tls-enable --tls-insecure-skip-verify \
                       --sasl-enable --sasl-username myuser --sasl-password mysecret

    export BOOTSTRAP_SERVER_MAPPING="192.168.99.100:32401,0.0.0.0:32402 192.168.99.100:32402,0.0.0.0:32403" && kafka-proxy server

### Proxy authentication example

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
* [X] Advertised proxy listeners e.g. bootstrap-server-mapping (remotehost:remoteport,localhost:localport,advhost:advport)
* [X] Pluggable authentication between client kafka-proxy and broker kafka-proxy a.k.a kafka-gateway
  1. additional handshake - protocol: magic, method, data
  2. google-id method
* [X] Registry for built-in plugins
* [X] Client cert check
* [X] Set TLS server CipherSuites and CurvePreferences
* [X] Optional ApiVersionsRequest before Local SASL Authentication Sequence
* [X] SaslHandshakeRequest v1 - Kafka 1.0.0
* [X] Connect to Kafka through SOCKS5 Proxy
* [ ] Performance tests and tuning
* [ ] Socket buffer sizing e.g. SO_RCVBUF = 32768, SO_SNDBUF = 131072
* [ ] Kafka connect tests
* [X] Different Kafka API versions tests
* [ ] Unit and integration tests
* [ ] Rolling upgrade test
* [ ] Graceful shutdown

### Embedded third-party source code 

* [Cloud SQL Proxy](https://github.com/GoogleCloudPlatform/cloudsql-proxy)
* [Sarama](https://github.com/Shopify/sarama)