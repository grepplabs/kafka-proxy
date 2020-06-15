Download protoc https://github.com/protocolbuffers/protobuf

Because there are now two protoc-gen-go projects we use old, because doesn't include grpc services
https://stackoverflow.com/questions/60578892/protoc-gen-go-grpc-program-not-found-or-is-not-executable
go get github.com/golang/protobuf/protoc-gen-go

generate with services
~/.protoc/protoc -I=plugin/authz-provider/proto --go_out=plugins=grpc:plugin/authz-provider/proto plugin/authz-provider/proto/authz.proto

tail -f /var/log/syslog|docker run -i --network=host confluentinc/cp-kafkacat kafkacat -b localhost:2000 -t client1topic -P

client1:

{
"client_id": "client1",
"client_secret": "21d0ef5f-6e70-4f51-be4a-208b69efef17",
"token_url": "http://localhost:8080/auth/realms/master/protocol/openid-connect/token",
"scopes": ["email"]
}

build/kafka-proxy server \
                         --sasl-enable \
                         --sasl-plugin-enable \
                         --sasl-plugin-mechanism "OAUTHBEARER" \
                         --sasl-plugin-command build/oidc-provider \
                         --sasl-plugin-param "--credentials-file=/tmp/creds1.json" \
                         --bootstrap-server-mapping "127.0.0.1:4000,0.0.0.0:2000" \
                         --http-disable

client2:

{
"client_id": "client2",
"client_secret": "cb9aab22-e389-4ecd-af7d-417ee1bc52a2",
"token_url": "http://localhost:8080/auth/realms/master/protocol/openid-connect/token",
"scopes": ["email"]
}

build/kafka-proxy server \
                        --sasl-enable \
                        --sasl-plugin-enable \
                        --sasl-plugin-mechanism "OAUTHBEARER" \
                        --sasl-plugin-command build/oidc-provider \
                        --sasl-plugin-param "--credentials-file=/tmp/creds2.json" \
                        --bootstrap-server-mapping "127.0.0.1:4000,0.0.0.0:3000" \
                        --http-disable

Proxy Server:

build/kafka-proxy server \
                         --auth-local-enable \
                         --auth-local-command build/unsecured-jwt-info \
                         --auth-local-mechanism "OAUTHBEARER" \
                         --bootstrap-server-mapping "172.31.0.5:9094,127.0.0.1:4000" \
                         --http-disable \
                         --authz-enable \
                         --authz-command build/opa-provider \
                         --authz-param "--authz-url=http://localhost:8181/v1/data/example/authz/allow"

policy upload must be with data-binary

curl -X PUT -H 'Content-Type: text/plain' --data-binary @mypol.reg http://localhost:8181/v1/policies/example
