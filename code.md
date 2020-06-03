Download protoc https://github.com/protocolbuffers/protobuf

Because there are now two protoc-gen-go projects we use old, because doesn't include grpc services
https://stackoverflow.com/questions/60578892/protoc-gen-go-grpc-program-not-found-or-is-not-executable
go get github.com/golang/protobuf/protoc-gen-go

generate with services
~/.protoc/protoc -I=plugin/authz/proto --go_out=plugins=grpc:plugin/authz/proto plugin/authz/proto/authz.proto

build/kafka-proxy server \
                         --sasl-enable \
                         --sasl-plugin-enable \
                         --sasl-plugin-mechanism "OAUTHBEARER" \
                         --sasl-plugin-command build/oidc-provider \
                         --sasl-plugin-param "--credentials-file=/tmp/creds.json" \
                         --bootstrap-server-mapping "127.0.0.1:4000,0.0.0.0:3000" \
                         --http-disable

{
"client_id": "fafa",
"client_secret": "a7415107-57aa-4714-ab79-63aad1307970",
"token_url": "http://localhost:8080/auth/realms/master/protocol/openid-connect/token",
"scopes": ["email"]
}

build/kafka-proxy server \
                         --auth-local-enable \
                         --auth-local-command build/unsecured-jwt-info \
                         --auth-local-mechanism "OAUTHBEARER" \
                         --bootstrap-server-mapping "172.31.0.3:9094,127.0.0.1:4000" \
                         --http-disable \
                         --authz-enable \
                         --authz-command build/opa-provider \
                         --authz-param "--authz-url=http://localhost:8181/v1/data/example/authz/allow"

policy upload must be with data-binary

curl -X PUT -H 'Content-Type: text/plain' --data-binary @mypol.reg http://localhost:8181/v1/policies/example