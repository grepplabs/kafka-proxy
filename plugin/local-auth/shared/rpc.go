package shared

import (
	"github.com/grepplabs/kafka-proxy/pkg/apis"
	"net/rpc"
)

type RPCClient struct{ client *rpc.Client }

func (m *RPCClient) Authenticate(username, password string) (bool, int32, error) {
	var resp map[string]interface{}
	err := m.client.Call("Plugin.Authenticate", map[string]interface{}{
		"username": username,
		"password": password,
	}, &resp)
	return resp["authenticated"].(bool), resp["status"].(int32), err
}

type RPCServer struct {
	Impl apis.PasswordAuthenticator
}

func (m *RPCServer) Authenticate(args map[string]interface{}, resp *map[string]interface{}) error {
	a, s, err := m.Impl.Authenticate(args["username"].(string), args["password"].(string))
	*resp = map[string]interface{}{
		"authenticated": a,
		"status":        s,
	}
	return err
}
