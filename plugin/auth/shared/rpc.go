package shared

import (
	"net/rpc"
)

type RPCClient struct{ client *rpc.Client }

func (m *RPCClient) Authenticate(username, password string) (bool, error) {
	var resp bool
	err := m.client.Call("Plugin.Authenticate", map[string]interface{}{
		"username": username,
		"password": password,
	}, &resp)
	return resp, err
}

type RPCServer struct {
	Impl PasswordAuthenticator
}

func (m *RPCServer) Authenticate(args map[string]interface{}, resp *bool) error {
	v, err := m.Impl.Authenticate(args["username"].(string), args["password"].(string))
	*resp = v
	return err
}
