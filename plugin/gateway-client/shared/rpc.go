package shared

import (
	"github.com/grepplabs/kafka-proxy/pkg/apis"
	"net/rpc"
)

type RPCClient struct{ client *rpc.Client }

func (m *RPCClient) GetToken(claims []string) (int32, string, error) {
	var resp map[string]interface{}
	err := m.client.Call("Plugin.GetToken", map[string]interface{}{
		"claims": claims,
	}, &resp)
	return resp["status"].(int32), resp["token"].(string), err
}

type RPCServer struct {
	Impl apis.TokenProvider
}

func (m *RPCServer) GetToken(args map[string]interface{}, resp *map[string]interface{}) error {
	s, t, err := m.Impl.GetToken(args["claims"].([]string))
	*resp = map[string]interface{}{
		"status": s,
		"token":  t,
	}
	return err
}
