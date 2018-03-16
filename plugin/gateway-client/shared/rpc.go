package shared

import (
	"github.com/grepplabs/kafka-proxy/pkg/apis"
	"net/rpc"
)

type RPCClient struct{ client *rpc.Client }

func (m *RPCClient) GetToken(request apis.TokenRequest) (apis.TokenResponse, error) {
	var resp map[string]interface{}
	err := m.client.Call("Plugin.GetToken", map[string]interface{}{
		"params": request.Params,
	}, &resp)
	return apis.TokenResponse{Success: resp["success"].(bool), Status: resp["status"].(int32), Token: resp["token"].(string)}, err
}

type RPCServer struct {
	Impl apis.TokenProvider
}

func (m *RPCServer) GetToken(args map[string]interface{}, resp *map[string]interface{}) error {

	r, err := m.Impl.GetToken(apis.TokenRequest{Params: args["params"].([]string)})
	*resp = map[string]interface{}{
		"success": r.Success,
		"status":  r.Status,
		"token":   r.Token,
	}
	return err
}
