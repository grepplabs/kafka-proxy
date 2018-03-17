package shared

import (
	"context"
	"github.com/grepplabs/kafka-proxy/pkg/apis"
	"net/rpc"
)

type RPCClient struct{ client *rpc.Client }

func (m *RPCClient) VerifyToken(ctx context.Context, request apis.VerifyRequest) (apis.VerifyResponse, error) {
	var resp map[string]interface{}
	err := m.client.Call("Plugin.VerifyToken", map[string]interface{}{
		"token":  request.Token,
		"params": request.Params,
	}, &resp)
	return apis.VerifyResponse{Success: resp["success"].(bool), Status: resp["status"].(int32)}, err
}

type RPCServer struct {
	Impl apis.TokenInfo
}

func (m *RPCServer) VerifyToken(args map[string]interface{}, resp *map[string]interface{}) error {

	r, err := m.Impl.VerifyToken(context.Background(), apis.VerifyRequest{Token: args["token"].(string), Params: args["params"].([]string)})
	*resp = map[string]interface{}{
		"success": r.Success,
		"status":  r.Status,
	}
	return err
}
