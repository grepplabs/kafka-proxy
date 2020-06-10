package shared

import (
	"context"
	"net/rpc"

	"github.com/grepplabs/kafka-proxy/pkg/apis"
)

type RPCClient struct{ client *rpc.Client }

func (m *RPCClient) Authorize(ctx context.Context, request apis.AuthzRequest) (apis.AuthzResponse, error) {
	var resp map[string]interface{}

	err := m.client.Call(
		"Plugin.Authorize",
		map[string]interface{}{
			"apikey":    request.Apikey,
			"user_info": request.UserInfo,
			"src_ip":    request.SrcIp,
			"dst_ip":    request.DstIp,
			"topics":    request.Topics,
			"client_id": request.ClientId,
		},
		&resp,
	)

	return apis.AuthzResponse{Success: resp["success"].(bool), Status: resp["status"].(int32)}, err
}

type RPCServer struct {
	Impl apis.AuthzProvider
}

func (m *RPCServer) Authorize(args map[string]interface{}, resp *map[string]interface{}) error {
	r, err := m.Impl.Authorize(
		context.Background(),
		apis.AuthzRequest{
			Apikey:   args["apikey"].(int32),
			UserInfo: args["user_info"].(string),
			SrcIp:    args["src_ip"].(string),
			DstIp:    args["dst_ip"].(string),
			Topics:   args["topics"].(string),
			ClientId: args["client_id"].(string),
		},
	)

	*resp = map[string]interface{}{
		"success": r.Success,
		"status":  r.Status,
	}

	return err
}
