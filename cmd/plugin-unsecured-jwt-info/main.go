package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"github.com/grepplabs/kafka-proxy/pkg/apis"
	"github.com/grepplabs/kafka-proxy/pkg/libs/util"
	"github.com/grepplabs/kafka-proxy/plugin/token-info/shared"
	"github.com/hashicorp/go-plugin"
	"github.com/sirupsen/logrus"
	"os"
	"strings"
	"time"
)

const (
	StatusOK                      = 0
	StatusEmptyToken              = 1
	StatusParseJWTFailed          = 2
	StatusWrongAlgorithm          = 3
	StatusUnauthorized            = 4
	StatusNoIssueTimeInToken      = 5
	StatusNoExpirationTimeInToken = 6
	StatusTokenTooEarly           = 7
	StatusTokenExpired            = 8

	AlgorithmNone = "none"
)

var (
	clockSkew = 1 * time.Minute
)

type UnsecuredJWTVerifier struct {
	claimSub map[string]struct{}
}

type pluginMeta struct {
	claimSub util.ArrayFlags
}

func (f *pluginMeta) flagSet() *flag.FlagSet {
	fs := flag.NewFlagSet("unsecured-jwt-info info settings", flag.ContinueOnError)
	fs.Var(&f.claimSub, "claim-sub", "Allowed subject claim (user name)")
	return fs
}

// Implements apis.TokenInfo
func (v UnsecuredJWTVerifier) VerifyToken(ctx context.Context, request apis.VerifyRequest) (apis.VerifyResponse, error) {
	if request.Token == "" {
		return getVerifyResponseResponse(StatusEmptyToken)
	}

	header, claimSet, err := Decode(request.Token)
	if err != nil {
		return getVerifyResponseResponse(StatusParseJWTFailed)
	}
	if header.Algorithm != AlgorithmNone {
		return getVerifyResponseResponse(StatusWrongAlgorithm)
	}

	if len(v.claimSub) != 0 {
		if _, ok := v.claimSub[claimSet.Sub]; !ok {
			return getVerifyResponseResponse(StatusUnauthorized)
		}
	}
	if claimSet.Iat < 1 {
		return getVerifyResponseResponse(StatusNoIssueTimeInToken)
	}
	if claimSet.Exp < 1 {
		return getVerifyResponseResponse(StatusNoExpirationTimeInToken)
	}

	earliest := int64(claimSet.Iat) - int64(clockSkew.Seconds())
	latest := int64(claimSet.Exp) + int64(clockSkew.Seconds())
	unix := time.Now().Unix()

	if unix < earliest {
		return getVerifyResponseResponse(StatusTokenTooEarly)
	}
	if unix > latest {
		return getVerifyResponseResponse(StatusTokenExpired)
	}
	return getVerifyResponseResponse(StatusOK)
}

type Header struct {
	Algorithm string `json:"alg"`
}

// kafka client sends float instead of int
type ClaimSet struct {
	Sub         string                 `json:"sub,omitempty"`
	Exp         float64                `json:"exp"`
	Iat         float64                `json:"iat"`
	OtherClaims map[string]interface{} `json:"-"`
}

func Decode(token string) (*Header, *ClaimSet, error) {
	args := strings.Split(token, ".")
	if len(args) < 2 {
		return nil, nil, errors.New("jws: invalid token received")
	}
	decodedHeader, err := base64.RawURLEncoding.DecodeString(args[0])
	if err != nil {
		return nil, nil, err
	}
	decodedPayload, err := base64.RawURLEncoding.DecodeString(args[1])
	if err != nil {
		return nil, nil, err
	}

	header := &Header{}
	err = json.NewDecoder(bytes.NewBuffer(decodedHeader)).Decode(header)
	if err != nil {
		return nil, nil, err
	}
	claimSet := &ClaimSet{}
	err = json.NewDecoder(bytes.NewBuffer(decodedPayload)).Decode(claimSet)
	if err != nil {
		return nil, nil, err
	}
	return header, claimSet, nil
}

func getVerifyResponseResponse(status int) (apis.VerifyResponse, error) {
	success := status == StatusOK
	return apis.VerifyResponse{Success: success, Status: int32(status)}, nil
}

func main() {
	pluginMeta := &pluginMeta{}
	fs := pluginMeta.flagSet()
	_ = fs.Parse(os.Args[1:])

	logrus.Infof("Unsecured JWT sub claims: %v", pluginMeta.claimSub)

	unsecuredJWTVerifier := &UnsecuredJWTVerifier{
		claimSub: pluginMeta.claimSub.AsMap(),
	}

	plugin.Serve(&plugin.ServeConfig{
		HandshakeConfig: shared.Handshake,
		Plugins: map[string]plugin.Plugin{
			"unsecuredJWTInfo": &shared.TokenInfoPlugin{Impl: unsecuredJWTVerifier},
		},
		// A non-nil value here enables gRPC serving for this plugin...
		GRPCServer: plugin.DefaultGRPCServer,
	})
}
