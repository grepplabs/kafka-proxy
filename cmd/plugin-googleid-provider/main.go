package main

import (
	"context"
	"flag"
	"github.com/grepplabs/kafka-proxy/pkg/apis"
	"github.com/grepplabs/kafka-proxy/plugin/gateway-client/shared"
	"github.com/hashicorp/go-plugin"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/oauth2/v2"
	"os"
	"time"
)

type TokenProvider struct {
	timeout int
}

//TODO: caching, expiry
//TODO: refresh in the half of time
//TODO: send claims
func (p TokenProvider) GetToken(request apis.TokenRequest) (apis.TokenResponse, error) {

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(p.timeout)*time.Second)
	defer cancel()

	tokenSource, err := google.DefaultTokenSource(ctx, oauth2.UserinfoEmailScope)
	if err != nil {
		return tokenResponse(false, 1, "")
	}
	token, err := tokenSource.Token()
	if err != nil {
		return tokenResponse(false, 2, "")
	}
	if token.Extra("id_token") == nil {
		return tokenResponse(false, 3, "")
	}
	idToken := token.Extra("id_token").(string)
	return tokenResponse(true, 0, idToken)

}

func tokenResponse(success bool, status int32, token string) (apis.TokenResponse, error) {
	return apis.TokenResponse{Success: success, Status: status, Token: token}, nil
}

func (f *TokenProvider) flagSet() *flag.FlagSet {
	fs := flag.NewFlagSet("google-id provider settings", flag.ContinueOnError)
	return fs
}

func main() {
	tokenProvider := &TokenProvider{}
	fs := tokenProvider.flagSet()
	fs.IntVar(&tokenProvider.timeout, "timeout", 5, "Request timeout")

	fs.Parse(os.Args[1:])

	plugin.Serve(&plugin.ServeConfig{
		HandshakeConfig: shared.Handshake,
		Plugins: map[string]plugin.Plugin{
			"tokenProvider": &shared.TokenProviderPlugin{Impl: tokenProvider},
		},
		// A non-nil value here enables gRPC serving for this plugin...
		GRPCServer: plugin.DefaultGRPCServer,
	})
}
