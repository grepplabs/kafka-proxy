package main

import (
	"context"
	"flag"
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
func (p TokenProvider) GetToken(claims []string) (int32, string, error) {

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(p.timeout)*time.Second)
	defer cancel()

	tokenSource, err := google.DefaultTokenSource(ctx, oauth2.UserinfoEmailScope)
	if err != nil {
		return tokenResponse(1, "")
	}
	token, err := tokenSource.Token()
	if err != nil {
		return tokenResponse(2, "")
	}
	if token.Extra("id_token") == nil {
		return tokenResponse(3, "")
	}
	idToken := token.Extra("id_token").(string)
	return tokenResponse(0, idToken)

}

func tokenResponse(status int32, token string) (int32, string, error) {
	return status, token, nil
}

func (f *TokenProvider) flagSet() *flag.FlagSet {
	fs := flag.NewFlagSet("google-id provider settings", flag.ContinueOnError)
	return fs
}

func main() {
	tokenProvider := &TokenProvider{}
	fs := tokenProvider.flagSet()
	fs.Parse(os.Args[1:])
	fs.IntVar(&tokenProvider.timeout, "timeout", 5, "Request timeout")

	plugin.Serve(&plugin.ServeConfig{
		HandshakeConfig: shared.Handshake,
		Plugins: map[string]plugin.Plugin{
			"tokenProvider": &shared.TokenProviderPlugin{Impl: tokenProvider},
		},
		// A non-nil value here enables gRPC serving for this plugin...
		GRPCServer: plugin.DefaultGRPCServer,
	})
}
