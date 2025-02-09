package oidcprovider

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/grepplabs/kafka-proxy/pkg/apis"
	"github.com/grepplabs/kafka-proxy/pkg/libs/oidc"
	"github.com/stretchr/testify/assert"
)

var testToken = `eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJtd1FWWkVxd2c0LUpNWDlwY3Q1QkQydkE3WjVDWVl2dnRSRWRxS2RtaDFRIn0.eyJleHAiOjE1OTI1NjI0ODMsImlhdCI6MTU5MjU2MjQyMywianRpIjoiODUzNTQ5OTItNTY5YS00NGUyLTg0MjYtMTAxMzVmZDdlMjQ5IiwiaXNzIjoiaHR0cDovL2xvY2FsaG9zdDo4MDgwL2F1dGgvcmVhbG1zL21hc3RlciIsImF1ZCI6ImFjY291bnQiLCJzdWIiOiJlYmVhOGY1ZS1jODVlLTQwZDMtYmY2ZC05ODI5ODM0NzVlMDAiLCJ0eXAiOiJCZWFyZXIiLCJhenAiOiJjbGllbnQxIiwic2Vzc2lvbl9zdGF0ZSI6ImVmOTE2YjYwLWZjZGQtNDMwZC04N2E2LWFlZDk1ZDM3NjYxOSIsImFjciI6IjEiLCJyZWFsbV9hY2Nlc3MiOnsicm9sZXMiOlsib2ZmbGluZV9hY2Nlc3MiLCJ1bWFfYXV0aG9yaXphdGlvbiJdfSwicmVzb3VyY2VfYWNjZXNzIjp7ImFjY291bnQiOnsicm9sZXMiOlsibWFuYWdlLWFjY291bnQiLCJtYW5hZ2UtYWNjb3VudC1saW5rcyIsInZpZXctcHJvZmlsZSJdfX0sInNjb3BlIjoicHJvZmlsZSBlbWFpbCIsImVtYWlsX3ZlcmlmaWVkIjpmYWxzZSwiY2xpZW50SG9zdCI6IjE3Mi4zMS4wLjEiLCJjbGllbnRJZCI6ImNsaWVudDEiLCJwcmVmZXJyZWRfdXNlcm5hbWUiOiJzZXJ2aWNlLWFjY291bnQtY2xpZW50MSIsImNsaWVudEFkZHJlc3MiOiIxNzIuMzEuMC4xIn0.Q_gev0O4avadzO3dr2kFSEtdMBjkMUZcMm8Y-dOk6E3ge0MwtqNlOf6U29IirZQmLblhkexTuEXnngtHE22ymZLQUzk3wzcMmD7Sqms8aso3zvd0G7DbyyKVWg67SOmZ8623-bLtT-09fYx4ExnWEg3iPQqvzG-IHbhpI9DFQeWPwNoO9ms4L082JNaz5OVv22medKYVaflkLWqdN5mjtWFHxUl6tI8ts_DENEcaTMsMoTCJCxNOaaijrdMALKh43mQI6mc44BVN_axjFqKLPT4ZsjISeQ33_0l9bJ2lgAbU5tXTYjzWbB6bifbpgwtgeFOYYR1eM4eUwX75VZWUNg`

type FakeIdTokenSource struct{}

func (s *FakeIdTokenSource) GetIDToken(ctx context.Context) (string, error) {
	return testToken, nil
}

type FakeErrorIdTokenSource struct{}

func (s *FakeErrorIdTokenSource) GetIDToken(ctx context.Context) (string, error) {
	return "", fmt.Errorf("On purpose test IdTokenSource GetIDToken error")
}

type FakeBadTokenIdTokenSource struct{}

func (s *FakeBadTokenIdTokenSource) GetIDToken(ctx context.Context) (string, error) {
	return "badtoken", nil
}

func TestRenewLatest(t *testing.T) {
	a := assert.New(t)
	token, err := oidc.ParseJWT(testToken)
	a.Nil(err)
	a.NotNil(token)

	token.ClaimSet.Exp = int64(time.Now().Add(180 * time.Second).Unix())

	if renewLatest(token) != false {
		t.Fatalf("Should be false got true - non-expired token")
	}

	token.ClaimSet.Exp = int64(time.Now().Unix() - 600)

	if renewLatest(token) != true {
		t.Fatalf("Should be true got false - expired token")
	}

	token = nil

	if renewLatest(token) != true {
		t.Fatalf("Should be true got false - nil token")
	}
}

func TestGetCurrentClaimSet(t *testing.T) {
	a := assert.New(t)
	token, err := oidc.ParseJWT(testToken)
	a.Nil(err)
	a.NotNil(token)

	prov := TokenProvider{
		timeout:       10 * time.Second,
		idTokenSource: &FakeIdTokenSource{},
	}

	prov.setCurrentToken(token)

	a.Equal("client1", prov.getCurrentClaimSet().Azp)
	a.Equal("account", prov.getCurrentClaimSet().Aud)
	a.Equal("ebea8f5e-c85e-40d3-bf6d-982983475e00", prov.getCurrentClaimSet().Sub)

}

func TestGetCurrentToken(t *testing.T) {
	a := assert.New(t)
	token, err := oidc.ParseJWT(testToken)
	a.Nil(err)
	a.NotNil(token)

	prov := TokenProvider{
		timeout:       10 * time.Second,
		idTokenSource: &FakeIdTokenSource{},
	}

	a.Equal("", prov.getCurrentToken())

	prov.setCurrentToken(token)

	token.ClaimSet.Exp = int64(time.Now().Unix() - 600)

	a.Equal("", prov.getCurrentToken())

	token.ClaimSet.Exp = int64(time.Now().Add(180 * time.Second).Unix())

	a.Equal(testToken, prov.getCurrentToken())
}

func TestGetToken(t *testing.T) {
	a := assert.New(t)
	token, err := oidc.ParseJWT(testToken)
	a.Nil(err)
	a.NotNil(token)

	prov := TokenProvider{
		timeout:       10 * time.Second,
		idTokenSource: &FakeIdTokenSource{},
	}

	actual, err := prov.GetToken(context.Background(), apis.TokenRequest{})
	a.NotNil(token)
	a.Nil(err)

	exp := apis.TokenResponse{Success: true, Status: int32(StatusOK), Token: testToken}
	a.Equal(exp, actual)

	prov.setCurrentToken(token)

	actual, err = prov.GetToken(context.Background(), apis.TokenRequest{})
	a.NotNil(token)
	a.Nil(err)

	exp = apis.TokenResponse{Success: true, Status: int32(StatusOK), Token: testToken}
	a.Equal(exp, actual)

	prov.idTokenSource = &FakeErrorIdTokenSource{}
	actual, err = prov.GetToken(context.Background(), apis.TokenRequest{})
	// question is if GetToken should have return type error as it is not used
	a.Nil(err)
	exp = apis.TokenResponse{Success: false, Status: int32(StatusGetTokenFailed), Token: ""}
	a.Equal(exp, actual)

	prov.idTokenSource = &FakeBadTokenIdTokenSource{}
	actual, err = prov.GetToken(context.Background(), apis.TokenRequest{})
	a.Nil(err)
	exp = apis.TokenResponse{Success: false, Status: int32(StatusParseTokenFailed), Token: ""}
	a.Equal(exp, actual)
}

func TestInitToken(t *testing.T) {
	a := assert.New(t)
	token, err := oidc.ParseJWT(testToken)
	a.Nil(err)
	a.NotNil(token)

	prov := &TokenProvider{
		timeout:       10 * time.Second,
		idTokenSource: &FakeIdTokenSource{},
	}

	err = initToken(prov)
	a.Nil(err)

	prov.setCurrentToken(token)

	err = initToken(prov)
	a.Nil(err)

	prov.idTokenSource = &FakeErrorIdTokenSource{}
	err = initToken(prov)
	a.NotNil(err)

	prov.idTokenSource = &FakeBadTokenIdTokenSource{}
	err = initToken(prov)
	a.NotNil(err)
}
