package oidcprovider

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/grepplabs/kafka-proxy/pkg/libs/oidc"
	"github.com/stretchr/testify/assert"
)

type FakeGeneratedIdTokenSource struct{}

func (s *FakeGeneratedIdTokenSource) GetIDToken(ctx context.Context) (string, error) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)

	if err != nil {
		return "", err
	}

	testHeader := `{
       "alg": "RS256",
       "kid": "978ca4118bf1883b316bbca6ce9044d9977f2027"
    }`
	testClaims := `{
       "azp": "4712.apps.googleusercontent.com",
       "aud": "4711.apps.googleusercontent.com",
       "sub": "100004711",
       "hd": "grepplabs.com",
       "email": "info@grepplabs.com",
       "email_verified": true,
       "exp": ` + strconv.Itoa(int(time.Now().Add(10*time.Second).Unix())) + `,
       "iss": "accounts.google.com",
       "iat": ` + strconv.Itoa(int(time.Now().Unix())) + `
       }`

	tokenString, err := encodeTestToken(testHeader, testClaims, privateKey)

	if err != nil {
		return "", nil
	}

	return tokenString, nil
}

func TestRenewEarliest(t *testing.T) {
	clockSkew = 1 * time.Second
	a := assert.New(t)
	source := &FakeGeneratedIdTokenSource{}
	tokenStr, err := source.GetIDToken(context.Background())
	a.Nil(err)
	a.NotNil(tokenStr)

	token, err := oidc.ParseJWT(tokenStr)
	a.Nil(err)

	actual := renewEarliest(nil)
	a.True(actual)

	token.ClaimSet.Exp = time.Now().Unix()
	token.ClaimSet.Iat = time.Now().Unix()

	actual = renewEarliest(token.ClaimSet)
	a.True(actual)

	token.ClaimSet.Exp = time.Now().Add(5 * time.Second).Unix()
	token.ClaimSet.Iat = time.Now().Unix() - (5)

	actual = renewEarliest(token.ClaimSet)
	a.True(actual)

	token.ClaimSet.Exp = time.Now().Add(10 * time.Second).Unix()
	token.ClaimSet.Iat = time.Now().Unix()

	actual = renewEarliest(token.ClaimSet)
	a.False(actual)
}

func TestNewToken(t *testing.T) {
	a := assert.New(t)
	prov := &TokenProvider{}
	prov.timeout = 10 * time.Second
	prov.idTokenSource = &FakeGeneratedIdTokenSource{}

	stopCh := make(chan bool)
	refresher := &TokenRefresher{
		tokenProvider: prov,
		stopChannel:   stopCh,
	}

	token, err := refresher.newToken()
	a.Nil(err)
	a.NotNil(token)

	refresher.tokenProvider.idTokenSource = &FakeErrorIdTokenSource{}
	token, err = refresher.newToken()
	a.NotNil(err)
	a.Nil(token)
}

func TestTryRefresh(t *testing.T) {
	t.Skip("This can take time as there is backoff in function")
	a := assert.New(t)
	prov := &TokenProvider{}
	prov.timeout = 10 * time.Second
	prov.idTokenSource = &FakeGeneratedIdTokenSource{}

	stopCh := make(chan bool)
	refresher := &TokenRefresher{
		tokenProvider: prov,
		stopChannel:   stopCh,
	}

	err := refresher.tryRefresh()
	a.Nil(err)

	refresher.tokenProvider.idTokenSource = &FakeErrorIdTokenSource{}
	err = refresher.tryRefresh()
	a.NotNil(err)
}

func TestRefreshTick(t *testing.T) {
	t.Skip("This can take time as there is backoff in function")
	a := assert.New(t)
	prov := &TokenProvider{}
	prov.timeout = 10 * time.Second
	prov.idTokenSource = &FakeGeneratedIdTokenSource{}

	stopCh := make(chan bool)
	refresher := &TokenRefresher{
		tokenProvider: prov,
		stopChannel:   stopCh,
	}

	a.Nil(prov.idToken)

	refresher.tokenProvider.idTokenSource = &FakeErrorIdTokenSource{}
	refresher.refreshTick()
	a.Nil(prov.idToken)

	refresher.refreshTick()
	a.NotNil(prov.idToken)
	a.Equal(prov.idToken.ClaimSet.Sub, "100004711")
}

func TestRefreshLoop(t *testing.T) {
	a := assert.New(t)
	prov := &TokenProvider{}
	prov.timeout = 10 * time.Second
	prov.idTokenSource = &FakeGeneratedIdTokenSource{}

	stopCh := make(chan bool)
	refresher := &TokenRefresher{
		tokenProvider: prov,
		stopChannel:   stopCh,
	}

	a.Nil(prov.idToken)

	refresher.tokenProvider.idTokenSource = &FakeGeneratedIdTokenSource{}
	go refresher.refreshLoop()

	time.AfterFunc(3*time.Second, func() {
		a.NotNil(prov.idToken)
		a.Equal(prov.idToken.ClaimSet.Sub, "100004711")
		firstTime := prov.idToken.ClaimSet.Exp

		time.AfterFunc(3*time.Second, func() {
			a.NotNil(prov.idToken)
			a.Equal(prov.idToken.ClaimSet.Sub, "100004711")
		})

		secondTime := prov.idToken.ClaimSet.Exp

		a.NotEqual(secondTime, firstTime)
	})

	stopCh <- false
}

func encodeTestToken(headerJSON string, claimsJSON string, key *rsa.PrivateKey) (string, error) {
	sg := func(data []byte) (sig []byte, err error) {
		h := sha256.New()
		h.Write(data)
		return rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, h.Sum(nil))
	}
	ss := fmt.Sprintf("%s.%s", base64.RawURLEncoding.EncodeToString([]byte(headerJSON)), base64.RawURLEncoding.EncodeToString([]byte(claimsJSON)))
	sig, err := sg([]byte(ss))
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s.%s", ss, base64.RawURLEncoding.EncodeToString(sig)), nil
}
