package googleid

import (
	"context"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"golang.org/x/net/context/ctxhttp"
	"io/ioutil"
	"math/big"
	"net/http"
	"time"
)

const (
	// https://accounts.google.com/.well-known/openid-configuration
	jwksUri = "https://www.googleapis.com/oauth2/v3/certs"
)

type Certs struct {
	Keys []Keys `json:"keys"`
}

// https://tools.ietf.org/html/rfc7517#appendix-A
type Keys struct {
	Kty string `json:"kty"`
	Alg string `json:"alg"`
	Use string `json:"use"`
	Kid string `json:"kid"`
	N   string `json:"n"`
	E   string `json:"e"`
}

func (key *Keys) GetPublicKey() (*rsa.PublicKey, error) {
	b, err := base64.RawURLEncoding.DecodeString(key.N)
	if err != nil {
		return nil, err
	}
	n := new(big.Int).SetBytes(b)

	b, err = base64.RawURLEncoding.DecodeString(key.E)
	if err != nil {
		return nil, err
	}
	e := new(big.Int).SetBytes(b)

	pub := &rsa.PublicKey{N: n, E: int(e.Uint64())}
	return pub, nil
}

func GetCerts(ctx context.Context) (*Certs, error) {
	client := &http.Client{
		Timeout: time.Second * 10,
	}
	resp, err := ctxhttp.Get(ctx, client, jwksUri)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if c := resp.StatusCode; c < 200 || c > 299 {
		return nil, fmt.Errorf("cannot fetch certs: %v\nResponse: %s", resp.Status, body)
	}
	var certs *Certs
	if err = json.Unmarshal(body, &certs); err != nil {
		return nil, err
	}
	return certs, nil
}
