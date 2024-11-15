package googleid

import (
	"context"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"golang.org/x/net/context/ctxhttp"
	"golang.org/x/oauth2/google"
	"golang.org/x/oauth2/jws"
	"google.golang.org/api/oauth2/v2"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

const (
	// https://accounts.google.com/.well-known/openid-configuration
	tokenEndpoint = "https://www.googleapis.com/oauth2/v4/token"
)

type ServiceAccountTokenSource struct {
	header        *jws.Header
	email         string
	privateClaims map[string]interface{}
	privateKey    *rsa.PrivateKey
}

func NewServiceAccountTokenSource(credentialsFile string, targetAudience string) (*ServiceAccountTokenSource, error) {
	data, err := os.ReadFile(credentialsFile)
	if err != nil {
		return nil, err
	}
	conf, err := google.JWTConfigFromJSON(data, oauth2.UserinfoEmailScope)
	if err != nil {
		return nil, err
	}
	header := &jws.Header{
		Algorithm: "RS256",
		Typ:       "JWT",
		KeyID:     conf.PrivateKeyID,
	}
	// "tcp://kafka-gateway.service.dev.recom-gcp.com"
	privateClaims := map[string]interface{}{"target_audience": targetAudience}

	parsed, err := parseKey(conf.PrivateKey)
	if err != nil {
		return nil, err
	}
	return &ServiceAccountTokenSource{
		header:        header,
		privateClaims: privateClaims,
		email:         conf.Email,
		privateKey:    parsed}, nil
}

// https://github.com/golang/oauth2/blob/master/internal/oauth2.go
func parseKey(key []byte) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode(key)
	if block != nil {
		key = block.Bytes
	}
	parsedKey, err := x509.ParsePKCS8PrivateKey(key)
	if err != nil {
		parsedKey, err = x509.ParsePKCS1PrivateKey(key)
		if err != nil {
			return nil, fmt.Errorf("private key should be a PEM or plain PKSC1 or PKCS8; parse error: %v", err)
		}
	}
	parsed, ok := parsedKey.(*rsa.PrivateKey)
	if !ok {
		return nil, errors.New("private key is invalid")
	}
	return parsed, nil
}

func (s *ServiceAccountTokenSource) GetIDToken(parent context.Context) (string, error) {
	iat := time.Now()
	exp := iat.Add(time.Hour)

	payload := &jws.ClaimSet{
		Iss:           s.email,
		Iat:           iat.Unix(),
		Exp:           exp.Unix(),
		Aud:           tokenEndpoint,
		PrivateClaims: s.privateClaims,
	}
	token, err := jws.Encode(s.header, payload, s.privateKey)
	if err != nil {
		return "", err
	}
	body, err := doExchange(parent, token)
	if err != nil {
		return "", err
	}
	var idTokenRes struct {
		IDToken string `json:"id_token"`
	}
	if err := json.Unmarshal(body, &idTokenRes); err != nil {
		return "", err
	}
	if idTokenRes.IDToken == "" {
		return "", errors.New("id_token is missing")
	}
	return idTokenRes.IDToken, nil
}

func doExchange(ctx context.Context, token string) ([]byte, error) {
	d := url.Values{}
	d.Set("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer")
	d.Add("assertion", token)

	client := &http.Client{
		Timeout: time.Second * 10,
	}
	req, err := http.NewRequest("POST", tokenEndpoint, strings.NewReader(d.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")

	resp, err := ctxhttp.Do(ctx, client, req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if c := resp.StatusCode; c < 200 || c > 299 {
		return nil, fmt.Errorf("cannot fetch id_token: %v\nResponse: %s", resp.Status, body)
	}
	return body, nil
}
