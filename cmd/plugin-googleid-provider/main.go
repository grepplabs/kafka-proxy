package main

import (
	"context"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"github.com/cenkalti/backoff"
	"github.com/grepplabs/kafka-proxy/pkg/apis"
	"github.com/grepplabs/kafka-proxy/pkg/libs/googleid"
	"github.com/grepplabs/kafka-proxy/pkg/libs/util"
	"github.com/grepplabs/kafka-proxy/plugin/gateway-client/shared"
	"github.com/hashicorp/go-plugin"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context/ctxhttp"
	"golang.org/x/oauth2/google"
	"golang.org/x/oauth2/jws"
	"google.golang.org/api/oauth2/v2"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	// https://accounts.google.com/.well-known/openid-configuration
	tokenEndpoint = "https://www.googleapis.com/oauth2/v4/token"
)

const (
	StatusOK               = 0
	StatusGetTokenFailed   = 1
	StatusParseTokenFailed = 2
)

var (
	clockSkew = 1 * time.Minute
	nowFn     = time.Now
)

type TokenProvider struct {
	timeout       time.Duration
	idTokenSource IDTokenSource

	idToken *googleid.Token
	l       sync.RWMutex
}

func (p *TokenProvider) getCurrentToken() string {
	p.l.RLock()
	defer p.l.RUnlock()

	if p.idToken == nil {
		return ""
	}
	if renewLatest(p.idToken) {
		return ""
	}
	return p.idToken.Raw
}

func (p *TokenProvider) getCurrentClaimSet() *googleid.ClaimSet {
	p.l.RLock()
	defer p.l.RUnlock()

	if p.idToken == nil {
		return nil
	}
	return p.idToken.ClaimSet
}

func (p *TokenProvider) setCurrentToken(idToken *googleid.Token) {
	p.l.Lock()
	defer p.l.Unlock()

	p.idToken = idToken
}

func renewLatest(token *googleid.Token) bool {
	if token == nil {
		return true
	}
	// renew before expiry
	advExp := token.ClaimSet.Exp - int64(clockSkew.Seconds())
	if nowFn().Unix() > advExp {
		return true
	}
	return false
}

//TODO: refresh PK if the file was changed  (force token refresh)
// GetToken implements apis.TokenProvider.GetToken method
func (p *TokenProvider) GetToken(parent context.Context, _ apis.TokenRequest) (apis.TokenResponse, error) {

	currentToken := p.getCurrentToken()
	if currentToken != "" {
		return getTokenResponse(currentToken, StatusOK)
	}

	ctx, cancel := context.WithTimeout(parent, p.timeout)
	defer cancel()

	token, err := p.idTokenSource.GetIDToken(ctx)
	if err != nil {
		logrus.Error(err)
		return getTokenResponse("", StatusGetTokenFailed)
	}

	idToken, err := googleid.ParseJWT(token)
	if err != nil {
		logrus.Error(err)
		return getTokenResponse("", StatusParseTokenFailed)
	}
	p.setCurrentToken(idToken)
	logrus.Infof("New token expiry %d (%v)", idToken.ClaimSet.Exp, time.Unix(idToken.ClaimSet.Exp, 0))

	return getTokenResponse(token, StatusOK)
}

func getTokenResponse(token string, status int) (apis.TokenResponse, error) {
	success := status == StatusOK
	return apis.TokenResponse{Success: success, Status: int32(status), Token: token}, nil
}

type IDTokenSource interface {
	GetIDToken(ctx context.Context) (string, error)
}

type AuthorizedUserSource struct {
}

// Retrieve ID-Token using Google Application Default Credentials: authorized_user will have id_token, service_account will not
func (s *AuthorizedUserSource) GetIDToken(ctx context.Context) (string, error) {
	tokenSource, err := google.DefaultTokenSource(ctx, oauth2.UserinfoEmailScope)
	if err != nil {
		return "", err
	}
	token, err := tokenSource.Token()
	if err != nil {
		return "", err
	}
	if token.Extra("id_token") == nil {
		return "", errors.New("id_token is missing")
	}
	idToken := token.Extra("id_token").(string)
	return idToken, nil
}

type ServiceAccountSource struct {
	credentialsFile string
	targetAudience  string

	config *ServiceAccountConfig
	l      sync.RWMutex
}

func (p *ServiceAccountSource) getServiceAccountConfig() *ServiceAccountConfig {
	p.l.RLock()
	defer p.l.RUnlock()

	return p.config
}

func (p *ServiceAccountSource) setServiceAccountConfig(config *ServiceAccountConfig) {
	p.l.Lock()
	defer p.l.Unlock()

	p.config = config
}

type ServiceAccountConfig struct {
	header        *jws.Header
	email         string
	privateClaims map[string]interface{}
	privateKey    *rsa.PrivateKey
}

func NewServiceAccountSource(credentialsFile string, targetAudience string) (*ServiceAccountSource, error) {
	config, err := readServiceAccountConfig(credentialsFile, targetAudience)
	if err != nil {
		return nil, err
	}

	return &ServiceAccountSource{
		credentialsFile: credentialsFile,
		targetAudience:  targetAudience,
		config:          config,
	}, nil
}

func readServiceAccountConfig(credentialsFile string, targetAudience string) (*ServiceAccountConfig, error) {
	data, err := ioutil.ReadFile(credentialsFile)
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
	return &ServiceAccountConfig{
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

func (s *ServiceAccountSource) GetIDToken(parent context.Context) (string, error) {
	return s.getServiceAccountConfig().GetIDToken(parent)
}

func (s *ServiceAccountConfig) GetIDToken(parent context.Context) (string, error) {
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

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if c := resp.StatusCode; c < 200 || c > 299 {
		return nil, fmt.Errorf("cannot fetch id_token: %v\nResponse: %s", resp.Status, body)
	}
	return body, nil
}

func (f *pluginMeta) flagSet() *flag.FlagSet {
	fs := flag.NewFlagSet("google-id provider settings", flag.ContinueOnError)
	return fs
}

type pluginMeta struct {
	timeout int
	adc     bool

	credentialsWatch bool
	credentialsFile  string
	targetAudience   string
}

func main() {
	pluginMeta := &pluginMeta{}
	fs := pluginMeta.flagSet()
	fs.IntVar(&pluginMeta.timeout, "timeout", 10, "Request timeout in seconds")
	fs.BoolVar(&pluginMeta.adc, "adc", false, "Use Google Application Default Credentials instead of ServiceAccount JSON")
	fs.StringVar(&pluginMeta.credentialsFile, "credentials-file", "", "Location of the JSON file with the application credentials")
	fs.BoolVar(&pluginMeta.credentialsWatch, "credentials-watch", true, "Watch credential for reload")
	fs.StringVar(&pluginMeta.targetAudience, "target-audience", "", "URI of audience claim")

	fs.Parse(os.Args[1:])

	if pluginMeta.credentialsFile == "" {
		logrus.Errorf("parameter credentials-file is required")
		os.Exit(1)
	}
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", pluginMeta.credentialsFile)

	if !pluginMeta.adc {
		if pluginMeta.targetAudience == "" {
			logrus.Errorf("parameter target-audience is required")
			os.Exit(1)
		}
	}
	stopChannel := make(chan bool, 1)

	var idTokenSource IDTokenSource
	if pluginMeta.adc {
		idTokenSource = &AuthorizedUserSource{}
	} else {
		serviceAccountSource, err := NewServiceAccountSource(pluginMeta.credentialsFile, pluginMeta.targetAudience)
		idTokenSource = serviceAccountSource
		if err != nil {
			logrus.Errorf("creation of service account source failed : %v")
			os.Exit(1)
		}
		if pluginMeta.credentialsWatch {
			action := func() {
				logrus.Infof("reloading credential file %s", serviceAccountSource.credentialsFile)
				newConfig, err := readServiceAccountConfig(serviceAccountSource.credentialsFile, serviceAccountSource.targetAudience)
				if err != nil {
					logrus.Errorf("error while reloading credentials files: %s", err)
					return
				}
				serviceAccountSource.setServiceAccountConfig(newConfig)
			}
			err = util.WatchForUpdates(pluginMeta.credentialsFile, stopChannel, action)
			if err != nil {
				logrus.Errorf("cannot watch credentials file: %v")
				os.Exit(1)
			}
		}
	}
	tokenProvider := &TokenProvider{timeout: time.Duration(pluginMeta.timeout) * time.Second, idTokenSource: idTokenSource}
	op := func() error {
		return initToken(tokenProvider)
	}
	err := backoff.Retry(op, backoff.WithMaxTries(backoff.NewConstantBackOff(1*time.Second), 3))
	if err != nil {
		logrus.Errorf("getting of initial google-id-token failed : %v", err)
		os.Exit(1)
	}

	tokenRefresher := &TokenRefresher{
		tokenProvider: tokenProvider,
		stopChannel:   stopChannel,
	}

	go tokenRefresher.refreshLoop()

	plugin.Serve(&plugin.ServeConfig{
		HandshakeConfig: shared.Handshake,
		Plugins: map[string]plugin.Plugin{
			"tokenProvider": &shared.TokenProviderPlugin{Impl: tokenProvider},
		},
		// A non-nil value here enables gRPC serving for this plugin...
		GRPCServer: plugin.DefaultGRPCServer,
	})
}

func initToken(tokenProvider *TokenProvider) error {
	ctx, cancel := context.WithTimeout(context.Background(), tokenProvider.timeout)
	defer cancel()

	resp, err := tokenProvider.GetToken(ctx, apis.TokenRequest{})
	if err != nil {
		return err
	}
	if !resp.Success {
		return fmt.Errorf("get token failed with status: %d", resp.Status)
	}
	return nil
}

type TokenRefresher struct {
	tokenProvider *TokenProvider
	stopChannel   chan bool
}

func (p *TokenRefresher) refreshLoop() {
	defer func() {
		if r := recover(); r != nil {
			var ok bool
			err, ok := r.(error)
			if ok {
				logrus.Error(fmt.Sprintf("token refresh loop error %v", err))
			}
		}
	}()

	syncTicker := time.NewTicker(2 * time.Second)
	for {
		select {
		case <-syncTicker.C:
			p.refreshTick()
		case <-p.stopChannel:
			return
		}
	}
}

func (p *TokenRefresher) newToken() (*googleid.Token, error) {
	ctx, cancel := context.WithTimeout(context.Background(), p.tokenProvider.timeout)
	defer cancel()
	token, err := p.tokenProvider.idTokenSource.GetIDToken(ctx)
	if err != nil {
		return nil, err
	}
	return googleid.ParseJWT(token)
}

func (p *TokenRefresher) tryRefresh() error {
	var idToken *googleid.Token
	op := func() error {
		var err error
		idToken, err = p.newToken()
		return err
	}
	backOff := backoff.NewExponentialBackOff()
	backOff.MaxElapsedTime = 60 * time.Second
	backOff.MaxInterval = 10 * time.Second
	err := backoff.Retry(op, backOff)
	if err != nil {
		return err
	}
	logrus.Infof("Refreshed token expiry %d (%v)", idToken.ClaimSet.Exp, time.Unix(idToken.ClaimSet.Exp, 0))
	p.tokenProvider.setCurrentToken(idToken)
	return nil
}

func (p *TokenRefresher) refreshTick() {
	claimSet := p.tokenProvider.getCurrentClaimSet()
	if renewEarliest(claimSet) {
		err := p.tryRefresh()
		if err != nil {
			logrus.Errorf("refreshing of google-id-token failed : %v", err)
		}
	}
}

func renewEarliest(claimSet *googleid.ClaimSet) bool {
	if claimSet == nil {
		return true
	}
	validity := claimSet.Exp - claimSet.Iat
	if validity <= 0 {
		// should would be invalid claim
		return true
	}
	refreshAfter := int64(validity / 2)
	refreshTime := claimSet.Exp - int64(clockSkew.Seconds()) - refreshAfter
	// logrus.Debugf("New refresh time %d (%v)", refreshTime, time.Unix(refreshTime, 0))
	if nowFn().Unix() > refreshTime {
		return true
	}
	return false
}
