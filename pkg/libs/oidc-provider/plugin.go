package oidcprovider

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/grepplabs/kafka-proxy/pkg/apis"
	"github.com/grepplabs/kafka-proxy/pkg/libs/oidc"
	"github.com/grepplabs/kafka-proxy/pkg/libs/util"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
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

// TokenProvider - represents TokenProvider
type TokenProvider struct {
	timeout       time.Duration
	idTokenSource idTokenSource

	idToken *oidc.Token
	l       sync.RWMutex
}

// TokenProviderOptions - options specific for oidc
type TokenProviderOptions struct {
	Timeout int

	CredentialsWatch bool
	CredentialsFile  string
	TargetAudience   string
}

type GrantType struct {
	Name string `json:"grant_type"`
}

// NewTokenProvider - Generate new OIDC token provider
func NewTokenProvider(options TokenProviderOptions) (*TokenProvider, error) {
	os.Setenv("OIDC_CREDENTIALS", options.CredentialsFile)
	stopChannel := make(chan bool, 1)
	var idTokenSource idTokenSource

	idTokenSource, err := getTokenSource(options.CredentialsFile, options.TargetAudience)

	if err != nil {
		return nil, err
	}

	if options.CredentialsWatch {
		action := func() {
			logrus.Infof("reloading credential file %s", options.CredentialsFile)

			idTokenSource, err = getTokenSource(options.CredentialsFile, options.TargetAudience)

			if err != nil {
				logrus.Errorf("error while reloading credentials files: %s", err)
				return
			}
		}

		err = util.WatchForUpdates(options.CredentialsFile, stopChannel, action)

		if err != nil {
			return nil, errors.Wrap(err, "cannot watch credentials file")
		}
	}

	tokenProvider := &TokenProvider{
		timeout:       time.Duration(options.Timeout) * time.Second,
		idTokenSource: idTokenSource}

	op := func() error {
		return initToken(tokenProvider)
	}

	err = backoff.Retry(
		op,
		backoff.WithMaxTries(backoff.NewConstantBackOff(1*time.Second), 3))

	if err != nil {
		return nil, errors.Wrap(err, "getting of initial oidc token failed")
	}

	tokenRefresher := &TokenRefresher{
		tokenProvider: tokenProvider,
		stopChannel:   stopChannel,
	}

	go tokenRefresher.refreshLoop()

	return tokenProvider, nil
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

func (p *TokenProvider) getCurrentClaimSet() *oidc.ClaimSet {
	p.l.RLock()
	defer p.l.RUnlock()

	if p.idToken == nil {
		return nil
	}

	return p.idToken.ClaimSet
}

func (p *TokenProvider) setCurrentToken(idToken *oidc.Token) {
	p.l.Lock()
	defer p.l.Unlock()

	p.idToken = idToken
}

func renewLatest(token *oidc.Token) bool {
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
		logrus.Errorf("GetIDToken failed %v", err)
		return getTokenResponse("", StatusGetTokenFailed)
	}

	idToken, err := oidc.ParseJWT(token)

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

func getTokenSource(credentialsFilePath string, targetAud string) (idTokenSource, error) {
	data, err := os.ReadFile(credentialsFilePath)

	if err != nil {
		return nil, err
	}

	grantType := &GrantType{}

	err = json.Unmarshal(data, grantType)

	if err != nil {
		return nil, err
	}

	switch grantType.Name {
	case "password":
		passwordGrantSource, err := NewPasswordGrantSource(
			credentialsFilePath,
			targetAud)

		if err != nil {
			return nil, errors.Wrap(err, "creation of password grant source failed")
		}

		return passwordGrantSource, nil
	default:
		serviceAccountSource, err := NewServiceAccountSource(
			credentialsFilePath,
			targetAud)

		if err != nil {
			return nil, errors.Wrap(err, "creation of service account source failed")
		}

		return serviceAccountSource, nil
	}
}

type idTokenSource interface {
	GetIDToken(ctx context.Context) (string, error)
}

type serviceAccountSource struct {
	source *oidc.ServiceAccountTokenSource
	l      sync.RWMutex
}

func (p *serviceAccountSource) getServiceAccountTokenSource() *oidc.ServiceAccountTokenSource {
	p.l.RLock()
	defer p.l.RUnlock()

	return p.source
}

func (p *serviceAccountSource) setServiceAccountTokenSource(source *oidc.ServiceAccountTokenSource) {
	p.l.Lock()
	defer p.l.Unlock()

	p.source = source
}

func NewServiceAccountSource(credentialsFile string, targetAudience string) (*serviceAccountSource, error) {
	source, err := oidc.NewServiceAccountTokenSource(credentialsFile, targetAudience)

	if err != nil {
		return nil, err
	}

	return &serviceAccountSource{
		source: source,
	}, nil
}

func (s *serviceAccountSource) GetIDToken(parent context.Context) (string, error) {
	return s.getServiceAccountTokenSource().GetIDToken(parent)
}

type passwordGrantSource struct {
	source *oidc.PasswordGrantTokenSource
	l      sync.RWMutex
}

func (p *passwordGrantSource) getPasswordGrantTokenSource() *oidc.PasswordGrantTokenSource {
	p.l.RLock()
	defer p.l.RUnlock()

	return p.source
}

func (p *passwordGrantSource) setPasswordGrantTokenSource(source *oidc.PasswordGrantTokenSource) {
	p.l.Lock()
	defer p.l.Unlock()

	p.source = source
}

func NewPasswordGrantSource(credentialsFile string, targetAudience string) (*passwordGrantSource, error) {
	source, err := oidc.NewPasswordGrantTokenSource(credentialsFile, targetAudience)

	if err != nil {
		return nil, err
	}

	return &passwordGrantSource{
		source: source,
	}, nil
}

func (s *passwordGrantSource) GetIDToken(parent context.Context) (string, error) {
	return s.getPasswordGrantTokenSource().GetIDToken(parent)
}
