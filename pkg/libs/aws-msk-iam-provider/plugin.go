package awsmskiamprovider

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/grepplabs/kafka-proxy/pkg/apis"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const (
	StatusOK             = 0
	StatusGetTokenFailed = 1
)

var (
	// Token expiry buffer - refresh token 5 minutes before expiry
	tokenExpiryBuffer = 5 * time.Minute
	nowFn             = time.Now
)

// TokenProvider - represents AWS MSK IAM TokenProvider
type TokenProvider struct {
	timeout time.Duration
	region  string

	// Authentication configuration
	profile     string
	roleARN     string
	externalID  string
	sessionName string

	// Token generation
	signer TokenSigner

	currentToken  string
	currentExpiry time.Time
	l             sync.RWMutex
}

// TokenProviderOptions - options specific for AWS MSK IAM
type TokenProviderOptions struct {
	Region      string
	Profile     string
	RoleARN     string
	ExternalID  string
	SessionName string
	Timeout     int
}

// NewTokenProvider - Generate new AWS MSK IAM token provider
func NewTokenProvider(options TokenProviderOptions, signer TokenSigner) (*TokenProvider, error) {
	tokenProvider := &TokenProvider{
		timeout:     time.Duration(options.Timeout) * time.Second,
		region:      options.Region,
		profile:     options.Profile,
		roleARN:     options.RoleARN,
		externalID:  options.ExternalID,
		sessionName: options.SessionName,
		signer:      signer,
	}

	// Initialize with first token
	op := func() error {
		return initToken(tokenProvider)
	}

	err := backoff.Retry(
		op,
		backoff.WithMaxTries(backoff.NewConstantBackOff(1*time.Second), 3))

	if err != nil {
		return nil, errors.Wrap(err, "getting of initial AWS MSK IAM token failed")
	}

	tokenRefresher := &TokenRefresher{
		tokenProvider: tokenProvider,
		stopChannel:   make(chan bool, 1),
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

	if p.currentToken == "" {
		return ""
	}

	if p.shouldRenew() {
		return ""
	}

	return p.currentToken
}

func (p *TokenProvider) shouldRenew() bool {
	return nowFn().After(p.currentExpiry.Add(-tokenExpiryBuffer))
}

func (p *TokenProvider) setCurrentToken(token string, expiry time.Time) {
	p.l.Lock()
	defer p.l.Unlock()

	p.currentToken = token
	p.currentExpiry = expiry
}

// GetToken implements apis.TokenProvider.GetToken method
func (p *TokenProvider) GetToken(parent context.Context, _ apis.TokenRequest) (apis.TokenResponse, error) {
	currentToken := p.getCurrentToken()

	if currentToken != "" {
		return getTokenResponse(currentToken, StatusOK)
	}

	ctx, cancel := context.WithTimeout(parent, p.timeout)
	defer cancel()

	var token string
	var err error

	// Generate token based on configuration
	if p.roleARN != "" {
		if p.externalID != "" {
			token, _, err = p.signer.GenerateAuthTokenFromRoleWithExternalId(
				ctx, p.region, p.roleARN, p.sessionName, p.externalID)
		} else {
			token, _, err = p.signer.GenerateAuthTokenFromRole(
				ctx, p.region, p.roleARN, p.sessionName)
		}
	} else if p.profile != "" {
		token, _, err = p.signer.GenerateAuthTokenFromProfile(ctx, p.region, p.profile)
	} else {
		token, _, err = p.signer.GenerateAuthToken(ctx, p.region)
	}

	if err != nil {
		logrus.Errorf("GenerateAuthToken failed: %v", err)
		return getTokenResponse("", StatusGetTokenFailed)
	}

	// AWS MSK IAM tokens are typically valid for 15 minutes
	// We'll set expiry to 14 minutes to be safe
	expiry := nowFn().Add(14 * time.Minute)
	p.setCurrentToken(token, expiry)

	logrus.Infof("New AWS MSK IAM token generated, expires at %v", expiry)

	return getTokenResponse(token, StatusOK)
}

func getTokenResponse(token string, status int) (apis.TokenResponse, error) {
	success := status == StatusOK
	return apis.TokenResponse{Success: success, Status: int32(status), Token: token}, nil
}
