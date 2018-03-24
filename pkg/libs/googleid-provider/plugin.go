package googleidprovider

import (
	"context"
	"fmt"
	"github.com/cenkalti/backoff"
	"github.com/grepplabs/kafka-proxy/pkg/apis"
	"github.com/grepplabs/kafka-proxy/pkg/libs/googleid"
	"github.com/grepplabs/kafka-proxy/pkg/libs/util"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"os"
	"sync"
	"time"
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
	idTokenSource idTokenSource

	idToken *googleid.Token
	l       sync.RWMutex
}

type TokenProviderOptions struct {
	Timeout int
	Adc     bool

	CredentialsWatch bool
	CredentialsFile  string
	TargetAudience   string
}

func NewTokenProvider(options TokenProviderOptions) (*TokenProvider, error) {
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", options.CredentialsFile)

	if !options.Adc {
		if options.TargetAudience == "" {
			return nil, errors.New("parameter target-audience is required")
		}
	}
	stopChannel := make(chan bool, 1)

	var idTokenSource idTokenSource
	if options.Adc {
		idTokenSource = newAuthorizedUserSource()
	} else {
		serviceAccountSource, err := NewServiceAccountSource(options.CredentialsFile, options.TargetAudience)
		idTokenSource = serviceAccountSource
		if err != nil {
			return nil, errors.Wrap(err, "creation of service account source failed")
		}
		if options.CredentialsWatch {
			action := func() {
				logrus.Infof("reloading credential file %s", options.CredentialsFile)
				newConfig, err := googleid.NewServiceAccountTokenSource(options.CredentialsFile, options.TargetAudience)
				if err != nil {
					logrus.Errorf("error while reloading credentials files: %s", err)
					return
				}
				serviceAccountSource.setServiceAccountTokenSource(newConfig)
			}
			err = util.WatchForUpdates(options.CredentialsFile, stopChannel, action)
			if err != nil {
				return nil, errors.Wrap(err, "cannot watch credentials file")
			}
		}
	}
	tokenProvider := &TokenProvider{timeout: time.Duration(options.Timeout) * time.Second, idTokenSource: idTokenSource}
	op := func() error {
		return initToken(tokenProvider)
	}
	err := backoff.Retry(op, backoff.WithMaxTries(backoff.NewConstantBackOff(1*time.Second), 3))
	if err != nil {
		return nil, errors.Wrap(err, "getting of initial google-id-token failed")
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

type idTokenSource interface {
	GetIDToken(ctx context.Context) (string, error)
}

type authorizedUserSource struct {
	source *googleid.AuthorizedUserTokenSource
}

func (s *authorizedUserSource) GetIDToken(ctx context.Context) (string, error) {
	return s.source.GetIDToken(ctx)
}
func newAuthorizedUserSource() *authorizedUserSource {
	source := googleid.NewAuthorizedUserTokenSource()
	return &authorizedUserSource{source: source}
}

type serviceAccountSource struct {
	source *googleid.ServiceAccountTokenSource
	l      sync.RWMutex
}

func (p *serviceAccountSource) getServiceAccountTokenSource() *googleid.ServiceAccountTokenSource {
	p.l.RLock()
	defer p.l.RUnlock()

	return p.source
}

func (p *serviceAccountSource) setServiceAccountTokenSource(source *googleid.ServiceAccountTokenSource) {
	p.l.Lock()
	defer p.l.Unlock()

	p.source = source
}

func NewServiceAccountSource(credentialsFile string, targetAudience string) (*serviceAccountSource, error) {
	source, err := googleid.NewServiceAccountTokenSource(credentialsFile, targetAudience)
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
