package oidcprovider

import (
	"context"
	"fmt"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/grepplabs/kafka-proxy/pkg/libs/oidc"
	"github.com/sirupsen/logrus"
)

// TokenRefresher - struct providing refreshing of tokens
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

func (p *TokenRefresher) newToken() (*oidc.Token, error) {
	ctx, cancel := context.WithTimeout(context.Background(), p.tokenProvider.timeout)
	defer cancel()

	token, err := p.tokenProvider.idTokenSource.GetIDToken(ctx)

	if err != nil {
		return nil, err
	}

	return oidc.ParseJWT(token)
}

func (p *TokenRefresher) tryRefresh() error {
	var idToken *oidc.Token

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

	logrus.Infof(
		"Refreshed token expiry %d (%v)",
		idToken.ClaimSet.Exp,
		time.Unix(idToken.ClaimSet.Exp, 0),
	)

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

func renewEarliest(claimSet *oidc.ClaimSet) bool {
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
