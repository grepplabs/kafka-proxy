package awsmskiamprovider

import (
	"context"
	"fmt"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/grepplabs/kafka-proxy/pkg/apis"
	"github.com/sirupsen/logrus"
)

// TokenRefresher - struct providing refreshing of AWS MSK IAM tokens
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
				logrus.Error(fmt.Sprintf("AWS MSK IAM token refresh loop error %v", err))
			}
		}
	}()

	// Check every 30 seconds if token needs refresh
	syncTicker := time.NewTicker(30 * time.Second)

	for {
		select {
		case <-syncTicker.C:
			p.refreshTick()
		case <-p.stopChannel:
			return
		}
	}
}

func (p *TokenRefresher) tryRefresh() error {
	ctx, cancel := context.WithTimeout(context.Background(), p.tokenProvider.timeout)
	defer cancel()

	resp, err := p.tokenProvider.GetToken(ctx, apis.TokenRequest{})

	if err != nil {
		return err
	}

	if !resp.Success {
		return fmt.Errorf("get token failed with status: %d", resp.Status)
	}

	return nil
}

func (p *TokenRefresher) refreshTick() {
	// Check if we need to refresh the token
	if p.tokenProvider.shouldRenew() {
		op := func() error {
			return p.tryRefresh()
		}

		backOff := backoff.NewExponentialBackOff()
		backOff.MaxElapsedTime = 60 * time.Second
		backOff.MaxInterval = 10 * time.Second
		err := backoff.Retry(op, backOff)

		if err != nil {
			logrus.Errorf("refreshing of AWS MSK IAM token failed: %v", err)
		} else {
			logrus.Debugf("AWS MSK IAM token refreshed successfully")
		}
	}
}
