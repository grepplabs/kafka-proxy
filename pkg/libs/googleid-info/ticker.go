package googleidinfo

import (
	"github.com/cenkalti/backoff"
	"github.com/sirupsen/logrus"
	"sort"
	"time"
)

type certsRefresher struct {
	tokenInfo   *TokenInfo
	stopChannel chan struct{}
	interval    time.Duration
}

func newCertsRefresher(tokenInfo *TokenInfo, stopChannel chan struct{}, interval time.Duration) *certsRefresher {
	return &certsRefresher{
		tokenInfo:   tokenInfo,
		stopChannel: stopChannel,
		interval:    interval,
	}
}

func (p *certsRefresher) refreshLoop() {
	defer func() {
		if r := recover(); r != nil {
			var ok bool
			err, ok := r.(error)
			if ok {
				logrus.Errorf("certs refresh loop error %v", err)
			}
		}
	}()
	logrus.Infof("Refreshing certs every: %v", p.interval)
	syncTicker := time.NewTicker(p.interval)
	for {
		select {
		case <-syncTicker.C:
			p.refreshTick()
		case <-p.stopChannel:
			return
		}
	}
}

func (p *certsRefresher) refreshTick() error {
	op := func() error {
		return p.tokenInfo.refreshCerts()
	}
	backOff := backoff.NewExponentialBackOff()
	backOff.MaxElapsedTime = 30 * time.Minute
	backOff.MaxInterval = 2 * time.Minute
	err := backoff.Retry(op, backOff)
	if err != nil {
		return err
	}
	kids := p.tokenInfo.getPublicKeyIDs()
	sort.Strings(kids)
	logrus.Infof("Refreshed certs Key IDs: %v", kids)
	return nil
}
