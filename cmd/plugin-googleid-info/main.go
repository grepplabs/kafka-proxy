package main

import (
	"context"
	"crypto/rsa"
	"errors"
	"flag"
	"fmt"
	"github.com/cenkalti/backoff"
	"github.com/grepplabs/kafka-proxy/pkg/apis"
	"github.com/grepplabs/kafka-proxy/pkg/libs/googleid"
	"github.com/grepplabs/kafka-proxy/plugin/gateway-server/shared"
	"github.com/hashicorp/go-plugin"
	"github.com/sirupsen/logrus"
	"golang.org/x/oauth2/jws"
	"os"
	"sort"
	"sync"
	"time"
)

const (
	StatusOK                      = 0
	StatusEmptyToken              = 1
	StatusParseJWTFailed          = 2
	StatusNoIssueTimeInToken      = 3
	StatusNoExpirationTimeInToken = 4
	StatusPublicKeyNotFound       = 5
	StatusWrongIssuer             = 6
	StatusWrongSignature          = 7
	StatusTokenTooEarly           = 8
	StatusTokenExpired            = 9
	StatusWrongAudience           = 10
	StatusWrongEmail              = 11
)

var (
	clockSkew = 1 * time.Minute
	nowFn     = time.Now

	issuers = map[string]struct{}{
		"accounts.google.com":         {},
		"https://accounts.google.com": {},
	}
)

type TokenInfo struct {
	timeout  time.Duration
	audience map[string]struct{}
	emails   map[string]struct{}

	publicKeys map[string]*rsa.PublicKey
	l          sync.RWMutex
}

func (p *TokenInfo) getPublicKey(kid string) *rsa.PublicKey {
	p.l.RLock()
	defer p.l.RUnlock()

	return p.publicKeys[kid]
}

func (p *TokenInfo) getPublicKeyIDs() []string {
	p.l.RLock()
	defer p.l.RUnlock()
	kids := make([]string, 0)
	for kid := range p.publicKeys {
		kids = append(kids, kid)
	}
	return kids
}

func (p *TokenInfo) setPublicKeys(publicKeys map[string]*rsa.PublicKey) {
	p.l.Lock()
	defer p.l.Unlock()

	p.publicKeys = publicKeys
}

func (p *TokenInfo) refreshCerts() error {
	ctx, cancel := context.WithTimeout(context.Background(), p.timeout)
	defer cancel()

	certs, err := googleid.GetCerts(ctx)
	if err != nil {
		return err
	}
	if len(certs.Keys) == 0 {
		return errors.New("certs keys must not be empty")
	}

	publicKeys := make(map[string]*rsa.PublicKey)
	for _, key := range certs.Keys {
		publicKey, err := key.GetPublicKey()
		if err != nil {
			return fmt.Errorf("cannot parse public key: %v", key.Kid)
		}
		publicKeys[key.Kid] = publicKey
	}

	p.setPublicKeys(publicKeys)

	return nil
}

func (p *TokenInfo) VerifyToken(parent context.Context, request apis.VerifyRequest) (apis.VerifyResponse, error) {
	// logrus.Infof("VerifyToken: %s", request.Token)
	if request.Token == "" {
		return getVerifyResponseResponse(StatusEmptyToken)
	}

	token, err := googleid.ParseJWT(request.Token)
	if err != nil {
		return getVerifyResponseResponse(StatusParseJWTFailed)
	}
	if _, ok := issuers[token.ClaimSet.Iss]; !ok {
		return getVerifyResponseResponse(StatusWrongIssuer)
	}
	if token.ClaimSet.Iat < 1 {
		return getVerifyResponseResponse(StatusNoIssueTimeInToken)
	}
	if token.ClaimSet.Exp < 1 {
		return getVerifyResponseResponse(StatusNoExpirationTimeInToken)
	}

	earliest := token.ClaimSet.Iat - int64(clockSkew.Seconds())
	latest := token.ClaimSet.Exp + int64(clockSkew.Seconds())
	unix := nowFn().Unix()

	if unix < earliest {
		return getVerifyResponseResponse(StatusTokenTooEarly)
	}
	if unix > latest {
		return getVerifyResponseResponse(StatusTokenExpired)
	}

	if len(p.audience) != 0 {
		if _, ok := p.audience[token.ClaimSet.Aud]; !ok {
			return getVerifyResponseResponse(StatusWrongAudience)
		}
	}
	if _, ok := p.emails[token.ClaimSet.Email]; !ok {
		return getVerifyResponseResponse(StatusWrongEmail)
	}
	publicKey := p.getPublicKey(token.Header.KeyID)

	if publicKey == nil {
		return getVerifyResponseResponse(StatusPublicKeyNotFound)
	}
	err = jws.Verify(request.Token, publicKey)
	if err != nil {
		return getVerifyResponseResponse(StatusWrongSignature)
	}
	return apis.VerifyResponse{Success: true}, nil
}

func getVerifyResponseResponse(status int) (apis.VerifyResponse, error) {
	success := status == StatusOK
	return apis.VerifyResponse{Success: success, Status: int32(status)}, nil
}

func (f *pluginMeta) flagSet() *flag.FlagSet {
	fs := flag.NewFlagSet("google-id info settings", flag.ContinueOnError)
	return fs
}

type pluginMeta struct {
	timeout              int
	certsRefreshInterval int
	audience             arrayFlags
	emails               arrayFlags
}

type arrayFlags []string

func (i *arrayFlags) String() string {
	return fmt.Sprintf("%v", *i)
}

func (i *arrayFlags) Set(value string) error {
	*i = append(*i, value)
	return nil
}

func (i *arrayFlags) asMap() map[string]struct{} {
	result := make(map[string]struct{})
	for _, elem := range *i {
		result[elem] = struct{}{}
	}
	return result
}

func main() {
	pluginMeta := &pluginMeta{}
	fs := pluginMeta.flagSet()
	fs.IntVar(&pluginMeta.timeout, "timeout", 10, "Request timeout in seconds")
	fs.IntVar(&pluginMeta.certsRefreshInterval, "certs-refresh-interval", 60*60, "Certificates refresh interval in seconds")

	fs.Var(&pluginMeta.audience, "audience", "The audience of a token")
	fs.Var(&pluginMeta.emails, "email", "Email claim")

	fs.Parse(os.Args[1:])

	logrus.Infof("Plugin metadata %v", pluginMeta)

	audience := pluginMeta.audience.asMap()
	emails := pluginMeta.emails.asMap()

	logrus.Infof("JWT target audience: %v", audience)
	logrus.Infof("JWT allowed emails: %v", emails)

	if len(emails) == 0 {
		logrus.Errorf("parameter email is required")
		os.Exit(1)
	}

	tokenInfo := &TokenInfo{timeout: time.Duration(pluginMeta.timeout) * time.Second, audience: audience, emails: emails}

	op := func() error {
		return tokenInfo.refreshCerts()
	}
	err := backoff.Retry(op, backoff.WithMaxTries(backoff.NewConstantBackOff(1*time.Second), 3))
	if err != nil {
		logrus.Errorf("getting of google certs failed : %v", err)
		os.Exit(1)
	}

	certsRefresher := &CertsRefresher{
		tokenInfo:   tokenInfo,
		stopChannel: make(chan struct{}, 1),
	}

	go certsRefresher.refreshLoop(time.Duration(pluginMeta.certsRefreshInterval) * time.Second)

	plugin.Serve(&plugin.ServeConfig{
		HandshakeConfig: shared.Handshake,
		Plugins: map[string]plugin.Plugin{
			"tokenProvider": &shared.TokenInfoPlugin{Impl: tokenInfo},
		},
		// A non-nil value here enables gRPC serving for this plugin...
		GRPCServer: plugin.DefaultGRPCServer,
	})
}

type CertsRefresher struct {
	tokenInfo   *TokenInfo
	stopChannel chan struct{}
}

func (p *CertsRefresher) refreshLoop(interval time.Duration) {
	defer func() {
		if r := recover(); r != nil {
			var ok bool
			err, ok := r.(error)
			if ok {
				logrus.Error(fmt.Sprintf("certs refresh loop error %v", err))
			}
		}
	}()
	logrus.Infof("Refreshing certs every: %v", interval)
	syncTicker := time.NewTicker(interval)
	for {
		select {
		case <-syncTicker.C:
			p.refreshTick()
		case <-p.stopChannel:
			return
		}
	}
}

func (p *CertsRefresher) refreshTick() error {
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
