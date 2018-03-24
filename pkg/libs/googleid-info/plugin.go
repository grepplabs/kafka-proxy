package googleidinfo

import (
	"context"
	"crypto/rsa"
	"fmt"
	"github.com/cenkalti/backoff"
	"github.com/grepplabs/kafka-proxy/pkg/apis"
	"github.com/grepplabs/kafka-proxy/pkg/libs/googleid"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/oauth2/jws"
	"regexp"
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

type TokenInfoOptions struct {
	Timeout              int
	CertsRefreshInterval int
	Audience             []string
	EmailsRegex          []string
}

type TokenInfo struct {
	timeout    time.Duration
	audience   map[string]struct{}
	emailRegex []*regexp.Regexp

	publicKeys map[string]*rsa.PublicKey
	l          sync.RWMutex
}

func NewTokenInfo(options TokenInfoOptions) (*TokenInfo, error) {
	emailRegex := make([]*regexp.Regexp, 0)
	for _, emailRe := range options.EmailsRegex {
		re, err := regexp.Compile(emailRe)
		if err != nil {
			return nil, errors.Wrapf(err, "cannot compile email regex %s: %v", emailRe, err)
		}
		emailRegex = append(emailRegex, re)
	}
	logrus.Infof("JWT target audience: %v", options.Audience)
	logrus.Infof("JWT emails regexp: %v", emailRegex)

	if len(emailRegex) == 0 {
		return nil, errors.New("parameter email (regex) is required")
	}

	audience := make(map[string]struct{})
	for _, elem := range options.Audience {
		audience[elem] = struct{}{}
	}

	tokenInfo := &TokenInfo{timeout: time.Duration(options.Timeout) * time.Second, audience: audience, emailRegex: emailRegex}

	op := func() error {
		return tokenInfo.refreshCerts()
	}
	err := backoff.Retry(op, backoff.WithMaxTries(backoff.NewConstantBackOff(1*time.Second), 3))
	if err != nil {
		return nil, errors.Wrapf(err, "getting of google certs failed")
	}
	certsRefresher := newCertsRefresher(tokenInfo, make(chan struct{}, 1), time.Duration(options.CertsRefreshInterval)*time.Second)
	go certsRefresher.refreshLoop()
	return tokenInfo, nil
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

// verify token implements apis.TokenInfo VerifyToken method
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
	if !p.checkEmail(token.ClaimSet.Email) {
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

func (p *TokenInfo) checkEmail(email string) bool {
	for _, re := range p.emailRegex {
		if re.MatchString(email) {
			return true
		}
	}
	return false
}

func getVerifyResponseResponse(status int) (apis.VerifyResponse, error) {
	success := status == StatusOK
	return apis.VerifyResponse{Success: success, Status: int32(status)}, nil
}
