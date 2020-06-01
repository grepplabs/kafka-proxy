package proxy

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/grepplabs/kafka-proxy/pkg/apis"
	"github.com/grepplabs/kafka-proxy/proxy/protocol"
	"github.com/sirupsen/logrus"
)

type errLocalAuthFailed struct {
	user string
}

func (e errLocalAuthFailed) Error() string {
	return fmt.Sprintf("user %s authentication failed", e.user)
}

type LocalSaslAuth interface {
	doLocalAuth(saslAuthBytes []byte) (err error, userInfo string)
}

type LocalSaslPlain struct {
	localAuthenticator apis.PasswordAuthenticator
}

func NewLocalSaslPlain(localAuthenticator apis.PasswordAuthenticator) *LocalSaslPlain {
	return &LocalSaslPlain{
		localAuthenticator: localAuthenticator,
	}
}

// implements LocalSaslAuth
func (p *LocalSaslPlain) doLocalAuth(saslAuthBytes []byte) (err error, userInfo string) {
	tokens := strings.Split(string(saslAuthBytes), "\x00")
	if len(tokens) != 3 {
		return fmt.Errorf("invalid SASL/PLAIN request: expected 3 tokens, got %d", len(tokens)), ""
	}
	if p.localAuthenticator == nil {
		return protocol.PacketDecodingError{Info: "Listener authenticator is not set"}, ""
	}

	logrus.Infof("Authenticating user: %s", tokens[1])

	ok, status, err := p.localAuthenticator.Authenticate(tokens[1], tokens[2])
	if err != nil {
		proxyLocalAuthTotal.WithLabelValues("error", "1").Inc()
		return err, ""
	}
	proxyLocalAuthTotal.WithLabelValues(strconv.FormatBool(ok), strconv.Itoa(int(status))).Inc()

	if !ok {
		return errLocalAuthFailed{
			user: tokens[1],
		}, tokens[1]
	}

	return nil, tokens[1]
}

type LocalSaslOauth struct {
	saslOAuthBearer    SaslOAuthBearer
	tokenAuthenticator apis.TokenInfo
}

func NewLocalSaslOauth(tokenAuthenticator apis.TokenInfo) *LocalSaslOauth {
	return &LocalSaslOauth{
		saslOAuthBearer:    SaslOAuthBearer{},
		tokenAuthenticator: tokenAuthenticator,
	}
}

// implements LocalSaslAuth
func (p *LocalSaslOauth) doLocalAuth(saslAuthBytes []byte) (err error, userInfo string) {
	token, _, _, err := p.saslOAuthBearer.GetClientInitialResponse(saslAuthBytes)
	if err != nil {
		return err, ""
	}
	resp, err := p.tokenAuthenticator.VerifyToken(context.Background(), apis.VerifyRequest{Token: token})
	if err != nil {
		return err, ""
	}
	if !resp.Success {
		return fmt.Errorf("local oauth verify token failed with status: %d", resp.Status), ""
	}
	return nil, token
}
