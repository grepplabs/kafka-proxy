package proxy

import (
	"context"
	"fmt"
	"github.com/grepplabs/kafka-proxy/pkg/apis"
	"github.com/grepplabs/kafka-proxy/proxy/protocol"
	"strconv"
	"strings"
)

type LocalSaslAuth interface {
	doLocalAuth(saslAuthBytes []byte) (err error)
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
func (p *LocalSaslPlain) doLocalAuth(saslAuthBytes []byte) (err error) {
	tokens := strings.Split(string(saslAuthBytes), "\x00")
	if len(tokens) != 3 {
		return fmt.Errorf("invalid SASL/PLAIN request: expected 3 tokens, got %d", len(tokens))
	}
	if p.localAuthenticator == nil {
		return protocol.PacketDecodingError{Info: "Listener authenticator is not set"}
	}

	// logrus.Infof("user: %s , password: %s", tokens[1], tokens[2])
	ok, status, err := p.localAuthenticator.Authenticate(tokens[1], tokens[2])
	if err != nil {
		proxyLocalAuthTotal.WithLabelValues("error", "1").Inc()
		return err
	}
	proxyLocalAuthTotal.WithLabelValues(strconv.FormatBool(ok), strconv.Itoa(int(status))).Inc()

	if !ok {
		return fmt.Errorf("user %s authentication failed", tokens[1])
	}
	return nil
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
func (p *LocalSaslOauth) doLocalAuth(saslAuthBytes []byte) (err error) {
	token, err := p.saslOAuthBearer.GetToken(saslAuthBytes)
	if err != nil {
		return err
	}
	resp, err := p.tokenAuthenticator.VerifyToken(context.Background(), apis.VerifyRequest{Token: token})
	if err != nil {
		return err
	}
	if !resp.Success {
		return fmt.Errorf("local oauth verify token failed with status: %d", resp.Status)
	}
	return nil
}
