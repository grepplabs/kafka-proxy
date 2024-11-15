package oidc

import (
	"context"
	"encoding/json"
	"errors"
	"os"

	"github.com/sirupsen/logrus"
	"golang.org/x/oauth2"
)

type PasswordGrantTokenSource struct {
	ClientID string `json:"client_id"`

	ClientSecret string `json:"client_secret"`

	Username string `json:"username"`

	Password string `json:"password"`

	// TokenURL is the resource server's token endpoint
	// URL. This is a constant specific to each server.
	TokenURL string `json:"token_url"`

	// Scope specifies optional requested permissions.
	Scopes []string `json:"scopes"`
}

func NewPasswordGrantTokenSource(credentialsFile string, targetAudience string) (*PasswordGrantTokenSource, error) {
	data, err := os.ReadFile(credentialsFile)
	source := &PasswordGrantTokenSource{}

	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(data, source)

	if err != nil {
		return nil, err
	}

	return source, nil
}

// GetIDToken - retrieve token from endpoint
func (s *PasswordGrantTokenSource) GetIDToken(parent context.Context) (string, error) {
	conf := &oauth2.Config{
		Scopes:       s.Scopes,
		ClientID:     s.ClientID,
		ClientSecret: s.ClientSecret,
		Endpoint: oauth2.Endpoint{
			TokenURL: s.TokenURL,
		},
	}

	logrus.Debugf("Configuration is %v", conf)

	token, err := conf.PasswordCredentialsToken(parent, s.Username, s.Password)

	if err != nil {
		logrus.Errorf("Retrieving oidc token failed %v", err)
		return "", err
	}

	var idTokenRes struct {
		IDToken string `json:"id_token"`
	}

	if token.AccessToken == "" {
		return "", errors.New("oidc access_token is missing")
	}

	idTokenRes.IDToken = token.AccessToken

	return idTokenRes.IDToken, nil
}
