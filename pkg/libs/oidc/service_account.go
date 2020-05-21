package oidc

import (
	"context"
	"encoding/json"
	"errors"
	"io/ioutil"

	"github.com/sirupsen/logrus"
	"golang.org/x/oauth2/clientcredentials"
)

const (
	// https://accounts.google.com/.well-known/openid-configuration
	tokenEndpoint = "https://www.googleapis.com/oauth2/v4/token"
)

type ServiceAccountTokenSource struct {
	// ClientID is the application's ID.
	ClientID string `json:"client_id"`

	// ClientSecret is the application's secret.
	ClientSecret string `json:"client_secret"`

	// TokenURL is the resource server's token endpoint
	// URL. This is a constant specific to each server.
	TokenURL string `json:"token_url"`

	// Scope specifies optional requested permissions.
	Scopes []string `json:"scopes"`
}

func NewServiceAccountTokenSource(credentialsFile string, targetAudience string) (*ServiceAccountTokenSource, error) {
	data, err := ioutil.ReadFile(credentialsFile)
	source := &ServiceAccountTokenSource{}

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
func (s *ServiceAccountTokenSource) GetIDToken(parent context.Context) (string, error) {
	conf := &clientcredentials.Config{
		ClientID:     s.ClientID,
		ClientSecret: s.ClientSecret,
		Scopes:       s.Scopes,
		TokenURL:     s.TokenURL,
	}

	logrus.Debugf("Configuration is %v", conf)

	token, err := conf.Token(parent)

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
