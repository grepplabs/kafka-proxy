package googleid

import (
	"context"
	"errors"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/oauth2/v2"
)

type AuthorizedUserTokenSource struct {
}

func NewAuthorizedUserTokenSource() *AuthorizedUserTokenSource {
	return &AuthorizedUserTokenSource{}
}

// Retrieve ID-Token using Google Application Default Credentials: authorized_user will have id_token, service_account will not
func (s *AuthorizedUserTokenSource) GetIDToken(ctx context.Context) (string, error) {
	tokenSource, err := google.DefaultTokenSource(ctx, oauth2.UserinfoEmailScope)
	if err != nil {
		return "", err
	}
	token, err := tokenSource.Token()
	if err != nil {
		return "", err
	}
	if token.Extra("id_token") == nil {
		return "", errors.New("id_token is missing")
	}
	idToken := token.Extra("id_token").(string)
	return idToken, nil
}
