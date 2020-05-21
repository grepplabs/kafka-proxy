package oidc

import (
	"context"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestGetCerts(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	certs, err := GetCerts(ctx)

	a := assert.New(t)
	a.Nil(err)
	a.NotNil(certs)
	a.True(len(certs.Keys) > 0)

	for _, key := range certs.Keys {
		a.NotEmpty(key.Kty)
		a.NotEmpty(key.Alg)
		a.NotEmpty(key.Use)
		a.NotEmpty(key.E)
		a.NotEmpty(key.N)
		a.NotEmpty(key.Kid)

		pk, err := key.GetPublicKey()
		a.Nil(err)
		a.NotNil(pk)
	}
}
