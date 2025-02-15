package oidc

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestGetServiceAccountIDToken(t *testing.T) {
	t.Skip() // Uncomment to execute
	a := assert.New(t)

	credentialsFile := filepath.Join(os.Getenv("HOME"), "kafka-gateway-service-account.json")
	src, err := NewServiceAccountTokenSource(credentialsFile, "tcp://kafka-gateway.grepplabs.com")
	a.Nil(err)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	token, err := src.GetIDToken(ctx)

	a.Nil(err)
	a.NotEmpty(token)
}
