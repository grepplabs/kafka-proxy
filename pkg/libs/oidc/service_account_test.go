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

	credentialsFile := filepath.Join(os.Getenv("HOME"), "kafka-gateway-service-account.json")
	src, err := NewServiceAccountTokenSource(credentialsFile, "tcp://kafka-gateway.grepplabs.com")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	token, err := src.GetIDToken(ctx)

	a := assert.New(t)
	a.Nil(err)
	a.NotEmpty(token)
}
