package proxy

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSchemaRegistryProxy_ValidateCredentials(t *testing.T) {
	tests := []struct {
		name     string
		username string
		password string
		wantErr  bool
		errMsg   string
	}{
		{
			name:     "valid credentials",
			username: "user",
			password: "pass",
			wantErr:  false,
		},
		{
			name:     "empty username",
			username: "",
			password: "pass",
			wantErr:  true,
			errMsg:   "schema Registry proxy requires both username and password",
		},
		{
			name:     "empty password",
			username: "user",
			password: "",
			wantErr:  true,
			errMsg:   "schema Registry proxy requires both username and password",
		},
		{
			name:     "both empty",
			username: "",
			password: "",
			wantErr:  true,
			errMsg:   "schema Registry proxy requires both username and password",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewSchemaRegistryProxy("localhost", tt.username, tt.password, 8081, 8082)
			if tt.wantErr {
				assert.Error(t, err)
				assert.Equal(t, tt.errMsg, err.Error())
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestSchemaRegistryProxy_Shutdown(t *testing.T) {
	proxy, err := NewSchemaRegistryProxy("localhost", "user", "pass", 8081, 8082)
	assert.NoError(t, err)

	go func() {
		err := proxy.Start()
		if err != nil && err != http.ErrServerClosed {
			t.Error(err)
		}
	}()

	time.Sleep(100 * time.Millisecond)

	// Test graceful shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = proxy.Stop(ctx)
	assert.NoError(t, err)
}
