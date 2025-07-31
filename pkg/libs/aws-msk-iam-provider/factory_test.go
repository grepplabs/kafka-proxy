package awsmskiamprovider

import (
	"testing"

	"github.com/grepplabs/kafka-proxy/pkg/apis"
	"github.com/stretchr/testify/assert"
)

func TestFactory_New(t *testing.T) {
	tests := []struct {
		name        string
		params      []string
		wantErr     bool
		errorSubstr string
	}{
		{
			name:        "missing region",
			params:      []string{},
			wantErr:     true,
			errorSubstr: "help requested",
		},
		{
			name:        "invalid flag",
			params:      []string{"--invalid-flag"},
			wantErr:     true,
			errorSubstr: "flag provided but not defined",
		},
		{
			name:    "happy path",
			params:  []string{"--region", "us-east-1"},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockSigner := NewMockTokenSigner()
			factory := NewFactory(mockSigner)
			provider, err := factory.New(tt.params)

			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, provider)
				if tt.errorSubstr != "" {
					assert.Contains(t, err.Error(), tt.errorSubstr)
				}
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, provider)
			}
		})
	}
}

func TestFactory_ImplementsInterface(t *testing.T) {
	mockSigner := NewMockTokenSigner()
	factory := NewFactory(mockSigner)
	assert.Implements(t, (*apis.TokenProviderFactory)(nil), factory)
}
