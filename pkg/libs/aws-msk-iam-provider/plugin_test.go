package awsmskiamprovider

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/grepplabs/kafka-proxy/pkg/apis"
	"github.com/stretchr/testify/assert"
)

func TestTokenProvider_NewTokenProvider(t *testing.T) {
	tests := []struct {
		name    string
		options TokenProviderOptions
		wantErr bool
	}{
		{
			name: "valid options with region only",
			options: TokenProviderOptions{
				Region:      "us-east-1",
				Timeout:     10,
				SessionName: "test-session",
			},
			wantErr: false,
		},
		{
			name: "valid options with profile",
			options: TokenProviderOptions{
				Region:      "us-west-2",
				Profile:     "test-profile",
				Timeout:     30,
				SessionName: "test-session",
			},
			wantErr: false,
		},
		{
			name: "valid options with role ARN",
			options: TokenProviderOptions{
				Region:      "eu-west-1",
				RoleARN:     "arn:aws:iam::123456789012:role/TestRole",
				Timeout:     15,
				SessionName: "test-session",
			},
			wantErr: false,
		},
		{
			name: "valid options with role ARN and external ID",
			options: TokenProviderOptions{
				Region:      "ap-southeast-1",
				RoleARN:     "arn:aws:iam::123456789012:role/TestRole",
				ExternalID:  "test-external-id",
				Timeout:     20,
				SessionName: "test-session",
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Use mock signer instead of real AWS calls
			mockSigner := NewMockTokenSigner()
			provider, err := NewTokenProvider(tt.options, mockSigner)

			assert.NoError(t, err)
			assert.NotNil(t, provider)
			assert.Equal(t, tt.options.Region, provider.region)
			assert.Equal(t, time.Duration(tt.options.Timeout)*time.Second, provider.timeout)
			assert.Equal(t, tt.options.SessionName, provider.sessionName)
		})
	}
}

func TestTokenProvider_GetToken(t *testing.T) {
	tests := []struct {
		name          string
		options       TokenProviderOptions
		wantErr       bool
		expectedToken string
	}{
		{
			name: "provider with region only",
			options: TokenProviderOptions{
				Region:      "us-east-1",
				Timeout:     10,
				SessionName: "test",
			},
			wantErr:       false,
			expectedToken: "mock-aws-msk-iam-token",
		},
		{
			name: "provider with profile",
			options: TokenProviderOptions{
				Region:      "us-west-2",
				Profile:     "test-profile",
				Timeout:     10,
				SessionName: "test",
			},
			wantErr:       false,
			expectedToken: "mock-aws-msk-iam-token",
		},
		{
			name: "provider with role ARN",
			options: TokenProviderOptions{
				Region:      "eu-west-1",
				RoleARN:     "arn:aws:iam::123456789012:role/TestRole",
				Timeout:     10,
				SessionName: "test",
			},
			wantErr:       false,
			expectedToken: "mock-aws-msk-iam-token",
		},
		{
			name: "provider with role ARN and external ID",
			options: TokenProviderOptions{
				Region:      "ap-southeast-1",
				RoleARN:     "arn:aws:iam::123456789012:role/TestRole",
				ExternalID:  "test-external-id",
				Timeout:     10,
				SessionName: "test",
			},
			wantErr:       false,
			expectedToken: "mock-aws-msk-iam-token",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockSigner := NewMockTokenSigner()
			provider, err := NewTokenProvider(tt.options, mockSigner)
			assert.NoError(t, err)

			ctx := context.Background()
			resp, err := provider.GetToken(ctx, apis.TokenRequest{})

			if tt.wantErr {
				assert.Error(t, err)
				assert.False(t, resp.Success)
				assert.Equal(t, int32(StatusGetTokenFailed), resp.Status)
			} else {
				assert.NoError(t, err)
				assert.True(t, resp.Success)
				assert.Equal(t, int32(StatusOK), resp.Status)
				assert.Equal(t, tt.expectedToken, resp.Token)
				assert.Equal(t, 1, mockSigner.GetCallCount())
			}
		})
	}
}

func TestTokenProvider_GetToken_WithCurrentToken(t *testing.T) {
	options := TokenProviderOptions{
		Region:      "us-east-1",
		Timeout:     10,
		SessionName: "test",
	}

	mockSigner := NewMockTokenSigner()
	provider, err := NewTokenProvider(options, mockSigner)
	assert.NoError(t, err)

	// The provider should already have a token from initialization
	// Let's verify it's using the cached token on subsequent calls
	initialCallCount := mockSigner.GetCallCount()

	ctx := context.Background()
	resp, err := provider.GetToken(ctx, apis.TokenRequest{})

	// Should return the current token without trying to generate a new one
	assert.NoError(t, err)
	assert.True(t, resp.Success)
	assert.Equal(t, int32(StatusOK), resp.Status)
	assert.NotEmpty(t, resp.Token)

	// Should not have called the mock signer again since we had a valid token
	assert.Equal(t, initialCallCount, mockSigner.GetCallCount())
}

func TestTokenProvider_shouldRenew(t *testing.T) {
	tests := []struct {
		name           string
		provider       *TokenProvider
		expectedResult bool
	}{
		{
			name:           "no current token",
			provider:       &TokenProvider{},
			expectedResult: true,
		},
		{
			name: "current token but no expiry set",
			provider: &TokenProvider{
				currentToken: "test-token",
			},
			expectedResult: true,
		},
		{
			name: "current token with future expiry",
			provider: func() *TokenProvider {
				p := &TokenProvider{
					currentToken: "test-token",
				}
				p.setCurrentToken("test-token", time.Now().Add(10*time.Minute))
				return p
			}(),
			expectedResult: false,
		},
		{
			name: "current token with past expiry",
			provider: func() *TokenProvider {
				p := &TokenProvider{
					currentToken: "test-token",
				}
				p.setCurrentToken("test-token", time.Now().Add(-10*time.Minute))
				return p
			}(),
			expectedResult: true,
		},
		{
			name: "current token expiring soon (within buffer)",
			provider: func() *TokenProvider {
				p := &TokenProvider{
					currentToken: "test-token",
				}
				// Set expiry to 3 minutes from now (less than 5-minute buffer)
				p.setCurrentToken("test-token", time.Now().Add(3*time.Minute))
				return p
			}(),
			expectedResult: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.provider.shouldRenew()
			assert.Equal(t, tt.expectedResult, result)
		})
	}
}

func TestTokenProvider_getCurrentToken(t *testing.T) {
	tests := []struct {
		name          string
		provider      *TokenProvider
		expectedToken string
	}{
		{
			name:          "no current token",
			provider:      &TokenProvider{},
			expectedToken: "",
		},
		{
			name: "current token but no expiry set",
			provider: &TokenProvider{
				currentToken: "test-token",
			},
			expectedToken: "",
		},
		{
			name: "current token with future expiry",
			provider: func() *TokenProvider {
				p := &TokenProvider{
					currentToken: "test-token",
				}
				p.setCurrentToken("test-token", time.Now().Add(10*time.Minute))
				return p
			}(),
			expectedToken: "test-token",
		},
		{
			name: "current token with past expiry",
			provider: func() *TokenProvider {
				p := &TokenProvider{
					currentToken: "test-token",
				}
				p.setCurrentToken("test-token", time.Now().Add(-10*time.Minute))
				return p
			}(),
			expectedToken: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			token := tt.provider.getCurrentToken()
			assert.Equal(t, tt.expectedToken, token)
		})
	}
}

func TestTokenProvider_setCurrentToken(t *testing.T) {
	provider := &TokenProvider{}
	expectedToken := "test-token"
	expectedExpiry := time.Now().Add(10 * time.Minute)

	provider.setCurrentToken(expectedToken, expectedExpiry)

	assert.Equal(t, expectedToken, provider.currentToken)
	assert.Equal(t, expectedExpiry, provider.currentExpiry)
}

func TestGetTokenResponse(t *testing.T) {
	tests := []struct {
		name            string
		token           string
		status          int
		expectedSuccess bool
		expectedStatus  int32
	}{
		{
			name:            "successful response",
			token:           "test-token",
			status:          StatusOK,
			expectedSuccess: true,
			expectedStatus:  int32(StatusOK),
		},
		{
			name:            "failed response",
			token:           "",
			status:          StatusGetTokenFailed,
			expectedSuccess: false,
			expectedStatus:  int32(StatusGetTokenFailed),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := getTokenResponse(tt.token, tt.status)

			assert.NoError(t, err)
			assert.Equal(t, tt.expectedSuccess, resp.Success)
			assert.Equal(t, tt.expectedStatus, resp.Status)
			assert.Equal(t, tt.token, resp.Token)
		})
	}
}

func TestTokenProvider_NewTokenProvider_WithMockError(t *testing.T) {
	options := TokenProviderOptions{
		Region:      "us-east-1",
		Timeout:     10,
		SessionName: "test",
	}

	mockSigner := NewMockTokenSigner()
	mockSigner.SetMockError(fmt.Errorf("mock AWS error"))
	provider, err := NewTokenProvider(options, mockSigner)

	// NewTokenProvider should fail when initial token generation fails
	assert.Error(t, err)
	assert.Nil(t, provider)
	assert.Contains(t, err.Error(), "getting of initial AWS MSK IAM token failed")
	// The retry logic will call the signer multiple times (3 retries + 1 initial attempt)
	assert.GreaterOrEqual(t, mockSigner.GetCallCount(), 1)
}

func TestTokenProvider_GetToken_WithMockError(t *testing.T) {
	options := TokenProviderOptions{
		Region:      "us-east-1",
		Timeout:     10,
		SessionName: "test",
	}

	mockSigner := NewMockTokenSigner()
	provider, err := NewTokenProvider(options, mockSigner)
	assert.NoError(t, err)

	mockSigner.SetMockError(fmt.Errorf("mock AWS error"))
	provider.setCurrentToken("test-token", time.Now().Add(-10*time.Minute))

	ctx := context.Background()
	resp, err := provider.GetToken(ctx, apis.TokenRequest{})

	assert.NoError(t, err)
	assert.False(t, resp.Success)
	assert.Equal(t, int32(StatusGetTokenFailed), resp.Status)
	assert.Equal(t, "", resp.Token)
	assert.Equal(t, 2, mockSigner.GetCallCount())
}

func TestTokenProvider_GetToken_MultipleCalls(t *testing.T) {
	options := TokenProviderOptions{
		Region:      "us-east-1",
		Timeout:     10,
		SessionName: "test",
	}

	mockSigner := NewMockTokenSigner()
	provider, err := NewTokenProvider(options, mockSigner)
	assert.NoError(t, err)

	ctx := context.Background()

	// First call - should generate a new token
	resp1, err := provider.GetToken(ctx, apis.TokenRequest{})
	assert.NoError(t, err)
	assert.True(t, resp1.Success)
	assert.Equal(t, 1, mockSigner.GetCallCount())

	// Second call - should return the cached token
	resp2, err := provider.GetToken(ctx, apis.TokenRequest{})
	assert.NoError(t, err)
	assert.True(t, resp2.Success)
	assert.Equal(t, resp1.Token, resp2.Token)
	assert.Equal(t, 1, mockSigner.GetCallCount()) // Should not have called again
}

func TestTokenProvider_GetToken_WithExpiredToken(t *testing.T) {
	options := TokenProviderOptions{
		Region:      "us-east-1",
		Timeout:     10,
		SessionName: "test",
	}

	mockSigner := NewMockTokenSigner()
	provider, err := NewTokenProvider(options, mockSigner)
	assert.NoError(t, err)

	// Set a token that's already expired
	provider.setCurrentToken("expired-token", time.Now().Add(-1*time.Hour))

	ctx := context.Background()
	resp, err := provider.GetToken(ctx, apis.TokenRequest{})

	// Should generate a new token since the current one is expired
	assert.NoError(t, err)
	assert.True(t, resp.Success)
	assert.Equal(t, int32(StatusOK), resp.Status)
	assert.NotEqual(t, "expired-token", resp.Token)
	assert.Equal(t, 2, mockSigner.GetCallCount()) // 1 from init + 1 from this call
}

func TestTokenProvider_GetToken_WithTokenExpiringSoon(t *testing.T) {
	options := TokenProviderOptions{
		Region:      "us-east-1",
		Timeout:     10,
		SessionName: "test",
	}

	mockSigner := NewMockTokenSigner()
	provider, err := NewTokenProvider(options, mockSigner)
	assert.NoError(t, err)

	// Set a token that expires in 3 minutes (within the 5-minute buffer)
	provider.setCurrentToken("expiring-soon-token", time.Now().Add(3*time.Minute))

	ctx := context.Background()
	resp, err := provider.GetToken(ctx, apis.TokenRequest{})

	// Should generate a new token since the current one expires soon
	assert.NoError(t, err)
	assert.True(t, resp.Success)
	assert.Equal(t, int32(StatusOK), resp.Status)
	assert.NotEqual(t, "expiring-soon-token", resp.Token)
	assert.Equal(t, 2, mockSigner.GetCallCount()) // 1 from init + 1 from this call
}

func TestTokenProvider_GetToken_WithValidToken(t *testing.T) {
	options := TokenProviderOptions{
		Region:      "us-east-1",
		Timeout:     10,
		SessionName: "test",
	}

	mockSigner := NewMockTokenSigner()
	provider, err := NewTokenProvider(options, mockSigner)
	assert.NoError(t, err)

	// Set a token that's valid for a long time
	provider.setCurrentToken("valid-token", time.Now().Add(30*time.Minute))

	ctx := context.Background()
	resp, err := provider.GetToken(ctx, apis.TokenRequest{})

	// Should return the existing token without generating a new one
	assert.NoError(t, err)
	assert.True(t, resp.Success)
	assert.Equal(t, int32(StatusOK), resp.Status)
	assert.Equal(t, "valid-token", resp.Token)
	assert.Equal(t, 1, mockSigner.GetCallCount()) // Only from init, not from this call
}

func TestTokenProvider_GetToken_WithEmptyToken(t *testing.T) {
	options := TokenProviderOptions{
		Region:      "us-east-1",
		Timeout:     10,
		SessionName: "test",
	}

	mockSigner := NewMockTokenSigner()
	provider, err := NewTokenProvider(options, mockSigner)
	assert.NoError(t, err)

	// Clear the current token
	provider.setCurrentToken("", time.Time{})

	ctx := context.Background()
	resp, err := provider.GetToken(ctx, apis.TokenRequest{})

	// Should generate a new token since there's no current token
	assert.NoError(t, err)
	assert.True(t, resp.Success)
	assert.Equal(t, int32(StatusOK), resp.Status)
	assert.NotEmpty(t, resp.Token)
	assert.Equal(t, 2, mockSigner.GetCallCount()) // 1 from init + 1 from this call
}
