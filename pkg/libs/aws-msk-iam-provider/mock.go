package awsmskiamprovider

import (
	"context"
	"time"
)

// MockTokenSigner provides a mock implementation for testing
type MockTokenSigner struct {
	// Mock responses
	MockToken  string
	MockError  error
	MockExpiry time.Time

	// Call tracking
	GenerateCallCount int
	LastRegion        string
	LastProfile       string
	LastRoleARN       string
	LastExternalID    string
	LastSessionName   string
}

// NewMockTokenSigner creates a new mock token signer
func NewMockTokenSigner() *MockTokenSigner {
	return &MockTokenSigner{
		MockToken:  "mock-aws-msk-iam-token",
		MockExpiry: time.Now().Add(15 * time.Minute),
	}
}

// GenerateAuthToken mocks the AWS MSK IAM token generation
func (m *MockTokenSigner) GenerateAuthToken(ctx context.Context, region string) (string, int64, error) {
	m.GenerateCallCount++
	m.LastRegion = region

	// Check if context is cancelled
	select {
	case <-ctx.Done():
		return "", 0, ctx.Err()
	default:
	}

	if m.MockError != nil {
		return "", 0, m.MockError
	}

	return m.MockToken, m.MockExpiry.Unix(), nil
}

// GenerateAuthTokenFromProfile mocks profile-based token generation
func (m *MockTokenSigner) GenerateAuthTokenFromProfile(ctx context.Context, region string, profile string) (string, int64, error) {
	m.GenerateCallCount++
	m.LastRegion = region
	m.LastProfile = profile

	// Check if context is cancelled
	select {
	case <-ctx.Done():
		return "", 0, ctx.Err()
	default:
	}

	if m.MockError != nil {
		return "", 0, m.MockError
	}

	return m.MockToken, m.MockExpiry.Unix(), nil
}

// GenerateAuthTokenFromRole mocks role-based token generation
func (m *MockTokenSigner) GenerateAuthTokenFromRole(ctx context.Context, region string, roleARN string, sessionName string) (string, int64, error) {
	m.GenerateCallCount++
	m.LastRegion = region
	m.LastRoleARN = roleARN
	m.LastSessionName = sessionName

	// Check if context is cancelled
	select {
	case <-ctx.Done():
		return "", 0, ctx.Err()
	default:
	}

	if m.MockError != nil {
		return "", 0, m.MockError
	}

	return m.MockToken, m.MockExpiry.Unix(), nil
}

// GenerateAuthTokenFromRoleWithExternalId mocks role-based token generation with external ID
func (m *MockTokenSigner) GenerateAuthTokenFromRoleWithExternalId(ctx context.Context, region string, roleARN string, sessionName string, externalID string) (string, int64, error) {
	m.GenerateCallCount++
	m.LastRegion = region
	m.LastRoleARN = roleARN
	m.LastSessionName = sessionName
	m.LastExternalID = externalID

	// Check if context is cancelled
	select {
	case <-ctx.Done():
		return "", 0, ctx.Err()
	default:
	}

	if m.MockError != nil {
		return "", 0, m.MockError
	}

	return m.MockToken, m.MockExpiry.Unix(), nil
}

// Reset resets the mock state
func (m *MockTokenSigner) Reset() {
	m.GenerateCallCount = 0
	m.LastRegion = ""
	m.LastProfile = ""
	m.LastRoleARN = ""
	m.LastExternalID = ""
	m.LastSessionName = ""
}

// SetMockResponse sets the mock response
func (m *MockTokenSigner) SetMockResponse(token string, expiry time.Time, err error) {
	m.MockToken = token
	m.MockExpiry = expiry
	m.MockError = err
}

// SetMockError sets just the mock error
func (m *MockTokenSigner) SetMockError(err error) {
	m.MockError = err
}

// SetMockToken sets just the mock token
func (m *MockTokenSigner) SetMockToken(token string) {
	m.MockToken = token
}

// SetMockExpiry sets just the mock expiry
func (m *MockTokenSigner) SetMockExpiry(expiry time.Time) {
	m.MockExpiry = expiry
}

// GetCallCount returns the number of times GenerateAuthToken was called
func (m *MockTokenSigner) GetCallCount() int {
	return m.GenerateCallCount
}

// WasCalledWith checks if the mock was called with specific parameters
func (m *MockTokenSigner) WasCalledWith(region, profile, roleARN, externalID, sessionName string) bool {
	return m.LastRegion == region &&
		m.LastProfile == profile &&
		m.LastRoleARN == roleARN &&
		m.LastExternalID == externalID &&
		m.LastSessionName == sessionName
}
