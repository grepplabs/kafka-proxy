package awsmskiamprovider

import (
	"context"

	"github.com/aws/aws-msk-iam-sasl-signer-go/signer"
)

// TokenSigner defines the interface for AWS MSK IAM token generation
type TokenSigner interface {
	GenerateAuthToken(ctx context.Context, region string) (string, int64, error)
	GenerateAuthTokenFromProfile(ctx context.Context, region string, profile string) (string, int64, error)
	GenerateAuthTokenFromRole(ctx context.Context, region string, roleARN string, sessionName string) (string, int64, error)
	GenerateAuthTokenFromRoleWithExternalId(ctx context.Context, region string, roleARN string, sessionName string, externalID string) (string, int64, error)
}

// AwsMskIamTokenSigner implements TokenSigner using the actual AWS MSK IAM SASL signer
type AwsMskIamTokenSigner struct{}

// NewAwsMskIamTokenSigner creates a new AWS MSK IAM token signer
func NewAwsMskIamTokenSigner() *AwsMskIamTokenSigner {
	return &AwsMskIamTokenSigner{}
}

// GenerateAuthToken generates a token using default credentials
func (r *AwsMskIamTokenSigner) GenerateAuthToken(ctx context.Context, region string) (string, int64, error) {
	return signer.GenerateAuthToken(ctx, region)
}

// GenerateAuthTokenFromProfile generates a token using a named profile
func (r *AwsMskIamTokenSigner) GenerateAuthTokenFromProfile(ctx context.Context, region string, profile string) (string, int64, error) {
	return signer.GenerateAuthTokenFromProfile(ctx, region, profile)
}

// GenerateAuthTokenFromRole generates a token using role assumption
func (r *AwsMskIamTokenSigner) GenerateAuthTokenFromRole(ctx context.Context, region string, roleARN string, sessionName string) (string, int64, error) {
	return signer.GenerateAuthTokenFromRole(ctx, region, roleARN, sessionName)
}

// GenerateAuthTokenFromRoleWithExternalId generates a token using role assumption with external ID
func (r *AwsMskIamTokenSigner) GenerateAuthTokenFromRoleWithExternalId(ctx context.Context, region string, roleARN string, sessionName string, externalID string) (string, int64, error) {
	return signer.GenerateAuthTokenFromRoleWithExternalId(ctx, region, roleARN, sessionName, externalID)
}
