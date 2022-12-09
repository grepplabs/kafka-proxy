package proxy

// This files is mostly copied from https://github.com/segmentio/kafka-go/tree/main/sasl/aws_msk_iam_v2

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	signer "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/pkg/errors"
)

const (
	// These constants come from https://github.com/aws/aws-msk-iam-auth#details and
	// https://github.com/aws/aws-msk-iam-auth/blob/main/src/main/java/software/amazon/msk/auth/iam/internals/AWS4SignedPayloadGenerator.java.
	signAction       = "kafka-cluster:Connect"
	signPayload      = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" // the hex encoded SHA-256 of an empty string
	signService      = "kafka-cluster"
	signVersion      = "2020_10_22"
	signActionKey    = "action"
	signHostKey      = "host"
	signUserAgentKey = "user-agent"
	signVersionKey   = "version"
	queryActionKey   = "Action"
	queryExpiryKey   = "X-Amz-Expires"
	mechanism        = "AWS_MSK_IAM"
	requestExpiry    = time.Minute * 5
)

var signUserAgent = "kafka-go/sasl/aws_msk_iam_v2/" + runtime.Version()

// Mechanism implements signing for the AWS_MSK_IAM mechanism, based on the official java implementation:
// https://github.com/aws/aws-msk-iam-auth
type Mechanism struct {
	// The sigv4.Signer of aws-sdk-go-v2 to use when signing the request. Required.
	Signer *signer.Signer
	// The aws.Config.Credentials or config.CredentialsProvider of aws-sdk-go-v2. Required.
	Credentials aws.CredentialsProvider
	// The region where the msk cluster is hosted, e.g. "us-east-1". Required.
	Region string
}

func (m *Mechanism) SASLToken(ctx context.Context, host string) ([]byte, error) {
	jsonDict, err := m.preSign(ctx, host)
	if err != nil {
		return nil, errors.Wrap(err, "generating presigned payload")
	}

	signedBytes, err := json.Marshal(jsonDict)
	if err != nil {
		return nil, errors.Wrap(err, "failed to serialize")
	}
	return signedBytes, nil
}

// preSign produces the authentication values required for AWS_MSK_IAM.
// It produces the following json, making use of the aws-sdk to produce the signed output.
//
//	{
//	  "version" : "2020_10_22",
//	  "host" : "<broker host>",
//	  "user-agent": "<user agent string from the client>",
//	  "action": "kafka-cluster:Connect",
//	  "x-amz-algorithm" : "<algorithm>",
//	  "x-amz-credential" : "<clientAWSAccessKeyID>/<date in yyyyMMdd format>/<region>/kafka-cluster/aws4_request",
//	  "x-amz-date" : "<timestamp in yyyyMMdd'T'HHmmss'Z' format>",
//	  "x-amz-security-token" : "<clientAWSSessionToken if any>",
//	  "x-amz-signedheaders" : "host",
//	  "x-amz-expires" : "<expiration in seconds>",
//	  "x-amz-signature" : "<AWS SigV4 signature computed by the client>"
//	}
func (m *Mechanism) preSign(ctx context.Context, host string) (map[string]string, error) {
	req, err := buildReq(ctx, host)
	if err != nil {
		return nil, errors.Wrap(err, "building request")
	}

	creds, err := m.Credentials.Retrieve(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "credential retrieval")
	}

	signedUrl, header, err := m.Signer.PresignHTTP(ctx, creds, req, signPayload, signService, m.Region, time.Now())
	if err != nil {
		return nil, errors.Wrap(err, "signing url")
	}

	u, err := url.Parse(signedUrl)
	if err != nil {
		return nil, errors.Wrap(err, "invalid url from presign")
	}
	return buildSignedMap(u, header), nil
}

// buildReq builds http.Request for aws PreSign.
func buildReq(ctx context.Context, host string) (*http.Request, error) {
	query := url.Values{
		queryActionKey: {signAction},
		queryExpiryKey: {strconv.FormatInt(int64(requestExpiry/time.Second), 10)},
	}

	signUrl := url.URL{
		Scheme:   "kafka",
		Host:     host,
		Path:     "/",
		RawQuery: query.Encode(),
	}

	req, err := http.NewRequest(http.MethodGet, signUrl.String(), nil)
	if err != nil {
		return nil, err
	}

	return req, nil
}

// buildSignedMap builds signed string map which will be used to authenticate with MSK.
func buildSignedMap(u *url.URL, header http.Header) map[string]string {
	signedMap := map[string]string{
		signVersionKey:   signVersion,
		signHostKey:      u.Host,
		signUserAgentKey: signUserAgent,
		signActionKey:    signAction,
	}
	// The protocol requires lowercase keys.
	for key, vals := range header {
		signedMap[strings.ToLower(key)] = vals[0]
	}
	for key, vals := range u.Query() {
		signedMap[strings.ToLower(key)] = vals[0]
	}

	return signedMap
}

// NewMechanism provides
func newMechanism(awsCfg aws.Config) *Mechanism {
	return &Mechanism{
		Signer:      signer.NewSigner(),
		Credentials: awsCfg.Credentials,
		Region:      awsCfg.Region,
	}
}
