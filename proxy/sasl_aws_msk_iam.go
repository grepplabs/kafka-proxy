package proxy

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	proxyconfig "github.com/grepplabs/kafka-proxy/config"
	"github.com/grepplabs/kafka-proxy/proxy/protocol"
	"github.com/sirupsen/logrus"
)

// AwsMSKIamAuth implements the SASLAuthByProxy interface and performs
// AWS IAM based authentication for MSK clusters.
type AwsMSKIamAuth struct {
	clientID string

	writeTimeout time.Duration
	readTimeout  time.Duration

	signer *Mechanism
}

func NewAwsMSKIamAuth(
	clientId string,
	readTimeout,
	writeTimeout time.Duration,
	awsConfig *proxyconfig.AWSConfig,
) (SASLAuthByProxy, error) {
	var optFns []func(*config.LoadOptions) error
	if awsConfig.Region != "" {
		optFns = append(optFns, config.WithRegion(awsConfig.Region))
	}
	if awsConfig.Profile != "" {
		optFns = append(optFns, config.WithSharedConfigProfile(awsConfig.Profile))
	}
	cfg, err := config.LoadDefaultConfig(context.Background(), optFns...)
	if err != nil {
		return nil, fmt.Errorf("loading aws config: %v", err)
	}
	if awsConfig.RoleArn != "" {
		stsClient := sts.NewFromConfig(cfg)
		assumeRoleProvider := stscreds.NewAssumeRoleProvider(stsClient, awsConfig.RoleArn)
		cfg.Credentials = aws.NewCredentialsCache(assumeRoleProvider)
	}
	if awsConfig.IdentityLookup {
		ctx := context.Background()
		ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()
		output, err := sts.NewFromConfig(cfg).GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
		if err != nil {
			return nil, fmt.Errorf("failed to get caller identity: %v", err)
		}
		logrus.Infof("AWS_MSK_IAM caller identity %s", aws.ToString(output.Arn))
	}
	return &AwsMSKIamAuth{
		clientID:     clientId,
		signer:       newMechanism(cfg),
		readTimeout:  readTimeout,
		writeTimeout: writeTimeout,
	}, nil

}

// sendAndReceiveSASLAuth handles the entire SASL authentication process
func (a *AwsMSKIamAuth) sendAndReceiveSASLAuth(conn DeadlineReaderWriter, brokerString string) error {
	if err := a.saslHandshake(conn); err != nil {
		return fmt.Errorf("handshake failed: %w", err)
	}

	if err := a.saslAuthenticate(conn, brokerString); err != nil {
		return fmt.Errorf("authenticate failed: %w", err)
	}

	return nil
}

func (a *AwsMSKIamAuth) saslHandshake(conn DeadlineReaderWriter) error {
	rb := &protocol.SaslHandshakeRequestV0orV1{
		Version:   1,
		Mechanism: mechanism,
	}
	req := &protocol.Request{
		ClientID: a.clientID,
		Body:     rb,
	}
	if err := a.write(conn, req); err != nil {
		return fmt.Errorf("writing SASL handshake: %w", err)
	}

	payload, err := a.read(conn)
	if err != nil {
		return fmt.Errorf("reading SASL handshake: %w", err)
	}

	res := &protocol.SaslHandshakeResponseV0orV1{}
	if err := protocol.Decode(payload, res); err != nil {
		return fmt.Errorf("parsing SASL handshake response: %w", err)
	}

	if !errors.Is(res.Err, protocol.ErrNoError) {
		return fmt.Errorf("sasl handshake protocol error: %w", res.Err)
	}
	logrus.Debugf("Successful IAM SASL handshake. Available mechanisms: %v", res.EnabledMechanisms)
	return nil
}

func (a *AwsMSKIamAuth) saslAuthenticate(conn DeadlineReaderWriter, brokerString string) error {
	host, _, err := net.SplitHostPort(brokerString)
	if err != nil {
		return fmt.Errorf("failed to parse host/port: %v", err)
	}

	authBytes, err := a.signer.SASLToken(context.Background(), host)
	if err != nil {
		return fmt.Errorf("failed to generate SASL token %v", err)
	}

	saslAuthReqV0 := &protocol.SaslAuthenticateRequestV0{SaslAuthBytes: authBytes}

	req := &protocol.Request{
		ClientID: a.clientID,
		Body:     saslAuthReqV0,
	}
	if err := a.write(conn, req); err != nil {
		return fmt.Errorf("writing SASL authentication request: %w", err)
	}

	payload, err := a.read(conn)
	if err != nil {
		return fmt.Errorf("reading SASL authentication response: %w", err)
	}

	res := &protocol.SaslAuthenticateResponseV0{}
	err = protocol.Decode(payload, res)
	if err != nil {
		return fmt.Errorf("parsing SASL authentication response: %w", err)
	}
	if !errors.Is(res.Err, protocol.ErrNoError) {
		return fmt.Errorf("sasl authentication protocol error: %w", res.Err)
	}
	return nil
}

func (a *AwsMSKIamAuth) write(conn DeadlineReaderWriter, req *protocol.Request) error {
	reqBuf, err := protocol.Encode(req)
	if err != nil {
		return fmt.Errorf("serializing request: %w", err)
	}

	sizeBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(sizeBuf, uint32(len(reqBuf)))

	if err := conn.SetWriteDeadline(time.Now().Add(a.writeTimeout)); err != nil {
		return fmt.Errorf("setting write deadline: %w", err)
	}

	if _, err := conn.Write(bytes.Join([][]byte{sizeBuf, reqBuf}, nil)); err != nil {
		return fmt.Errorf("writing bytes: %w", err)
	}
	return nil
}

func (a *AwsMSKIamAuth) read(conn DeadlineReaderWriter) ([]byte, error) {
	if err := conn.SetReadDeadline(time.Now().Add(a.readTimeout)); err != nil {
		return nil, fmt.Errorf("setting read deadline: %w", err)
	}

	//wait for the handshake response
	header := make([]byte, 8) // response header
	if _, err := io.ReadFull(conn, header); err != nil {
		return nil, fmt.Errorf("reading header: %w", err)
	}

	length := binary.BigEndian.Uint32(header[:4])
	payload := make([]byte, length-4)
	if _, err := io.ReadFull(conn, payload); err != nil {
		return nil, fmt.Errorf("reading payload: %w", err)
	}

	return payload, nil
}
