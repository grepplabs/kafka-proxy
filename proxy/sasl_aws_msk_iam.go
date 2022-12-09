package proxy

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/grepplabs/kafka-proxy/proxy/protocol"
	"github.com/pkg/errors"
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
	region string,
	readTimeout,
	writeTimeout time.Duration,
) (SASLAuthByProxy, error) {
	var optFns []func(*config.LoadOptions) error
	if region != "" {
		optFns = append(optFns, config.WithRegion(region))
	}
	cfg, err := config.LoadDefaultConfig(context.TODO(), optFns...)
	if err != nil {
		return nil, fmt.Errorf("loading aws config: %v", err)
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
		return errors.Wrap(err, "handshake failed")
	}

	if err := a.saslAuthenticate(conn, brokerString); err != nil {
		return errors.Wrap(err, "authenticate failed")
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
		return errors.Wrap(err, "writing SASL handshake")
	}

	payload, err := a.read(conn)
	if err != nil {
		return errors.Wrap(err, "reading SASL handshake")
	}

	res := &protocol.SaslHandshakeResponseV0orV1{}
	if err := protocol.Decode(payload, res); err != nil {
		return errors.Wrap(err, "parsing SASL handshake response")
	}

	if res.Err != protocol.ErrNoError {
		return errors.Wrap(res.Err, "sasl handshake protocol error")
	}
	logrus.Debugf("Successful IAM SASL handshake. Available mechanisms: %v", res.EnabledMechanisms)
	return nil
}

func (a *AwsMSKIamAuth) saslAuthenticate(conn DeadlineReaderWriter, brokerString string) error {
	host, _, err := net.SplitHostPort(brokerString)
	if err != nil {
		return fmt.Errorf("failed to parse host/port: %v", err)
	}

	authBytes, err := a.signer.SASLToken(context.TODO(), host)
	if err != nil {
		return fmt.Errorf("failed to generate SASL token %v", err)
	}

	saslAuthReqV0 := &protocol.SaslAuthenticateRequestV0{SaslAuthBytes: authBytes}

	req := &protocol.Request{
		ClientID: a.clientID,
		Body:     saslAuthReqV0,
	}
	if err := a.write(conn, req); err != nil {
		return errors.Wrap(err, "writing SASL authentication request")
	}

	payload, err := a.read(conn)
	if err != nil {
		return errors.Wrap(err, "reading SASL authentication response")
	}

	res := &protocol.SaslAuthenticateResponseV0{}
	err = protocol.Decode(payload, res)
	if err != nil {
		return errors.Wrap(err, "parsing SASL authentication response")
	}
	if res.Err != protocol.ErrNoError {
		return errors.Wrap(res.Err, "sasl authentication protocol error")
	}
	return nil
}

func (a *AwsMSKIamAuth) write(conn DeadlineReaderWriter, req *protocol.Request) error {
	reqBuf, err := protocol.Encode(req)
	if err != nil {
		return errors.Wrap(err, "serializing request")
	}

	sizeBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(sizeBuf, uint32(len(reqBuf)))

	if err := conn.SetWriteDeadline(time.Now().Add(a.writeTimeout)); err != nil {
		return errors.Wrap(err, "setting write deadline")
	}

	if _, err := conn.Write(bytes.Join([][]byte{sizeBuf, reqBuf}, nil)); err != nil {
		return errors.Wrap(err, "writing bytes")
	}
	return nil
}

func (a *AwsMSKIamAuth) read(conn DeadlineReaderWriter) ([]byte, error) {
	if err := conn.SetReadDeadline(time.Now().Add(a.readTimeout)); err != nil {
		return nil, errors.Wrap(err, "setting read deadline")
	}

	//wait for the handshake response
	header := make([]byte, 8) // response header
	if _, err := io.ReadFull(conn, header); err != nil {
		return nil, errors.Wrap(err, "reading header")
	}

	length := binary.BigEndian.Uint32(header[:4])
	payload := make([]byte, length-4)
	if _, err := io.ReadFull(conn, payload); err != nil {
		return nil, errors.Wrap(err, "reading payload")
	}

	return payload, nil
}
