package proxy

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/grepplabs/kafka-proxy/proxy/protocol"
	"github.com/sirupsen/logrus"
	"github.com/xdg/scram"
	"io"
	"time"
)

// Most of this is a direct copy from Shopify's Sarama found here:
// https://github.com/Shopify/sarama
// The commented out lines of code match with Sarama's

type SASLSCRAMAuth struct {
	clientID string

	writeTimeout time.Duration
	readTimeout  time.Duration

	username      string
	password      string
	mechanism     string
	correlationID int32

	// authz id used for SASL/SCRAM authentication
	SCRAMAuthzID string
}

// Maps to Sarama sendAndReceiveSASLSCRAMv1
func (b *SASLSCRAMAuth) sendAndReceiveSASLAuth(conn DeadlineReaderWriter) error {

	err := b.sendAndReceiveSASLHandshake(conn)
	if err != nil {
		logrus.Debugf("SASL Handshake fails")
		return err
	}

	var scramClient *scram.Client
	if b.mechanism == "SCRAM-SHA-256" {
		scramClient, err = scram.SHA256.NewClient(b.username, b.password, "")
		if err != nil {
			logrus.Debugf("Unable to make scram client for SCRAM-SHA-256: %v", err)
			return err
		}
	} else if b.mechanism == "SCRAM-SHA-512" {
		// Awaiting upstream acceptance in the scram library for this to work
		// scramClient, err = scram.SHA512.NewClient(b.username, b.password, "")
		//if err != nil {
		//	logrus.Debugf("Unable to make scram client for SCRAM-SHA-512: %v", err)
		//	return err
		//}
	} else {
		return fmt.Errorf("Invalid SCRAM specification provided: %s. Expected one of [\"SCRAM-SHA-256\",\"SCRAM-SHA-512\"]", b.mechanism)
	}

	//if err := scramClient.Begin(b.username, b.password, b.SCRAMAuthzID); err != nil {
	//	return fmt.Errorf("failed to start SCRAM exchange with the server: %s", err.Error())
	//}
	scramConversation := scramClient.NewConversation()

	//msg, err := scramClient.Step("")
	msg, err := scramConversation.Step("")
	if err != nil {
		return fmt.Errorf("failed to advance the SCRAM exchange: %s", err.Error())
	}

	logrus.Debugf("Commencing scram loop")
	for !scramConversation.Done() {
		//requestTime := time.Now()
		correlationID := b.correlationID
		// bytesWritten, err := b.sendSaslAuthenticateRequest(correlationID, []byte(msg))
		_, err := b.sendSaslAuthenticateRequest(conn, correlationID, []byte(msg))
		if err != nil {
			//Logger.Printf("Failed to write SASL auth header to broker %s: %s\n", b.addr, err.Error())
			logrus.Debugf("Failed to write SASL auth header to broker: %s\n", err.Error())
			return err
		}

		b.correlationID++
		//challenge, err := b.receiveSaslAuthenticateResponse(correlationID)
		challenge, err := b.receiveSaslAuthenticateResponse(conn, correlationID)
		if err != nil {
			//Logger.Printf("Failed to read response while authenticating with SASL to broker %s: %s\n", b.addr, err.Error())
			logrus.Debugf("Failed to read response while authenticating with SASL to broker: %s\n", err.Error())
			return err
		}

		msg, err = scramConversation.Step(string(challenge))
		if err != nil {
			logrus.Debugf("SASL authentication failed", err)
			//Logger.Println("SASL authentication failed", err)
			return err
		}
	}

	logrus.Debugf("SASL SCRAM authentication succeeded")
	return nil
}

func (b *SASLSCRAMAuth) sendAndReceiveSASLHandshake(conn DeadlineReaderWriter) error {
	logrus.Debugf("SASLSCRAM: Doing handshake. Mechanism: %s", b.mechanism)

	rb := &protocol.SaslHandshakeRequestV0orV1{
		Version:   1,
		Mechanism: b.mechanism,
	}

	req := &protocol.Request{ClientID: b.clientID, Body: rb}
	//req := &protocol.Request{CorrelationID: b.correlationID, ClientID: b.clientID, Body: rb}
	buf, err := protocol.Encode(req)
	if err != nil {
		logrus.Debugf("Error encoding protocol.Request: %v", err)
		return err
	}
	sizeBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(sizeBuf, uint32(len(buf)))

	if err := conn.SetWriteDeadline(time.Now().Add(b.writeTimeout)); err != nil {
		return err
	}

	bytes, err := conn.Write(bytes.Join([][]byte{sizeBuf, buf}, nil))
	//bytes, err := conn.Write(buf)
	if err != nil {
		logrus.Debugf("Failed to send SASL handshake: %s bytes: %v\n", err.Error(), bytes)
		return err
	}

	b.correlationID++
	//wait for the response
	header := make([]byte, 8) // response header
	bytes, err = io.ReadFull(conn, header)
	if err != nil {
		logrus.Debugf("Failed to read SASL handshake header [%v]: %v\n", bytes, err)
		return err
	}

	length := binary.BigEndian.Uint32(header[:4])
	payload := make([]byte, length-4)
	n, err := io.ReadFull(conn, payload)
	if err != nil {
		logrus.Debugf("Failed to read SASL handshake payload : %s bytes: %v\n", err.Error(), n)
		return err
	}

	res := &protocol.SaslHandshakeResponseV0orV1{}

	err = protocol.Decode(payload, res)
	if err != nil {
		logrus.Debugf("Failed to parse SASL handshake : %s\n", err.Error())
		return err
	}

	if res.Err != protocol.ErrNoError {
		logrus.Debugf("Invalid SASL Mechanism : %s\n", res.Err.Error())
		return res.Err
	}

	logrus.Debugf("Successful SASL handshake. Available mechanisms: %v", res.EnabledMechanisms)

	return nil
}

func (b *SASLSCRAMAuth) sendSaslAuthenticateRequest(conn DeadlineReaderWriter, correlationID int32, msg []byte) (int, error) {
	//	rb := &SaslAuthenticateRequest{msg}
	rb := &protocol.SaslAuthenticateRequestV0{msg}
	//req := &request{correlationID: correlationID, clientID: b.conf.ClientID, body: rb}
	req := &protocol.Request{CorrelationID: correlationID, ClientID: b.clientID, Body: rb}
	//buf, err := encode(req, b.conf.MetricRegistry)
	buf, err := protocol.Encode(req)
	if err != nil {
		logrus.Debugf("Failed to encode")
		return 0, err
	}

	if err := conn.SetWriteDeadline(time.Now().Add(b.writeTimeout)); err != nil {
		return 0, err
	}

	sizeBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(sizeBuf, uint32(len(buf)))
	return conn.Write(bytes.Join([][]byte{sizeBuf, buf}, nil))
}

func (b *SASLSCRAMAuth) receiveSaslAuthenticateResponse(conn DeadlineReaderWriter, correlationID int32) ([]byte, error) {
	const responseLengthSize = 4
	const correlationIDSize = 4

	buf := make([]byte, responseLengthSize+correlationIDSize)
	_, err := io.ReadFull(conn, buf)
	if err != nil {
		logrus.Debugf("Failed to read from broker: %v", err)
		return nil, err
	}

	//header := responseHeader{}
	header := protocol.ResponseHeader{}
	//err = decode(buf, &header)
	err = protocol.Decode(buf, &header)
	if err != nil {
		return nil, err
	}

	if header.CorrelationID != correlationID {
		return nil, fmt.Errorf("correlation ID didn't match, wanted %d, got %d", correlationID, header.CorrelationID)
	}

	buf = make([]byte, header.Length-correlationIDSize)
	_, err = io.ReadFull(conn, buf)
	if err != nil {
		return nil, err
	}

	//res := &SaslAuthenticateResponse{}
	res := &protocol.SaslAuthenticateResponseV0{}
	//if err := versionedDecode(buf, res, 0); err != nil {
	err = protocol.Decode(buf, res)
	if err != nil {
		return nil, err
	}
	if res.Err != protocol.ErrNoError {
		return nil, res.Err
	}
	return res.SaslAuthBytes, nil
}
