package proxy

import (
	"bytes"
	"encoding/hex"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestLocalSaslReceiveAndSendAuthV1(t *testing.T) {
	tests := []struct {
		name      string
		reqHex    string
		resHex    string
		username  string
		password  string
		authError error
	}{
		{
			name:      "SaslAuthenticate v0, Request Header v1, kafka-clients 2.0.1, success",
			reqHex:    "00000040002400000000000100144b61666b614578616d706c6550726f64756365720000001e006d792d746573742d75736572006d792d746573742d70617373776f7264",
			resHex:    "0000000c000000010000ffff00000000",
			username:  "my-test-user",
			password:  "my-test-password",
			authError: nil,
		},
		{
			name:      "SaslAuthenticate v0, Request Header v1, kafka-clients 2.0.1, auth failed",
			reqHex:    "00000040002400000000000100144b61666b614578616d706c6550726f64756365720000001e006d792d746573742d75736572006d792d746573742d70617373776f7264",
			resHex:    "0000003300000001003a002775736572206d792d746573742d757365722061757468656e7469636174696f6e206661696c656400000000",
			username:  "my-test-user",
			password:  "bad-password",
			authError: errLocalAuthFailed{user: "my-test-user"},
		},
		{
			name:      "SaslAuthenticate v1, Request Header v1, kafka-clients 2.4.1, success",
			reqHex:    "00000040002400010000000200144b61666b614578616d706c6550726f64756365720000001e006d792d746573742d75736572006d792d746573742d70617373776f7264",
			resHex:    "00000014000000020000ffff000000000000000000000000",
			username:  "my-test-user",
			password:  "my-test-password",
			authError: nil,
		},
		{
			name:      "SaslAuthenticate v1, Request Header v1, kafka-clients 2.4.1, auth failed",
			reqHex:    "00000040002400010000000200144b61666b614578616d706c6550726f64756365720000001e006d792d746573742d75736572006d792d746573742d70617373776f7264",
			resHex:    "0000003b00000002003a002775736572206d792d746573742d757365722061757468656e7469636174696f6e206661696c6564000000000000000000000000",
			username:  "my-test-user",
			password:  "bad-password",
			authError: errLocalAuthFailed{user: "my-test-user"},
		},
		{
			name:      "SaslAuthenticate v2, Request Header v2, kafka-clients 2.5.0, success",
			reqHex:    "0000003f002400020000000200144b61666b614578616d706c6550726f6475636572001f006d792d746573742d75736572006d792d746573742d70617373776f726400",
			resHex:    "00000012000000020000000001000000000000000000",
			username:  "my-test-user",
			password:  "my-test-password",
			authError: nil,
		},
		{
			name:      "SaslAuthenticate v2, Request Header v2, kafka-clients 2.5.0, auth failed",
			reqHex:    "0000003f002400020000000200144b61666b614578616d706c6550726f6475636572001f006d792d746573742d75736572006d792d746573742d70617373776f726400",
			resHex:    "000000390000000200003a2875736572206d792d746573742d757365722061757468656e7469636174696f6e206661696c656401000000000000000000",
			username:  "my-test-user",
			password:  "bad-password",
			authError: errLocalAuthFailed{user: "my-test-user"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			a := assert.New(t)

			reqBytes, err := hex.DecodeString(tc.reqHex)
			a.Nil(err)

			conn := &fakeDeadlineReaderWriter{
				reader: bytes.NewBuffer(reqBytes),
				writer: new(bytes.Buffer),
			}
			localSaslAuth := NewLocalSaslPlain(&fakePasswordAuthenticator{
				Username: tc.username,
				Password: tc.password,
			})
			localSasl := &LocalSasl{}
			err = localSasl.receiveAndSendAuthV1(conn, localSaslAuth)
			a.Equal(tc.authError, err)

			written := conn.writer.Bytes()
			a.Equal(tc.resHex, hex.EncodeToString(written))
		})
	}
}

type fakePasswordAuthenticator struct {
	Username string
	Password string
}

func (pa fakePasswordAuthenticator) Authenticate(username, password string) (bool, int32, error) {
	return username == pa.Username && password == pa.Password, 0, nil
}

type fakeDeadlineReaderWriter struct {
	reader *bytes.Buffer
	writer *bytes.Buffer
}

func (r *fakeDeadlineReaderWriter) Read(p []byte) (n int, err error) {
	return r.reader.Read(p)
}

func (r *fakeDeadlineReaderWriter) Write(p []byte) (n int, err error) {
	return r.writer.Write(p)
}

func (r *fakeDeadlineReaderWriter) SetDeadline(t time.Time) error {
	return nil
}

func (r *fakeDeadlineReaderWriter) SetReadDeadline(t time.Time) error {
	return nil
}

func (r *fakeDeadlineReaderWriter) SetWriteDeadline(t time.Time) error {
	return nil
}
