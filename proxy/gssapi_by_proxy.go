package proxy

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/jcmturner/gofork/encoding/asn1"
	"github.com/jcmturner/gokrb5/v8/asn1tools"
	krb5client "github.com/jcmturner/gokrb5/v8/client"
	krb5config "github.com/jcmturner/gokrb5/v8/config"
	"github.com/jcmturner/gokrb5/v8/gssapi"
	"github.com/jcmturner/gokrb5/v8/iana/chksumtype"
	"github.com/jcmturner/gokrb5/v8/iana/keyusage"
	"github.com/jcmturner/gokrb5/v8/keytab"
	"github.com/jcmturner/gokrb5/v8/messages"
	"github.com/jcmturner/gokrb5/v8/types"
	"github.com/sirupsen/logrus"
	"io"
	"math"
	"strings"
	"time"
)

const (
	TOK_ID_KRB_AP_REQ   = 256
	GSS_API_GENERIC_TAG = 0x60
)

type GSSAPIConfig struct {
	KeyTabPath         string
	KerberosConfigPath string
	ServiceName        string
	Username           string
	Password           string
	Realm              string
}

type GSSAPIKerberosAuth struct {
	Config *GSSAPIConfig
}

func (krbAuth *GSSAPIKerberosAuth) writePackage(conn DeadlineReaderWriter, payload []byte) (int, error) {
	length := uint64(len(payload))
	size := length + 4 // 4 byte length header + payload
	if size > math.MaxInt32 {
		return 0, errors.New("payload too large, will overflow int32")
	}
	finalPackage := make([]byte, size)
	copy(finalPackage[4:], payload)
	binary.BigEndian.PutUint32(finalPackage, uint32(length))
	bytes, err := conn.Write(finalPackage)
	if err != nil {
		return bytes, err
	}
	return bytes, nil
}

func (krbAuth *GSSAPIKerberosAuth) readPackage(conn DeadlineReaderWriter) ([]byte, int, error) {
	bytesRead := 0
	lengthInBytes := make([]byte, 4)
	bytes, err := io.ReadFull(conn, lengthInBytes)
	if err != nil {
		return nil, bytesRead, err
	}
	bytesRead += bytes
	payloadLength := binary.BigEndian.Uint32(lengthInBytes)
	payloadBytes := make([]byte, payloadLength)  // buffer for read..
	bytes, err = io.ReadFull(conn, payloadBytes) // read bytes
	if err != nil {
		return payloadBytes, bytesRead, err
	}
	bytesRead += bytes
	return payloadBytes, bytesRead, nil
}

func (krbAuth *GSSAPIKerberosAuth) newAuthenticatorChecksum() []byte {
	a := make([]byte, 24)
	flags := []int{gssapi.ContextFlagInteg, gssapi.ContextFlagConf}
	binary.LittleEndian.PutUint32(a[:4], 16)
	for _, i := range flags {
		f := binary.LittleEndian.Uint32(a[20:24])
		f |= uint32(i)
		binary.LittleEndian.PutUint32(a[20:24], f)
	}
	return a
}

func (krbAuth *GSSAPIKerberosAuth) createApReq(
	domain string, cname types.PrincipalName, ticket messages.Ticket, sessionKey types.EncryptionKey) ([]byte, error) {

	auth, err := types.NewAuthenticator(domain, cname)
	if err != nil {
		return nil, err
	}
	auth.Cksum = types.Checksum{
		CksumType: chksumtype.GSSAPI,
		Checksum:  krbAuth.newAuthenticatorChecksum(),
	}
	APReq, err := messages.NewAPReq(
		ticket,
		sessionKey,
		auth,
	)
	if err != nil {
		return nil, err
	}
	aprBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(aprBytes, TOK_ID_KRB_AP_REQ)
	tb, err := APReq.Marshal()
	if err != nil {
		return nil, err
	}
	aprBytes = append(aprBytes, tb...)

	oidBytes, err := asn1.Marshal(gssapi.OIDKRB5.OID())
	if err != nil {
		return nil, err
	}
	tkoLengthBytes := asn1tools.MarshalLengthBytes(len(oidBytes) + len(aprBytes))
	gssHeader := append([]byte{GSS_API_GENERIC_TAG}, tkoLengthBytes...)
	gssHeader = append(gssHeader, oidBytes...)
	gssPackage := append(gssHeader, aprBytes...)
	return gssPackage, nil

}

func (krbAuth *GSSAPIKerberosAuth) sendAndReceiveGSSAPIAuth(conn DeadlineReaderWriter, brokerAddress string) error {
	logrus.Info("GSSAPI Authorize")
	cfg, err := krb5config.Load(krbAuth.Config.KerberosConfigPath)
	if err != nil {
		return err
	}
	kt, err := keytab.Load(krbAuth.Config.KeyTabPath)
	if err != nil {
		return err
	}
	kerberosClient := krb5client.NewWithKeytab(krbAuth.Config.Username, krbAuth.Config.Realm, kt, cfg)

	// AS_REQ
	err = kerberosClient.Login()
	if err != nil {
		logrus.Errorf("Failed to send GSSAPI AS_REQ")
		return err
	}

	host := strings.SplitN(brokerAddress, ":", 2)[0]
	spn := fmt.Sprintf("%s/%s", krbAuth.Config.ServiceName, host)

	// TGS_REQ
	ticket, encKey, err := kerberosClient.GetServiceTicket(spn)

	if err != nil {
		logrus.Errorf("Failed to send GSSAPI TGS_REQ")
		return err
	}
	defer kerberosClient.Destroy()

	//AP_REQ
	aprBytes, err := krbAuth.createApReq(
		kerberosClient.Credentials.Domain(),
		kerberosClient.Credentials.CName(),
		ticket, encKey)
	if err != nil {
		return err
	}

	requestTime := time.Now()
	_, err = krbAuth.writePackage(conn, aprBytes)
	if err != nil {
		logrus.Errorf("Failed to send GSSAPI AP_REQ1")
		return err
	}
	var receivedBytes []byte = nil
	bytesLen := 0
	receivedBytes, bytesLen, err = krbAuth.readPackage(conn)
	requestLatency := time.Since(requestTime)
	if err != nil {
		return err
	}
	logrus.Debugf("send GSSAPI AP_REQ Success. bytesRead: %d, requestLatency:%d", bytesLen, requestLatency)

	wrapTokenReq := gssapi.WrapToken{}
	if err := wrapTokenReq.Unmarshal(receivedBytes, true); err != nil {
		return err
	}

	isValid, err := wrapTokenReq.Verify(encKey, keyusage.GSSAPI_ACCEPTOR_SEAL)
	if !isValid {
		return err
	}

	wrapTokenResponse, err := gssapi.NewInitiatorWrapToken(wrapTokenReq.Payload, encKey)
	if err != nil {
		return err
	}
	packBytes, err := wrapTokenResponse.Marshal()
	if err != nil {
		return err
	}
	_, err = krbAuth.writePackage(conn, packBytes)

	if err != nil {
		return err
	}

	return nil

}
