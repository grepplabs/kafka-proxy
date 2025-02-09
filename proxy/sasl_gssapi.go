package proxy

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/grepplabs/kafka-proxy/config"
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

	"github.com/jcmturner/gofork/encoding/asn1"
)

const (
	TOK_ID_KRB_AP_REQ   = 256
	GSS_API_GENERIC_TAG = 0x60
)

type SASLGSSAPIAuth struct {
	writeTimeout time.Duration
	readTimeout  time.Duration
	gssapiConfig *config.GSSAPIConfig
}

func (g *SASLGSSAPIAuth) sendAndReceiveSASLAuth(conn DeadlineReaderWriter, brokerAddress string) error {
	logrus.Debugf("GSSAPI Authorize %s / %s", g.gssapiConfig.Username, brokerAddress)

	kerberosClient, err := newKerberosClient(g.gssapiConfig)
	if err != nil {
		return err
	}
	// AS_REQ
	err = kerberosClient.Login()
	if err != nil {
		logrus.Errorf("Failed to send GSSAPI AS_REQ for user %s", g.gssapiConfig.Username)
		return err
	}

	host := strings.SplitN(brokerAddress, ":", 2)[0]
	if h, ok := g.gssapiConfig.SPNHostsMapping[host]; ok && h != "" {
		// map service principal host e.g. 127.0.0.1 => localhost
		host = h
	}
	spn := fmt.Sprintf("%s/%s", g.gssapiConfig.ServiceName, host)
	// TGS_REQ
	ticket, encKey, err := kerberosClient.GetServiceTicket(spn)

	if err != nil {
		logrus.Errorf("Failed to send GSSAPI TGS_REQ for SPN %s", spn)
		return err
	}
	defer kerberosClient.Destroy()

	//AP_REQ
	aprBytes, err := g.createApReq(
		kerberosClient.Domain(),
		kerberosClient.CName(),
		ticket, encKey)
	if err != nil {
		return err
	}

	requestTime := time.Now()

	_, err = g.writePackage(conn, aprBytes)
	if err != nil {
		logrus.Errorf("Failed to send GSSAPI AP_REQ1")
		return err
	}
	var receivedBytes []byte
	bytesLen := 0
	receivedBytes, bytesLen, err = g.readPackage(conn)
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
	_, err = g.writePackage(conn, packBytes)

	if err != nil {
		return err
	}

	return nil
}

func (g *SASLGSSAPIAuth) writePackage(conn DeadlineReaderWriter, payload []byte) (int, error) {
	if err := conn.SetWriteDeadline(time.Now().Add(g.writeTimeout)); err != nil {
		return 0, err
	}
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

func (g *SASLGSSAPIAuth) readPackage(conn DeadlineReaderWriter) ([]byte, int, error) {
	if err := conn.SetReadDeadline(time.Now().Add(g.readTimeout)); err != nil {
		return nil, 0, err
	}
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

func (g *SASLGSSAPIAuth) newAuthenticatorChecksum() []byte {
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

func (g *SASLGSSAPIAuth) createApReq(
	domain string, cname types.PrincipalName, ticket messages.Ticket, sessionKey types.EncryptionKey) ([]byte, error) {

	auth, err := types.NewAuthenticator(domain, cname)
	if err != nil {
		return nil, err
	}
	auth.Cksum = types.Checksum{
		CksumType: chksumtype.GSSAPI,
		Checksum:  g.newAuthenticatorChecksum(),
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

type KerberosClient interface {
	Login() error
	GetServiceTicket(spn string) (messages.Ticket, types.EncryptionKey, error)
	Domain() string
	CName() types.PrincipalName
	Destroy()
}

type KerberosGoKrb5Client struct {
	krb5client.Client
}

func (c *KerberosGoKrb5Client) Domain() string {
	return c.Credentials.Domain()
}

func (c *KerberosGoKrb5Client) CName() types.PrincipalName {
	return c.Credentials.CName()
}

func newKerberosClient(config *config.GSSAPIConfig) (KerberosClient, error) {
	cfg, err := krb5config.Load(config.KerberosConfigPath)
	if err != nil {
		return nil, err
	}
	return createClient(config, cfg)
}

func createClient(gssapiConfig *config.GSSAPIConfig, cfg *krb5config.Config) (KerberosClient, error) {
	var client *krb5client.Client
	if gssapiConfig.AuthType == config.KRB5_KEYTAB_AUTH {
		kt, err := keytab.Load(gssapiConfig.KeyTabPath)
		if err != nil {
			return nil, err
		}
		client = krb5client.NewWithKeytab(gssapiConfig.Username, gssapiConfig.Realm, kt, cfg, krb5client.DisablePAFXFAST(gssapiConfig.DisablePAFXFAST))
	} else {
		client = krb5client.NewWithPassword(gssapiConfig.Username, gssapiConfig.Realm, gssapiConfig.Password, cfg, krb5client.DisablePAFXFAST(gssapiConfig.DisablePAFXFAST))
	}
	return &KerberosGoKrb5Client{*client}, nil
}
