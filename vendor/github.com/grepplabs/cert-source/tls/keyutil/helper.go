package keyutil

import (
	"bytes"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	cryptorand "crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"os"
)

const (
	X509CRLBlockType       = "X509 CRL"
	CertificateBlockType   = "CERTIFICATE"
	ECPrivateKeyBlockType  = "EC PRIVATE KEY"
	RSAPrivateKeyBlockType = "RSA PRIVATE KEY"
	PrivateKeyBlockType    = "PRIVATE KEY"
	PublicKeyBlockType     = "PUBLIC KEY"
	ECPublicKeyType        = "EC PUBLIC KEY"
)

func ParseCRLsPEM(pemCrls []byte) ([]*x509.RevocationList, error) {
	ok := false
	var lists []*x509.RevocationList
	for len(pemCrls) > 0 {
		var block *pem.Block
		block, pemCrls = pem.Decode(pemCrls)
		if block == nil {
			break
		}
		if block.Type != X509CRLBlockType {
			continue
		}
		list, err := x509.ParseRevocationList(block.Bytes)
		if err != nil {
			return lists, err
		}
		lists = append(lists, list)
		ok = true
	}
	if !ok {
		return lists, errors.New("data does not contain any valid CRL")
	}
	return lists, nil
}

func ParseCertsPEM(pemCerts []byte) ([]*x509.Certificate, error) {
	ok := false
	var certs []*x509.Certificate
	for len(pemCerts) > 0 {
		var block *pem.Block
		block, pemCerts = pem.Decode(pemCerts)
		if block == nil {
			break
		}
		if block.Type != CertificateBlockType || len(block.Headers) != 0 {
			continue
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return certs, err
		}

		certs = append(certs, cert)
		ok = true
	}

	if !ok {
		return certs, errors.New("data does not contain any valid RSA or ECDSA certificates")
	}
	return certs, nil
}

func GetHexFormatted(buf []byte, sep string) string {
	var ret bytes.Buffer
	for _, cur := range buf {
		if ret.Len() > 0 {
			_, _ = ret.WriteString(sep)
		}
		_, _ = fmt.Fprintf(&ret, "%02x", cur)
	}
	return ret.String()
}

func GenerateECKeys() (crypto.PrivateKey, []byte, crypto.PublicKey, []byte, error) {
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), cryptorand.Reader)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	return marshalKeysToPEM(privateKey, privateKey.Public())
}

func GenerateRSAKeys() (crypto.PrivateKey, []byte, crypto.PublicKey, []byte, error) {
	privateKey, err := rsa.GenerateKey(cryptorand.Reader, 4096)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	return marshalKeysToPEM(privateKey, privateKey.Public())
}

func KeysMatch(priv crypto.PrivateKey, pub crypto.PublicKey) bool {
	privKey, ok := priv.(interface {
		Public() crypto.PublicKey
	})
	if !ok {
		return false
	}
	pubKey, ok := privKey.Public().(interface {
		Equal(crypto.PublicKey) bool
	})
	if !ok {
		return false
	}
	return pubKey.Equal(pub)
}

func marshalKeysToPEM(privateKey crypto.PrivateKey, publicKey crypto.PublicKey) (crypto.PrivateKey, []byte, crypto.PublicKey, []byte, error) {
	privatePem, err := MarshalPrivateKeyToPEM(privateKey)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	publicPem, err := MarshalPublicKeyToPEM(publicKey)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	return privateKey, privatePem, publicKey, publicPem, nil
}

func ParsePrivateKeyPEM(keyData []byte) (crypto.PrivateKey, error) {
	var privateKeyPemBlock *pem.Block
	for {
		privateKeyPemBlock, keyData = pem.Decode(keyData)
		if privateKeyPemBlock == nil {
			break
		}

		switch privateKeyPemBlock.Type {
		case ECPrivateKeyBlockType:
			if key, err := x509.ParseECPrivateKey(privateKeyPemBlock.Bytes); err == nil {
				return key, nil
			}
		case RSAPrivateKeyBlockType:
			if key, err := x509.ParsePKCS1PrivateKey(privateKeyPemBlock.Bytes); err == nil {
				return key, nil
			}
		case PrivateKeyBlockType:
			if key, err := x509.ParsePKCS8PrivateKey(privateKeyPemBlock.Bytes); err == nil {
				return key, nil
			}
		}
	}
	return nil, fmt.Errorf("data does not contain a valid RSA or ECDSA private key")
}

func ParsePublicKeysPEM(keyData []byte) ([]crypto.PublicKey, error) {
	var block *pem.Block
	var keys []crypto.PublicKey
	for {
		block, keyData = pem.Decode(keyData)
		if block == nil {
			break
		}
		if privateKey, err := parseRSAPrivateKey(block.Bytes); err == nil {
			keys = append(keys, &privateKey.PublicKey)
			continue
		}
		if publicKey, err := parseRSAPublicKey(block.Bytes); err == nil {
			keys = append(keys, publicKey)
			continue
		}
		if privateKey, err := parseECPrivateKey(block.Bytes); err == nil {
			keys = append(keys, &privateKey.PublicKey)
			continue
		}
		if publicKey, err := parseECPublicKey(block.Bytes); err == nil {
			keys = append(keys, publicKey)
			continue
		}
	}

	if len(keys) == 0 {
		return nil, fmt.Errorf("data does not contain any valid RSA or ECDSA public keys")
	}
	return keys, nil
}

func MarshalPrivateKeyToPEM(privateKey crypto.PrivateKey) ([]byte, error) {
	switch t := privateKey.(type) {
	case *ecdsa.PrivateKey:
		derBytes, err := x509.MarshalECPrivateKey(t)
		if err != nil {
			return nil, err
		}
		block := &pem.Block{
			Type:  ECPrivateKeyBlockType,
			Bytes: derBytes,
		}
		return pem.EncodeToMemory(block), nil
	case *rsa.PrivateKey:
		block := &pem.Block{
			Type:  RSAPrivateKeyBlockType,
			Bytes: x509.MarshalPKCS1PrivateKey(t),
		}
		return pem.EncodeToMemory(block), nil
	default:
		return nil, fmt.Errorf("private key is not a recognized type: %T", privateKey)
	}
}

func MarshalPublicKeyToPEM(publicKey crypto.PublicKey) ([]byte, error) {
	switch t := publicKey.(type) {
	case *ecdsa.PublicKey:
		derBytes, err := x509.MarshalPKIXPublicKey(t)
		if err != nil {
			return nil, err
		}
		return pem.EncodeToMemory(
			&pem.Block{
				Type:  ECPublicKeyType,
				Bytes: derBytes,
			},
		), nil
	case *rsa.PublicKey:
		derBytes, err := x509.MarshalPKIXPublicKey(t)
		if err != nil {
			return nil, err
		}
		return pem.EncodeToMemory(
			&pem.Block{
				Type:  PublicKeyBlockType,
				Bytes: derBytes,
			},
		), nil
	default:
		return nil, fmt.Errorf("private key is not a recognized type: %T", publicKey)
	}
}

func ReadPrivateKey(r io.Reader) (crypto.PrivateKey, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	key, err := ParsePrivateKeyPEM(data)
	if err != nil {
		return nil, fmt.Errorf("error reading private key: %v", err)
	}
	return key, nil
}

func ReadPrivateKeyFile(filename string) (crypto.PrivateKey, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	key, err := ParsePrivateKeyPEM(data)
	if err != nil {
		return nil, fmt.Errorf("error reading private key: %v", err)
	}
	return key, nil
}

func ReadPublicKeys(r io.Reader) ([]crypto.PublicKey, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	keys, err := ParsePublicKeysPEM(data)
	if err != nil {
		return nil, fmt.Errorf("error reading public key: %v", err)
	}
	return keys, nil
}

func ReadPublicKeyFile(filename string) (crypto.PublicKey, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	keys, err := ParsePublicKeysPEM(data)
	if err != nil {
		return nil, fmt.Errorf("error reading public key: %v", err)
	}
	return keys[0], nil
}

func parseRSAPublicKey(data []byte) (*rsa.PublicKey, error) {
	var err error
	var parsedKey interface{}
	if parsedKey, err = x509.ParsePKIXPublicKey(data); err != nil {
		if cert, err := x509.ParseCertificate(data); err == nil {
			parsedKey = cert.PublicKey
		} else {
			return nil, err
		}
	}
	var pubKey *rsa.PublicKey
	var ok bool
	if pubKey, ok = parsedKey.(*rsa.PublicKey); !ok {
		return nil, fmt.Errorf("data doesn't contain valid RSA Public Key")
	}

	return pubKey, nil
}

func parseRSAPrivateKey(data []byte) (*rsa.PrivateKey, error) {
	var err error
	var parsedKey any
	if parsedKey, err = x509.ParsePKCS1PrivateKey(data); err != nil {
		if parsedKey, err = x509.ParsePKCS8PrivateKey(data); err != nil {
			return nil, err
		}
	}
	var privKey *rsa.PrivateKey
	var ok bool
	if privKey, ok = parsedKey.(*rsa.PrivateKey); !ok {
		return nil, fmt.Errorf("data doesn't contain valid RSA Private Key")
	}

	return privKey, nil
}

func parseECPublicKey(data []byte) (*ecdsa.PublicKey, error) {
	var err error

	var parsedKey any
	if parsedKey, err = x509.ParsePKIXPublicKey(data); err != nil {
		if cert, err := x509.ParseCertificate(data); err == nil {
			parsedKey = cert.PublicKey
		} else {
			return nil, err
		}
	}

	var pubKey *ecdsa.PublicKey
	var ok bool
	if pubKey, ok = parsedKey.(*ecdsa.PublicKey); !ok {
		return nil, fmt.Errorf("data doesn't contain valid ECDSA Public Key")
	}

	return pubKey, nil
}

func parseECPrivateKey(data []byte) (*ecdsa.PrivateKey, error) {
	var err error

	var parsedKey any
	if parsedKey, err = x509.ParseECPrivateKey(data); err != nil {
		return nil, err
	}

	var privKey *ecdsa.PrivateKey
	var ok bool
	if privKey, ok = parsedKey.(*ecdsa.PrivateKey); !ok {
		return nil, fmt.Errorf("data doesn't contain valid ECDSA Private Key")
	}

	return privKey, nil
}
