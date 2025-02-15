package keyutil

import (
	"crypto/x509"
	"encoding/pem"
	"errors"
	"strings"

	"github.com/youmark/pkcs8"
)

func DecryptPrivateKeyPEM(pemData []byte, password string) ([]byte, error) {
	keyBlock, _ := pem.Decode(pemData)
	if keyBlock == nil {
		return nil, errors.New("failed to parse PEM")
	}
	//nolint:staticcheck  // Ignore SA1019: Support legacy encrypted PEM blocks
	if x509.IsEncryptedPEMBlock(keyBlock) {
		if password == "" {
			return nil, errors.New("PEM is encrypted, but password is empty")
		}
		key, err := x509.DecryptPEMBlock(keyBlock, []byte(password))
		if err != nil {
			return nil, err
		}
		block := &pem.Block{
			Type:  "RSA PRIVATE KEY",
			Bytes: key,
		}
		return pem.EncodeToMemory(block), nil
	} else if strings.Contains(string(pemData), "ENCRYPTED PRIVATE KEY") {
		if password == "" {
			return nil, errors.New("PEM is encrypted, but password is empty")
		}
		key, err := pkcs8.ParsePKCS8PrivateKey(keyBlock.Bytes, []byte(password))
		if err != nil {
			return nil, err
		}
		return MarshalPrivateKeyToPEM(key)
	}
	return pemData, nil
}

func EncryptPKCS8PrivateKeyPEM(pemData []byte, password string) ([]byte, error) {
	if password == "" {
		return nil, errors.New("password cannot be empty")
	}
	keyBlock, _ := pem.Decode(pemData)
	if keyBlock == nil {
		return nil, errors.New("failed to parse PEM")
	}

	var (
		key any
		err error
	)
	switch keyBlock.Type {
	case "PRIVATE KEY":
		key, err = x509.ParsePKCS8PrivateKey(keyBlock.Bytes)
		if err != nil {
			return nil, err
		}
	case "RSA PRIVATE KEY":
		rsaKey, err := x509.ParsePKCS1PrivateKey(keyBlock.Bytes)
		if err != nil {
			return nil, err
		}
		key, err = x509.MarshalPKCS8PrivateKey(rsaKey)
		if err != nil {
			return nil, err
		}
		// Parse back to interface{} to match the signature for pkcs8.MarshalPrivateKey
		key, err = x509.ParsePKCS8PrivateKey(key.([]byte))
		if err != nil {
			return nil, err
		}
	default:
		return nil, errors.New("unsupported key type: " + keyBlock.Type)
	}

	encryptedBytes, err := pkcs8.MarshalPrivateKey(key, []byte(password), pkcs8.DefaultOpts)
	if err != nil {
		return nil, err
	}
	encryptedBlock := &pem.Block{
		Type:  "ENCRYPTED PRIVATE KEY",
		Bytes: encryptedBytes,
	}

	return pem.EncodeToMemory(encryptedBlock), nil
}
