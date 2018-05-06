package googleid

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestParseJWT(t *testing.T) {
	a := assert.New(t)
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	a.Nil(err)

	testHeader := `{
       "alg": "RS256",
       "kid": "978ca4118bf1883b316bbca6ce9044d9977f2027"
    }`
	testClaims := `{
       "azp": "4712.apps.googleusercontent.com",
       "aud": "4711.apps.googleusercontent.com",
       "sub": "100004711",
       "hd": "grepplabs.com",
       "email": "info@grepplabs.com",
       "email_verified": true,
       "exp": 2114380800,
       "iss": "accounts.google.com",
       "iat": 1516304351
       }`

	tokenString, err := encodeTestToken(testHeader, testClaims, privateKey)
	a.Nil(err)
	a.NotEmpty(tokenString)

	token, err := ParseJWT(tokenString)
	a.Nil(err)
	a.NotNil(token)

	a.Equal(token.Raw, tokenString)
	a.Equal(token.Header.Algorithm, "RS256")
	a.Equal(token.Header.KeyID, "978ca4118bf1883b316bbca6ce9044d9977f2027")

	a.Equal(token.ClaimSet.Azp, "4712.apps.googleusercontent.com")
	a.Equal(token.ClaimSet.Aud, "4711.apps.googleusercontent.com")
	a.Equal(token.ClaimSet.Sub, "100004711")
	a.Equal(token.ClaimSet.Email, "info@grepplabs.com")
	a.Equal(token.ClaimSet.EmailVerified, true)
	a.Equal(token.ClaimSet.Exp, int64(2114380800))
	a.Equal(token.ClaimSet.Iss, "accounts.google.com")
	a.Equal(token.ClaimSet.Iat, int64(1516304351))

}

func encodeTestToken(headerJSON string, claimsJSON string, key *rsa.PrivateKey) (string, error) {
	sg := func(data []byte) (sig []byte, err error) {
		h := sha256.New()
		h.Write(data)
		return rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, h.Sum(nil))
	}
	ss := fmt.Sprintf("%s.%s", base64.RawURLEncoding.EncodeToString([]byte(headerJSON)), base64.RawURLEncoding.EncodeToString([]byte(claimsJSON)))
	sig, err := sg([]byte(ss))
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s.%s", ss, base64.RawURLEncoding.EncodeToString(sig)), nil
}
