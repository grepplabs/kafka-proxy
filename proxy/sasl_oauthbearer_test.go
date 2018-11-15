package proxy

import (
	"encoding/hex"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSaslOauthParseToken(t *testing.T) {
	a := assert.New(t)

	saslBytes := "6e2c2c01617574683d4265617265722065794a68624763694f694a756232356c496e302e65794a6c654841694f6a45754e544d354e5445324e6a6b304e44453452546b73496d6c68644349364d5334314d7a6b314d544d774f5451304d5468464f53776963335669496a6f695957787059325579496e302e0101"
	saslAuthBytes, err := hex.DecodeString(saslBytes)
	a.Nil(err)

	token, authzid, extensions, err := SaslOAuthBearer{}.GetClientInitialResponse(saslAuthBytes)
	a.Nil(err)
	a.Empty(authzid)
	a.Empty(extensions)
	a.Equal("eyJhbGciOiJub25lIn0.eyJleHAiOjEuNTM5NTE2Njk0NDE4RTksImlhdCI6MS41Mzk1MTMwOTQ0MThFOSwic3ViIjoiYWxpY2UyIn0.", token)

	a.Equal(saslAuthBytes, SaslOAuthBearer{}.ToBytes(token, authzid, extensions))

}
func TestSaslOAuthBearerToBytes(t *testing.T) {
	a := assert.New(t)
	token, authzid, extensions, err := SaslOAuthBearer{}.GetClientInitialResponse([]byte("n,,\u0001auth=Bearer 123.345.567\u0001nineteen=42\u0001\u0001"))
	a.Nil(err)
	a.Equal("123.345.567", token)
	a.Empty(authzid)
	a.Equal(map[string]string{"nineteen": "42"}, extensions)
}

func TestSaslOAuthBearerAuthorizationId(t *testing.T) {
	a := assert.New(t)
	token, authzid, extensions, err := SaslOAuthBearer{}.GetClientInitialResponse([]byte("n,a=myuser,\u0001auth=Bearer 345\u0001\u0001"))
	a.Nil(err)
	a.Equal("345", token)
	a.Equal("myuser", authzid)
	a.Empty(extensions)
}

func TestSaslOAuthBearerExtensions(t *testing.T) {
	a := assert.New(t)
	token, authzid, extensions, err := SaslOAuthBearer{}.GetClientInitialResponse([]byte("n,,\u0001propA=valueA1, valueA2\u0001auth=Bearer 567\u0001propB=valueB\u0001\u0001"))
	a.Nil(err)
	a.Equal("567", token)
	a.Empty(authzid)
	a.Equal(map[string]string{"propA": "valueA1, valueA2", "propB": "valueB"}, extensions)
}

func TestSaslOAuthBearerRfc7688Example(t *testing.T) {
	a := assert.New(t)
	message := "n,a=user@example.com,\u0001host=server.example.com\u0001port=143\u0001" +
		"auth=Bearer vF9dft4qmTc2Nvb3RlckBhbHRhdmlzdGEuY29tCg\u0001\u0001"
	token, authzid, extensions, err := SaslOAuthBearer{}.GetClientInitialResponse([]byte(message))
	a.Nil(err)
	a.Equal("vF9dft4qmTc2Nvb3RlckBhbHRhdmlzdGEuY29tCg", token)
	a.Equal("user@example.com", authzid)
	a.Equal(map[string]string{"host": "server.example.com", "port": "143"}, extensions)
}

func TestSaslOAuthBearerNoExtensionsFromByteArray(t *testing.T) {
	a := assert.New(t)
	message := "n,a=user@example.com,\u0001" +
		"auth=Bearer vF9dft4qmTc2Nvb3RlckBhbHRhdmlzdGEuY29tCg\u0001\u0001"
	token, authzid, extensions, err := SaslOAuthBearer{}.GetClientInitialResponse([]byte(message))
	a.Nil(err)
	a.Equal("vF9dft4qmTc2Nvb3RlckBhbHRhdmlzdGEuY29tCg", token)
	a.Equal("user@example.com", authzid)
	a.Empty(extensions)
}
