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

	token, err := SaslOAuthBearer{}.GetToken(saslAuthBytes)
	a.Nil(err)
	a.Equal("eyJhbGciOiJub25lIn0.eyJleHAiOjEuNTM5NTE2Njk0NDE4RTksImlhdCI6MS41Mzk1MTMwOTQ0MThFOSwic3ViIjoiYWxpY2UyIn0.", token)
}
