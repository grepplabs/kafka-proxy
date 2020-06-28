package protocol

import (
	"encoding/hex"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestEncodeDecodeResponseHeader(t *testing.T) {
	a := assert.New(t)

	reqHex := "0000003300000001"
	reqBytes, err := hex.DecodeString(reqHex)
	a.Nil(err)

	rh := &ResponseHeader{}
	err = Decode(reqBytes, rh)
	a.Nil(err)

	a.EqualValues(51, rh.Length)
	a.EqualValues(01, rh.CorrelationID)

	encoded, err := Encode(rh)
	a.Nil(err)
	a.EqualValues(reqHex, hex.EncodeToString(encoded))
}
