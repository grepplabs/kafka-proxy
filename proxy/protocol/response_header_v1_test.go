package protocol

import (
	"encoding/hex"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestEncodeDecodeResponseHeaderV1(t *testing.T) {
	a := assert.New(t)

	reqHex := "000000390000000200"
	reqBytes, err := hex.DecodeString(reqHex)
	a.Nil(err)

	rh := &ResponseHeaderV1{}
	err = Decode(reqBytes, rh)
	a.Nil(err)

	a.EqualValues(57, rh.Length)
	a.EqualValues(02, rh.CorrelationID)
	a.Len(rh.RawTaggedFields, 0)

	encoded, err := Encode(rh)
	a.Nil(err)
	a.EqualValues(reqHex, hex.EncodeToString(encoded))
}
