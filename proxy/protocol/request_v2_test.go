package protocol

import (
	"encoding/binary"
	"encoding/hex"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestEncodeDecodeRequestV2(t *testing.T) {
	a := assert.New(t)

	reqHex := "0000003f002400020000000200144b61666b614578616d706c6550726f6475636572001f006d792d746573742d75736572006d792d746573742d70617373776f726400"
	reqBytes, err := hex.DecodeString(reqHex)
	a.Nil(err)

	size := int32(binary.BigEndian.Uint32(reqBytes[0:4]))
	a.EqualValues(len(reqBytes)-4, size)

	requestKeyVersion := &RequestKeyVersion{}
	err = Decode(reqBytes[0:8], requestKeyVersion) // Size => int32 + ApiKey => int16 + ApiVersion => int16
	a.Nil(err)

	a.EqualValues(63, requestKeyVersion.Length)
	a.EqualValues(36, requestKeyVersion.ApiKey)
	a.EqualValues(02, requestKeyVersion.ApiVersion)

	body := &SaslAuthenticateRequestV2{}
	request := &RequestV2{Body: body}
	err = Decode(reqBytes[4:], request)
	a.Nil(err)

	a.EqualValues(2, request.CorrelationID)
	a.EqualValues("KafkaExampleProducer", request.ClientID)
	a.EqualValues("006d792d746573742d75736572006d792d746573742d70617373776f7264", hex.EncodeToString(body.SaslAuthBytes))

	encoded, err := Encode(request)
	a.Nil(err)
	a.EqualValues(reqHex[4*2:], hex.EncodeToString(encoded))
	_ = encoded
}
