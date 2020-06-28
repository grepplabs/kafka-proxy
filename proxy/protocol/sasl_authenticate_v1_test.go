package protocol

import (
	"encoding/hex"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestEncodeDecodeSaslAuthenticateRequestV1(t *testing.T) {
	a := assert.New(t)

	reqHex := "002400010000000200144b61666b614578616d706c6550726f64756365720000001e006d792d746573742d75736572006d792d746573742d70617373776f7264"
	reqBytes, err := hex.DecodeString(reqHex)
	a.Nil(err)

	body := &SaslAuthenticateRequestV1{}
	request := &Request{Body: body}
	err = Decode(reqBytes, request)
	a.Nil(err)

	a.EqualValues(2, request.CorrelationID)
	a.EqualValues("KafkaExampleProducer", request.ClientID)
	a.EqualValues("006d792d746573742d75736572006d792d746573742d70617373776f7264", hex.EncodeToString(body.SaslAuthBytes))

	encoded, err := Encode(request)
	a.Nil(err)
	a.EqualValues(reqHex, hex.EncodeToString(encoded))

}
