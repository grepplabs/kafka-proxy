package protocol

import (
	"errors"
	"fmt"
)

// ErrInsufficientData is returned when decoding and the packet is truncated. This can be expected
// when requesting messages, since as an optimization the server is allowed to return a partial message at the end
// of the message set.
var ErrInsufficientData = errors.New("kafka: insufficient data to decode packet, more bytes expected")

// PacketEncodingError is returned from a failure while encoding a Kafka packet. This can happen, for example,
// if you try to encode a string over 2^15 characters in length, since Kafka's encoding rules do not permit that.
type PacketEncodingError struct {
	Info string
}

func (err PacketEncodingError) Error() string {
	return fmt.Sprintf("kafka: error encoding packet: %s", err.Info)
}

// PacketDecodingError is returned when there was an error (other than truncated data) decoding the Kafka broker's response.
// This can be a bad CRC or length field, or any other invalid value.
type PacketDecodingError struct {
	Info string
}

func (err PacketDecodingError) Error() string {
	return fmt.Sprintf("kafka: error decoding packet: %s", err.Info)
}

// SchemaEncodingError is returned from a failure while encoding a schema .
type SchemaEncodingError struct {
	Info string
}

func (err SchemaEncodingError) Error() string {
	return fmt.Sprintf("schema: error encoding value: %s", err.Info)
}

// SchemaDecodingError is returned from a failure while decoding a schema .
type SchemaDecodingError struct {
	Info string
}

func (err SchemaDecodingError) Error() string {
	return fmt.Sprintf("schema: error decoding value: %s", err.Info)
}
