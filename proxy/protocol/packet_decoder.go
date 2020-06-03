package protocol

// PacketDecoder is the interface providing helpers for reading with Kafka's encoding rules.
// Types implementing Decoder only need to worry about calling methods like GetString,
// not about how a string is represented in Kafka.
type packetDecoder interface {
	// Primitives
	skipBytes(skip int) error
	getInt8() (int8, error)
	getInt16() (int16, error)
	getInt32() (int32, error)
	getInt64() (int64, error)
	getVarint() (int64, error)
	getArrayLength() (int, error)
	getBool() (bool, error)

	getBytes() ([]byte, error)
	getString() (string, error)
	getNullableString() (*string, error)
	getInt32Array() ([]int32, error)
	getInt64Array() ([]int64, error)
	getStringArray() ([]string, error)

	getVarintBytes() ([]byte, error)

	getCompactBytes() ([]byte, error)
	getCompactString() (string, error)
	getCompactNullableString() (*string, error)
	getCompactArrayLength() (int, error)
	getCompactNullableArrayLength() (int, error)

	// Subsets
	remaining() int
}
