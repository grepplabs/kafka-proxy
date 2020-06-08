package protocol

// PacketEncoder is the interface providing helpers for writing with Kafka's encoding rules.
// Types implementing Encoder only need to worry about calling methods like PutString,
// not about how a string is represented in Kafka.
type packetEncoder interface {
	// Primitives
	putInt8(in int8)
	putInt16(in int16)
	putInt32(in int32)
	putInt64(in int64)
	putVarint(in int64)
	putArrayLength(in int) error
	putBool(in bool)

	putBytes(in []byte) error
	putString(in string) error
	putNullableString(in *string) error
	putStringArray(in []string) error
	putInt32Array(in []int32) error
	putInt64Array(in []int64) error

	putVarintBytes(in []byte) error
	putVarintString(in string) error

	putCompactBytes(in []byte) error
	putCompactString(in string) error
	putCompactNullableString(in *string) error
	putCompactArrayLength(in int) error
	putCompactNullableArrayLength(in int) error

	// Provide the current offset to record the batch size metric
	offset() int
}
