package protocol

import (
	"testing"
)

func TestProduceRequestV0(t *testing.T) {
	messageSchema := NewSchemaStruct("message_schema",
		&field{name: "crc", ty: typeInt32},
		&field{name: "magic_byte", ty: typeInt8},
		&field{name: "attributes", ty: typeInt8},
		&field{name: "key", ty: typeBytes},
		&field{name: "value", ty: typeBytes},
	)

	messageSetItemSchema := NewSchemaStruct("message_set_item_schema",
		&field{name: "offset", ty: typeInt64},
		&field{name: "message_size", ty: typeInt32},
		&field{name: "message", ty: messageSchema},
	)

	partitionSchema := NewSchemaStruct("partition_schema",
		&field{name: "partition", ty: typeInt32},
		&messageSet{name: "message_set", size: 56, ty: messageSetItemSchema},
	)

	topicSchema := NewSchemaStruct("topic_schema",
		&field{name: "topic_name", ty: typeStr},
		&array{name: "partitions", ty: partitionSchema},
	)

	schemaProduce := NewSchemaStruct("produce_v0",
		&field{name: "key", ty: typeInt16},
		&field{name: "version", ty: typeInt16},
		&field{name: "correlation_id", ty: typeInt32},
		&field{name: "client_id", ty: typeStr},
		&field{name: "required_acks", ty: typeInt16},
		&field{name: "timeout", ty: typeInt32},
		&array{name: "topics", ty: topicSchema},
	)

	message1 := Struct{
		schema: messageSchema,
		values: make([]interface{}, 0),
	}

	message2 := Struct{
		schema: messageSchema,
		values: make([]interface{}, 0),
	}

	message1.values = append(message1.values, int32(0))
	message1.values = append(message1.values, int8(0))
	message1.values = append(message1.values, int8(0))
	message1.values = append(message1.values, []byte{0})
	message1.values = append(message1.values, []byte{1})

	message2.values = append(message2.values, int32(0))
	message2.values = append(message2.values, int8(0))
	message2.values = append(message2.values, int8(0))
	message2.values = append(message2.values, []byte{1})
	message2.values = append(message2.values, []byte{2})

	messageSetItem1 := Struct{
		schema: messageSetItemSchema,
		values: make([]interface{}, 0),
	}

	messageSetItem2 := Struct{
		schema: messageSetItemSchema,
		values: make([]interface{}, 0),
	}

	messageSetItem1.values = append(messageSetItem1.values, int64(0))
	messageSetItem1.values = append(messageSetItem1.values, int32(0))
	messageSetItem1.values = append(messageSetItem1.values, &message1)

	messageSetItem2.values = append(messageSetItem2.values, int64(0))
	messageSetItem2.values = append(messageSetItem2.values, int32(0))
	messageSetItem2.values = append(messageSetItem2.values, &message2)

	msgSet := make([]interface{}, 0)
	msgSet = append(msgSet, &messageSetItem1)
	msgSet = append(msgSet, &messageSetItem2)

	partitionData := Struct{
		schema: partitionSchema,
		values: make([]interface{}, 0),
	}

	topicData1 := Struct{
		schema: topicSchema,
		values: make([]interface{}, 0),
	}

	topicData2 := Struct{
		schema: topicSchema,
		values: make([]interface{}, 0),
	}

	produceData := Struct{
		schema: schemaProduce,
		values: make([]interface{}, 0),
	}

	topics := []string{"topic1", "topic2"}
	partitionData.values = append(partitionData.values, int32(0))
	partitionData.values = append(partitionData.values, msgSet)

	topicData1.values = append(topicData1.values, topics[0])
	partArr := make([]interface{}, 0)
	partArr = append(partArr, &partitionData)
	topicData1.values = append(topicData1.values, partArr)

	topicData2.values = append(topicData2.values, topics[1])
	topicData2.values = append(topicData2.values, partArr)

	reqT := &ProduceRequestV0{}

	produceData.values = append(produceData.values, int16(reqT.key()))
	produceData.values = append(produceData.values, int16(reqT.version()))
	produceData.values = append(produceData.values, int32(0))
	produceData.values = append(produceData.values, "custom")
	produceData.values = append(produceData.values, int16(0))
	produceData.values = append(produceData.values, int32(0))
	arr := make([]interface{}, 0)
	arr = append(arr, &topicData1)
	arr = append(arr, &topicData2)
	produceData.values = append(produceData.values, arr)

	encoded, err := EncodeSchema(&produceData, schemaProduce)

	if err != nil {
		t.Fatalf("Test failed, encoding schema %s", err)
	}

	req := &Request{Body: reqT}
	if err = Decode(encoded, req); err != nil {
		t.Fatalf("Test failed decoding request, %s", err)
	}

	if len(reqT.GetTopics()) != len(topics) {
		t.Fatalf("Number of topics after decoding not same as tested, decoded: %v, test_input: %v", reqT.GetTopics(), arr)
	}

	for i, val := range reqT.GetTopics() {
		if val != topics[i] {
			t.Fatalf("Topics gathered after decoding not same as tested, decoded: %v, test_input: %v", reqT.GetTopics(), arr)
		}
	}
}

func TestProduceRequestV1(t *testing.T) {
	messageSchema := NewSchemaStruct("message_schema",
		&field{name: "crc", ty: typeInt32},
		&field{name: "magic_byte", ty: typeInt8},
		&field{name: "attributes", ty: typeInt8},
		&field{name: "timestamp", ty: typeInt64},
		&field{name: "key", ty: typeBytes},
		&field{name: "value", ty: typeBytes},
	)

	messageSetItemSchema := NewSchemaStruct("message_set_item_schema",
		&field{name: "offset", ty: typeInt64},
		&field{name: "message_size", ty: typeInt32},
		&field{name: "message", ty: messageSchema},
	)

	partitionSchema := NewSchemaStruct("partition_schema",
		&field{name: "partition", ty: typeInt32},
		&messageSet{name: "message_set", size: 72, ty: messageSetItemSchema},
	)

	topicSchema := NewSchemaStruct("topic_schema",
		&field{name: "topic_name", ty: typeStr},
		&array{name: "partitions", ty: partitionSchema},
	)

	schemaProduce := NewSchemaStruct("produce_v1",
		&field{name: "key", ty: typeInt16},
		&field{name: "version", ty: typeInt16},
		&field{name: "correlation_id", ty: typeInt32},
		&field{name: "client_id", ty: typeStr},
		&field{name: "required_acks", ty: typeInt16},
		&field{name: "timeout", ty: typeInt32},
		&array{name: "topics", ty: topicSchema},
	)

	message1 := Struct{
		schema: messageSchema,
		values: make([]interface{}, 0),
	}

	message2 := Struct{
		schema: messageSchema,
		values: make([]interface{}, 0),
	}

	message1.values = append(message1.values, int32(0))
	message1.values = append(message1.values, int8(0))
	message1.values = append(message1.values, int8(0))
	message1.values = append(message1.values, int64(0))
	message1.values = append(message1.values, []byte{0})
	message1.values = append(message1.values, []byte{1})

	message2.values = append(message2.values, int32(0))
	message2.values = append(message2.values, int8(0))
	message2.values = append(message2.values, int8(0))
	message2.values = append(message2.values, int64(0))
	message2.values = append(message2.values, []byte{1})
	message2.values = append(message2.values, []byte{2})

	messageSetItem1 := Struct{
		schema: messageSetItemSchema,
		values: make([]interface{}, 0),
	}

	messageSetItem2 := Struct{
		schema: messageSetItemSchema,
		values: make([]interface{}, 0),
	}

	messageSetItem1.values = append(messageSetItem1.values, int64(0))
	messageSetItem1.values = append(messageSetItem1.values, int32(0))
	messageSetItem1.values = append(messageSetItem1.values, &message1)

	messageSetItem2.values = append(messageSetItem2.values, int64(0))
	messageSetItem2.values = append(messageSetItem2.values, int32(0))
	messageSetItem2.values = append(messageSetItem2.values, &message2)

	msgSet := make([]interface{}, 0)
	msgSet = append(msgSet, &messageSetItem1)
	msgSet = append(msgSet, &messageSetItem2)

	partitionData := Struct{
		schema: partitionSchema,
		values: make([]interface{}, 0),
	}

	topicData1 := Struct{
		schema: topicSchema,
		values: make([]interface{}, 0),
	}

	topicData2 := Struct{
		schema: topicSchema,
		values: make([]interface{}, 0),
	}

	produceData := Struct{
		schema: schemaProduce,
		values: make([]interface{}, 0),
	}

	topics := []string{"topic1", "topic2"}
	partitionData.values = append(partitionData.values, int32(0))
	partitionData.values = append(partitionData.values, msgSet)

	topicData1.values = append(topicData1.values, topics[0])
	partArr := make([]interface{}, 0)
	partArr = append(partArr, &partitionData)
	topicData1.values = append(topicData1.values, partArr)

	topicData2.values = append(topicData2.values, topics[1])
	topicData2.values = append(topicData2.values, partArr)

	reqT := &ProduceRequestV1{}

	produceData.values = append(produceData.values, int16(reqT.key()))
	produceData.values = append(produceData.values, int16(reqT.version()))
	produceData.values = append(produceData.values, int32(0))
	produceData.values = append(produceData.values, "custom")
	produceData.values = append(produceData.values, int16(0))
	produceData.values = append(produceData.values, int32(0))
	arr := make([]interface{}, 0)
	arr = append(arr, &topicData1)
	arr = append(arr, &topicData2)
	produceData.values = append(produceData.values, arr)

	encoded, err := EncodeSchema(&produceData, schemaProduce)

	if err != nil {
		t.Fatalf("Test failed, encoding schema %s", err)
	}

	req := &Request{Body: reqT}
	if err = Decode(encoded, req); err != nil {
		t.Fatalf("Test failed decoding request, %s", err)
	}

	if len(reqT.GetTopics()) != len(topics) {
		t.Fatalf("Number of topics after decoding not same as tested, decoded: %v, test_input: %v", reqT.GetTopics(), arr)
	}

	for i, val := range reqT.GetTopics() {
		if val != topics[i] {
			t.Fatalf("Topics gathered after decoding not same as tested, decoded: %v, test_input: %v", reqT.GetTopics(), arr)
		}
	}
}

// func TestProduceRequestV2(t *testing.T) {
// 	headerSchema := NewSchemaStruct("header_schema",
// 		&field{name: "headerKey", ty: typeVarintString},
// 		&field{name: "headerVal", ty: typeVarintBytes},
// 	)
//
// 	recordSchema := NewSchemaStruct("record_schema",
// 		&field{name: "length", ty: typeVarint},
// 		&field{name: "attributes", ty: typeInt8},
// 		&field{name: "timestampDelta", ty: typeVarint},
// 		&field{name: "offsetDelta", ty: typeVarint},
// 		&field{name: "key", ty: typeVarintBytes},
// 		&field{name: "value", ty: typeVarintBytes},
// 		&array{name: "headers", ty: headerSchema},
// 	)
//
// 	messageSetSchema := NewSchemaStruct("message_set_schema",
// 		&field{name: "baseOffset", ty: typeInt64},
// 		&field{name: "batchLength", ty: typeInt32},
// 		&field{name: "partitionLeaderEpoch", ty: typeInt32},
// 		&field{name: "magic", ty: typeInt8},
// 		&field{name: "crc", ty: typeInt32},
// 		&field{name: "attributes", ty: typeInt16},
// 		&field{name: "lastOffsetDelta", ty: typeInt32},
// 		&field{name: "firstTimestamp", ty: typeInt64},
// 		&field{name: "maxTimestamp", ty: typeInt64},
// 		&field{name: "producerId", ty: typeInt64},
// 		&field{name: "producerEpoch", ty: typeInt16},
// 		&field{name: "baseSequence", ty: typeInt32},
// 		&array{name: "records", ty: recordSchema},
// 	)
//
// 	partitionSchema := NewSchemaStruct("partition_schema",
// 		&field{name: "partition", ty: typeInt32},
// 		&messageSet{name: "message_set", size: 72, ty: messageSetSchema},
// 	)
//
// 	topicSchema := NewSchemaStruct("topic_schema",
// 		&field{name: "topic_name", ty: typeStr},
// 		&array{name: "partitions", ty: partitionSchema},
// 	)
//
// 	schemaProduce := NewSchemaStruct("produce_v1",
// 		&field{name: "key", ty: typeInt16},
// 		&field{name: "version", ty: typeInt16},
// 		&field{name: "correlation_id", ty: typeInt32},
// 		&field{name: "client_id", ty: typeStr},
// 		&field{name: "required_acks", ty: typeInt16},
// 		&field{name: "timeout", ty: typeInt32},
// 		&array{name: "topics", ty: topicSchema},
// 	)
//
// 	message1 := Struct{
// 		schema: messageSchema,
// 		values: make([]interface{}, 0),
// 	}
//
// 	message2 := Struct{
// 		schema: messageSchema,
// 		values: make([]interface{}, 0),
// 	}
//
// 	message1.values = append(message1.values, int32(0))
// 	message1.values = append(message1.values, int8(0))
// 	message1.values = append(message1.values, int8(0))
// 	message1.values = append(message1.values, int64(0))
// 	message1.values = append(message1.values, []byte{0})
// 	message1.values = append(message1.values, []byte{1})
//
// 	message2.values = append(message2.values, int32(0))
// 	message2.values = append(message2.values, int8(0))
// 	message2.values = append(message2.values, int8(0))
// 	message2.values = append(message2.values, int64(0))
// 	message2.values = append(message2.values, []byte{1})
// 	message2.values = append(message2.values, []byte{2})
//
// 	messageSetItem1 := Struct{
// 		schema: messageSetItemSchema,
// 		values: make([]interface{}, 0),
// 	}
//
// 	messageSetItem2 := Struct{
// 		schema: messageSetItemSchema,
// 		values: make([]interface{}, 0),
// 	}
//
// 	messageSetItem1.values = append(messageSetItem1.values, int64(0))
// 	messageSetItem1.values = append(messageSetItem1.values, int32(0))
// 	messageSetItem1.values = append(messageSetItem1.values, &message1)
//
// 	messageSetItem2.values = append(messageSetItem2.values, int64(0))
// 	messageSetItem2.values = append(messageSetItem2.values, int32(0))
// 	messageSetItem2.values = append(messageSetItem2.values, &message2)
//
// 	msgSet := make([]interface{}, 0)
// 	msgSet = append(msgSet, &messageSetItem1)
// 	msgSet = append(msgSet, &messageSetItem2)
//
// 	partitionData := Struct{
// 		schema: partitionSchema,
// 		values: make([]interface{}, 0),
// 	}
//
// 	topicData1 := Struct{
// 		schema: topicSchema,
// 		values: make([]interface{}, 0),
// 	}
//
// 	topicData2 := Struct{
// 		schema: topicSchema,
// 		values: make([]interface{}, 0),
// 	}
//
// 	produceData := Struct{
// 		schema: schemaProduce,
// 		values: make([]interface{}, 0),
// 	}
//
// 	topics := []string{"topic1", "topic2"}
// 	partitionData.values = append(partitionData.values, int32(0))
// 	partitionData.values = append(partitionData.values, msgSet)
//
// 	topicData1.values = append(topicData1.values, topics[0])
// 	partArr := make([]interface{}, 0)
// 	partArr = append(partArr, &partitionData)
// 	topicData1.values = append(topicData1.values, partArr)
//
// 	topicData2.values = append(topicData2.values, topics[1])
// 	topicData2.values = append(topicData2.values, partArr)
//
// 	reqT := &ProduceRequestV1{}
//
// 	produceData.values = append(produceData.values, int16(reqT.key()))
// 	produceData.values = append(produceData.values, int16(reqT.version()))
// 	produceData.values = append(produceData.values, int32(0))
// 	produceData.values = append(produceData.values, "custom")
// 	produceData.values = append(produceData.values, int16(0))
// 	produceData.values = append(produceData.values, int32(0))
// 	arr := make([]interface{}, 0)
// 	arr = append(arr, &topicData1)
// 	arr = append(arr, &topicData2)
// 	produceData.values = append(produceData.values, arr)
//
// 	encoded, err := EncodeSchema(&produceData, schemaProduce)
//
// 	if err != nil {
// 		t.Fatalf("Test failed, encoding schema %s", err)
// 	}
//
// 	req := &Request{Body: reqT}
// 	if err = Decode(encoded, req); err != nil {
// 		t.Fatalf("Test failed decoding request, %s", err)
// 	}
//
// 	if len(reqT.GetTopics()) != len(topics) {
// 		t.Fatalf("Number of topics after decoding not same as tested, decoded: %v, test_input: %v", reqT.GetTopics(), arr)
// 	}
//
// 	for i, val := range reqT.GetTopics() {
// 		if val != topics[i] {
// 			t.Fatalf("Topics gathered after decoding not same as tested, decoded: %v, test_input: %v", reqT.GetTopics(), arr)
// 		}
// 	}
// }
