package protocol

import (
	"testing"
)

func TestCreateTopicsV0(t *testing.T) {
	assignmentSchema := NewSchemaStruct("assignment_schema",
		&field{name: "partition_index", ty: typeInt32},
		&array{name: "broker_ids", ty: typeInt32},
	)

	configSchema := NewSchemaStruct("config_schema",
		&field{name: "name", ty: typeStr},
		&field{name: "value", ty: typeNullableStr},
	)

	topicSchema := NewSchemaStruct("topic_schema",
		&field{name: "topic_name", ty: typeStr},
		&field{name: "num_partitions", ty: typeInt32},
		&field{name: "replication_factor", ty: typeInt16},
		&array{name: "assignments", ty: assignmentSchema},
		&array{name: "configs", ty: configSchema},
	)

	schemaFetch := NewSchemaStruct("createtopics_v0",
		&field{name: "key", ty: typeInt16},
		&field{name: "version", ty: typeInt16},
		&field{name: "correlation_id", ty: typeInt32},
		&field{name: "client_id", ty: typeStr},
		&array{name: "topics", ty: topicSchema},
		&field{name: "timeout_ms", ty: typeInt32},
	)

	assignData := Struct{
		schema: assignmentSchema,
		values: make([]interface{}, 0),
	}

	configData := Struct{
		schema: configSchema,
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

	createTopicsData := Struct{
		schema: schemaFetch,
		values: make([]interface{}, 0),
	}

	topics := []string{"topic1", "topic2"}
	brokerIds := make([]interface{}, 0)
	brokerIds = append(brokerIds, int32(0))
	brokerIds = append(brokerIds, int32(1))
	assignData.values = append(assignData.values, int32(0))
	assignData.values = append(assignData.values, brokerIds)

	configVal := "configvalue"
	configData.values = append(configData.values, "myconfig")
	configData.values = append(configData.values, &configVal)

	topicData1.values = append(topicData1.values, topics[0])
	topicData1.values = append(topicData1.values, int32(0))
	topicData1.values = append(topicData1.values, int16(0))
	partArr := make([]interface{}, 0)
	partArr = append(partArr, &assignData)
	topicData1.values = append(topicData1.values, partArr)
	configArr := make([]interface{}, 0)
	configArr = append(configArr, &configData)
	topicData1.values = append(topicData1.values, configArr)

	topicData2.values = append(topicData2.values, topics[1])
	topicData2.values = append(topicData2.values, int32(0))
	topicData2.values = append(topicData2.values, int16(0))
	topicData2.values = append(topicData2.values, partArr)
	topicData2.values = append(topicData2.values, configArr)

	reqT := &CreateTopicsRequestV0{}

	createTopicsData.values = append(createTopicsData.values, int16(reqT.key()))
	createTopicsData.values = append(createTopicsData.values, int16(reqT.version()))
	createTopicsData.values = append(createTopicsData.values, int32(0))
	createTopicsData.values = append(createTopicsData.values, "custom")
	arr := make([]interface{}, 0)
	arr = append(arr, &topicData1)
	arr = append(arr, &topicData2)
	createTopicsData.values = append(createTopicsData.values, arr)
	createTopicsData.values = append(createTopicsData.values, int32(0))

	encoded, err := EncodeSchema(&createTopicsData, schemaFetch)

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

func TestCreateTopicsV1(t *testing.T) {
	assignmentSchema := NewSchemaStruct("assignment_schema",
		&field{name: "partition_index", ty: typeInt32},
		&array{name: "broker_ids", ty: typeInt32},
	)

	configSchema := NewSchemaStruct("config_schema",
		&field{name: "name", ty: typeStr},
		&field{name: "value", ty: typeNullableStr},
	)

	topicSchema := NewSchemaStruct("topic_schema",
		&field{name: "topic_name", ty: typeStr},
		&field{name: "num_partitions", ty: typeInt32},
		&field{name: "replication_factor", ty: typeInt16},
		&array{name: "assignments", ty: assignmentSchema},
		&array{name: "configs", ty: configSchema},
	)

	schemaFetch := NewSchemaStruct("createtopics_v1",
		&field{name: "key", ty: typeInt16},
		&field{name: "version", ty: typeInt16},
		&field{name: "correlation_id", ty: typeInt32},
		&field{name: "client_id", ty: typeStr},
		&array{name: "topics", ty: topicSchema},
		&field{name: "timeout_ms", ty: typeInt32},
		&field{name: "validate_only", ty: typeBool},
	)

	assignData := Struct{
		schema: assignmentSchema,
		values: make([]interface{}, 0),
	}

	configData := Struct{
		schema: configSchema,
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

	createTopicsData := Struct{
		schema: schemaFetch,
		values: make([]interface{}, 0),
	}

	topics := []string{"topic1", "topic2"}
	brokerIds := make([]interface{}, 0)
	brokerIds = append(brokerIds, int32(0))
	brokerIds = append(brokerIds, int32(1))
	assignData.values = append(assignData.values, int32(0))
	assignData.values = append(assignData.values, brokerIds)

	configVal := "configvalue"
	configData.values = append(configData.values, "myconfig")
	configData.values = append(configData.values, &configVal)

	topicData1.values = append(topicData1.values, topics[0])
	topicData1.values = append(topicData1.values, int32(0))
	topicData1.values = append(topicData1.values, int16(0))
	partArr := make([]interface{}, 0)
	partArr = append(partArr, &assignData)
	topicData1.values = append(topicData1.values, partArr)
	configArr := make([]interface{}, 0)
	configArr = append(configArr, &configData)
	topicData1.values = append(topicData1.values, configArr)

	topicData2.values = append(topicData2.values, topics[1])
	topicData2.values = append(topicData2.values, int32(0))
	topicData2.values = append(topicData2.values, int16(0))
	topicData2.values = append(topicData2.values, partArr)
	topicData2.values = append(topicData2.values, configArr)

	reqT := &CreateTopicsRequestV1{}

	createTopicsData.values = append(createTopicsData.values, int16(reqT.key()))
	createTopicsData.values = append(createTopicsData.values, int16(reqT.version()))
	createTopicsData.values = append(createTopicsData.values, int32(0))
	createTopicsData.values = append(createTopicsData.values, "custom")
	arr := make([]interface{}, 0)
	arr = append(arr, &topicData1)
	arr = append(arr, &topicData2)
	createTopicsData.values = append(createTopicsData.values, arr)
	createTopicsData.values = append(createTopicsData.values, int32(0))
	createTopicsData.values = append(createTopicsData.values, false)

	encoded, err := EncodeSchema(&createTopicsData, schemaFetch)

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

func TestCreateTopicsV2(t *testing.T) {
	assignmentSchema := NewSchemaStruct("assignment_schema",
		&field{name: "partition_index", ty: typeInt32},
		&array{name: "broker_ids", ty: typeInt32},
	)

	configSchema := NewSchemaStruct("config_schema",
		&field{name: "name", ty: typeStr},
		&field{name: "value", ty: typeNullableStr},
	)

	topicSchema := NewSchemaStruct("topic_schema",
		&field{name: "topic_name", ty: typeStr},
		&field{name: "num_partitions", ty: typeInt32},
		&field{name: "replication_factor", ty: typeInt16},
		&array{name: "assignments", ty: assignmentSchema},
		&array{name: "configs", ty: configSchema},
	)

	schemaFetch := NewSchemaStruct("createtopics_v2",
		&field{name: "key", ty: typeInt16},
		&field{name: "version", ty: typeInt16},
		&field{name: "correlation_id", ty: typeInt32},
		&field{name: "client_id", ty: typeStr},
		&array{name: "topics", ty: topicSchema},
		&field{name: "timeout_ms", ty: typeInt32},
		&field{name: "validate_only", ty: typeBool},
	)

	assignData := Struct{
		schema: assignmentSchema,
		values: make([]interface{}, 0),
	}

	configData := Struct{
		schema: configSchema,
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

	createTopicsData := Struct{
		schema: schemaFetch,
		values: make([]interface{}, 0),
	}

	topics := []string{"topic1", "topic2"}
	brokerIds := make([]interface{}, 0)
	brokerIds = append(brokerIds, int32(0))
	brokerIds = append(brokerIds, int32(1))
	assignData.values = append(assignData.values, int32(0))
	assignData.values = append(assignData.values, brokerIds)

	configVal := "configvalue"
	configData.values = append(configData.values, "myconfig")
	configData.values = append(configData.values, &configVal)

	topicData1.values = append(topicData1.values, topics[0])
	topicData1.values = append(topicData1.values, int32(0))
	topicData1.values = append(topicData1.values, int16(0))
	partArr := make([]interface{}, 0)
	partArr = append(partArr, &assignData)
	topicData1.values = append(topicData1.values, partArr)
	configArr := make([]interface{}, 0)
	configArr = append(configArr, &configData)
	topicData1.values = append(topicData1.values, configArr)

	topicData2.values = append(topicData2.values, topics[1])
	topicData2.values = append(topicData2.values, int32(0))
	topicData2.values = append(topicData2.values, int16(0))
	topicData2.values = append(topicData2.values, partArr)
	topicData2.values = append(topicData2.values, configArr)

	reqT := &CreateTopicsRequestV2{}

	createTopicsData.values = append(createTopicsData.values, int16(reqT.key()))
	createTopicsData.values = append(createTopicsData.values, int16(reqT.version()))
	createTopicsData.values = append(createTopicsData.values, int32(0))
	createTopicsData.values = append(createTopicsData.values, "custom")
	arr := make([]interface{}, 0)
	arr = append(arr, &topicData1)
	arr = append(arr, &topicData2)
	createTopicsData.values = append(createTopicsData.values, arr)
	createTopicsData.values = append(createTopicsData.values, int32(0))
	createTopicsData.values = append(createTopicsData.values, false)

	encoded, err := EncodeSchema(&createTopicsData, schemaFetch)

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

func TestCreateTopicsV3(t *testing.T) {
	assignmentSchema := NewSchemaStruct("assignment_schema",
		&field{name: "partition_index", ty: typeInt32},
		&array{name: "broker_ids", ty: typeInt32},
	)

	configSchema := NewSchemaStruct("config_schema",
		&field{name: "name", ty: typeStr},
		&field{name: "value", ty: typeNullableStr},
	)

	topicSchema := NewSchemaStruct("topic_schema",
		&field{name: "topic_name", ty: typeStr},
		&field{name: "num_partitions", ty: typeInt32},
		&field{name: "replication_factor", ty: typeInt16},
		&array{name: "assignments", ty: assignmentSchema},
		&array{name: "configs", ty: configSchema},
	)

	schemaFetch := NewSchemaStruct("createtopics_v3",
		&field{name: "key", ty: typeInt16},
		&field{name: "version", ty: typeInt16},
		&field{name: "correlation_id", ty: typeInt32},
		&field{name: "client_id", ty: typeStr},
		&array{name: "topics", ty: topicSchema},
		&field{name: "timeout_ms", ty: typeInt32},
		&field{name: "validate_only", ty: typeBool},
	)

	assignData := Struct{
		schema: assignmentSchema,
		values: make([]interface{}, 0),
	}

	configData := Struct{
		schema: configSchema,
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

	createTopicsData := Struct{
		schema: schemaFetch,
		values: make([]interface{}, 0),
	}

	topics := []string{"topic1", "topic2"}
	brokerIds := make([]interface{}, 0)
	brokerIds = append(brokerIds, int32(0))
	brokerIds = append(brokerIds, int32(1))
	assignData.values = append(assignData.values, int32(0))
	assignData.values = append(assignData.values, brokerIds)

	configVal := "configvalue"
	configData.values = append(configData.values, "myconfig")
	configData.values = append(configData.values, &configVal)

	topicData1.values = append(topicData1.values, topics[0])
	topicData1.values = append(topicData1.values, int32(0))
	topicData1.values = append(topicData1.values, int16(0))
	partArr := make([]interface{}, 0)
	partArr = append(partArr, &assignData)
	topicData1.values = append(topicData1.values, partArr)
	configArr := make([]interface{}, 0)
	configArr = append(configArr, &configData)
	topicData1.values = append(topicData1.values, configArr)

	topicData2.values = append(topicData2.values, topics[1])
	topicData2.values = append(topicData2.values, int32(0))
	topicData2.values = append(topicData2.values, int16(0))
	topicData2.values = append(topicData2.values, partArr)
	topicData2.values = append(topicData2.values, configArr)

	reqT := &CreateTopicsRequestV3{}

	createTopicsData.values = append(createTopicsData.values, int16(reqT.key()))
	createTopicsData.values = append(createTopicsData.values, int16(reqT.version()))
	createTopicsData.values = append(createTopicsData.values, int32(0))
	createTopicsData.values = append(createTopicsData.values, "custom")
	arr := make([]interface{}, 0)
	arr = append(arr, &topicData1)
	arr = append(arr, &topicData2)
	createTopicsData.values = append(createTopicsData.values, arr)
	createTopicsData.values = append(createTopicsData.values, int32(0))
	createTopicsData.values = append(createTopicsData.values, false)

	encoded, err := EncodeSchema(&createTopicsData, schemaFetch)

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

func TestCreateTopicsV4(t *testing.T) {
	assignmentSchema := NewSchemaStruct("assignment_schema",
		&field{name: "partition_index", ty: typeInt32},
		&array{name: "broker_ids", ty: typeInt32},
	)

	configSchema := NewSchemaStruct("config_schema",
		&field{name: "name", ty: typeStr},
		&field{name: "value", ty: typeNullableStr},
	)

	topicSchema := NewSchemaStruct("topic_schema",
		&field{name: "topic_name", ty: typeStr},
		&field{name: "num_partitions", ty: typeInt32},
		&field{name: "replication_factor", ty: typeInt16},
		&array{name: "assignments", ty: assignmentSchema},
		&array{name: "configs", ty: configSchema},
	)

	schemaFetch := NewSchemaStruct("createtopics_v4",
		&field{name: "key", ty: typeInt16},
		&field{name: "version", ty: typeInt16},
		&field{name: "correlation_id", ty: typeInt32},
		&field{name: "client_id", ty: typeStr},
		&array{name: "topics", ty: topicSchema},
		&field{name: "timeout_ms", ty: typeInt32},
		&field{name: "validate_only", ty: typeBool},
	)

	assignData := Struct{
		schema: assignmentSchema,
		values: make([]interface{}, 0),
	}

	configData := Struct{
		schema: configSchema,
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

	createTopicsData := Struct{
		schema: schemaFetch,
		values: make([]interface{}, 0),
	}

	topics := []string{"topic1", "topic2"}
	brokerIds := make([]interface{}, 0)
	brokerIds = append(brokerIds, int32(0))
	brokerIds = append(brokerIds, int32(1))
	assignData.values = append(assignData.values, int32(0))
	assignData.values = append(assignData.values, brokerIds)

	configVal := "configvalue"
	configData.values = append(configData.values, "myconfig")
	configData.values = append(configData.values, &configVal)

	topicData1.values = append(topicData1.values, topics[0])
	topicData1.values = append(topicData1.values, int32(0))
	topicData1.values = append(topicData1.values, int16(0))
	partArr := make([]interface{}, 0)
	partArr = append(partArr, &assignData)
	topicData1.values = append(topicData1.values, partArr)
	configArr := make([]interface{}, 0)
	configArr = append(configArr, &configData)
	topicData1.values = append(topicData1.values, configArr)

	topicData2.values = append(topicData2.values, topics[1])
	topicData2.values = append(topicData2.values, int32(0))
	topicData2.values = append(topicData2.values, int16(0))
	topicData2.values = append(topicData2.values, partArr)
	topicData2.values = append(topicData2.values, configArr)

	reqT := &CreateTopicsRequestV4{}

	createTopicsData.values = append(createTopicsData.values, int16(reqT.key()))
	createTopicsData.values = append(createTopicsData.values, int16(reqT.version()))
	createTopicsData.values = append(createTopicsData.values, int32(0))
	createTopicsData.values = append(createTopicsData.values, "custom")
	arr := make([]interface{}, 0)
	arr = append(arr, &topicData1)
	arr = append(arr, &topicData2)
	createTopicsData.values = append(createTopicsData.values, arr)
	createTopicsData.values = append(createTopicsData.values, int32(0))
	createTopicsData.values = append(createTopicsData.values, false)

	encoded, err := EncodeSchema(&createTopicsData, schemaFetch)

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
