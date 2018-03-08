package protocol

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

var (
	emptyMetadataResponse = []byte{
		// brokers
		0x00, 0x00, 0x00, 0x00,
		// topic_metadata
		0x00, 0x00, 0x00, 0x00}

	brokersNoTopicsMetadataResponse = []byte{
		// brokers
		0x00, 0x00, 0x00, 0x02,
		// brokers[0]
		0x00, 0x00, 0xab, 0xff, // 44031
		0x00, 0x09, 'l', 'o', 'c', 'a', 'l', 'h', 'o', 's', 't',
		0x00, 0x00, 0x00, 0x33, // 51
		// brokers[1]
		0x00, 0x01, 0x02, 0x03, // 66051
		0x00, 0x0a, 'g', 'o', 'o', 'g', 'l', 'e', '.', 'c', 'o', 'm',
		0x00, 0x00, 0x01, 0x11, // 273

		// topic_metadata
		0x00, 0x00, 0x00, 0x00}

	topicsNoBrokersMetadataResponse = []byte{
		// brokers
		0x00, 0x00, 0x00, 0x00,
		// topic_metadata
		0x00, 0x00, 0x00, 0x02,

		// topic_metadata[0]
		0x00, 0x00,
		0x00, 0x03, 'f', 'o', 'o',
		// partition_metadata
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x04,
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00, 0x00, 0x07,
		0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x03,
		0x00, 0x00, 0x00, 0x00,

		// topic_metadata[1]
		0x00, 0x00,
		0x00, 0x03, 'b', 'a', 'r',
		// partition_metadata
		0x00, 0x00, 0x00, 0x00}
)

func TestDecodeEmptyMetadataResponseV0(t *testing.T) {
	a := assert.New(t)

	schema := metadataResponseSchemaVersions[0]
	bytes := emptyMetadataResponse

	s, err := DecodeSchema(bytes, schema)
	a.Nil(err)
	a.Equal(`metadata_response_v0{brokers:[],topic_metadata:[]}`, s.String())

	a.Len(s.values, 2)

	brokers, ok := s.Get("brokers").([]interface{})
	a.True(ok)
	a.Len(brokers, 0)

	topicMetadata, ok := s.Get("topic_metadata").([]interface{})
	a.True(ok)
	a.Len(topicMetadata, 0)

	resp, err := EncodeSchema(s, schema)
	a.Nil(err)
	a.Equal(bytes, resp)
}

func TestMetadataResponseV0WithBrokers(t *testing.T) {
	a := assert.New(t)

	schema := metadataResponseSchemaVersions[0]
	bytes := brokersNoTopicsMetadataResponse

	s, err := DecodeSchema(bytes, schema)
	a.Nil(err)
	a.Equal(`metadata_response_v0{brokers:[metadata_broker_v0{node_id:44031,host:localhost,port:51} metadata_broker_v0{node_id:66051,host:google.com,port:273}],topic_metadata:[]}`, s.String())
	a.Len(s.values, 2)

	brokers, ok := s.Get("brokers").([]interface{})
	a.True(ok)
	a.Len(brokers, 2)

	// broker[0]
	brokerStruct, ok := brokers[0].(*Struct)
	a.True(ok)
	a.Len(brokerStruct.values, 3)

	nodeId, ok := brokerStruct.Get("node_id").(int32)
	a.True(ok)
	a.Equal(int32(0xabff), nodeId)

	host, ok := brokerStruct.Get("host").(string)
	a.True(ok)
	a.Equal("localhost", host)

	port, ok := brokerStruct.Get("port").(int32)
	a.True(ok)
	a.Equal(int32(0x33), port)

	// broker[1]
	brokerStruct, ok = brokers[1].(*Struct)
	a.True(ok)
	a.Len(brokerStruct.values, 3)

	nodeId, ok = brokerStruct.Get("node_id").(int32)
	a.True(ok)
	a.Equal(int32(0x00010203), nodeId)

	host, ok = brokerStruct.Get("host").(string)
	a.True(ok)
	a.Equal("google.com", host)

	port, ok = brokerStruct.Get("port").(int32)
	a.True(ok)
	a.Equal(int32(0x0111), port)

	topicMetadata, ok := s.Get("topic_metadata").([]interface{})
	a.True(ok)
	a.Len(topicMetadata, 0)

	resp, err := EncodeSchema(s, schema)
	a.Nil(err)
	a.Equal(bytes, resp)
}

func TestMetadataResponseV0WithTopics(t *testing.T) {
	a := assert.New(t)

	schema := metadataResponseSchemaVersions[0]
	bytes := topicsNoBrokersMetadataResponse

	s, err := DecodeSchema(bytes, schema)
	a.Nil(err)
	a.Equal(`metadata_response_v0{brokers:[],topic_metadata:[topic_metadata_v0{error_code:0,topic:foo,partition_metadata:[partition_metadata_v0{error_code:4,partition:1,leader:7,replicas:[1 2 3],isr:[]}]} topic_metadata_v0{error_code:0,topic:bar,partition_metadata:[]}]}`, s.String())
	a.Len(s.values, 2)

	brokers, ok := s.Get("brokers").([]interface{})
	a.True(ok)
	a.Len(brokers, 0)

	topicMetadata, ok := s.Get("topic_metadata").([]interface{})
	a.True(ok)
	a.Len(topicMetadata, 2)

	// topic_metadata[0]
	topicMetadataStruct, ok := topicMetadata[0].(*Struct)
	a.True(ok)
	a.Len(topicMetadataStruct.values, 3)

	errorCode, ok := topicMetadataStruct.Get("error_code").(int16)
	a.True(ok)
	a.Equal(int16(0), errorCode)

	topic, ok := topicMetadataStruct.Get("topic").(string)
	a.True(ok)
	a.Equal("foo", topic)

	partitionMetadata, ok := topicMetadataStruct.Get("partition_metadata").([]interface{})
	a.True(ok)
	a.Len(partitionMetadata, 1)

	// partitionMetadata[0]
	partitionMetadataStruct, ok := partitionMetadata[0].(*Struct)
	a.True(ok)
	a.Len(partitionMetadataStruct.values, 5)

	errorCode, ok = partitionMetadataStruct.Get("error_code").(int16)
	a.True(ok)
	a.Equal(int16(4), errorCode)

	partition, ok := partitionMetadataStruct.Get("partition").(int32)
	a.True(ok)
	a.Equal(int32(1), partition)

	leader, ok := partitionMetadataStruct.Get("leader").(int32)
	a.True(ok)
	a.Equal(int32(7), leader)

	replicas, ok := partitionMetadataStruct.Get("replicas").([]interface{})
	a.True(ok)
	a.Len(replicas, 3)

	a.Equal(int32(1), replicas[0])
	a.Equal(int32(2), replicas[1])
	a.Equal(int32(3), replicas[2])

	isr, ok := partitionMetadataStruct.Get("isr").([]interface{})
	a.True(ok)
	a.Len(isr, 0)

	// topic_metadata[1]
	topicMetadataStruct, ok = topicMetadata[1].(*Struct)
	a.True(ok)
	a.Len(topicMetadataStruct.values, 3)

	errorCode, ok = topicMetadataStruct.Get("error_code").(int16)
	a.True(ok)
	a.Equal(int16(0), errorCode)

	topic, ok = topicMetadataStruct.Get("topic").(string)
	a.True(ok)
	a.Equal("bar", topic)

	partitionMetadata, ok = topicMetadataStruct.Get("partition_metadata").([]interface{})
	a.True(ok)
	a.Len(partitionMetadata, 0)

	resp, err := EncodeSchema(s, schema)
	a.Nil(err)
	a.Equal(bytes, resp)
}
