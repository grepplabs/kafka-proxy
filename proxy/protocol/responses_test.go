package protocol

import (
	"encoding/hex"
	"fmt"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

var (
	emptyMetadataResponse = []byte{
		// brokers
		0x00, 0x00, 0x00, 0x00,
		// topic_metadata
		0x00, 0x00, 0x00, 0x00}

	testResponseModifier = func(brokerHost string, brokerPort int32) (listenerHost string, listenerPort int32, err error) {
		if brokerHost == "localhost" && brokerPort == 51 {
			return "myhost1", 34001, nil
		} else if brokerHost == "google.com" && brokerPort == 273 {
			return "myhost2", 34002, nil
		} else if brokerHost == "kafka.org" && brokerPort == 53503 {
			return "myhost3", 34003, nil
		}
		return "", 0, errors.New("unexpected data")
	}

	testResponseModifier2 = func(brokerHost string, brokerPort int32) (listenerHost string, listenerPort int32, err error) {
		if brokerHost == "localhost" && brokerPort == 19092 {
			return "myhost1", 34001, nil
		} else if brokerHost == "localhost" && brokerPort == 29092 {
			return "myhost2", 34002, nil
		} else if brokerHost == "localhost" && brokerPort == 39092 {
			return "myhost3", 34003, nil
		}
		return "", 0, errors.New("unexpected data")
	}
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
	// The Hard Way
	bytes := []byte{
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

	a := assert.New(t)

	schema := metadataResponseSchemaVersions[0]

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
	// The Hard Way

	bytes := []byte{
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

	a := assert.New(t)

	schema := metadataResponseSchemaVersions[0]

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

func TestEmptyMetadataResponseV0(t *testing.T) {
	a := assert.New(t)

	schema := metadataResponseSchemaVersions[0]
	bytes := emptyMetadataResponse

	s, err := DecodeSchema(bytes, schema)
	a.Nil(err)
	dc := NewDecodeCheck()
	dc.Traverse(s)
	expected := []string{
		"[brokers]",
		"[topic_metadata]",
	}
	a.Equal(expected, dc.AttrValues())
	resp, err := EncodeSchema(s, schema)
	a.Nil(err)
	a.Equal(bytes, resp)
}

func TestMetadataResponseV0(t *testing.T) {
	/*
	   Metadata Response (Version: 0) => [brokers] [topic_metadata]
	     brokers => node_id host port
	       node_id => INT32
	       host => STRING
	       port => INT32
	     topic_metadata => error_code topic [partition_metadata]
	       error_code => INT16
	       topic => STRING
	       partition_metadata => error_code partition leader [replicas] [isr]
	         error_code => INT16
	         partition => INT32
	         leader => INT32
	         replicas => INT32
	         isr => INT32
	*/

	apiVersion := int16(0)

	bytes := []byte{
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

	a := assert.New(t)

	schema := metadataResponseSchemaVersions[apiVersion]

	s, err := DecodeSchema(bytes, schema)
	a.Nil(err)
	dc := NewDecodeCheck()
	dc.Traverse(s)
	expected := []string{
		"[brokers]",
		"brokers struct",
		"node_id int32 44031",
		"host string localhost",
		"port int32 51",
		"brokers struct",
		"node_id int32 66051",
		"host string google.com",
		"port int32 273",
		"[topic_metadata]",
		"topic_metadata struct",
		"error_code int16 0",
		"topic string foo",
		"[partition_metadata]",
		"partition_metadata struct",
		"error_code int16 4",
		"partition int32 1",
		"leader int32 7",
		"[replicas]",
		"replicas int32 1",
		"replicas int32 2",
		"replicas int32 3",
		"[isr]",
		"topic_metadata struct",
		"error_code int16 0",
		"topic string bar",
		"[partition_metadata]",
	}
	a.Equal(expected, dc.AttrValues())
	resp, err := EncodeSchema(s, schema)
	a.Nil(err)
	a.Equal(bytes, resp)

	modifier, err := GetResponseModifier(apiKeyMetadata, apiVersion, func(brokerHost string, brokerPort int32) (listenerHost string, listenerPort int32, err error) {
		if brokerHost == "localhost" && brokerPort == 51 {
			return "azure.microsoft.com", 34001, nil
		} else if brokerHost == "google.com" && brokerPort == 273 {
			return "aws.com", 34999, nil
		}
		return "", 0, errors.New("unexpected data")
	})
	a.Nil(err)
	resp, err = modifier.Apply(resp)
	a.Nil(err)
	s, err = DecodeSchema(resp, schema)
	a.Nil(err)
	dc = NewDecodeCheck()
	dc.Traverse(s)
	expected = []string{
		"[brokers]",
		"brokers struct",
		"node_id int32 44031",
		"host string azure.microsoft.com", // replaced
		"port int32 34001",                // replaced
		"brokers struct",
		"node_id int32 66051",
		"host string aws.com", // replaced
		"port int32 34999",    // replaced
		"[topic_metadata]",
		"topic_metadata struct",
		"error_code int16 0",
		"topic string foo",
		"[partition_metadata]",
		"partition_metadata struct",
		"error_code int16 4",
		"partition int32 1",
		"leader int32 7",
		"[replicas]",
		"replicas int32 1",
		"replicas int32 2",
		"replicas int32 3",
		"[isr]",
		"topic_metadata struct",
		"error_code int16 0",
		"topic string bar",
		"[partition_metadata]",
	}
	a.Equal(expected, dc.AttrValues())
}

func TestMetadataResponseV1(t *testing.T) {
	/*
	   Metadata Response (Version: 1) => [brokers] controller_id [topic_metadata]
	     brokers => node_id host port rack
	       node_id => INT32
	       host => STRING
	       port => INT32
	       rack => NULLABLE_STRING
	     controller_id => INT32
	     topic_metadata => error_code topic is_internal [partition_metadata]
	       error_code => INT16
	       topic => STRING
	       is_internal => BOOLEAN
	       partition_metadata => error_code partition leader [replicas] [isr]
	         error_code => INT16
	         partition => INT32
	         leader => INT32
	         replicas => INT32
	         isr => INT32
	*/

	apiVersion := int16(1)

	bytes := []byte{
		// brokers
		0x00, 0x00, 0x00, 0x03,
		// brokers[0]
		0x00, 0x00, 0xab, 0xff, // 44031
		0x00, 0x09, 'l', 'o', 'c', 'a', 'l', 'h', 'o', 's', 't',
		0x00, 0x00, 0x00, 0x33, // 51
		0x00, 0x00, // ''
		// brokers[1]
		0x00, 0x01, 0x02, 0x03, // 66051
		0x00, 0x0a, 'g', 'o', 'o', 'g', 'l', 'e', '.', 'c', 'o', 'm',
		0x00, 0x00, 0x01, 0x11, // 273
		0x00, 0x07, 'e', 'u', 'w', 'e', 's', 't', '1',
		// brokers[2]
		0x00, 0x00, 0x00, 0x02, // 2
		0x00, 0x09, 'k', 'a', 'f', 'k', 'a', '.', 'o', 'r', 'g',
		0x00, 0x00, 0xd0, 0xff, // 53503
		0xff, 0xff, //  -1 is nil'

		// controller_id
		0x00, 0x00, 0xe1, 0xb2, // 57778

		// topic_metadata
		0x00, 0x00, 0x00, 0x02,

		// topic_metadata[0]
		0x00, 0x00,
		0x00, 0x03, 'f', 'o', 'o',
		0x01, // true
		// partition_metadata
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x04,
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00, 0x00, 0x07,
		0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x03,
		0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x02,

		// topic_metadata[1]
		0x00, 0x00,
		0x00, 0x03, 'b', 'a', 'r',
		0x00, // false
		// partition_metadata
		0x00, 0x00, 0x00, 0x00}

	a := assert.New(t)

	schema := metadataResponseSchemaVersions[apiVersion]

	s, err := DecodeSchema(bytes, schema)
	a.Nil(err)

	dc := NewDecodeCheck()
	dc.Traverse(s)

	expected := []string{
		"[brokers]",
		"brokers struct",
		"node_id int32 44031",
		"host string localhost",
		"port int32 51",
		"rack *string ",
		"brokers struct",
		"node_id int32 66051",
		"host string google.com",
		"port int32 273",
		"rack *string euwest1",
		"brokers struct",
		"node_id int32 2",
		"host string kafka.org",
		"port int32 53503",
		"rack *string <nil>",
		"controller_id int32 57778",
		"[topic_metadata]",
		"topic_metadata struct",
		"error_code int16 0",
		"topic string foo",
		"is_internal bool true",
		"[partition_metadata]",
		"partition_metadata struct",
		"error_code int16 4",
		"partition int32 1",
		"leader int32 7",
		"[replicas]",
		"replicas int32 1",
		"replicas int32 2",
		"replicas int32 3",
		"[isr]",
		"isr int32 3",
		"isr int32 2",
		"topic_metadata struct",
		"error_code int16 0",
		"topic string bar",
		"is_internal bool false",
		"[partition_metadata]",
	}
	a.Equal(expected, dc.AttrValues())
	resp, err := EncodeSchema(s, schema)
	a.Nil(err)
	a.Equal(bytes, resp)

	modifier, err := GetResponseModifier(apiKeyMetadata, apiVersion, testResponseModifier)
	a.Nil(err)
	resp, err = modifier.Apply(resp)
	a.Nil(err)
	s, err = DecodeSchema(resp, schema)
	a.Nil(err)
	dc = NewDecodeCheck()
	dc.Traverse(s)
	expected = []string{
		"[brokers]",
		"brokers struct",
		"node_id int32 44031",
		"host string myhost1", // replaced
		"port int32 34001",    // replaced
		"rack *string ",
		"brokers struct",
		"node_id int32 66051",
		"host string myhost2", // replaced
		"port int32 34002",    // replaced
		"rack *string euwest1",
		"brokers struct",
		"node_id int32 2",
		"host string myhost3", // replaced
		"port int32 34003",    // replaced
		"rack *string <nil>",
		"controller_id int32 57778",
		"[topic_metadata]",
		"topic_metadata struct",
		"error_code int16 0",
		"topic string foo",
		"is_internal bool true",
		"[partition_metadata]",
		"partition_metadata struct",
		"error_code int16 4",
		"partition int32 1",
		"leader int32 7",
		"[replicas]",
		"replicas int32 1",
		"replicas int32 2",
		"replicas int32 3",
		"[isr]",
		"isr int32 3",
		"isr int32 2",
		"topic_metadata struct",
		"error_code int16 0",
		"topic string bar",
		"is_internal bool false",
		"[partition_metadata]",
	}
	a.Equal(expected, dc.AttrValues())
}

func TestMetadataResponseV2(t *testing.T) {
	/*

	   Metadata Response (Version: 2) => [brokers] cluster_id controller_id [topic_metadata]
	     brokers => node_id host port rack
	       node_id => INT32
	       host => STRING
	       port => INT32
	       rack => NULLABLE_STRING
	     cluster_id => NULLABLE_STRING
	     controller_id => INT32
	     topic_metadata => error_code topic is_internal [partition_metadata]
	       error_code => INT16
	       topic => STRING
	       is_internal => BOOLEAN
	       partition_metadata => error_code partition leader [replicas] [isr]
	         error_code => INT16
	         partition => INT32
	         leader => INT32
	         replicas => INT32
	         isr => INT32
	*/

	apiVersion := int16(2)

	bytes := []byte{
		// brokers
		0x00, 0x00, 0x00, 0x03,
		// brokers[0]
		0x00, 0x00, 0xab, 0xff, // 44031
		0x00, 0x09, 'l', 'o', 'c', 'a', 'l', 'h', 'o', 's', 't',
		0x00, 0x00, 0x00, 0x33, // 51
		0x00, 0x00, // ''
		// brokers[1]
		0x00, 0x01, 0x02, 0x03, // 66051
		0x00, 0x0a, 'g', 'o', 'o', 'g', 'l', 'e', '.', 'c', 'o', 'm',
		0x00, 0x00, 0x01, 0x11, // 273
		0x00, 0x07, 'e', 'u', 'w', 'e', 's', 't', '1',
		// brokers[2]
		0x00, 0x00, 0x00, 0x02, // 2
		0x00, 0x09, 'k', 'a', 'f', 'k', 'a', '.', 'o', 'r', 'g',
		0x00, 0x00, 0xd0, 0xff, // 53503
		0xff, 0xff, //  -1 is nil'

		// cluster_id
		0xff, 0xff, // nil

		// controller_id
		0x00, 0x00, 0xe1, 0xb2, // 57778

		// topic_metadata
		0x00, 0x00, 0x00, 0x02,

		// topic_metadata[0]
		0x00, 0x00,
		0x00, 0x03, 'f', 'o', 'o',
		0x01, // true
		// partition_metadata
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x04,
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00, 0x00, 0x07,
		0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x03,
		0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x02,

		// topic_metadata[1]
		0x00, 0x00,
		0x00, 0x03, 'b', 'a', 'r',
		0x00, // false
		// partition_metadata
		0x00, 0x00, 0x00, 0x00}

	a := assert.New(t)

	schema := metadataResponseSchemaVersions[apiVersion]

	s, err := DecodeSchema(bytes, schema)
	a.Nil(err)

	dc := NewDecodeCheck()
	dc.Traverse(s)

	expected := []string{
		"[brokers]",
		"brokers struct",
		"node_id int32 44031",
		"host string localhost",
		"port int32 51",
		"rack *string ",
		"brokers struct",
		"node_id int32 66051",
		"host string google.com",
		"port int32 273",
		"rack *string euwest1",
		"brokers struct",
		"node_id int32 2",
		"host string kafka.org",
		"port int32 53503",
		"rack *string <nil>",
		"cluster_id *string <nil>",
		"controller_id int32 57778",
		"[topic_metadata]",
		"topic_metadata struct",
		"error_code int16 0",
		"topic string foo",
		"is_internal bool true",
		"[partition_metadata]",
		"partition_metadata struct",
		"error_code int16 4",
		"partition int32 1",
		"leader int32 7",
		"[replicas]",
		"replicas int32 1",
		"replicas int32 2",
		"replicas int32 3",
		"[isr]",
		"isr int32 3",
		"isr int32 2",
		"topic_metadata struct",
		"error_code int16 0",
		"topic string bar",
		"is_internal bool false",
		"[partition_metadata]",
	}
	a.Equal(expected, dc.AttrValues())
	resp, err := EncodeSchema(s, schema)
	a.Nil(err)
	a.Equal(bytes, resp)

	modifier, err := GetResponseModifier(apiKeyMetadata, apiVersion, testResponseModifier)
	a.Nil(err)
	resp, err = modifier.Apply(resp)
	a.Nil(err)
	s, err = DecodeSchema(resp, schema)
	a.Nil(err)
	dc = NewDecodeCheck()
	dc.Traverse(s)
	expected = []string{
		"[brokers]",
		"brokers struct",
		"node_id int32 44031",
		"host string myhost1", // replaced
		"port int32 34001",    // replaced
		"rack *string ",
		"brokers struct",
		"node_id int32 66051",
		"host string myhost2", // replaced
		"port int32 34002",    // replaced
		"rack *string euwest1",
		"brokers struct",
		"node_id int32 2",
		"host string myhost3", // replaced
		"port int32 34003",    // replaced
		"rack *string <nil>",
		"cluster_id *string <nil>",
		"controller_id int32 57778",
		"[topic_metadata]",
		"topic_metadata struct",
		"error_code int16 0",
		"topic string foo",
		"is_internal bool true",
		"[partition_metadata]",
		"partition_metadata struct",
		"error_code int16 4",
		"partition int32 1",
		"leader int32 7",
		"[replicas]",
		"replicas int32 1",
		"replicas int32 2",
		"replicas int32 3",
		"[isr]",
		"isr int32 3",
		"isr int32 2",
		"topic_metadata struct",
		"error_code int16 0",
		"topic string bar",
		"is_internal bool false",
		"[partition_metadata]",
	}
	a.Equal(expected, dc.AttrValues())
}

func TestMetadataResponseV3(t *testing.T) {
	/*
	   Metadata Response (Version: 3) => throttle_time_ms [brokers] cluster_id controller_id [topic_metadata]
	     throttle_time_ms => INT32
	     brokers => node_id host port rack
	       node_id => INT32
	       host => STRING
	       port => INT32
	       rack => NULLABLE_STRING
	     cluster_id => NULLABLE_STRING
	     controller_id => INT32
	     topic_metadata => error_code topic is_internal [partition_metadata]
	       error_code => INT16
	       topic => STRING
	       is_internal => BOOLEAN
	       partition_metadata => error_code partition leader [replicas] [isr]
	         error_code => INT16
	         partition => INT32
	         leader => INT32
	         replicas => INT32
	         isr => INT32

	*/

	apiVersion := int16(3)

	bytes := []byte{
		// throttle_time_ms
		0x00, 0x00, 0x00, 0x01, // 1
		// brokers
		0x00, 0x00, 0x00, 0x03,
		// brokers[0]
		0x00, 0x00, 0xab, 0xff, // 44031
		0x00, 0x09, 'l', 'o', 'c', 'a', 'l', 'h', 'o', 's', 't',
		0x00, 0x00, 0x00, 0x33, // 51
		0x00, 0x00, // ''
		// brokers[1]
		0x00, 0x01, 0x02, 0x03, // 66051
		0x00, 0x0a, 'g', 'o', 'o', 'g', 'l', 'e', '.', 'c', 'o', 'm',
		0x00, 0x00, 0x01, 0x11, // 273
		0x00, 0x07, 'e', 'u', 'w', 'e', 's', 't', '1',
		// brokers[2]
		0x00, 0x00, 0x00, 0x02, // 2
		0x00, 0x09, 'k', 'a', 'f', 'k', 'a', '.', 'o', 'r', 'g',
		0x00, 0x00, 0xd0, 0xff, // 53503
		0xff, 0xff, //  -1 is nil'

		// cluster_id
		0x00, 0x07, 'm', 'y', 'k', 'a', 'f', 'k', 'a',

		// controller_id
		0x00, 0x00, 0xe1, 0xb2, // 57778

		// topic_metadata
		0x00, 0x00, 0x00, 0x02,

		// topic_metadata[0]
		0x00, 0x00,
		0x00, 0x03, 'f', 'o', 'o',
		0x01, // true
		// partition_metadata
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x04,
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00, 0x00, 0x07,
		0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x03,
		0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x02,

		// topic_metadata[1]
		0x00, 0x00,
		0x00, 0x03, 'b', 'a', 'r',
		0x00, // false
		// partition_metadata
		0x00, 0x00, 0x00, 0x00}

	a := assert.New(t)

	schema := metadataResponseSchemaVersions[apiVersion]

	s, err := DecodeSchema(bytes, schema)
	a.Nil(err)

	dc := NewDecodeCheck()
	dc.Traverse(s)

	expected := []string{
		"throttle_time_ms int32 1",
		"[brokers]",
		"brokers struct",
		"node_id int32 44031",
		"host string localhost",
		"port int32 51",
		"rack *string ",
		"brokers struct",
		"node_id int32 66051",
		"host string google.com",
		"port int32 273",
		"rack *string euwest1",
		"brokers struct",
		"node_id int32 2",
		"host string kafka.org",
		"port int32 53503",
		"rack *string <nil>",
		"cluster_id *string mykafka",
		"controller_id int32 57778",
		"[topic_metadata]",
		"topic_metadata struct",
		"error_code int16 0",
		"topic string foo",
		"is_internal bool true",
		"[partition_metadata]",
		"partition_metadata struct",
		"error_code int16 4",
		"partition int32 1",
		"leader int32 7",
		"[replicas]",
		"replicas int32 1",
		"replicas int32 2",
		"replicas int32 3",
		"[isr]",
		"isr int32 3",
		"isr int32 2",
		"topic_metadata struct",
		"error_code int16 0",
		"topic string bar",
		"is_internal bool false",
		"[partition_metadata]",
	}
	a.Equal(expected, dc.AttrValues())
	resp, err := EncodeSchema(s, schema)
	a.Nil(err)
	a.Equal(bytes, resp)

	modifier, err := GetResponseModifier(apiKeyMetadata, apiVersion, testResponseModifier)
	a.Nil(err)
	resp, err = modifier.Apply(resp)
	a.Nil(err)
	s, err = DecodeSchema(resp, schema)
	a.Nil(err)
	dc = NewDecodeCheck()
	dc.Traverse(s)
	expected = []string{
		"throttle_time_ms int32 1",
		"[brokers]",
		"brokers struct",
		"node_id int32 44031",
		"host string myhost1", // replaced
		"port int32 34001",    // replaced
		"rack *string ",
		"brokers struct",
		"node_id int32 66051",
		"host string myhost2", // replaced
		"port int32 34002",    // replaced
		"rack *string euwest1",
		"brokers struct",
		"node_id int32 2",
		"host string myhost3", // replaced
		"port int32 34003",    // replaced
		"rack *string <nil>",
		"cluster_id *string mykafka",
		"controller_id int32 57778",
		"[topic_metadata]",
		"topic_metadata struct",
		"error_code int16 0",
		"topic string foo",
		"is_internal bool true",
		"[partition_metadata]",
		"partition_metadata struct",
		"error_code int16 4",
		"partition int32 1",
		"leader int32 7",
		"[replicas]",
		"replicas int32 1",
		"replicas int32 2",
		"replicas int32 3",
		"[isr]",
		"isr int32 3",
		"isr int32 2",
		"topic_metadata struct",
		"error_code int16 0",
		"topic string bar",
		"is_internal bool false",
		"[partition_metadata]",
	}
	a.Equal(expected, dc.AttrValues())
}

func TestMetadataResponseV4(t *testing.T) {
	// SAME as V3
	/*
	   Metadata Response (Version: 4) => throttle_time_ms [brokers] cluster_id controller_id [topic_metadata]
	     throttle_time_ms => INT32
	     brokers => node_id host port rack
	       node_id => INT32
	       host => STRING
	       port => INT32
	       rack => NULLABLE_STRING
	     cluster_id => NULLABLE_STRING
	     controller_id => INT32
	     topic_metadata => error_code topic is_internal [partition_metadata]
	       error_code => INT16
	       topic => STRING
	       is_internal => BOOLEAN
	       partition_metadata => error_code partition leader [replicas] [isr]
	         error_code => INT16
	         partition => INT32
	         leader => INT32
	         replicas => INT32
	         isr => INT32
	*/

	apiVersion := int16(4)

	bytes := []byte{
		// throttle_time_ms
		0x00, 0x00, 0x00, 0x01, // 1
		// brokers
		0x00, 0x00, 0x00, 0x03,
		// brokers[0]
		0x00, 0x00, 0xab, 0xff, // 44031
		0x00, 0x09, 'l', 'o', 'c', 'a', 'l', 'h', 'o', 's', 't',
		0x00, 0x00, 0x00, 0x33, // 51
		0x00, 0x00, // ''
		// brokers[1]
		0x00, 0x01, 0x02, 0x03, // 66051
		0x00, 0x0a, 'g', 'o', 'o', 'g', 'l', 'e', '.', 'c', 'o', 'm',
		0x00, 0x00, 0x01, 0x11, // 273
		0x00, 0x07, 'e', 'u', 'w', 'e', 's', 't', '1',
		// brokers[2]
		0x00, 0x00, 0x00, 0x02, // 2
		0x00, 0x09, 'k', 'a', 'f', 'k', 'a', '.', 'o', 'r', 'g',
		0x00, 0x00, 0xd0, 0xff, // 53503
		0xff, 0xff, //  -1 is nil'

		// cluster_id
		0x00, 0x07, 'm', 'y', 'k', 'a', 'f', 'k', 'a',

		// controller_id
		0x00, 0x00, 0xe1, 0xb2, // 57778

		// topic_metadata
		0x00, 0x00, 0x00, 0x02,

		// topic_metadata[0]
		0x00, 0x00,
		0x00, 0x03, 'f', 'o', 'o',
		0x01, // true
		// partition_metadata
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x04,
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00, 0x00, 0x07,
		0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x03,
		0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x02,

		// topic_metadata[1]
		0x00, 0x00,
		0x00, 0x03, 'b', 'a', 'r',
		0x00, // false
		// partition_metadata
		0x00, 0x00, 0x00, 0x00}

	a := assert.New(t)

	schema := metadataResponseSchemaVersions[apiVersion]

	s, err := DecodeSchema(bytes, schema)
	a.Nil(err)

	dc := NewDecodeCheck()
	dc.Traverse(s)

	expected := []string{
		"throttle_time_ms int32 1",
		"[brokers]",
		"brokers struct",
		"node_id int32 44031",
		"host string localhost",
		"port int32 51",
		"rack *string ",
		"brokers struct",
		"node_id int32 66051",
		"host string google.com",
		"port int32 273",
		"rack *string euwest1",
		"brokers struct",
		"node_id int32 2",
		"host string kafka.org",
		"port int32 53503",
		"rack *string <nil>",
		"cluster_id *string mykafka",
		"controller_id int32 57778",
		"[topic_metadata]",
		"topic_metadata struct",
		"error_code int16 0",
		"topic string foo",
		"is_internal bool true",
		"[partition_metadata]",
		"partition_metadata struct",
		"error_code int16 4",
		"partition int32 1",
		"leader int32 7",
		"[replicas]",
		"replicas int32 1",
		"replicas int32 2",
		"replicas int32 3",
		"[isr]",
		"isr int32 3",
		"isr int32 2",
		"topic_metadata struct",
		"error_code int16 0",
		"topic string bar",
		"is_internal bool false",
		"[partition_metadata]",
	}
	a.Equal(expected, dc.AttrValues())
	resp, err := EncodeSchema(s, schema)
	a.Nil(err)
	a.Equal(bytes, resp)

	modifier, err := GetResponseModifier(apiKeyMetadata, apiVersion, testResponseModifier)
	a.Nil(err)
	resp, err = modifier.Apply(resp)
	a.Nil(err)
	s, err = DecodeSchema(resp, schema)
	a.Nil(err)
	dc = NewDecodeCheck()
	dc.Traverse(s)
	expected = []string{
		"throttle_time_ms int32 1",
		"[brokers]",
		"brokers struct",
		"node_id int32 44031",
		"host string myhost1", // replaced
		"port int32 34001",    // replaced
		"rack *string ",
		"brokers struct",
		"node_id int32 66051",
		"host string myhost2", // replaced
		"port int32 34002",    // replaced
		"rack *string euwest1",
		"brokers struct",
		"node_id int32 2",
		"host string myhost3", // replaced
		"port int32 34003",    // replaced
		"rack *string <nil>",
		"cluster_id *string mykafka",
		"controller_id int32 57778",
		"[topic_metadata]",
		"topic_metadata struct",
		"error_code int16 0",
		"topic string foo",
		"is_internal bool true",
		"[partition_metadata]",
		"partition_metadata struct",
		"error_code int16 4",
		"partition int32 1",
		"leader int32 7",
		"[replicas]",
		"replicas int32 1",
		"replicas int32 2",
		"replicas int32 3",
		"[isr]",
		"isr int32 3",
		"isr int32 2",
		"topic_metadata struct",
		"error_code int16 0",
		"topic string bar",
		"is_internal bool false",
		"[partition_metadata]",
	}
	a.Equal(expected, dc.AttrValues())
}

func TestMetadataResponseV5(t *testing.T) {
	/*
	   Metadata Response (Version: 5) => throttle_time_ms [brokers] cluster_id controller_id [topic_metadata]
	     throttle_time_ms => INT32
	     brokers => node_id host port rack
	       node_id => INT32
	       host => STRING
	       port => INT32
	       rack => NULLABLE_STRING
	     cluster_id => NULLABLE_STRING
	     controller_id => INT32
	     topic_metadata => error_code topic is_internal [partition_metadata]
	       error_code => INT16
	       topic => STRING
	       is_internal => BOOLEAN
	       partition_metadata => error_code partition leader [replicas] [isr] [offline_replicas]
	         error_code => INT16
	         partition => INT32
	         leader => INT32
	         replicas => INT32
	         isr => INT32
	         offline_replicas => INT32
	*/

	apiVersion := int16(5)

	bytes := []byte{
		// throttle_time_ms
		0x00, 0x00, 0x00, 0x01, // 1
		// brokers
		0x00, 0x00, 0x00, 0x03,
		// brokers[0]
		0x00, 0x00, 0xab, 0xff, // 44031
		0x00, 0x09, 'l', 'o', 'c', 'a', 'l', 'h', 'o', 's', 't',
		0x00, 0x00, 0x00, 0x33, // 51
		0x00, 0x00, // ''
		// brokers[1]
		0x00, 0x01, 0x02, 0x03, // 66051
		0x00, 0x0a, 'g', 'o', 'o', 'g', 'l', 'e', '.', 'c', 'o', 'm',
		0x00, 0x00, 0x01, 0x11, // 273
		0x00, 0x07, 'e', 'u', 'w', 'e', 's', 't', '1',
		// brokers[2]
		0x00, 0x00, 0x00, 0x02, // 2
		0x00, 0x09, 'k', 'a', 'f', 'k', 'a', '.', 'o', 'r', 'g',
		0x00, 0x00, 0xd0, 0xff, // 53503
		0xff, 0xff, //  -1 is nil'

		// cluster_id
		0x00, 0x07, 'm', 'y', 'k', 'a', 'f', 'k', 'a',

		// controller_id
		0x00, 0x00, 0xe1, 0xb2, // 57778

		// topic_metadata
		0x00, 0x00, 0x00, 0x02,

		// topic_metadata[0]
		0x00, 0x00,
		0x00, 0x03, 'f', 'o', 'o',
		0x01, // true
		// partition_metadata
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x04,
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00, 0x00, 0x07,
		0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x03,
		0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x02,
		0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x06, 0x00, 0x00, 0x00, 0x07,
		// topic_metadata[1]
		0x00, 0x00,
		0x00, 0x03, 'b', 'a', 'r',
		0x00, // false
		// partition_metadata
		0x00, 0x00, 0x00, 0x00}

	a := assert.New(t)

	schema := metadataResponseSchemaVersions[apiVersion]

	s, err := DecodeSchema(bytes, schema)
	a.Nil(err)

	dc := NewDecodeCheck()
	dc.Traverse(s)

	expected := []string{
		"throttle_time_ms int32 1",
		"[brokers]",
		"brokers struct",
		"node_id int32 44031",
		"host string localhost",
		"port int32 51",
		"rack *string ",
		"brokers struct",
		"node_id int32 66051",
		"host string google.com",
		"port int32 273",
		"rack *string euwest1",
		"brokers struct",
		"node_id int32 2",
		"host string kafka.org",
		"port int32 53503",
		"rack *string <nil>",
		"cluster_id *string mykafka",
		"controller_id int32 57778",
		"[topic_metadata]",
		"topic_metadata struct",
		"error_code int16 0",
		"topic string foo",
		"is_internal bool true",
		"[partition_metadata]",
		"partition_metadata struct",
		"error_code int16 4",
		"partition int32 1",
		"leader int32 7",
		"[replicas]",
		"replicas int32 1",
		"replicas int32 2",
		"replicas int32 3",
		"[isr]",
		"isr int32 3",
		"isr int32 2",
		"[offline_replicas]",
		"offline_replicas int32 5",
		"offline_replicas int32 6",
		"offline_replicas int32 7",
		"topic_metadata struct",
		"error_code int16 0",
		"topic string bar",
		"is_internal bool false",
		"[partition_metadata]",
	}
	a.Equal(expected, dc.AttrValues())
	resp, err := EncodeSchema(s, schema)
	a.Nil(err)
	a.Equal(bytes, resp)

	modifier, err := GetResponseModifier(apiKeyMetadata, apiVersion, testResponseModifier)
	a.Nil(err)
	resp, err = modifier.Apply(resp)
	a.Nil(err)
	s, err = DecodeSchema(resp, schema)
	a.Nil(err)
	dc = NewDecodeCheck()
	dc.Traverse(s)
	expected = []string{
		"throttle_time_ms int32 1",
		"[brokers]",
		"brokers struct",
		"node_id int32 44031",
		"host string myhost1", // replaced
		"port int32 34001",    // replaced
		"rack *string ",
		"brokers struct",
		"node_id int32 66051",
		"host string myhost2", // replaced
		"port int32 34002",    // replaced
		"rack *string euwest1",
		"brokers struct",
		"node_id int32 2",
		"host string myhost3", // replaced
		"port int32 34003",    // replaced
		"rack *string <nil>",
		"cluster_id *string mykafka",
		"controller_id int32 57778",
		"[topic_metadata]",
		"topic_metadata struct",
		"error_code int16 0",
		"topic string foo",
		"is_internal bool true",
		"[partition_metadata]",
		"partition_metadata struct",
		"error_code int16 4",
		"partition int32 1",
		"leader int32 7",
		"[replicas]",
		"replicas int32 1",
		"replicas int32 2",
		"replicas int32 3",
		"[isr]",
		"isr int32 3",
		"isr int32 2",
		"[offline_replicas]",
		"offline_replicas int32 5",
		"offline_replicas int32 6",
		"offline_replicas int32 7",
		"topic_metadata struct",
		"error_code int16 0",
		"topic string bar",
		"is_internal bool false",
		"[partition_metadata]",
	}
	a.Equal(expected, dc.AttrValues())
}

func TestMetadataResponseV6(t *testing.T) {
	/*
		Metadata Response (Version: 6) => throttle_time_ms [brokers] cluster_id controller_id [topic_metadata]
		  throttle_time_ms => INT32
		  brokers => node_id host port rack
			node_id => INT32
			host => STRING
			port => INT32
			rack => NULLABLE_STRING
		  cluster_id => NULLABLE_STRING
		  controller_id => INT32
		  topic_metadata => error_code topic is_internal [partition_metadata]
			error_code => INT16
			topic => STRING
			is_internal => BOOLEAN
			partition_metadata => error_code partition leader [replicas] [isr] [offline_replicas]
			  error_code => INT16
			  partition => INT32
			  leader => INT32
			  replicas => INT32
			  isr => INT32
			  offline_replicas => INT32

	*/

	apiVersion := int16(6)

	bytes := []byte{
		// throttle_time_ms
		0x00, 0x00, 0x00, 0x01, // 1
		// brokers
		0x00, 0x00, 0x00, 0x03,
		// brokers[0]
		0x00, 0x00, 0xab, 0xff, // 44031
		0x00, 0x09, 'l', 'o', 'c', 'a', 'l', 'h', 'o', 's', 't',
		0x00, 0x00, 0x00, 0x33, // 51
		0x00, 0x00, // ''
		// brokers[1]
		0x00, 0x01, 0x02, 0x03, // 66051
		0x00, 0x0a, 'g', 'o', 'o', 'g', 'l', 'e', '.', 'c', 'o', 'm',
		0x00, 0x00, 0x01, 0x11, // 273
		0x00, 0x07, 'e', 'u', 'w', 'e', 's', 't', '1',
		// brokers[2]
		0x00, 0x00, 0x00, 0x02, // 2
		0x00, 0x09, 'k', 'a', 'f', 'k', 'a', '.', 'o', 'r', 'g',
		0x00, 0x00, 0xd0, 0xff, // 53503
		0xff, 0xff, //  -1 is nil'

		// cluster_id
		0x00, 0x07, 'm', 'y', 'k', 'a', 'f', 'k', 'a',

		// controller_id
		0x00, 0x00, 0xe1, 0xb2, // 57778

		// topic_metadata
		0x00, 0x00, 0x00, 0x02,

		// topic_metadata[0]
		0x00, 0x00,
		0x00, 0x03, 'f', 'o', 'o',
		0x01, // true
		// partition_metadata
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x04,
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00, 0x00, 0x07,
		0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x03,
		0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x02,
		0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x06, 0x00, 0x00, 0x00, 0x07,
		// topic_metadata[1]
		0x00, 0x00,
		0x00, 0x03, 'b', 'a', 'r',
		0x00, // false
		// partition_metadata
		0x00, 0x00, 0x00, 0x00}

	a := assert.New(t)

	schema := metadataResponseSchemaVersions[apiVersion]

	s, err := DecodeSchema(bytes, schema)
	a.Nil(err)

	dc := NewDecodeCheck()
	dc.Traverse(s)

	expected := []string{
		"throttle_time_ms int32 1",
		"[brokers]",
		"brokers struct",
		"node_id int32 44031",
		"host string localhost",
		"port int32 51",
		"rack *string ",
		"brokers struct",
		"node_id int32 66051",
		"host string google.com",
		"port int32 273",
		"rack *string euwest1",
		"brokers struct",
		"node_id int32 2",
		"host string kafka.org",
		"port int32 53503",
		"rack *string <nil>",
		"cluster_id *string mykafka",
		"controller_id int32 57778",
		"[topic_metadata]",
		"topic_metadata struct",
		"error_code int16 0",
		"topic string foo",
		"is_internal bool true",
		"[partition_metadata]",
		"partition_metadata struct",
		"error_code int16 4",
		"partition int32 1",
		"leader int32 7",
		"[replicas]",
		"replicas int32 1",
		"replicas int32 2",
		"replicas int32 3",
		"[isr]",
		"isr int32 3",
		"isr int32 2",
		"[offline_replicas]",
		"offline_replicas int32 5",
		"offline_replicas int32 6",
		"offline_replicas int32 7",
		"topic_metadata struct",
		"error_code int16 0",
		"topic string bar",
		"is_internal bool false",
		"[partition_metadata]",
	}
	a.Equal(expected, dc.AttrValues())
	resp, err := EncodeSchema(s, schema)
	a.Nil(err)
	a.Equal(bytes, resp)

	modifier, err := GetResponseModifier(apiKeyMetadata, apiVersion, testResponseModifier)
	a.Nil(err)
	resp, err = modifier.Apply(resp)
	a.Nil(err)
	s, err = DecodeSchema(resp, schema)
	a.Nil(err)
	dc = NewDecodeCheck()
	dc.Traverse(s)
	expected = []string{
		"throttle_time_ms int32 1",
		"[brokers]",
		"brokers struct",
		"node_id int32 44031",
		"host string myhost1", // replaced
		"port int32 34001",    // replaced
		"rack *string ",
		"brokers struct",
		"node_id int32 66051",
		"host string myhost2", // replaced
		"port int32 34002",    // replaced
		"rack *string euwest1",
		"brokers struct",
		"node_id int32 2",
		"host string myhost3", // replaced
		"port int32 34003",    // replaced
		"rack *string <nil>",
		"cluster_id *string mykafka",
		"controller_id int32 57778",
		"[topic_metadata]",
		"topic_metadata struct",
		"error_code int16 0",
		"topic string foo",
		"is_internal bool true",
		"[partition_metadata]",
		"partition_metadata struct",
		"error_code int16 4",
		"partition int32 1",
		"leader int32 7",
		"[replicas]",
		"replicas int32 1",
		"replicas int32 2",
		"replicas int32 3",
		"[isr]",
		"isr int32 3",
		"isr int32 2",
		"[offline_replicas]",
		"offline_replicas int32 5",
		"offline_replicas int32 6",
		"offline_replicas int32 7",
		"topic_metadata struct",
		"error_code int16 0",
		"topic string bar",
		"is_internal bool false",
		"[partition_metadata]",
	}
	a.Equal(expected, dc.AttrValues())
}

func TestMetadataResponseV7(t *testing.T) {
	/*
		Metadata Response (Version: 7) => throttle_time_ms [brokers] cluster_id controller_id [topic_metadata]
		  throttle_time_ms => INT32
		  brokers => node_id host port rack
			node_id => INT32
			host => STRING
			port => INT32
			rack => NULLABLE_STRING
		  cluster_id => NULLABLE_STRING
		  controller_id => INT32
		  topic_metadata => error_code topic is_internal [partition_metadata]
			error_code => INT16
			topic => STRING
			is_internal => BOOLEAN
			partition_metadata => error_code partition leader leader_epoch [replicas] [isr] [offline_replicas]
			  error_code => INT16
			  partition => INT32
			  leader => INT32
			  leader_epoch => INT32
			  replicas => INT32
			  isr => INT32
			  offline_replicas => INT32

	*/

	apiVersion := int16(7)

	bytes := []byte{
		// throttle_time_ms
		0x00, 0x00, 0x00, 0x01, // 1
		// brokers
		0x00, 0x00, 0x00, 0x03,
		// brokers[0]
		0x00, 0x00, 0xab, 0xff, // 44031
		0x00, 0x09, 'l', 'o', 'c', 'a', 'l', 'h', 'o', 's', 't',
		0x00, 0x00, 0x00, 0x33, // 51
		0x00, 0x00, // ''
		// brokers[1]
		0x00, 0x01, 0x02, 0x03, // 66051
		0x00, 0x0a, 'g', 'o', 'o', 'g', 'l', 'e', '.', 'c', 'o', 'm',
		0x00, 0x00, 0x01, 0x11, // 273
		0x00, 0x07, 'e', 'u', 'w', 'e', 's', 't', '1',
		// brokers[2]
		0x00, 0x00, 0x00, 0x02, // 2
		0x00, 0x09, 'k', 'a', 'f', 'k', 'a', '.', 'o', 'r', 'g',
		0x00, 0x00, 0xd0, 0xff, // 53503
		0xff, 0xff, //  -1 is nil'

		// cluster_id
		0x00, 0x07, 'm', 'y', 'k', 'a', 'f', 'k', 'a',

		// controller_id
		0x00, 0x00, 0xe1, 0xb2, // 57778

		// topic_metadata
		0x00, 0x00, 0x00, 0x02,

		// topic_metadata[0]
		0x00, 0x00,
		0x00, 0x03, 'f', 'o', 'o',
		0x01, // true
		// partition_metadata
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x04,
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00, 0x00, 0x07,
		0x00, 0x00, 0x00, 0x08,
		0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x03,
		0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x02,
		0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x06, 0x00, 0x00, 0x00, 0x07,
		// topic_metadata[1]
		0x00, 0x00,
		0x00, 0x03, 'b', 'a', 'r',
		0x00, // false
		// partition_metadata
		0x00, 0x00, 0x00, 0x00}

	a := assert.New(t)

	schema := metadataResponseSchemaVersions[apiVersion]

	s, err := DecodeSchema(bytes, schema)
	a.Nil(err)

	dc := NewDecodeCheck()
	dc.Traverse(s)

	expected := []string{
		"throttle_time_ms int32 1",
		"[brokers]",
		"brokers struct",
		"node_id int32 44031",
		"host string localhost",
		"port int32 51",
		"rack *string ",
		"brokers struct",
		"node_id int32 66051",
		"host string google.com",
		"port int32 273",
		"rack *string euwest1",
		"brokers struct",
		"node_id int32 2",
		"host string kafka.org",
		"port int32 53503",
		"rack *string <nil>",
		"cluster_id *string mykafka",
		"controller_id int32 57778",
		"[topic_metadata]",
		"topic_metadata struct",
		"error_code int16 0",
		"topic string foo",
		"is_internal bool true",
		"[partition_metadata]",
		"partition_metadata struct",
		"error_code int16 4",
		"partition int32 1",
		"leader int32 7",
		"leader_epoch int32 8",
		"[replicas]",
		"replicas int32 1",
		"replicas int32 2",
		"replicas int32 3",
		"[isr]",
		"isr int32 3",
		"isr int32 2",
		"[offline_replicas]",
		"offline_replicas int32 5",
		"offline_replicas int32 6",
		"offline_replicas int32 7",
		"topic_metadata struct",
		"error_code int16 0",
		"topic string bar",
		"is_internal bool false",
		"[partition_metadata]",
	}
	a.Equal(expected, dc.AttrValues())
	resp, err := EncodeSchema(s, schema)
	a.Nil(err)
	a.Equal(bytes, resp)

	modifier, err := GetResponseModifier(apiKeyMetadata, apiVersion, testResponseModifier)
	a.Nil(err)
	resp, err = modifier.Apply(resp)
	a.Nil(err)
	s, err = DecodeSchema(resp, schema)
	a.Nil(err)
	dc = NewDecodeCheck()
	dc.Traverse(s)
	expected = []string{
		"throttle_time_ms int32 1",
		"[brokers]",
		"brokers struct",
		"node_id int32 44031",
		"host string myhost1", // replaced
		"port int32 34001",    // replaced
		"rack *string ",
		"brokers struct",
		"node_id int32 66051",
		"host string myhost2", // replaced
		"port int32 34002",    // replaced
		"rack *string euwest1",
		"brokers struct",
		"node_id int32 2",
		"host string myhost3", // replaced
		"port int32 34003",    // replaced
		"rack *string <nil>",
		"cluster_id *string mykafka",
		"controller_id int32 57778",
		"[topic_metadata]",
		"topic_metadata struct",
		"error_code int16 0",
		"topic string foo",
		"is_internal bool true",
		"[partition_metadata]",
		"partition_metadata struct",
		"error_code int16 4",
		"partition int32 1",
		"leader int32 7",
		"leader_epoch int32 8",
		"[replicas]",
		"replicas int32 1",
		"replicas int32 2",
		"replicas int32 3",
		"[isr]",
		"isr int32 3",
		"isr int32 2",
		"[offline_replicas]",
		"offline_replicas int32 5",
		"offline_replicas int32 6",
		"offline_replicas int32 7",
		"topic_metadata struct",
		"error_code int16 0",
		"topic string bar",
		"is_internal bool false",
		"[partition_metadata]",
	}
	a.Equal(expected, dc.AttrValues())
}

func TestMetadataResponseV8(t *testing.T) {
	/*
		Metadata Response (Version: 8) => throttle_time_ms [brokers] cluster_id controller_id [topics] cluster_authorized_operations
		  throttle_time_ms => INT32
		  brokers => node_id host port rack
		    node_id => INT32
		    host => STRING
		    port => INT32
		    rack => NULLABLE_STRING
		  cluster_id => NULLABLE_STRING
		  controller_id => INT32
		  topics => error_code name is_internal [partitions] topic_authorized_operations
		    error_code => INT16
		    name => STRING
		    is_internal => BOOLEAN
		    partitions => error_code partition_index leader_id leader_epoch [replica_nodes] [isr_nodes] [offline_replicas]
		      error_code => INT16
		      partition_index => INT32
		      leader_id => INT32
		      leader_epoch => INT32
		      replica_nodes => INT32
		      isr_nodes => INT32
		      offline_replicas => INT32
		    topic_authorized_operations => INT32
		  cluster_authorized_operations => INT32
	*/

	apiVersion := int16(8)

	bytes := []byte{
		// throttle_time_ms
		0x00, 0x00, 0x00, 0x01, // 1
		// brokers
		0x00, 0x00, 0x00, 0x03,
		// brokers[0]
		0x00, 0x00, 0xab, 0xff, // 44031
		0x00, 0x09, 'l', 'o', 'c', 'a', 'l', 'h', 'o', 's', 't',
		0x00, 0x00, 0x00, 0x33, // 51
		0x00, 0x00, // ''
		// brokers[1]
		0x00, 0x01, 0x02, 0x03, // 66051
		0x00, 0x0a, 'g', 'o', 'o', 'g', 'l', 'e', '.', 'c', 'o', 'm',
		0x00, 0x00, 0x01, 0x11, // 273
		0x00, 0x07, 'e', 'u', 'w', 'e', 's', 't', '1',
		// brokers[2]
		0x00, 0x00, 0x00, 0x02, // 2
		0x00, 0x09, 'k', 'a', 'f', 'k', 'a', '.', 'o', 'r', 'g',
		0x00, 0x00, 0xd0, 0xff, // 53503
		0xff, 0xff, //  -1 is nil'

		// cluster_id
		0x00, 0x07, 'm', 'y', 'k', 'a', 'f', 'k', 'a',

		// controller_id
		0x00, 0x00, 0xe1, 0xb2, // 57778

		// topic_metadata
		0x00, 0x00, 0x00, 0x02,

		// topic_metadata[0]
		0x00, 0x00,
		0x00, 0x03, 'f', 'o', 'o',
		0x01, // true
		// partition_metadata
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x04,
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00, 0x00, 0x07,
		0x00, 0x00, 0x00, 0x08,
		0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x03,
		0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x02,
		0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x06, 0x00, 0x00, 0x00, 0x07,
		0x00, 0x00, 0x00, 0x08, // topic_authorized_operations 8
		// topic_metadata[1]
		0x00, 0x00,
		0x00, 0x03, 'b', 'a', 'r',
		0x00, // false
		// partition_metadata
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x04, // topic_authorized_operations 4
		// cluster_authorized_operations 5
		0x00, 0x00, 0x00, 0x05,
	}

	a := assert.New(t)

	schema := metadataResponseSchemaVersions[apiVersion]

	s, err := DecodeSchema(bytes, schema)
	a.Nil(err)

	dc := NewDecodeCheck()
	dc.Traverse(s)

	expected := []string{
		"throttle_time_ms int32 1",
		"[brokers]",
		"brokers struct",
		"node_id int32 44031",
		"host string localhost",
		"port int32 51",
		"rack *string ",
		"brokers struct",
		"node_id int32 66051",
		"host string google.com",
		"port int32 273",
		"rack *string euwest1",
		"brokers struct",
		"node_id int32 2",
		"host string kafka.org",
		"port int32 53503",
		"rack *string <nil>",
		"cluster_id *string mykafka",
		"controller_id int32 57778",
		"[topic_metadata]",
		"topic_metadata struct",
		"error_code int16 0",
		"name string foo",
		"is_internal bool true",
		"[partition_metadata]",
		"partition_metadata struct",
		"error_code int16 4",
		"partition int32 1",
		"leader int32 7",
		"leader_epoch int32 8",
		"[replicas]",
		"replicas int32 1",
		"replicas int32 2",
		"replicas int32 3",
		"[isr]",
		"isr int32 3",
		"isr int32 2",
		"[offline_replicas]",
		"offline_replicas int32 5",
		"offline_replicas int32 6",
		"offline_replicas int32 7",
		"topic_authorized_operations int32 8",
		"topic_metadata struct",
		"error_code int16 0",
		"name string bar",
		"is_internal bool false",
		"[partition_metadata]",
		"topic_authorized_operations int32 4",
		"cluster_authorized_operations int32 5",
	}
	a.Equal(expected, dc.AttrValues())
	resp, err := EncodeSchema(s, schema)
	a.Nil(err)
	a.Equal(bytes, resp)

	modifier, err := GetResponseModifier(apiKeyMetadata, apiVersion, testResponseModifier)
	a.Nil(err)
	resp, err = modifier.Apply(resp)
	a.Nil(err)
	s, err = DecodeSchema(resp, schema)
	a.Nil(err)
	dc = NewDecodeCheck()
	dc.Traverse(s)
	expected = []string{
		"throttle_time_ms int32 1",
		"[brokers]",
		"brokers struct",
		"node_id int32 44031",
		"host string myhost1", // replaced
		"port int32 34001",    // replaced
		"rack *string ",
		"brokers struct",
		"node_id int32 66051",
		"host string myhost2", // replaced
		"port int32 34002",    // replaced
		"rack *string euwest1",
		"brokers struct",
		"node_id int32 2",
		"host string myhost3", // replaced
		"port int32 34003",    // replaced
		"rack *string <nil>",
		"cluster_id *string mykafka",
		"controller_id int32 57778",
		"[topic_metadata]",
		"topic_metadata struct",
		"error_code int16 0",
		"name string foo",
		"is_internal bool true",
		"[partition_metadata]",
		"partition_metadata struct",
		"error_code int16 4",
		"partition int32 1",
		"leader int32 7",
		"leader_epoch int32 8",
		"[replicas]",
		"replicas int32 1",
		"replicas int32 2",
		"replicas int32 3",
		"[isr]",
		"isr int32 3",
		"isr int32 2",
		"[offline_replicas]",
		"offline_replicas int32 5",
		"offline_replicas int32 6",
		"offline_replicas int32 7",
		"topic_authorized_operations int32 8",
		"topic_metadata struct",
		"error_code int16 0",
		"name string bar",
		"is_internal bool false",
		"[partition_metadata]",
		"topic_authorized_operations int32 4",
		"cluster_authorized_operations int32 5",
	}
	a.Equal(expected, dc.AttrValues())
}

func TestMetadataResponseV9(t *testing.T) {
	apiVersion := int16(9)

	bytes, err := hex.DecodeString("0000000004000000020a6c6f63616c686f7374000071a40000000000030a6c6f63616c686f7374000098b40000000000010a6c6f63616c686f737400004a9400001763754b7373754b3052726d4950586164374259426b670000000202000010746573742d6e6f2d686561646572730002000000000000000000030000000002000000030200000003010000000000000000000000")
	if err != nil {
		t.Fatal(err)
	}
	a := assert.New(t)

	schema := metadataResponseSchemaVersions[apiVersion]

	s, err := DecodeSchema(bytes, schema)
	a.Nil(err)

	dc := NewDecodeCheck()
	dc.Traverse(s)

	expected := []string{
		"throttle_time_ms int32 0",
		"[brokers]",
		"brokers struct",
		"node_id int32 2",
		"host string localhost",
		"port int32 29092",
		"rack *string <nil>",
		"brokers struct",
		"node_id int32 3",
		"host string localhost",
		"port int32 39092",
		"rack *string <nil>",
		"brokers struct",
		"node_id int32 1",
		"host string localhost",
		"port int32 19092",
		"rack *string <nil>",
		"cluster_id *string cuKssuK0RrmIPXad7BYBkg",
		"controller_id int32 2",
		"[topic_metadata]",
		"topic_metadata struct",
		"error_code int16 0",
		"name string test-no-headers",
		"is_internal bool false",
		"[partition_metadata]",
		"partition_metadata struct",
		"error_code int16 0",
		"partition int32 0",
		"leader int32 3",
		"leader_epoch int32 0",
		"[replicas]",
		"replicas int32 3",
		"[isr]",
		"isr int32 3",
		"[offline_replicas]",
		"topic_authorized_operations int32 0",
		"cluster_authorized_operations int32 0"}

	a.Equal(expected, dc.AttrValues())
	resp, err := EncodeSchema(s, schema)
	a.Nil(err)
	a.Equal(bytes, resp)

	modifier, err := GetResponseModifier(apiKeyMetadata, apiVersion, testResponseModifier2)
	if err != nil {
		t.Fatal(err)
	}
	a.Nil(err)
	resp, err = modifier.Apply(resp)
	a.Nil(err)
	s, err = DecodeSchema(resp, schema)
	a.Nil(err)
	dc = NewDecodeCheck()
	dc.Traverse(s)

	expected = []string{
		"throttle_time_ms int32 0",
		"[brokers]",
		"brokers struct",
		"node_id int32 2",
		"host string myhost2", // replaced
		"port int32 34002",    // replaced
		"rack *string <nil>",
		"brokers struct",
		"node_id int32 3",
		"host string myhost3", // replaced
		"port int32 34003",    // replaced
		"rack *string <nil>",
		"brokers struct",
		"node_id int32 1",
		"host string myhost1", // replaced
		"port int32 34001",    // replaced
		"rack *string <nil>",
		"cluster_id *string cuKssuK0RrmIPXad7BYBkg",
		"controller_id int32 2",
		"[topic_metadata]",
		"topic_metadata struct",
		"error_code int16 0",
		"name string test-no-headers",
		"is_internal bool false",
		"[partition_metadata]",
		"partition_metadata struct",
		"error_code int16 0",
		"partition int32 0",
		"leader int32 3",
		"leader_epoch int32 0",
		"[replicas]",
		"replicas int32 3",
		"[isr]",
		"isr int32 3",
		"[offline_replicas]",
		"topic_authorized_operations int32 0",
		"cluster_authorized_operations int32 0"}

	a.Equal(expected, dc.AttrValues())
}

func TestFindCoordinatorResponseV0(t *testing.T) {
	/*
	   FindCoordinator Response (Version: 0) => error_code coordinator
	     error_code => INT16
	     coordinator => node_id host port
	       node_id => INT32
	       host => STRING
	       port => INT32
	*/
	apiVersion := int16(0)

	bytes := []byte{
		0x00, 0x00,
		// coordinator
		0x00, 0x00, 0x00, 0xAB,
		0x00, 0x09, 'l', 'o', 'c', 'a', 'l', 'h', 'o', 's', 't',
		0x00, 0x00, 0x00, 0x33, // 51
	}
	a := assert.New(t)

	schema := findCoordinatorResponseSchemaVersions[apiVersion]

	s, err := DecodeSchema(bytes, schema)
	a.Nil(err)
	dc := NewDecodeCheck()
	dc.Traverse(s)

	expected := []string{
		"error_code int16 0",
		"coordinator struct",
		"node_id int32 171",
		"host string localhost",
		"port int32 51",
	}
	a.Equal(expected, dc.AttrValues())
	resp, err := EncodeSchema(s, schema)
	a.Nil(err)
	a.Equal(bytes, resp)

	modifier, err := GetResponseModifier(apiKeyFindCoordinator, apiVersion, testResponseModifier)
	a.Nil(err)
	resp, err = modifier.Apply(resp)
	a.Nil(err)
	s, err = DecodeSchema(resp, schema)
	a.Nil(err)
	dc = NewDecodeCheck()
	dc.Traverse(s)
	expected = []string{
		"error_code int16 0",
		"coordinator struct",
		"node_id int32 171",
		"host string myhost1", // replaced
		"port int32 34001",    // replaced
	}
	a.Equal(expected, dc.AttrValues())
}

func TestFindCoordinatorResponseV1(t *testing.T) {
	/*
	   FindCoordinator Response (Version: 1) => throttle_time_ms error_code error_message coordinator
	     throttle_time_ms => INT32
	     error_code => INT16
	     error_message => NULLABLE_STRING
	     coordinator => node_id host port
	       node_id => INT32
	       host => STRING
	       port => INT32
	*/
	apiVersion := int16(1)

	bytes := []byte{
		// throttle_time_ms
		0x00, 0x00, 0x00, 0x01, // 1
		0x00, 0x00,
		0xff, 0xff,
		// coordinator
		0x00, 0x00, 0x00, 0xAB,
		0x00, 0x09, 'l', 'o', 'c', 'a', 'l', 'h', 'o', 's', 't',
		0x00, 0x00, 0x00, 0x33, // 51
	}
	a := assert.New(t)

	schema := findCoordinatorResponseSchemaVersions[apiVersion]

	s, err := DecodeSchema(bytes, schema)
	a.Nil(err)
	dc := NewDecodeCheck()
	dc.Traverse(s)

	expected := []string{
		"throttle_time_ms int32 1",
		"error_code int16 0",
		"error_message *string <nil>",
		"coordinator struct",
		"node_id int32 171",
		"host string localhost",
		"port int32 51",
	}
	a.Equal(expected, dc.AttrValues())
	resp, err := EncodeSchema(s, schema)
	a.Nil(err)
	a.Equal(bytes, resp)

	modifier, err := GetResponseModifier(apiKeyFindCoordinator, apiVersion, testResponseModifier)
	a.Nil(err)
	resp, err = modifier.Apply(resp)
	a.Nil(err)
	s, err = DecodeSchema(resp, schema)
	a.Nil(err)
	dc = NewDecodeCheck()
	dc.Traverse(s)
	expected = []string{
		"throttle_time_ms int32 1",
		"error_code int16 0",
		"error_message *string <nil>",
		"coordinator struct",
		"node_id int32 171",
		"host string myhost1", // replaced
		"port int32 34001",    // replaced
	}
	a.Equal(expected, dc.AttrValues())
}

func TestFindCoordinatorResponseV2(t *testing.T) {
	/*
	   FindCoordinator Response (Version: 2) => throttle_time_ms error_code error_message coordinator
	     throttle_time_ms => INT32
	     error_code => INT16
	     error_message => NULLABLE_STRING
	     coordinator => node_id host port
	       node_id => INT32
	       host => STRING
	       port => INT32
	*/
	apiVersion := int16(2)

	bytes := []byte{
		// throttle_time_ms
		0x00, 0x00, 0x00, 0x01, // 1
		0x00, 0x00,
		0xff, 0xff,
		// coordinator
		0x00, 0x00, 0x00, 0xAB,
		0x00, 0x09, 'l', 'o', 'c', 'a', 'l', 'h', 'o', 's', 't',
		0x00, 0x00, 0x00, 0x33, // 51
	}
	a := assert.New(t)

	schema := findCoordinatorResponseSchemaVersions[apiVersion]

	s, err := DecodeSchema(bytes, schema)
	a.Nil(err)
	dc := NewDecodeCheck()
	dc.Traverse(s)

	expected := []string{
		"throttle_time_ms int32 1",
		"error_code int16 0",
		"error_message *string <nil>",
		"coordinator struct",
		"node_id int32 171",
		"host string localhost",
		"port int32 51",
	}
	a.Equal(expected, dc.AttrValues())
	resp, err := EncodeSchema(s, schema)
	a.Nil(err)
	a.Equal(bytes, resp)

	modifier, err := GetResponseModifier(apiKeyFindCoordinator, apiVersion, testResponseModifier)
	a.Nil(err)
	resp, err = modifier.Apply(resp)
	a.Nil(err)
	s, err = DecodeSchema(resp, schema)
	a.Nil(err)
	dc = NewDecodeCheck()
	dc.Traverse(s)
	expected = []string{
		"throttle_time_ms int32 1",
		"error_code int16 0",
		"error_message *string <nil>",
		"coordinator struct",
		"node_id int32 171",
		"host string myhost1", // replaced
		"port int32 34001",    // replaced
	}
	a.Equal(expected, dc.AttrValues())
}

func TestFindCoordinatorResponseV3(t *testing.T) {
	apiVersion := int16(3)

	// "00000000000f2254686520636f6f7264696e61746f72206973206e6f7420617661696c61626c652effffffff01ffffffff00"
	bytes, err := hex.DecodeString("000000000000054e4f4e45000000030a6c6f63616c686f7374000098b400")
	if err != nil {
		t.Fatal(err)
	}
	a := assert.New(t)

	schema := findCoordinatorResponseSchemaVersions[apiVersion]

	s, err := DecodeSchema(bytes, schema)
	a.Nil(err)
	dc := NewDecodeCheck()
	dc.Traverse(s)

	expected := []string{
		"throttle_time_ms int32 0",
		"error_code int16 0",
		"error_message *string NONE",
		"coordinator struct",
		"node_id int32 3",
		"host string localhost",
		"port int32 39092",
	}
	a.Equal(expected, dc.AttrValues())
	resp, err := EncodeSchema(s, schema)
	a.Nil(err)
	a.Equal(bytes, resp)

	modifier, err := GetResponseModifier(apiKeyFindCoordinator, apiVersion, testResponseModifier2)
	a.Nil(err)
	resp, err = modifier.Apply(resp)
	a.Nil(err)
	s, err = DecodeSchema(resp, schema)
	a.Nil(err)
	dc = NewDecodeCheck()
	dc.Traverse(s)
	expected = []string{
		"throttle_time_ms int32 0",
		"error_code int16 0",
		"error_message *string NONE",
		"coordinator struct",
		"node_id int32 3",
		"host string myhost3", // replaced
		"port int32 34003",    // replaced
	}
	a.Equal(expected, dc.AttrValues())
}

type decodeCheck struct {
	attrValues []string
}

func NewDecodeCheck() *decodeCheck {
	return &decodeCheck{attrValues: make([]string, 0)}
}

func (t *decodeCheck) Traverse(s *Struct) error {
	for i, _ := range s.schema.fields {
		arg := s.values[i]
		if err := t.value(s, arg, i); err != nil {
			return err
		}
	}
	return nil
}

func (t *decodeCheck) value(s *Struct, arg interface{}, sindex int) error {
	name := s.schema.fields[sindex].def.GetName()

	switch v := arg.(type) {
	case bool:
		t.append(name, "bool", v)
	case int16:
		t.append(name, "int16", v)
	case int32:
		t.append(name, "int32", v)
	case string:
		t.append(name, "string", v)
	case *string:
		if v != nil {
			t.append(name, "*string", *v)
		} else {
			t.append(name, "*string", nil)
		}
	case *Struct:
		t.append(name, "struct")
		t.Traverse(v)
	case []interface{}:
		t.append(fmt.Sprintf("[%s]", name))
		for _, v := range v {
			if err := t.value(s, v, sindex); err != nil {
				return nil
			}
		}
	default:
		return fmt.Errorf("unknow type for value %v", arg)
	}
	return nil
}

func (t *decodeCheck) append(vs ...interface{}) {
	ss := make([]string, 0)
	for _, v := range vs {
		ss = append(ss, fmt.Sprint(v))
	}
	t.attrValues = append(t.attrValues, strings.Join(ss, " "))
}

func (t *decodeCheck) AttrValues() []string {
	return t.attrValues
}
func (t *decodeCheck) Dump() {
	for _, v := range t.attrValues {
		fmt.Printf("\"%s\",\n", v)
	}
}
