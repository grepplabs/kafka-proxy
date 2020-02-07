package protocol

import (
	"errors"
	"fmt"
	"github.com/grepplabs/kafka-proxy/config"
)

const (
	apiKeyMetadata        = 3
	apiKeyFindCoordinator = 10

	brokersKeyName = "brokers"
	hostKeyName    = "host"
	portKeyName    = "port"

	coordinatorKeyName = "coordinator"
)

var (
	metadataResponseSchemaVersions        = createMetadataResponseSchemaVersions()
	findCoordinatorResponseSchemaVersions = createFindCoordinatorResponseSchemaVersions()
)

func createMetadataResponseSchemaVersions() []Schema {
	metadataBrokerV0 := NewSchema("metadata_broker_v0",
		&field{name: "node_id", ty: typeInt32},
		&field{name: hostKeyName, ty: typeStr},
		&field{name: portKeyName, ty: typeInt32},
	)

	partitionMetadataV0 := NewSchema("partition_metadata_v0",
		&field{name: "error_code", ty: typeInt16},
		&field{name: "partition", ty: typeInt32},
		&field{name: "leader", ty: typeInt32},
		&array{name: "replicas", ty: typeInt32},
		&array{name: "isr", ty: typeInt32},
	)

	topicMetadataV0 := NewSchema("topic_metadata_v0",
		&field{name: "error_code", ty: typeInt16},
		&field{name: "topic", ty: typeStr},
		&array{name: "partition_metadata", ty: partitionMetadataV0},
	)

	metadataResponseV0 := NewSchema("metadata_response_v0",
		&array{name: brokersKeyName, ty: metadataBrokerV0},
		&array{name: "topic_metadata", ty: topicMetadataV0},
	)

	metadataBrokerV1 := NewSchema("metadata_broker_v1",
		&field{name: "node_id", ty: typeInt32},
		&field{name: hostKeyName, ty: typeStr},
		&field{name: portKeyName, ty: typeInt32},
		&field{name: "rack", ty: typeNullableStr},
	)

	metadataBrokerSchema9 := NewSchema("metadata_broker_schema9",
		&field{name: "node_id", ty: typeInt32},
		&field{name: hostKeyName, ty: typeCompactStr},
		&field{name: portKeyName, ty: typeInt32},
		&field{name: "rack", ty: typeCompactNullableStr},
		&taggedFields{"broker_tagged_fields"},
	)

	partitionMetadataV1 := partitionMetadataV0

	partitionMetadataV2 := NewSchema("partition_metadata_v2",
		&field{name: "error_code", ty: typeInt16},
		&field{name: "partition", ty: typeInt32},
		&field{name: "leader", ty: typeInt32},
		&array{name: "replicas", ty: typeInt32},
		&array{name: "isr", ty: typeInt32},
		&array{name: "offline_replicas", ty: typeInt32},
	)

	partitionMetadataV7 := NewSchema("partition_metadata_v7",
		&field{name: "error_code", ty: typeInt16},
		&field{name: "partition", ty: typeInt32},
		&field{name: "leader", ty: typeInt32},
		&field{name: "leader_epoch", ty: typeInt32},
		&array{name: "replicas", ty: typeInt32},
		&array{name: "isr", ty: typeInt32},
		&array{name: "offline_replicas", ty: typeInt32},
	)

	partitionMetadataSchema9 := NewSchema("partition_metadata_schema9",
		&field{name: "error_code", ty: typeInt16},
		&field{name: "partition", ty: typeInt32},
		&field{name: "leader", ty: typeInt32},
		&field{name: "leader_epoch", ty: typeInt32},
		&compactArray{name: "replicas", ty: typeInt32},
		&compactArray{name: "isr", ty: typeInt32},
		&compactArray{name: "offline_replicas", ty: typeInt32},
		&taggedFields{name: "partition_metadata_tagged_fields"},
	)

	topicMetadataV1 := NewSchema("topic_metadata_v1",
		&field{name: "error_code", ty: typeInt16},
		&field{name: "topic", ty: typeStr},
		&field{name: "is_internal", ty: typeBool},
		&array{name: "partition_metadata", ty: partitionMetadataV1},
	)

	topicMetadataV2 := NewSchema("topic_metadata_v2",
		&field{name: "error_code", ty: typeInt16},
		&field{name: "topic", ty: typeStr},
		&field{name: "is_internal", ty: typeBool},
		&array{name: "partition_metadata", ty: partitionMetadataV2},
	)

	topicMetadataV7 := NewSchema("topic_metadata_v7",
		&field{name: "error_code", ty: typeInt16},
		&field{name: "topic", ty: typeStr},
		&field{name: "is_internal", ty: typeBool},
		&array{name: "partition_metadata", ty: partitionMetadataV7},
	)

	topicMetadataV8 := NewSchema("topic_metadata_v8",
		&field{name: "error_code", ty: typeInt16},
		&field{name: "name", ty: typeStr},
		&field{name: "is_internal", ty: typeBool},
		&array{name: "partition_metadata", ty: partitionMetadataV7},
		&field{name: "topic_authorized_operations", ty: typeInt32},
	)

	topicMetadataSchema9 := NewSchema("topic_metadata_schema9",
		&field{name: "error_code", ty: typeInt16},
		&field{name: "name", ty: typeCompactStr},
		&field{name: "is_internal", ty: typeBool},
		&compactArray{name: "partition_metadata", ty: partitionMetadataSchema9},
		&field{name: "topic_authorized_operations", ty: typeInt32},
		&taggedFields{name: "topic_metadata_tagged_fields"},
	)

	metadataResponseV1 := NewSchema("metadata_response_v1",
		&array{name: brokersKeyName, ty: metadataBrokerV1},
		&field{name: "controller_id", ty: typeInt32},
		&array{name: "topic_metadata", ty: topicMetadataV1},
	)

	metadataResponseV2 := NewSchema("metadata_response_v2",
		&array{name: brokersKeyName, ty: metadataBrokerV1},
		&field{name: "cluster_id", ty: typeNullableStr},
		&field{name: "controller_id", ty: typeInt32},
		&array{name: "topic_metadata", ty: topicMetadataV1},
	)

	metadataResponseV3 := NewSchema("metadata_response_v3",
		&field{name: "throttle_time_ms", ty: typeInt32},
		&array{name: brokersKeyName, ty: metadataBrokerV1},
		&field{name: "cluster_id", ty: typeNullableStr},
		&field{name: "controller_id", ty: typeInt32},
		&array{name: "topic_metadata", ty: topicMetadataV1},
	)

	metadataResponseV4 := metadataResponseV3

	metadataResponseV5 := NewSchema("metadata_response_v5",
		&field{name: "throttle_time_ms", ty: typeInt32},
		&array{name: brokersKeyName, ty: metadataBrokerV1},
		&field{name: "cluster_id", ty: typeNullableStr},
		&field{name: "controller_id", ty: typeInt32},
		&array{name: "topic_metadata", ty: topicMetadataV2},
	)

	metadataResponseV6 := metadataResponseV5

	metadataResponseV7 := NewSchema("metadata_response_v7",
		&field{name: "throttle_time_ms", ty: typeInt32},
		&array{name: brokersKeyName, ty: metadataBrokerV1},
		&field{name: "cluster_id", ty: typeNullableStr},
		&field{name: "controller_id", ty: typeInt32},
		&array{name: "topic_metadata", ty: topicMetadataV7},
	)

	metadataResponseV8 := NewSchema("metadata_response_v8",
		&field{name: "throttle_time_ms", ty: typeInt32},
		&array{name: brokersKeyName, ty: metadataBrokerV1},
		&field{name: "cluster_id", ty: typeNullableStr},
		&field{name: "controller_id", ty: typeInt32},
		&array{name: "topic_metadata", ty: topicMetadataV8},
		&field{name: "cluster_authorized_operations", ty: typeInt32},
	)

	metadataResponseV9 := NewSchema("metadata_response_v9",
		&field{name: "throttle_time_ms", ty: typeInt32},
		&compactArray{name: brokersKeyName, ty: metadataBrokerSchema9},
		&field{name: "cluster_id", ty: typeCompactNullableStr},
		&field{name: "controller_id", ty: typeInt32},
		&compactArray{name: "topic_metadata", ty: topicMetadataSchema9},
		&field{name: "cluster_authorized_operations", ty: typeInt32},
		&taggedFields{name: "response_tagged_fields"},
	)

	return []Schema{metadataResponseV0, metadataResponseV1, metadataResponseV2, metadataResponseV3, metadataResponseV4, metadataResponseV5, metadataResponseV6, metadataResponseV7, metadataResponseV8, metadataResponseV9}
}

func createFindCoordinatorResponseSchemaVersions() []Schema {
	findCoordinatorBrokerV0 := NewSchema("find_coordinator_broker_v0",
		&field{name: "node_id", ty: typeInt32},
		&field{name: hostKeyName, ty: typeStr},
		&field{name: portKeyName, ty: typeInt32},
	)

	findCoordinatorBrokerSchema9 := NewSchema("find_coordinator_broker_schema9",
		&field{name: "node_id", ty: typeInt32},
		&field{name: hostKeyName, ty: typeCompactStr},
		&field{name: portKeyName, ty: typeInt32},
	)

	findCoordinatorResponseV0 := NewSchema("find_coordinator_response_v0",
		&field{name: "error_code", ty: typeInt16},
		&field{name: coordinatorKeyName, ty: findCoordinatorBrokerV0},
	)

	findCoordinatorResponseV1 := NewSchema("find_coordinator_response_v1",
		&field{name: "throttle_time_ms", ty: typeInt32},
		&field{name: "error_code", ty: typeInt16},
		&field{name: "error_message", ty: typeNullableStr},
		&field{name: coordinatorKeyName, ty: findCoordinatorBrokerV0},
	)

	findCoordinatorResponseV2 := findCoordinatorResponseV1

	findCoordinatorResponseV3 := NewSchema("find_coordinator_response_v3",
		&field{name: "throttle_time_ms", ty: typeInt32},
		&field{name: "error_code", ty: typeInt16},
		&field{name: "error_message", ty: typeCompactNullableStr},
		&field{name: coordinatorKeyName, ty: findCoordinatorBrokerSchema9},
		&taggedFields{name: "response_tagged_fields"},
	)

	return []Schema{findCoordinatorResponseV0, findCoordinatorResponseV1, findCoordinatorResponseV2, findCoordinatorResponseV3}
}

func modifyMetadataResponse(decodedStruct *Struct, fn config.NetAddressMappingFunc) error {
	if decodedStruct == nil {
		return errors.New("decoded struct must not be nil")
	}
	if fn == nil {
		return errors.New("net address mapper must not be nil")
	}
	brokersArray, ok := decodedStruct.Get(brokersKeyName).([]interface{})
	if !ok {
		return errors.New("brokers list not found")
	}
	for _, brokerElement := range brokersArray {
		broker := brokerElement.(*Struct)
		host, ok := broker.Get(hostKeyName).(string)
		if !ok {
			return errors.New("broker.host not found")
		}
		port, ok := broker.Get(portKeyName).(int32)
		if !ok {
			return errors.New("broker.port not found")
		}

		if host == "" && port <= 0 {
			continue
		}

		newHost, newPort, err := fn(host, port)
		if err != nil {
			return err
		}
		if host != newHost {
			err := broker.Replace(hostKeyName, newHost)
			if err != nil {
				return err
			}
		}
		if port != newPort {
			err = broker.Replace(portKeyName, int32(newPort))
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func modifyFindCoordinatorResponse(decodedStruct *Struct, fn config.NetAddressMappingFunc) error {
	if decodedStruct == nil {
		return errors.New("decoded struct must not be nil")
	}
	if fn == nil {
		return errors.New("net address mapper must not be nil")
	}
	coordinator, ok := decodedStruct.Get(coordinatorKeyName).(*Struct)
	if !ok {
		return errors.New("coordinator not found")
	}
	host, ok := coordinator.Get(hostKeyName).(string)
	if !ok {
		return errors.New("coordinator.host not found")
	}
	port, ok := coordinator.Get(portKeyName).(int32)
	if !ok {
		return errors.New("coordinator.port not found")
	}

	if host == "" && port <= 0 {
		return nil
	}

	newHost, newPort, err := fn(host, port)
	if err != nil {
		return err
	}
	if host != newHost {
		err := coordinator.Replace(hostKeyName, newHost)
		if err != nil {
			return err
		}
	}
	if port != newPort {
		err = coordinator.Replace(portKeyName, int32(newPort))
		if err != nil {
			return err
		}
	}
	return nil
}

type ResponseModifier interface {
	Apply(resp []byte) ([]byte, error)
}

type modifyResponseFunc func(decodedStruct *Struct, fn config.NetAddressMappingFunc) error

type responseModifier struct {
	schema                Schema
	modifyResponseFunc    modifyResponseFunc
	netAddressMappingFunc config.NetAddressMappingFunc
}

func (f *responseModifier) Apply(resp []byte) ([]byte, error) {
	decodedStruct, err := DecodeSchema(resp, f.schema)
	if err != nil {
		return nil, err
	}
	err = f.modifyResponseFunc(decodedStruct, f.netAddressMappingFunc)
	if err != nil {
		return nil, err
	}
	return EncodeSchema(decodedStruct, f.schema)
}

func GetResponseModifier(apiKey int16, apiVersion int16, addressMappingFunc config.NetAddressMappingFunc) (ResponseModifier, error) {
	switch apiKey {
	case apiKeyMetadata:
		return newResponseModifier(apiKey, apiVersion, addressMappingFunc, metadataResponseSchemaVersions, modifyMetadataResponse)
	case apiKeyFindCoordinator:
		return newResponseModifier(apiKey, apiVersion, addressMappingFunc, findCoordinatorResponseSchemaVersions, modifyFindCoordinatorResponse)
	default:
		return nil, nil
	}
}

func newResponseModifier(apiKey int16, apiVersion int16, netAddressMappingFunc config.NetAddressMappingFunc, schemas []Schema, modifyResponseFunc modifyResponseFunc) (ResponseModifier, error) {
	schema, err := getResponseSchema(apiKey, apiVersion, schemas)
	if err != nil {
		return nil, err
	}
	return &responseModifier{
		schema:                schema,
		modifyResponseFunc:    modifyResponseFunc,
		netAddressMappingFunc: netAddressMappingFunc,
	}, nil
}

func getResponseSchema(apiKey, apiVersion int16, schemas []Schema) (Schema, error) {
	if apiVersion < 0 || int(apiVersion) >= len(schemas) {
		return nil, fmt.Errorf("Unsupported response schema version %d for key %d ", apiVersion, apiKey)
	}
	return schemas[apiVersion], nil
}
