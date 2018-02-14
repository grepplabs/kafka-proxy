package protocol

import (
	"errors"
	"fmt"
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

	partitionMetadataV1 := partitionMetadataV0

	partitionMetadataV2 := NewSchema("partition_metadata_v2",
		&field{name: "error_code", ty: typeInt16},
		&field{name: "partition", ty: typeInt32},
		&field{name: "leader", ty: typeInt32},
		&array{name: "replicas", ty: typeInt32},
		&array{name: "isr", ty: typeInt32},
		&array{name: "offline_replicas", ty: typeInt32},
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
	return []Schema{metadataResponseV0, metadataResponseV1, metadataResponseV2, metadataResponseV3, metadataResponseV4, metadataResponseV5}
}

func createFindCoordinatorResponseSchemaVersions() []Schema {
	findCoordinatorBrokerV0 := NewSchema("find_coordinator_broker_v0",
		&field{name: "node_id", ty: typeInt32},
		&field{name: hostKeyName, ty: typeStr},
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
	return []Schema{findCoordinatorResponseV0, findCoordinatorResponseV1}
}

type NetAddressMappingFunc func(brokerHost string, brokerPort int32) (listenHost string, listenPort int32, err error)

func modifyMetadataResponse(decodedStruct *Struct, fn NetAddressMappingFunc) error {
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

func modifyFindCoordinatorResponse(decodedStruct *Struct, fn NetAddressMappingFunc) error {
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

type modifyResponseFunc func(decodedStruct *Struct, fn NetAddressMappingFunc) error

type responseModifier struct {
	schema                Schema
	modifyResponseFunc    modifyResponseFunc
	netAddressMappingFunc NetAddressMappingFunc
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

func GetResponseModifier(apiKey int16, apiVersion int16, addressMappingFunc NetAddressMappingFunc) (ResponseModifier, error) {
	switch apiKey {
	case apiKeyMetadata:
		return newResponseModifier(apiKey, apiVersion, addressMappingFunc, metadataResponseSchemaVersions, modifyMetadataResponse)
	case apiKeyFindCoordinator:
		return newResponseModifier(apiKey, apiVersion, addressMappingFunc, findCoordinatorResponseSchemaVersions, modifyFindCoordinatorResponse)
	default:
		return nil, nil
	}
}

func newResponseModifier(apiKey int16, apiVersion int16, netAddressMappingFunc NetAddressMappingFunc, schemas []Schema, modifyResponseFunc modifyResponseFunc) (ResponseModifier, error) {
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
