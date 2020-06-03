package protocol

import "fmt"

type TopicMetadataRequestFactory struct{}

func (f *TopicMetadataRequestFactory) Produce(requestKeyVersion *RequestKeyVersion) (req ProtocolBody, err error) {
	switch requestKeyVersion.ApiVersion {
	case 0:
		return &TopicMetadataRequestV0{}, nil
	case 1:
		return &TopicMetadataRequestV1{}, nil
	case 2:
		return &TopicMetadataRequestV2{}, nil
	case 3:
		return &TopicMetadataRequestV3{}, nil
	case 4:
		return &TopicMetadataRequestV4{}, nil
	case 5:
		return &TopicMetadataRequestV5{}, nil
	case 6:
		return &TopicMetadataRequestV6{}, nil
	case 7:
		return &TopicMetadataRequestV7{}, nil
	case 8:
		return &TopicMetadataRequestV8{}, nil
	default:
		return nil, fmt.Errorf("Not supported topic metadata request %d", requestKeyVersion.ApiVersion)
	}
}

type TopicMetadataRequestV0 struct {
	Topics []string
}

func (r *TopicMetadataRequestV0) encode(pe packetEncoder) error {
	return nil
}

func (r *TopicMetadataRequestV0) key() int16 {
	return 3
}

func (r *TopicMetadataRequestV0) version() int16 {
	return 0
}

func (r *TopicMetadataRequestV0) decode(pd packetDecoder) (err error) {
	r.Topics, err = pd.getStringArray()
	if err != nil {
		return err
	}

	return err
}

func (r *TopicMetadataRequestV0) GetTopics() []string {
	return r.Topics
}

type TopicMetadataRequestV1 struct {
	Topics []string
}

func (r *TopicMetadataRequestV1) encode(pe packetEncoder) error {
	return nil
}

func (r *TopicMetadataRequestV1) key() int16 {
	return 3
}

func (r *TopicMetadataRequestV1) version() int16 {
	return 1
}

func (r *TopicMetadataRequestV1) decode(pd packetDecoder) (err error) {
	r.Topics, err = pd.getStringArray()
	if err != nil {
		return err
	}

	return err
}

func (r *TopicMetadataRequestV1) GetTopics() []string {
	return r.Topics
}

type TopicMetadataRequestV2 struct {
	Topics []string
}

func (r *TopicMetadataRequestV2) encode(pe packetEncoder) error {
	return nil
}

func (r *TopicMetadataRequestV2) key() int16 {
	return 3
}

func (r *TopicMetadataRequestV2) version() int16 {
	return 2
}

func (r *TopicMetadataRequestV2) decode(pd packetDecoder) (err error) {
	r.Topics, err = pd.getStringArray()
	if err != nil {
		return err
	}

	return err
}

func (r *TopicMetadataRequestV2) GetTopics() []string {
	return r.Topics
}

type TopicMetadataRequestV3 struct {
	Topics []string
}

func (r *TopicMetadataRequestV3) encode(pe packetEncoder) error {
	return nil
}

func (r *TopicMetadataRequestV3) key() int16 {
	return 3
}

func (r *TopicMetadataRequestV3) version() int16 {
	return 3
}

func (r *TopicMetadataRequestV3) decode(pd packetDecoder) (err error) {
	r.Topics, err = pd.getStringArray()
	if err != nil {
		return err
	}

	return err
}

func (r *TopicMetadataRequestV3) GetTopics() []string {
	return r.Topics
}

type TopicMetadataRequestV4 struct {
	Topics []string
}

func (r *TopicMetadataRequestV4) encode(pe packetEncoder) error {
	return nil
}

func (r *TopicMetadataRequestV4) key() int16 {
	return 3
}

func (r *TopicMetadataRequestV4) version() int16 {
	return 4
}

func (r *TopicMetadataRequestV4) decode(pd packetDecoder) (err error) {
	r.Topics, err = pd.getStringArray()
	if err != nil {
		return err
	}
	// allow_auto_topic_creation
	_, err = pd.getBool()
	if err != nil {
		return err
	}

	return err
}

func (r *TopicMetadataRequestV4) GetTopics() []string {
	return r.Topics
}

type TopicMetadataRequestV5 struct {
	Topics []string
}

func (r *TopicMetadataRequestV5) encode(pe packetEncoder) error {
	return nil
}

func (r *TopicMetadataRequestV5) key() int16 {
	return 3
}

func (r *TopicMetadataRequestV5) version() int16 {
	return 5
}

func (r *TopicMetadataRequestV5) decode(pd packetDecoder) (err error) {
	r.Topics, err = pd.getStringArray()
	if err != nil {
		return err
	}
	// allow_auto_topic_creation
	_, err = pd.getBool()
	if err != nil {
		return err
	}

	return err
}

func (r *TopicMetadataRequestV5) GetTopics() []string {
	return r.Topics
}

type TopicMetadataRequestV6 struct {
	Topics []string
}

func (r *TopicMetadataRequestV6) encode(pe packetEncoder) error {
	return nil
}

func (r *TopicMetadataRequestV6) key() int16 {
	return 3
}

func (r *TopicMetadataRequestV6) version() int16 {
	return 6
}

func (r *TopicMetadataRequestV6) decode(pd packetDecoder) (err error) {
	r.Topics, err = pd.getStringArray()
	if err != nil {
		return err
	}
	// allow_auto_topic_creation
	_, err = pd.getBool()
	if err != nil {
		return err
	}

	return err
}

func (r *TopicMetadataRequestV6) GetTopics() []string {
	return r.Topics
}

type TopicMetadataRequestV7 struct {
	Topics []string
}

func (r *TopicMetadataRequestV7) encode(pe packetEncoder) error {
	return nil
}

func (r *TopicMetadataRequestV7) key() int16 {
	return 3
}

func (r *TopicMetadataRequestV7) version() int16 {
	return 7
}

func (r *TopicMetadataRequestV7) decode(pd packetDecoder) (err error) {
	r.Topics, err = pd.getStringArray()
	if err != nil {
		return err
	}
	// allow_auto_topic_creation
	_, err = pd.getBool()
	if err != nil {
		return err
	}

	return err
}

func (r *TopicMetadataRequestV7) GetTopics() []string {
	return r.Topics
}

type TopicMetadataRequestV8 struct {
	Topics []string
}

func (r *TopicMetadataRequestV8) encode(pe packetEncoder) error {
	return nil
}

func (r *TopicMetadataRequestV8) key() int16 {
	return 3
}

func (r *TopicMetadataRequestV8) version() int16 {
	return 8
}

func (r *TopicMetadataRequestV8) decode(pd packetDecoder) (err error) {
	r.Topics, err = pd.getStringArray()
	if err != nil {
		return err
	}
	// allow_auto_topic_creation
	_, err = pd.getBool()
	if err != nil {
		return err
	}
	// include_cluster_authorized_operations
	_, err = pd.getBool()
	if err != nil {
		return err
	}
	// include_topic_authorized_operations
	_, err = pd.getBool()
	if err != nil {
		return err
	}

	return err
}

func (r *TopicMetadataRequestV8) GetTopics() []string {
	return r.Topics
}
