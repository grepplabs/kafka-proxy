// Code generated by protoc-gen-go. DO NOT EDIT.
// source: authz-provider.proto

package proto

import (
	context "context"
	fmt "fmt"
	proto "github.com/golang/protobuf/proto"
	grpc "google.golang.org/grpc"
	math "math"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion3 // please upgrade the proto package

type AuthzRequest struct {
	Apikey               int32    `protobuf:"varint,1,opt,name=apikey,proto3" json:"apikey,omitempty"`
	Apiversion           int32    `protobuf:"varint,2,opt,name=apiversion,proto3" json:"apiversion,omitempty"`
	UserInfo             string   `protobuf:"bytes,3,opt,name=user_info,json=userInfo,proto3" json:"user_info,omitempty"`
	SrcIp                string   `protobuf:"bytes,4,opt,name=src_ip,json=srcIp,proto3" json:"src_ip,omitempty"`
	DstIp                string   `protobuf:"bytes,5,opt,name=dst_ip,json=dstIp,proto3" json:"dst_ip,omitempty"`
	Topics               string   `protobuf:"bytes,6,opt,name=topics,proto3" json:"topics,omitempty"`
	ClientId             string   `protobuf:"bytes,7,opt,name=client_id,json=clientId,proto3" json:"client_id,omitempty"`
	ConsumerGroups       string   `protobuf:"bytes,8,opt,name=consumer_groups,json=consumerGroups,proto3" json:"consumer_groups,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *AuthzRequest) Reset()         { *m = AuthzRequest{} }
func (m *AuthzRequest) String() string { return proto.CompactTextString(m) }
func (*AuthzRequest) ProtoMessage()    {}
func (*AuthzRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_dcbfe955c405ab5c, []int{0}
}

func (m *AuthzRequest) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_AuthzRequest.Unmarshal(m, b)
}
func (m *AuthzRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_AuthzRequest.Marshal(b, m, deterministic)
}
func (m *AuthzRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_AuthzRequest.Merge(m, src)
}
func (m *AuthzRequest) XXX_Size() int {
	return xxx_messageInfo_AuthzRequest.Size(m)
}
func (m *AuthzRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_AuthzRequest.DiscardUnknown(m)
}

var xxx_messageInfo_AuthzRequest proto.InternalMessageInfo

func (m *AuthzRequest) GetApikey() int32 {
	if m != nil {
		return m.Apikey
	}
	return 0
}

func (m *AuthzRequest) GetApiversion() int32 {
	if m != nil {
		return m.Apiversion
	}
	return 0
}

func (m *AuthzRequest) GetUserInfo() string {
	if m != nil {
		return m.UserInfo
	}
	return ""
}

func (m *AuthzRequest) GetSrcIp() string {
	if m != nil {
		return m.SrcIp
	}
	return ""
}

func (m *AuthzRequest) GetDstIp() string {
	if m != nil {
		return m.DstIp
	}
	return ""
}

func (m *AuthzRequest) GetTopics() string {
	if m != nil {
		return m.Topics
	}
	return ""
}

func (m *AuthzRequest) GetClientId() string {
	if m != nil {
		return m.ClientId
	}
	return ""
}

func (m *AuthzRequest) GetConsumerGroups() string {
	if m != nil {
		return m.ConsumerGroups
	}
	return ""
}

type AuthzResponse struct {
	Success              bool     `protobuf:"varint,1,opt,name=success,proto3" json:"success,omitempty"`
	Status               int32    `protobuf:"varint,2,opt,name=status,proto3" json:"status,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *AuthzResponse) Reset()         { *m = AuthzResponse{} }
func (m *AuthzResponse) String() string { return proto.CompactTextString(m) }
func (*AuthzResponse) ProtoMessage()    {}
func (*AuthzResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_dcbfe955c405ab5c, []int{1}
}

func (m *AuthzResponse) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_AuthzResponse.Unmarshal(m, b)
}
func (m *AuthzResponse) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_AuthzResponse.Marshal(b, m, deterministic)
}
func (m *AuthzResponse) XXX_Merge(src proto.Message) {
	xxx_messageInfo_AuthzResponse.Merge(m, src)
}
func (m *AuthzResponse) XXX_Size() int {
	return xxx_messageInfo_AuthzResponse.Size(m)
}
func (m *AuthzResponse) XXX_DiscardUnknown() {
	xxx_messageInfo_AuthzResponse.DiscardUnknown(m)
}

var xxx_messageInfo_AuthzResponse proto.InternalMessageInfo

func (m *AuthzResponse) GetSuccess() bool {
	if m != nil {
		return m.Success
	}
	return false
}

func (m *AuthzResponse) GetStatus() int32 {
	if m != nil {
		return m.Status
	}
	return 0
}

func init() {
	proto.RegisterType((*AuthzRequest)(nil), "proto.AuthzRequest")
	proto.RegisterType((*AuthzResponse)(nil), "proto.AuthzResponse")
}

func init() { proto.RegisterFile("authz-provider.proto", fileDescriptor_dcbfe955c405ab5c) }

var fileDescriptor_dcbfe955c405ab5c = []byte{
	// 280 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x54, 0xd0, 0xcd, 0x4a, 0x03, 0x31,
	0x10, 0x07, 0x70, 0x56, 0xdd, 0x6d, 0x3b, 0xf8, 0x01, 0xb1, 0x4a, 0x50, 0x90, 0xd2, 0x8b, 0xbd,
	0xd8, 0x83, 0x82, 0xf7, 0x9e, 0xca, 0xde, 0x64, 0x5f, 0x60, 0x59, 0xb3, 0x53, 0x0d, 0x6a, 0x26,
	0x66, 0x92, 0x82, 0x7d, 0x69, 0x5f, 0x41, 0x92, 0x4d, 0xa1, 0x9e, 0x96, 0xff, 0x6f, 0x96, 0xcc,
	0x07, 0x4c, 0xbb, 0xe0, 0xdf, 0x77, 0x0f, 0xd6, 0xd1, 0x56, 0xf7, 0xe8, 0x96, 0xd6, 0x91, 0x27,
	0x51, 0xa6, 0xcf, 0xfc, 0xb7, 0x80, 0xd3, 0x55, 0xac, 0x37, 0xf8, 0x1d, 0x90, 0xbd, 0xb8, 0x86,
	0xaa, 0xb3, 0xfa, 0x03, 0x7f, 0x64, 0x31, 0x2b, 0x16, 0x65, 0x93, 0x93, 0xb8, 0x03, 0xe8, 0xac,
	0xde, 0xa2, 0x63, 0x4d, 0x46, 0x1e, 0xa5, 0xda, 0x81, 0x88, 0x5b, 0x98, 0x04, 0x46, 0xd7, 0x6a,
	0xb3, 0x21, 0x79, 0x3c, 0x2b, 0x16, 0x93, 0x66, 0x1c, 0xa1, 0x36, 0x1b, 0x12, 0x57, 0x50, 0xb1,
	0x53, 0xad, 0xb6, 0xf2, 0x24, 0x55, 0x4a, 0x76, 0xaa, 0xb6, 0x91, 0x7b, 0xf6, 0x91, 0xcb, 0x81,
	0x7b, 0xf6, 0xb5, 0x8d, 0x23, 0x78, 0xb2, 0x5a, 0xb1, 0xac, 0x12, 0xe7, 0x14, 0x5b, 0xa8, 0x4f,
	0x8d, 0xc6, 0xb7, 0xba, 0x97, 0xa3, 0xa1, 0xc5, 0x00, 0x75, 0x2f, 0xee, 0xe1, 0x42, 0x91, 0xe1,
	0xf0, 0x85, 0xae, 0x7d, 0x73, 0x14, 0x2c, 0xcb, 0x71, 0xfa, 0xe5, 0x7c, 0xcf, 0xeb, 0xa4, 0xf3,
	0x15, 0x9c, 0xe5, 0x85, 0xd9, 0x92, 0x61, 0x14, 0x12, 0x46, 0x1c, 0x94, 0x42, 0xe6, 0xb4, 0xf2,
	0xb8, 0xd9, 0xc7, 0x38, 0x08, 0xfb, 0xce, 0x07, 0xce, 0xfb, 0xe6, 0xf4, 0xb8, 0xce, 0x4f, 0xbc,
	0xe4, 0x93, 0x8a, 0x67, 0x98, 0x44, 0x20, 0xa7, 0x77, 0x28, 0x2e, 0x87, 0x0b, 0x2f, 0x0f, 0xcf,
	0x7a, 0x33, 0xfd, 0x8f, 0x43, 0xeb, 0xd7, 0x2a, 0xe1, 0xd3, 0x5f, 0x00, 0x00, 0x00, 0xff, 0xff,
	0x83, 0x13, 0xb2, 0xfc, 0xa3, 0x01, 0x00, 0x00,
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConn

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion4

// AuthzProviderClient is the client API for AuthzProvider service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type AuthzProviderClient interface {
	Authorize(ctx context.Context, in *AuthzRequest, opts ...grpc.CallOption) (*AuthzResponse, error)
}

type authzProviderClient struct {
	cc *grpc.ClientConn
}

func NewAuthzProviderClient(cc *grpc.ClientConn) AuthzProviderClient {
	return &authzProviderClient{cc}
}

func (c *authzProviderClient) Authorize(ctx context.Context, in *AuthzRequest, opts ...grpc.CallOption) (*AuthzResponse, error) {
	out := new(AuthzResponse)
	err := c.cc.Invoke(ctx, "/proto.AuthzProvider/Authorize", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// AuthzProviderServer is the server API for AuthzProvider service.
type AuthzProviderServer interface {
	Authorize(context.Context, *AuthzRequest) (*AuthzResponse, error)
}

func RegisterAuthzProviderServer(s *grpc.Server, srv AuthzProviderServer) {
	s.RegisterService(&_AuthzProvider_serviceDesc, srv)
}

func _AuthzProvider_Authorize_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(AuthzRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(AuthzProviderServer).Authorize(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/proto.AuthzProvider/Authorize",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(AuthzProviderServer).Authorize(ctx, req.(*AuthzRequest))
	}
	return interceptor(ctx, in, info, handler)
}

var _AuthzProvider_serviceDesc = grpc.ServiceDesc{
	ServiceName: "proto.AuthzProvider",
	HandlerType: (*AuthzProviderServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Authorize",
			Handler:    _AuthzProvider_Authorize_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "authz-provider.proto",
}
