// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.3.0
// - protoc             v5.28.2
// source: pb/serie.proto

package pb

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

const (
	Serie_Write_FullMethodName       = "/pb.Serie/Write"
	Serie_Read_FullMethodName        = "/pb.Serie/Read"
	Serie_ReadStream_FullMethodName  = "/pb.Serie/ReadStream"
	Serie_Query_FullMethodName       = "/pb.Serie/Query"
	Serie_WriteStream_FullMethodName = "/pb.Serie/WriteStream"
)

// SerieClient is the client API for Serie service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type SerieClient interface {
	Write(ctx context.Context, in *WriteRequest, opts ...grpc.CallOption) (*WriteResponse, error)
	Read(ctx context.Context, in *ReadRequest, opts ...grpc.CallOption) (*ReadResponse, error)
	ReadStream(ctx context.Context, in *ReadRequest, opts ...grpc.CallOption) (Serie_ReadStreamClient, error)
	Query(ctx context.Context, in *QueryRequest, opts ...grpc.CallOption) (*QueryResponse, error)
	WriteStream(ctx context.Context, opts ...grpc.CallOption) (Serie_WriteStreamClient, error)
}

type serieClient struct {
	cc grpc.ClientConnInterface
}

func NewSerieClient(cc grpc.ClientConnInterface) SerieClient {
	return &serieClient{cc}
}

func (c *serieClient) Write(ctx context.Context, in *WriteRequest, opts ...grpc.CallOption) (*WriteResponse, error) {
	out := new(WriteResponse)
	err := c.cc.Invoke(ctx, Serie_Write_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *serieClient) Read(ctx context.Context, in *ReadRequest, opts ...grpc.CallOption) (*ReadResponse, error) {
	out := new(ReadResponse)
	err := c.cc.Invoke(ctx, Serie_Read_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *serieClient) ReadStream(ctx context.Context, in *ReadRequest, opts ...grpc.CallOption) (Serie_ReadStreamClient, error) {
	stream, err := c.cc.NewStream(ctx, &Serie_ServiceDesc.Streams[0], Serie_ReadStream_FullMethodName, opts...)
	if err != nil {
		return nil, err
	}
	x := &serieReadStreamClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type Serie_ReadStreamClient interface {
	Recv() (*ReadResponse, error)
	grpc.ClientStream
}

type serieReadStreamClient struct {
	grpc.ClientStream
}

func (x *serieReadStreamClient) Recv() (*ReadResponse, error) {
	m := new(ReadResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *serieClient) Query(ctx context.Context, in *QueryRequest, opts ...grpc.CallOption) (*QueryResponse, error) {
	out := new(QueryResponse)
	err := c.cc.Invoke(ctx, Serie_Query_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *serieClient) WriteStream(ctx context.Context, opts ...grpc.CallOption) (Serie_WriteStreamClient, error) {
	stream, err := c.cc.NewStream(ctx, &Serie_ServiceDesc.Streams[1], Serie_WriteStream_FullMethodName, opts...)
	if err != nil {
		return nil, err
	}
	x := &serieWriteStreamClient{stream}
	return x, nil
}

type Serie_WriteStreamClient interface {
	Send(*WriteRequest) error
	Recv() (*WriteResponse, error)
	grpc.ClientStream
}

type serieWriteStreamClient struct {
	grpc.ClientStream
}

func (x *serieWriteStreamClient) Send(m *WriteRequest) error {
	return x.ClientStream.SendMsg(m)
}

func (x *serieWriteStreamClient) Recv() (*WriteResponse, error) {
	m := new(WriteResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// SerieServer is the server API for Serie service.
// All implementations must embed UnimplementedSerieServer
// for forward compatibility
type SerieServer interface {
	Write(context.Context, *WriteRequest) (*WriteResponse, error)
	Read(context.Context, *ReadRequest) (*ReadResponse, error)
	ReadStream(*ReadRequest, Serie_ReadStreamServer) error
	Query(context.Context, *QueryRequest) (*QueryResponse, error)
	WriteStream(Serie_WriteStreamServer) error
	mustEmbedUnimplementedSerieServer()
}

// UnimplementedSerieServer must be embedded to have forward compatible implementations.
type UnimplementedSerieServer struct {
}

func (UnimplementedSerieServer) Write(context.Context, *WriteRequest) (*WriteResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Write not implemented")
}
func (UnimplementedSerieServer) Read(context.Context, *ReadRequest) (*ReadResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Read not implemented")
}
func (UnimplementedSerieServer) ReadStream(*ReadRequest, Serie_ReadStreamServer) error {
	return status.Errorf(codes.Unimplemented, "method ReadStream not implemented")
}
func (UnimplementedSerieServer) Query(context.Context, *QueryRequest) (*QueryResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Query not implemented")
}
func (UnimplementedSerieServer) WriteStream(Serie_WriteStreamServer) error {
	return status.Errorf(codes.Unimplemented, "method WriteStream not implemented")
}
func (UnimplementedSerieServer) mustEmbedUnimplementedSerieServer() {}

// UnsafeSerieServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to SerieServer will
// result in compilation errors.
type UnsafeSerieServer interface {
	mustEmbedUnimplementedSerieServer()
}

func RegisterSerieServer(s grpc.ServiceRegistrar, srv SerieServer) {
	s.RegisterService(&Serie_ServiceDesc, srv)
}

func _Serie_Write_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(WriteRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(SerieServer).Write(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: Serie_Write_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(SerieServer).Write(ctx, req.(*WriteRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Serie_Read_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ReadRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(SerieServer).Read(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: Serie_Read_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(SerieServer).Read(ctx, req.(*ReadRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Serie_ReadStream_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(ReadRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(SerieServer).ReadStream(m, &serieReadStreamServer{stream})
}

type Serie_ReadStreamServer interface {
	Send(*ReadResponse) error
	grpc.ServerStream
}

type serieReadStreamServer struct {
	grpc.ServerStream
}

func (x *serieReadStreamServer) Send(m *ReadResponse) error {
	return x.ServerStream.SendMsg(m)
}

func _Serie_Query_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(QueryRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(SerieServer).Query(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: Serie_Query_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(SerieServer).Query(ctx, req.(*QueryRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Serie_WriteStream_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(SerieServer).WriteStream(&serieWriteStreamServer{stream})
}

type Serie_WriteStreamServer interface {
	Send(*WriteResponse) error
	Recv() (*WriteRequest, error)
	grpc.ServerStream
}

type serieWriteStreamServer struct {
	grpc.ServerStream
}

func (x *serieWriteStreamServer) Send(m *WriteResponse) error {
	return x.ServerStream.SendMsg(m)
}

func (x *serieWriteStreamServer) Recv() (*WriteRequest, error) {
	m := new(WriteRequest)
	if err := x.ServerStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// Serie_ServiceDesc is the grpc.ServiceDesc for Serie service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var Serie_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "pb.Serie",
	HandlerType: (*SerieServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Write",
			Handler:    _Serie_Write_Handler,
		},
		{
			MethodName: "Read",
			Handler:    _Serie_Read_Handler,
		},
		{
			MethodName: "Query",
			Handler:    _Serie_Query_Handler,
		},
	},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "ReadStream",
			Handler:       _Serie_ReadStream_Handler,
			ServerStreams: true,
		},
		{
			StreamName:    "WriteStream",
			Handler:       _Serie_WriteStream_Handler,
			ServerStreams: true,
			ClientStreams: true,
		},
	},
	Metadata: "pb/serie.proto",
}
