// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.34.1
// 	protoc        v5.28.2
// source: pb/serie.proto

package pb

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type Point struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Metric    string            `protobuf:"bytes,1,opt,name=metric,proto3" json:"metric,omitempty"`
	Value     float64           `protobuf:"fixed64,2,opt,name=value,proto3" json:"value,omitempty"`
	Timestamp int64             `protobuf:"varint,3,opt,name=timestamp,proto3" json:"timestamp,omitempty"`
	Tags      map[string]string `protobuf:"bytes,4,rep,name=tags,proto3" json:"tags,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
}

func (x *Point) Reset() {
	*x = Point{}
	if protoimpl.UnsafeEnabled {
		mi := &file_pb_serie_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Point) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Point) ProtoMessage() {}

func (x *Point) ProtoReflect() protoreflect.Message {
	mi := &file_pb_serie_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Point.ProtoReflect.Descriptor instead.
func (*Point) Descriptor() ([]byte, []int) {
	return file_pb_serie_proto_rawDescGZIP(), []int{0}
}

func (x *Point) GetMetric() string {
	if x != nil {
		return x.Metric
	}
	return ""
}

func (x *Point) GetValue() float64 {
	if x != nil {
		return x.Value
	}
	return 0
}

func (x *Point) GetTimestamp() int64 {
	if x != nil {
		return x.Timestamp
	}
	return 0
}

func (x *Point) GetTags() map[string]string {
	if x != nil {
		return x.Tags
	}
	return nil
}

// Query results are mainly just for aggregate functions
type QueryResult struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Aggregate string             `protobuf:"bytes,1,opt,name=aggregate,proto3" json:"aggregate,omitempty"`
	Values    map[string]float64 `protobuf:"bytes,2,rep,name=values,proto3" json:"values,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"fixed64,2,opt,name=value,proto3"`
}

func (x *QueryResult) Reset() {
	*x = QueryResult{}
	if protoimpl.UnsafeEnabled {
		mi := &file_pb_serie_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *QueryResult) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*QueryResult) ProtoMessage() {}

func (x *QueryResult) ProtoReflect() protoreflect.Message {
	mi := &file_pb_serie_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use QueryResult.ProtoReflect.Descriptor instead.
func (*QueryResult) Descriptor() ([]byte, []int) {
	return file_pb_serie_proto_rawDescGZIP(), []int{1}
}

func (x *QueryResult) GetAggregate() string {
	if x != nil {
		return x.Aggregate
	}
	return ""
}

func (x *QueryResult) GetValues() map[string]float64 {
	if x != nil {
		return x.Values
	}
	return nil
}

type TimestampQuery struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	MaxTimestamp int64  `protobuf:"varint,1,opt,name=max_timestamp,json=maxTimestamp,proto3" json:"max_timestamp,omitempty"`
	MinTimestamp int64  `protobuf:"varint,2,opt,name=min_timestamp,json=minTimestamp,proto3" json:"min_timestamp,omitempty"`
	Metric       string `protobuf:"bytes,3,opt,name=metric,proto3" json:"metric,omitempty"`
}

func (x *TimestampQuery) Reset() {
	*x = TimestampQuery{}
	if protoimpl.UnsafeEnabled {
		mi := &file_pb_serie_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *TimestampQuery) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*TimestampQuery) ProtoMessage() {}

func (x *TimestampQuery) ProtoReflect() protoreflect.Message {
	mi := &file_pb_serie_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use TimestampQuery.ProtoReflect.Descriptor instead.
func (*TimestampQuery) Descriptor() ([]byte, []int) {
	return file_pb_serie_proto_rawDescGZIP(), []int{2}
}

func (x *TimestampQuery) GetMaxTimestamp() int64 {
	if x != nil {
		return x.MaxTimestamp
	}
	return 0
}

func (x *TimestampQuery) GetMinTimestamp() int64 {
	if x != nil {
		return x.MinTimestamp
	}
	return 0
}

func (x *TimestampQuery) GetMetric() string {
	if x != nil {
		return x.Metric
	}
	return ""
}

type WriteRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Points []*Point `protobuf:"bytes,1,rep,name=points,proto3" json:"points,omitempty"`
}

func (x *WriteRequest) Reset() {
	*x = WriteRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_pb_serie_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *WriteRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*WriteRequest) ProtoMessage() {}

func (x *WriteRequest) ProtoReflect() protoreflect.Message {
	mi := &file_pb_serie_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use WriteRequest.ProtoReflect.Descriptor instead.
func (*WriteRequest) Descriptor() ([]byte, []int) {
	return file_pb_serie_proto_rawDescGZIP(), []int{3}
}

func (x *WriteRequest) GetPoints() []*Point {
	if x != nil {
		return x.Points
	}
	return nil
}

type WriteResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
}

func (x *WriteResponse) Reset() {
	*x = WriteResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_pb_serie_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *WriteResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*WriteResponse) ProtoMessage() {}

func (x *WriteResponse) ProtoReflect() protoreflect.Message {
	mi := &file_pb_serie_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use WriteResponse.ProtoReflect.Descriptor instead.
func (*WriteResponse) Descriptor() ([]byte, []int) {
	return file_pb_serie_proto_rawDescGZIP(), []int{4}
}

type ReadRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	TsQuery *TimestampQuery `protobuf:"bytes,1,opt,name=ts_query,json=tsQuery,proto3" json:"ts_query,omitempty"`
}

func (x *ReadRequest) Reset() {
	*x = ReadRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_pb_serie_proto_msgTypes[5]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ReadRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ReadRequest) ProtoMessage() {}

func (x *ReadRequest) ProtoReflect() protoreflect.Message {
	mi := &file_pb_serie_proto_msgTypes[5]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ReadRequest.ProtoReflect.Descriptor instead.
func (*ReadRequest) Descriptor() ([]byte, []int) {
	return file_pb_serie_proto_rawDescGZIP(), []int{5}
}

func (x *ReadRequest) GetTsQuery() *TimestampQuery {
	if x != nil {
		return x.TsQuery
	}
	return nil
}

type ReadResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Points []*Point `protobuf:"bytes,1,rep,name=points,proto3" json:"points,omitempty"`
}

func (x *ReadResponse) Reset() {
	*x = ReadResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_pb_serie_proto_msgTypes[6]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ReadResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ReadResponse) ProtoMessage() {}

func (x *ReadResponse) ProtoReflect() protoreflect.Message {
	mi := &file_pb_serie_proto_msgTypes[6]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ReadResponse.ProtoReflect.Descriptor instead.
func (*ReadResponse) Descriptor() ([]byte, []int) {
	return file_pb_serie_proto_rawDescGZIP(), []int{6}
}

func (x *ReadResponse) GetPoints() []*Point {
	if x != nil {
		return x.Points
	}
	return nil
}

type QueryRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Query string `protobuf:"bytes,1,opt,name=query,proto3" json:"query,omitempty"`
}

func (x *QueryRequest) Reset() {
	*x = QueryRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_pb_serie_proto_msgTypes[7]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *QueryRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*QueryRequest) ProtoMessage() {}

func (x *QueryRequest) ProtoReflect() protoreflect.Message {
	mi := &file_pb_serie_proto_msgTypes[7]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use QueryRequest.ProtoReflect.Descriptor instead.
func (*QueryRequest) Descriptor() ([]byte, []int) {
	return file_pb_serie_proto_rawDescGZIP(), []int{7}
}

func (x *QueryRequest) GetQuery() string {
	if x != nil {
		return x.Query
	}
	return ""
}

type QueryResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Result []*QueryResult `protobuf:"bytes,1,rep,name=result,proto3" json:"result,omitempty"`
}

func (x *QueryResponse) Reset() {
	*x = QueryResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_pb_serie_proto_msgTypes[8]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *QueryResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*QueryResponse) ProtoMessage() {}

func (x *QueryResponse) ProtoReflect() protoreflect.Message {
	mi := &file_pb_serie_proto_msgTypes[8]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use QueryResponse.ProtoReflect.Descriptor instead.
func (*QueryResponse) Descriptor() ([]byte, []int) {
	return file_pb_serie_proto_rawDescGZIP(), []int{8}
}

func (x *QueryResponse) GetResult() []*QueryResult {
	if x != nil {
		return x.Result
	}
	return nil
}

var File_pb_serie_proto protoreflect.FileDescriptor

var file_pb_serie_proto_rawDesc = []byte{
	0x0a, 0x0e, 0x70, 0x62, 0x2f, 0x73, 0x65, 0x72, 0x69, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x12, 0x02, 0x70, 0x62, 0x22, 0xb5, 0x01, 0x0a, 0x05, 0x50, 0x6f, 0x69, 0x6e, 0x74, 0x12, 0x16,
	0x0a, 0x06, 0x6d, 0x65, 0x74, 0x72, 0x69, 0x63, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x06,
	0x6d, 0x65, 0x74, 0x72, 0x69, 0x63, 0x12, 0x14, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18,
	0x02, 0x20, 0x01, 0x28, 0x01, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x12, 0x1c, 0x0a, 0x09,
	0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x18, 0x03, 0x20, 0x01, 0x28, 0x03, 0x52,
	0x09, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x12, 0x27, 0x0a, 0x04, 0x74, 0x61,
	0x67, 0x73, 0x18, 0x04, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x13, 0x2e, 0x70, 0x62, 0x2e, 0x50, 0x6f,
	0x69, 0x6e, 0x74, 0x2e, 0x54, 0x61, 0x67, 0x73, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x52, 0x04, 0x74,
	0x61, 0x67, 0x73, 0x1a, 0x37, 0x0a, 0x09, 0x54, 0x61, 0x67, 0x73, 0x45, 0x6e, 0x74, 0x72, 0x79,
	0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x03, 0x6b,
	0x65, 0x79, 0x12, 0x14, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x3a, 0x02, 0x38, 0x01, 0x22, 0x9b, 0x01, 0x0a,
	0x0b, 0x51, 0x75, 0x65, 0x72, 0x79, 0x52, 0x65, 0x73, 0x75, 0x6c, 0x74, 0x12, 0x1c, 0x0a, 0x09,
	0x61, 0x67, 0x67, 0x72, 0x65, 0x67, 0x61, 0x74, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52,
	0x09, 0x61, 0x67, 0x67, 0x72, 0x65, 0x67, 0x61, 0x74, 0x65, 0x12, 0x33, 0x0a, 0x06, 0x76, 0x61,
	0x6c, 0x75, 0x65, 0x73, 0x18, 0x02, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x1b, 0x2e, 0x70, 0x62, 0x2e,
	0x51, 0x75, 0x65, 0x72, 0x79, 0x52, 0x65, 0x73, 0x75, 0x6c, 0x74, 0x2e, 0x56, 0x61, 0x6c, 0x75,
	0x65, 0x73, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x52, 0x06, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x73, 0x1a,
	0x39, 0x0a, 0x0b, 0x56, 0x61, 0x6c, 0x75, 0x65, 0x73, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x12, 0x10,
	0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x03, 0x6b, 0x65, 0x79,
	0x12, 0x14, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x01, 0x52,
	0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x3a, 0x02, 0x38, 0x01, 0x22, 0x72, 0x0a, 0x0e, 0x54, 0x69,
	0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x51, 0x75, 0x65, 0x72, 0x79, 0x12, 0x23, 0x0a, 0x0d,
	0x6d, 0x61, 0x78, 0x5f, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x18, 0x01, 0x20,
	0x01, 0x28, 0x03, 0x52, 0x0c, 0x6d, 0x61, 0x78, 0x54, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d,
	0x70, 0x12, 0x23, 0x0a, 0x0d, 0x6d, 0x69, 0x6e, 0x5f, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61,
	0x6d, 0x70, 0x18, 0x02, 0x20, 0x01, 0x28, 0x03, 0x52, 0x0c, 0x6d, 0x69, 0x6e, 0x54, 0x69, 0x6d,
	0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x12, 0x16, 0x0a, 0x06, 0x6d, 0x65, 0x74, 0x72, 0x69, 0x63,
	0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x06, 0x6d, 0x65, 0x74, 0x72, 0x69, 0x63, 0x22, 0x31,
	0x0a, 0x0c, 0x57, 0x72, 0x69, 0x74, 0x65, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x21,
	0x0a, 0x06, 0x70, 0x6f, 0x69, 0x6e, 0x74, 0x73, 0x18, 0x01, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x09,
	0x2e, 0x70, 0x62, 0x2e, 0x50, 0x6f, 0x69, 0x6e, 0x74, 0x52, 0x06, 0x70, 0x6f, 0x69, 0x6e, 0x74,
	0x73, 0x22, 0x0f, 0x0a, 0x0d, 0x57, 0x72, 0x69, 0x74, 0x65, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e,
	0x73, 0x65, 0x22, 0x3c, 0x0a, 0x0b, 0x52, 0x65, 0x61, 0x64, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73,
	0x74, 0x12, 0x2d, 0x0a, 0x08, 0x74, 0x73, 0x5f, 0x71, 0x75, 0x65, 0x72, 0x79, 0x18, 0x01, 0x20,
	0x01, 0x28, 0x0b, 0x32, 0x12, 0x2e, 0x70, 0x62, 0x2e, 0x54, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61,
	0x6d, 0x70, 0x51, 0x75, 0x65, 0x72, 0x79, 0x52, 0x07, 0x74, 0x73, 0x51, 0x75, 0x65, 0x72, 0x79,
	0x22, 0x31, 0x0a, 0x0c, 0x52, 0x65, 0x61, 0x64, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65,
	0x12, 0x21, 0x0a, 0x06, 0x70, 0x6f, 0x69, 0x6e, 0x74, 0x73, 0x18, 0x01, 0x20, 0x03, 0x28, 0x0b,
	0x32, 0x09, 0x2e, 0x70, 0x62, 0x2e, 0x50, 0x6f, 0x69, 0x6e, 0x74, 0x52, 0x06, 0x70, 0x6f, 0x69,
	0x6e, 0x74, 0x73, 0x22, 0x24, 0x0a, 0x0c, 0x51, 0x75, 0x65, 0x72, 0x79, 0x52, 0x65, 0x71, 0x75,
	0x65, 0x73, 0x74, 0x12, 0x14, 0x0a, 0x05, 0x71, 0x75, 0x65, 0x72, 0x79, 0x18, 0x01, 0x20, 0x01,
	0x28, 0x09, 0x52, 0x05, 0x71, 0x75, 0x65, 0x72, 0x79, 0x22, 0x38, 0x0a, 0x0d, 0x51, 0x75, 0x65,
	0x72, 0x79, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x27, 0x0a, 0x06, 0x72, 0x65,
	0x73, 0x75, 0x6c, 0x74, 0x18, 0x01, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x0f, 0x2e, 0x70, 0x62, 0x2e,
	0x51, 0x75, 0x65, 0x72, 0x79, 0x52, 0x65, 0x73, 0x75, 0x6c, 0x74, 0x52, 0x06, 0x72, 0x65, 0x73,
	0x75, 0x6c, 0x74, 0x32, 0x83, 0x02, 0x0a, 0x05, 0x53, 0x65, 0x72, 0x69, 0x65, 0x12, 0x2e, 0x0a,
	0x05, 0x57, 0x72, 0x69, 0x74, 0x65, 0x12, 0x10, 0x2e, 0x70, 0x62, 0x2e, 0x57, 0x72, 0x69, 0x74,
	0x65, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x11, 0x2e, 0x70, 0x62, 0x2e, 0x57, 0x72,
	0x69, 0x74, 0x65, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00, 0x12, 0x2b, 0x0a,
	0x04, 0x52, 0x65, 0x61, 0x64, 0x12, 0x0f, 0x2e, 0x70, 0x62, 0x2e, 0x52, 0x65, 0x61, 0x64, 0x52,
	0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x10, 0x2e, 0x70, 0x62, 0x2e, 0x52, 0x65, 0x61, 0x64,
	0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00, 0x12, 0x33, 0x0a, 0x0a, 0x52, 0x65,
	0x61, 0x64, 0x53, 0x74, 0x72, 0x65, 0x61, 0x6d, 0x12, 0x0f, 0x2e, 0x70, 0x62, 0x2e, 0x52, 0x65,
	0x61, 0x64, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x10, 0x2e, 0x70, 0x62, 0x2e, 0x52,
	0x65, 0x61, 0x64, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00, 0x30, 0x01, 0x12,
	0x2e, 0x0a, 0x05, 0x51, 0x75, 0x65, 0x72, 0x79, 0x12, 0x10, 0x2e, 0x70, 0x62, 0x2e, 0x51, 0x75,
	0x65, 0x72, 0x79, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x11, 0x2e, 0x70, 0x62, 0x2e,
	0x51, 0x75, 0x65, 0x72, 0x79, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00, 0x12,
	0x38, 0x0a, 0x0b, 0x57, 0x72, 0x69, 0x74, 0x65, 0x53, 0x74, 0x72, 0x65, 0x61, 0x6d, 0x12, 0x10,
	0x2e, 0x70, 0x62, 0x2e, 0x57, 0x72, 0x69, 0x74, 0x65, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74,
	0x1a, 0x11, 0x2e, 0x70, 0x62, 0x2e, 0x57, 0x72, 0x69, 0x74, 0x65, 0x52, 0x65, 0x73, 0x70, 0x6f,
	0x6e, 0x73, 0x65, 0x22, 0x00, 0x28, 0x01, 0x30, 0x01, 0x42, 0x1b, 0x5a, 0x19, 0x67, 0x69, 0x74,
	0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x6e, 0x69, 0x72, 0x65, 0x6f, 0x2f, 0x73, 0x65,
	0x72, 0x69, 0x65, 0x2f, 0x70, 0x62, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_pb_serie_proto_rawDescOnce sync.Once
	file_pb_serie_proto_rawDescData = file_pb_serie_proto_rawDesc
)

func file_pb_serie_proto_rawDescGZIP() []byte {
	file_pb_serie_proto_rawDescOnce.Do(func() {
		file_pb_serie_proto_rawDescData = protoimpl.X.CompressGZIP(file_pb_serie_proto_rawDescData)
	})
	return file_pb_serie_proto_rawDescData
}

var file_pb_serie_proto_msgTypes = make([]protoimpl.MessageInfo, 11)
var file_pb_serie_proto_goTypes = []interface{}{
	(*Point)(nil),          // 0: pb.Point
	(*QueryResult)(nil),    // 1: pb.QueryResult
	(*TimestampQuery)(nil), // 2: pb.TimestampQuery
	(*WriteRequest)(nil),   // 3: pb.WriteRequest
	(*WriteResponse)(nil),  // 4: pb.WriteResponse
	(*ReadRequest)(nil),    // 5: pb.ReadRequest
	(*ReadResponse)(nil),   // 6: pb.ReadResponse
	(*QueryRequest)(nil),   // 7: pb.QueryRequest
	(*QueryResponse)(nil),  // 8: pb.QueryResponse
	nil,                    // 9: pb.Point.TagsEntry
	nil,                    // 10: pb.QueryResult.ValuesEntry
}
var file_pb_serie_proto_depIdxs = []int32{
	9,  // 0: pb.Point.tags:type_name -> pb.Point.TagsEntry
	10, // 1: pb.QueryResult.values:type_name -> pb.QueryResult.ValuesEntry
	0,  // 2: pb.WriteRequest.points:type_name -> pb.Point
	2,  // 3: pb.ReadRequest.ts_query:type_name -> pb.TimestampQuery
	0,  // 4: pb.ReadResponse.points:type_name -> pb.Point
	1,  // 5: pb.QueryResponse.result:type_name -> pb.QueryResult
	3,  // 6: pb.Serie.Write:input_type -> pb.WriteRequest
	5,  // 7: pb.Serie.Read:input_type -> pb.ReadRequest
	5,  // 8: pb.Serie.ReadStream:input_type -> pb.ReadRequest
	7,  // 9: pb.Serie.Query:input_type -> pb.QueryRequest
	3,  // 10: pb.Serie.WriteStream:input_type -> pb.WriteRequest
	4,  // 11: pb.Serie.Write:output_type -> pb.WriteResponse
	6,  // 12: pb.Serie.Read:output_type -> pb.ReadResponse
	6,  // 13: pb.Serie.ReadStream:output_type -> pb.ReadResponse
	8,  // 14: pb.Serie.Query:output_type -> pb.QueryResponse
	4,  // 15: pb.Serie.WriteStream:output_type -> pb.WriteResponse
	11, // [11:16] is the sub-list for method output_type
	6,  // [6:11] is the sub-list for method input_type
	6,  // [6:6] is the sub-list for extension type_name
	6,  // [6:6] is the sub-list for extension extendee
	0,  // [0:6] is the sub-list for field type_name
}

func init() { file_pb_serie_proto_init() }
func file_pb_serie_proto_init() {
	if File_pb_serie_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_pb_serie_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Point); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_pb_serie_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*QueryResult); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_pb_serie_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*TimestampQuery); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_pb_serie_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*WriteRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_pb_serie_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*WriteResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_pb_serie_proto_msgTypes[5].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ReadRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_pb_serie_proto_msgTypes[6].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ReadResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_pb_serie_proto_msgTypes[7].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*QueryRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_pb_serie_proto_msgTypes[8].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*QueryResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_pb_serie_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   11,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_pb_serie_proto_goTypes,
		DependencyIndexes: file_pb_serie_proto_depIdxs,
		MessageInfos:      file_pb_serie_proto_msgTypes,
	}.Build()
	File_pb_serie_proto = out.File
	file_pb_serie_proto_rawDesc = nil
	file_pb_serie_proto_goTypes = nil
	file_pb_serie_proto_depIdxs = nil
}