package grpc

import (
	b "bytes"
	"encoding/json"
	"fmt"
	"strings"

	oldjsonpb "github.com/golang/protobuf/jsonpb"
	oldproto "github.com/golang/protobuf/proto"
	bytes "github.com/unistack-org/micro-codec-bytes"
	"github.com/unistack-org/micro/v3/codec"
	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding"
	jsonpb "google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type jsonCodec struct{}
type protoCodec struct{}
type bytesCodec struct{}
type wrapCodec struct{ encoding.Codec }

var jsonpbMarshaler = &jsonpb.MarshalOptions{}
var oldjsonpbMarshaler = &oldjsonpb.Marshaler{}
var useNumber bool

var (
	defaultGRPCCodecs = map[string]encoding.Codec{
		"application/json":         jsonCodec{},
		"application/proto":        protoCodec{},
		"application/protobuf":     protoCodec{},
		"application/octet-stream": protoCodec{},
		"application/grpc":         protoCodec{},
		"application/grpc+json":    jsonCodec{},
		"application/grpc+proto":   protoCodec{},
		"application/grpc+bytes":   bytesCodec{},
	}
)

// UseNumber fix unmarshal Number(8234567890123456789) to interface(8.234567890123457e+18)
func UseNumber() {
	useNumber = true
}

func (w wrapCodec) String() string {
	return w.Codec.Name()
}

func (w wrapCodec) Marshal(v interface{}) ([]byte, error) {
	switch m := v.(type) {
	case *bytes.Frame:
		return m.Data, nil
	}
	return w.Codec.Marshal(v)
}

func (w wrapCodec) Unmarshal(data []byte, v interface{}) error {
	if len(data) == 0 {
		return nil
	}
	if v == nil {
		return nil
	}
	switch m := v.(type) {
	case *bytes.Frame:
		m.Data = data
		return nil
	}
	return w.Codec.Unmarshal(data, v)
}

func (protoCodec) Marshal(v interface{}) ([]byte, error) {
	switch m := v.(type) {
	case *bytes.Frame:
		return m.Data, nil
	case proto.Message:
		return proto.Marshal(m)
	case oldproto.Message:
		return oldproto.Marshal(m)
	}
	return nil, fmt.Errorf("failed to marshal: %v is not type of *bytes.Frame or proto.Message", v)
}

func (protoCodec) Unmarshal(data []byte, v interface{}) error {
	if len(data) == 0 {
		return nil
	}
	if v == nil {
		return nil
	}
	switch m := v.(type) {
	case *bytes.Frame:
		m.Data = data
		return nil
	case proto.Message:
		return proto.Unmarshal(data, m)
	case oldproto.Message:
		return oldproto.Unmarshal(data, m)
	}
	return fmt.Errorf("failed to unmarshal: %v is not type of *bytes.Frame or proto.Message", v)
}

func (protoCodec) Name() string {
	return "proto"
}

func (bytesCodec) Marshal(v interface{}) ([]byte, error) {
	switch m := v.(type) {
	case *[]byte:
		return *m, nil
	}
	return nil, fmt.Errorf("failed to marshal: %v is not type of *[]byte", v)
}

func (bytesCodec) Unmarshal(data []byte, v interface{}) error {
	if len(data) == 0 {
		return nil
	}
	if v == nil {
		return nil
	}
	switch m := v.(type) {
	case *[]byte:
		*m = data
		return nil
	}
	return fmt.Errorf("failed to unmarshal: %v is not type of *[]byte", v)
}

func (bytesCodec) Name() string {
	return "bytes"
}

func (jsonCodec) Marshal(v interface{}) ([]byte, error) {
	switch m := v.(type) {
	case *bytes.Frame:
		return m.Data, nil
	case proto.Message:
		return jsonpbMarshaler.Marshal(m)
	case oldproto.Message:
		buf, err := oldjsonpbMarshaler.MarshalToString(m)
		return []byte(buf), err
	}

	return json.Marshal(v)
}

func (jsonCodec) Unmarshal(data []byte, v interface{}) error {
	if len(data) == 0 {
		return nil
	}
	if v == nil {
		return nil
	}
	switch m := v.(type) {
	case *bytes.Frame:
		m.Data = data
		return nil
	case proto.Message:
		return jsonpb.Unmarshal(data, m)
	case oldproto.Message:
		return oldjsonpb.Unmarshal(b.NewReader(data), m)
	}
	dec := json.NewDecoder(b.NewReader(data))
	if useNumber {
		dec.UseNumber()
	}
	return dec.Decode(v)
}

func (jsonCodec) Name() string {
	return "json"
}

type grpcCodec struct {
	// headers
	id       string
	target   string
	method   string
	endpoint string

	s grpc.ClientStream
	c encoding.Codec
}

func (g *grpcCodec) ReadHeader(m *codec.Message, mt codec.MessageType) error {
	md, err := g.s.Header()
	if err != nil {
		return err
	}
	if m == nil {
		m = new(codec.Message)
	}
	if m.Header == nil {
		m.Header = make(map[string]string, len(md))
	}
	for k, v := range md {
		m.Header[k] = strings.Join(v, ",")
	}
	m.Id = g.id
	m.Target = g.target
	m.Method = g.method
	m.Endpoint = g.endpoint
	return nil
}

func (g *grpcCodec) ReadBody(v interface{}) error {
	switch m := v.(type) {
	case *bytes.Frame:
		return g.s.RecvMsg(m)
	}
	return g.s.RecvMsg(v)
}

func (g *grpcCodec) Write(m *codec.Message, v interface{}) error {
	// if we don't have a body
	if v != nil {
		return g.s.SendMsg(v)
	}
	// write the body using the framing codec
	return g.s.SendMsg(&bytes.Frame{Data: m.Body})
}

func (g *grpcCodec) Close() error {
	return g.s.CloseSend()
}

func (g *grpcCodec) String() string {
	return g.c.Name()
}
