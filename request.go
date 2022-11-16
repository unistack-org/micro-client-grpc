package grpc

import (
	"fmt"
	"strings"

	"go.unistack.org/micro/v3/client"
	"go.unistack.org/micro/v3/codec"
)

type grpcRequest struct {
	request     interface{}
	codec       codec.Codec
	service     string
	method      string
	contentType string
	opts        client.RequestOptions
}

// service Struct.Method /service.Struct/Method
func methodToGRPC(service, method string) string {
	// no method or already grpc method
	if len(method) == 0 || method[0] == '/' {
		return method
	}

	// assume method is Foo.Bar
	mParts := strings.Split(method, ".")
	if len(mParts) != 2 {
		return method
	}

	if len(service) == 0 {
		return fmt.Sprintf("/%s/%s", mParts[0], mParts[1])
	}

	// return /pkg.Foo/Bar
	return fmt.Sprintf("/%s.%s/%s", service, mParts[0], mParts[1])
}

func newGRPCRequest(service, method string, request interface{}, contentType string, opts ...client.RequestOption) client.Request {
	options := client.NewRequestOptions(opts...)

	// set the content-type specified
	if len(options.ContentType) > 0 {
		contentType = options.ContentType
	}

	return &grpcRequest{
		service:     service,
		method:      method,
		request:     request,
		contentType: contentType,
		opts:        options,
	}
}

func (g *grpcRequest) ContentType() string {
	return g.contentType
}

func (g *grpcRequest) Service() string {
	return g.service
}

func (g *grpcRequest) Method() string {
	return g.method
}

func (g *grpcRequest) Endpoint() string {
	return g.method
}

func (g *grpcRequest) Codec() codec.Codec {
	return g.codec
}

func (g *grpcRequest) Body() interface{} {
	return g.request
}

func (g *grpcRequest) Stream() bool {
	return g.opts.Stream
}
