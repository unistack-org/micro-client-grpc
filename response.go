package grpc

import (
	"strings"

	"go.unistack.org/micro/v3/codec"
	"go.unistack.org/micro/v3/metadata"
	"google.golang.org/grpc"
)

type response struct {
	conn   *PoolConn
	stream grpc.ClientStream
	codec  codec.Codec
}

// Read the response
func (r *response) Codec() codec.Codec {
	return r.codec
}

// read the header
func (r *response) Header() metadata.Metadata {
	meta, err := r.stream.Header()
	if err != nil {
		return nil
	}
	md := metadata.New(len(meta))
	for k, v := range meta {
		md.Set(k, strings.Join(v, ","))
	}
	return md
}

// Read the undecoded response
func (r *response) Read() ([]byte, error) {
	return nil, nil
}
