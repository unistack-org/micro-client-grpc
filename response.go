package grpc

import (
	"strings"

	bytes "github.com/unistack-org/micro-codec-bytes"
	"github.com/unistack-org/micro/v3/codec"
	"github.com/unistack-org/micro/v3/metadata"
	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding"
)

type response struct {
	conn   *poolConn
	stream grpc.ClientStream
	codec  encoding.Codec
	gcodec codec.Codec
}

// Read the response
func (r *response) Codec() codec.Reader {
	return r.gcodec
}

// read the header
func (r *response) Header() metadata.Metadata {
	meta, err := r.stream.Header()
	if err != nil {
		return metadata.New(0)
	}
	md := metadata.New(len(meta))
	for k, v := range meta {
		md.Set(k, strings.Join(v, ","))
	}
	return md
}

// Read the undecoded response
func (r *response) Read() ([]byte, error) {
	f := &bytes.Frame{}
	if err := r.gcodec.ReadBody(f); err != nil {
		return nil, err
	}
	return f.Data, nil
}
