package grpc

import (
	"go.unistack.org/micro/v4/codec"
	"go.unistack.org/micro/v4/metadata"
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

	return metadata.Metadata(meta.Copy())
}

// Read the undecoded response
func (r *response) Read() ([]byte, error) {
	f := &codec.Frame{}
	wrap := &wrapStream{r.stream}

	_, err := wrap.Read(f.Data)
	if err != nil {
		return nil, err
	}

	return f.Data, nil
}
