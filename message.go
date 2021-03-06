package grpc

import (
	"github.com/unistack-org/micro/v3/client"
)

type grpcEvent struct {
	payload     interface{}
	topic       string
	contentType string
}

func newGRPCEvent(topic string, payload interface{}, contentType string, opts ...client.MessageOption) client.Message {
	var options client.MessageOptions
	for _, o := range opts {
		o(&options)
	}

	if len(options.ContentType) > 0 {
		contentType = options.ContentType
	}

	return &grpcEvent{
		payload:     payload,
		topic:       topic,
		contentType: contentType,
	}
}

func (g *grpcEvent) ContentType() string {
	return g.contentType
}

func (g *grpcEvent) Topic() string {
	return g.topic
}

func (g *grpcEvent) Payload() interface{} {
	return g.payload
}
