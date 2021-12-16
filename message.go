package grpc

import (
	"go.unistack.org/micro/v3/client"
	"go.unistack.org/micro/v3/metadata"
)

type grpcEvent struct {
	payload     interface{}
	topic       string
	contentType string
	opts        client.MessageOptions
}

func newGRPCEvent(topic string, payload interface{}, contentType string, opts ...client.MessageOption) client.Message {
	options := client.NewMessageOptions(opts...)

	if len(options.ContentType) > 0 {
		contentType = options.ContentType
	}

	return &grpcEvent{
		payload:     payload,
		topic:       topic,
		contentType: contentType,
		opts:        options,
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

func (g *grpcEvent) Metadata() metadata.Metadata {
	return g.opts.Metadata
}
