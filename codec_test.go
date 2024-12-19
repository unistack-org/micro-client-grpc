package grpc

import (
	"context"
	"go.unistack.org/micro/v3/codec"
	gmetadata "google.golang.org/grpc/metadata"
	"testing"
)

type mockStream struct {
	msg any
}

func (m mockStream) Header() (gmetadata.MD, error) {
	return nil, nil
}

func (m mockStream) Trailer() gmetadata.MD {
	return nil
}

func (m mockStream) CloseSend() error {
	return nil
}

func (m mockStream) Context() context.Context {
	return nil
}

func (m *mockStream) SendMsg(msg any) error {
	m.msg = msg
	return nil
}

func (m *mockStream) RecvMsg(msg any) error {

	c := msg.(*codec.Frame)
	c.Data = m.msg.(*codec.Frame).Data

	return nil
}

func Test_ReadWrap(t *testing.T) {

	wp := wrapStream{
		&mockStream{},
	}

	write, err := wp.Write([]byte("test_data"))
	if err != nil {
		t.Fatal(err)
	}
	if write != 9 {
		t.Error("uncorrected number wrote bytes")
	}

	b := make([]byte, write)
	read, err := wp.Read(b)
	if err != nil {
		t.Fatal(err)
	}

	if read != 9 || string(b) != "test_data" {
		t.Error("uncorrected number wrote bytes or data")
	}
}
