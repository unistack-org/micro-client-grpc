package grpc

import (
	"context"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	pb "github.com/unistack-org/micro-client-grpc/proto"
	rmemory "github.com/unistack-org/micro-registry-memory"
	regRouter "github.com/unistack-org/micro-router-registry"
	pberr "github.com/unistack-org/micro-server-grpc/errors"
	"github.com/unistack-org/micro/v3/client"
	"github.com/unistack-org/micro/v3/errors"
	"github.com/unistack-org/micro/v3/registry"
	"github.com/unistack-org/micro/v3/router"
	pgrpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type testServer struct {
	pb.UnimplementedTestServer
}

func (g *testServer) CallNative(ctx context.Context, in *pb.Request) (*pb.Response, error) {
	if in.Name == "Error" {
		st := status.New(codes.InvalidArgument, "error request")
		st, err := st.WithDetails(&pberr.Error{Id: "id", Code: 99, Detail: "detail"})
		if err != nil {
			return nil, err
		}
		return nil, st.Err()
	}
	return &pb.Response{Msg: "Hello " + in.Name}, nil
}

func (g *testServer) CallMicro(ctx context.Context, in *pb.Request) (*pb.Response, error) {
	if in.Name == "Error" {
		return nil, &pberr.Error{Id: "id", Code: 99, Detail: "detail"}
	}
	return &pb.Response{Msg: "Hello " + in.Name}, nil
}

func (g *testServer) Stream(stream pb.Test_StreamServer) error {
	rsp := &pb.Response{}
	for {
		req, err := stream.Recv()
		if err != nil && err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		rsp.Msg = req.Name
		if err = stream.Send(rsp); err != nil {
			return err
		}
		time.Sleep(200 * time.Millisecond)

	}

	return nil
}

func TestGRPCClient(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer l.Close()

	s := pgrpc.NewServer()
	pb.RegisterTestServer(s, &testServer{})

	go func() {
		if err := s.Serve(l); err != nil {
			t.Log(err)
		}
	}()
	defer s.Stop()

	// create mock registry
	r := rmemory.NewRegistry()

	// register service
	if err := r.Register(&registry.Service{
		Name:    "helloworld",
		Version: "test",
		Nodes: []*registry.Node{
			{
				Id:      "test-1",
				Address: l.Addr().String(),
				Metadata: map[string]string{
					"protocol": "grpc",
				},
			},
		},
	}); err != nil {
		t.Fatal(err)
	}

	// create router
	rtr := regRouter.NewRouter(router.Registry(r))

	// create client
	c := NewClient(client.Router(rtr))

	testMethods := []string{
		"/helloworld.Test/CallNative",
		"Test.CallNative",
		"/helloworld.Test/CallMicro",
		"Test.CallMicro",
	}

	for _, method := range testMethods {
		req := c.NewRequest("helloworld", method, &pb.Request{
			Name: "John",
		})

		rsp := pb.Response{}

		err = c.Call(context.TODO(), req, &rsp)
		if err != nil {
			t.Fatal(err)
		}

		if rsp.Msg != "Hello John" {
			t.Fatalf("Got unexpected response %v", rsp.Msg)
		}
	}

	for _, method := range testMethods {
		req := c.NewRequest("helloworld", method, &pb.Request{
			Name: "Error",
		})

		rsp := pb.Response{}

		err = c.Call(context.TODO(), req, &rsp)
		if err == nil {
			t.Fatal("nil error received")
		}

		verr, ok := err.(*errors.Error)
		if !ok {
			t.Fatalf("invalid error received %#+v\n", err)
		}

		if verr.Code != 99 && verr.Id != "id" && verr.Detail != "detail" {
			t.Fatalf("invalid error received %#+v\n", verr)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	req := c.NewRequest("helloworld", "Test.Stream", &pb.Request{}, client.StreamingRequest())
	stream, err := c.Stream(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()

	go func() {
		for i := 0; i < 5; i++ {
			fmt.Printf("send to stream\n")
			if err = stream.Send(&pb.Request{Name: "test name"}); err != nil {
				t.Fatal(err)
			}
		}
	}()

	rsp := &pb.Response{}

	for i := 0; i < 5; i++ {
		fmt.Printf("recv from stream\n")
		if err = stream.Recv(rsp); err != nil {
			t.Fatal(err)
		}
		if rsp.Msg != "test name" {
			t.Fatalf("invalid msg: %v", rsp)
		}
	}

}
