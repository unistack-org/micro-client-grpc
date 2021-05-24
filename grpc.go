// Package grpc provides a gRPC client
package grpc

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/unistack-org/micro/v3/broker"
	"github.com/unistack-org/micro/v3/client"
	"github.com/unistack-org/micro/v3/codec"
	"github.com/unistack-org/micro/v3/errors"
	"github.com/unistack-org/micro/v3/metadata"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/encoding"
	gmetadata "google.golang.org/grpc/metadata"
)

const (
	defaultContentType = "application/grpc+proto"
)

type grpcClient struct {
	opts client.Options
	pool *pool
	init bool
	sync.RWMutex
}

// secure returns the dial option for whether its a secure or insecure connection
func (g *grpcClient) secure(addr string) grpc.DialOption {
	// first we check if theres'a  tls config
	if g.opts.TLSConfig != nil {
		creds := credentials.NewTLS(g.opts.TLSConfig)
		// return tls config if it exists
		return grpc.WithTransportCredentials(creds)
	}

	// default config
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
	defaultCreds := grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))

	// check if the address is prepended with https
	if strings.HasPrefix(addr, "https://") {
		return defaultCreds
	}

	// if no port is specified or port is 443 default to tls
	_, port, err := net.SplitHostPort(addr)
	// assuming with no port its going to be secured
	if port == "443" {
		return defaultCreds
	} else if err != nil && strings.Contains(err.Error(), "missing port in address") {
		return defaultCreds
	}

	// other fallback to insecure
	return grpc.WithInsecure()
}

func (g *grpcClient) call(ctx context.Context, addr string, req client.Request, rsp interface{}, opts client.CallOptions) error {
	var header map[string]string

	if md, ok := metadata.FromOutgoingContext(ctx); ok {
		header = make(map[string]string, len(md))
		for k, v := range md {
			header[strings.ToLower(k)] = v
		}
	} else {
		header = make(map[string]string, 2)
	}

	// set timeout in nanoseconds
	header["timeout"] = fmt.Sprintf("%d", opts.RequestTimeout)
	// set the content type for the request
	header["x-content-type"] = req.ContentType()

	md := gmetadata.New(header)
	ctx = gmetadata.NewOutgoingContext(ctx, md)

	cf, err := g.newCodec(req.ContentType())
	if err != nil {
		return errors.InternalServerError("go.micro.client", err.Error())
	}

	maxRecvMsgSize := g.maxRecvMsgSizeValue()
	maxSendMsgSize := g.maxSendMsgSizeValue()

	var grr error

	var dialCtx context.Context
	var cancel context.CancelFunc
	if opts.DialTimeout >= 0 {
		dialCtx, cancel = context.WithTimeout(ctx, opts.DialTimeout)
	} else {
		dialCtx, cancel = context.WithCancel(ctx)
	}
	defer cancel()

	grpcDialOptions := []grpc.DialOption{
		g.secure(addr),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(maxRecvMsgSize),
			grpc.MaxCallSendMsgSize(maxSendMsgSize),
		),
	}

	if opts := g.getGrpcDialOptions(); opts != nil {
		grpcDialOptions = append(grpcDialOptions, opts...)
	}

	cc, err := g.pool.getConn(dialCtx, addr, grpcDialOptions...)
	if err != nil {
		return errors.InternalServerError("go.micro.client", fmt.Sprintf("Error sending request: %v", err))
	}
	defer func() {
		// defer execution of release
		g.pool.release(cc, grr)
	}()

	ch := make(chan error, 1)

	go func() {
		grpcCallOptions := []grpc.CallOption{
			grpc.ForceCodec(&wrapMicroCodec{cf}),
			grpc.CallContentSubtype((&wrapMicroCodec{cf}).Name()),
		}
		if opts := g.getGrpcCallOptions(); opts != nil {
			grpcCallOptions = append(grpcCallOptions, opts...)
		}
		err := cc.Invoke(ctx, methodToGRPC(req.Service(), req.Endpoint()), req.Body(), rsp, grpcCallOptions...)
		ch <- microError(err)
	}()

	select {
	case err := <-ch:
		grr = err
	case <-ctx.Done():
		grr = errors.Timeout("go.micro.client", "%v", ctx.Err())
	}

	return grr
}

func (g *grpcClient) stream(ctx context.Context, addr string, req client.Request, rsp interface{}, opts client.CallOptions) error {
	var header map[string]string

	if md, ok := metadata.FromOutgoingContext(ctx); ok {
		header = make(map[string]string, len(md))
		for k, v := range md {
			header[k] = v
		}
	} else {
		header = make(map[string]string)
	}

	// set timeout in nanoseconds
	if opts.StreamTimeout > time.Duration(0) {
		header["timeout"] = fmt.Sprintf("%d", opts.StreamTimeout)
	}
	// set the content type for the request
	header["x-content-type"] = req.ContentType()

	md := gmetadata.New(header)
	ctx = gmetadata.NewOutgoingContext(ctx, md)

	cf, err := g.newCodec(req.ContentType())
	if err != nil {
		return errors.InternalServerError("go.micro.client", err.Error())
	}

	var dialCtx context.Context
	var cancel context.CancelFunc
	if opts.DialTimeout >= 0 {
		dialCtx, cancel = context.WithTimeout(ctx, opts.DialTimeout)
	} else {
		dialCtx, cancel = context.WithCancel(ctx)
	}
	defer cancel()

	wc := &wrapMicroCodec{cf}

	grpcDialOptions := []grpc.DialOption{
		g.secure(addr),
	}

	if opts := g.getGrpcDialOptions(); opts != nil {
		grpcDialOptions = append(grpcDialOptions, opts...)
	}

	cc, err := g.pool.getConn(dialCtx, addr, grpcDialOptions...)
	if err != nil {
		return errors.InternalServerError("go.micro.client", fmt.Sprintf("Error sending request: %v", err))
	}

	desc := &grpc.StreamDesc{
		StreamName:    req.Service() + req.Endpoint(),
		ClientStreams: true,
		ServerStreams: true,
	}

	grpcCallOptions := []grpc.CallOption{
		grpc.ForceCodec(wc),
		grpc.CallContentSubtype(wc.Name()),
	}
	if opts := g.getGrpcCallOptions(); opts != nil {
		grpcCallOptions = append(grpcCallOptions, opts...)
	}

	// create a new cancelling context
	newCtx, cancel := context.WithCancel(ctx)

	st, err := cc.NewStream(newCtx, desc, methodToGRPC(req.Service(), req.Endpoint()), grpcCallOptions...)
	if err != nil {
		// we need to cleanup as we dialled and created a context
		// cancel the context
		cancel()
		// release the connection
		g.pool.release(cc, err)
		// now return the error
		return errors.InternalServerError("go.micro.client", fmt.Sprintf("Error creating stream: %v", err))
	}

	// set request codec
	if r, ok := req.(*grpcRequest); ok {
		r.codec = cf
	}

	// setup the stream response
	stream := &grpcStream{
		ClientStream: st,
		context:      ctx,
		request:      req,
		response: &response{
			conn:   cc,
			stream: st,
			codec:  cf,
		},
		conn: cc,
		close: func(err error) {
			// cancel the context if an error occurred
			if err != nil {
				cancel()
			}

			// defer execution of release
			g.pool.release(cc, err)
		},
	}

	// set the stream as the response
	val := reflect.ValueOf(rsp).Elem()
	val.Set(reflect.ValueOf(stream).Elem())
	return nil
}

func (g *grpcClient) poolMaxStreams() int {
	if g.opts.Context == nil {
		return DefaultPoolMaxStreams
	}
	v := g.opts.Context.Value(poolMaxStreams{})
	if v == nil {
		return DefaultPoolMaxStreams
	}
	return v.(int)
}

func (g *grpcClient) poolMaxIdle() int {
	if g.opts.Context == nil {
		return DefaultPoolMaxIdle
	}
	v := g.opts.Context.Value(poolMaxIdle{})
	if v == nil {
		return DefaultPoolMaxIdle
	}
	return v.(int)
}

func (g *grpcClient) maxRecvMsgSizeValue() int {
	if g.opts.Context == nil {
		return DefaultMaxRecvMsgSize
	}
	v := g.opts.Context.Value(maxRecvMsgSizeKey{})
	if v == nil {
		return DefaultMaxRecvMsgSize
	}
	return v.(int)
}

func (g *grpcClient) maxSendMsgSizeValue() int {
	if g.opts.Context == nil {
		return DefaultMaxSendMsgSize
	}
	v := g.opts.Context.Value(maxSendMsgSizeKey{})
	if v == nil {
		return DefaultMaxSendMsgSize
	}
	return v.(int)
}

func (g *grpcClient) newCodec(ct string) (codec.Codec, error) {
	g.RLock()
	defer g.RUnlock()

	if idx := strings.IndexRune(ct, ';'); idx >= 0 {
		ct = ct[:idx]
	}

	if c, ok := g.opts.Codecs[ct]; ok {
		return c, nil
	}
	return nil, codec.ErrUnknownContentType
}

func (g *grpcClient) Init(opts ...client.Option) error {
	if len(opts) == 0 && g.init {
		return nil
	}
	size := g.opts.PoolSize
	ttl := g.opts.PoolTTL

	for _, o := range opts {
		o(&g.opts)
	}

	// update pool configuration if the options changed
	if size != g.opts.PoolSize || ttl != g.opts.PoolTTL {
		g.pool.Lock()
		g.pool.size = g.opts.PoolSize
		g.pool.ttl = int64(g.opts.PoolTTL.Seconds())
		g.pool.Unlock()
	}

	if err := g.opts.Broker.Init(); err != nil {
		return err
	}
	if err := g.opts.Tracer.Init(); err != nil {
		return err
	}
	if err := g.opts.Router.Init(); err != nil {
		return err
	}
	if err := g.opts.Logger.Init(); err != nil {
		return err
	}
	if err := g.opts.Meter.Init(); err != nil {
		return err
	}
	if err := g.opts.Transport.Init(); err != nil {
		return err
	}

	g.init = true

	return nil
}

func (g *grpcClient) Options() client.Options {
	return g.opts
}

func (g *grpcClient) NewMessage(topic string, msg interface{}, opts ...client.MessageOption) client.Message {
	return newGRPCEvent(topic, msg, g.opts.ContentType, opts...)
}

func (g *grpcClient) NewRequest(service, method string, req interface{}, reqOpts ...client.RequestOption) client.Request {
	return newGRPCRequest(service, method, req, g.opts.ContentType, reqOpts...)
}

func (g *grpcClient) Call(ctx context.Context, req client.Request, rsp interface{}, opts ...client.CallOption) error {
	if req == nil {
		return errors.InternalServerError("go.micro.client", "req is nil")
	} else if rsp == nil {
		return errors.InternalServerError("go.micro.client", "rsp is nil")
	}
	// make a copy of call opts
	callOpts := g.opts.CallOptions
	for _, opt := range opts {
		opt(&callOpts)
	}

	// check if we already have a deadline
	d, ok := ctx.Deadline()
	if !ok {
		// no deadline so we create a new one
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, callOpts.RequestTimeout)
		defer cancel()
	} else {
		// got a deadline so no need to setup context
		// but we need to set the timeout we pass along
		opt := client.WithRequestTimeout(time.Until(d))
		opt(&callOpts)
	}

	// should we noop right here?
	select {
	case <-ctx.Done():
		return errors.New("go.micro.client", fmt.Sprintf("%v", ctx.Err()), 408)
	default:
	}

	// make copy of call method
	gcall := g.call

	// wrap the call in reverse
	for i := len(callOpts.CallWrappers); i > 0; i-- {
		gcall = callOpts.CallWrappers[i-1](gcall)
	}

	// use the router passed as a call option, or fallback to the rpc clients router
	if callOpts.Router == nil {
		callOpts.Router = g.opts.Router
	}

	if callOpts.Selector == nil {
		callOpts.Selector = g.opts.Selector
	}

	// inject proxy address
	// TODO: don't even bother using Lookup/Select in this case
	if len(g.opts.Proxy) > 0 {
		callOpts.Address = []string{g.opts.Proxy}
	}

	// lookup the route to send the reques to
	// TODO apply any filtering here
	routes, err := g.opts.Lookup(ctx, req, callOpts)
	if err != nil {
		return errors.InternalServerError("go.micro.client", err.Error())
	}

	// balance the list of nodes
	next, err := callOpts.Selector.Select(routes)
	if err != nil {
		return err
	}

	// return errors.New("go.micro.client", "request timeout", 408)
	call := func(i int) error {
		// call backoff first. Someone may want an initial start delay
		t, err := callOpts.Backoff(ctx, req, i)
		if err != nil {
			return errors.InternalServerError("go.micro.client", err.Error())
		}

		// only sleep if greater than 0
		if t.Seconds() > 0 {
			time.Sleep(t)
		}

		// get the next node
		node := next()

		// make the call
		err = gcall(ctx, node, req, rsp, callOpts)

		// record the result of the call to inform future routing decisions
		if verr := g.opts.Selector.Record(node, err); verr != nil {
			return verr
		}

		// try and transform the error to a go-micro error
		if verr, ok := err.(*errors.Error); ok {
			return verr
		}

		return err
	}

	ch := make(chan error, callOpts.Retries+1)
	var gerr error

	for i := 0; i <= callOpts.Retries; i++ {
		go func(i int) {
			ch <- call(i)
		}(i)

		select {
		case <-ctx.Done():
			return errors.New("go.micro.client", fmt.Sprintf("%v", ctx.Err()), 408)
		case err := <-ch:
			// if the call succeeded lets bail early
			if err == nil {
				return nil
			}

			retry, rerr := callOpts.Retry(ctx, req, i, err)
			if rerr != nil {
				return rerr
			}

			if !retry {
				return err
			}

			gerr = err
		}
	}

	return gerr
}

func (g *grpcClient) Stream(ctx context.Context, req client.Request, opts ...client.CallOption) (client.Stream, error) {
	// make a copy of call opts
	callOpts := g.opts.CallOptions
	for _, opt := range opts {
		opt(&callOpts)
	}

	// #200 - streams shouldn't have a request timeout set on the context

	// should we noop right here?
	select {
	case <-ctx.Done():
		return nil, errors.New("go.micro.client", fmt.Sprintf("%v", ctx.Err()), 408)
	default:
	}

	// make a copy of stream
	gstream := g.stream

	// wrap the call in reverse
	for i := len(callOpts.CallWrappers); i > 0; i-- {
		gstream = callOpts.CallWrappers[i-1](gstream)
	}

	// use the router passed as a call option, or fallback to the rpc clients router
	if callOpts.Router == nil {
		callOpts.Router = g.opts.Router
	}

	if callOpts.Selector == nil {
		callOpts.Selector = g.opts.Selector
	}

	// inject proxy address
	// TODO: don't even bother using Lookup/Select in this case
	if len(g.opts.Proxy) > 0 {
		callOpts.Address = []string{g.opts.Proxy}
	}

	// lookup the route to send the reques to
	// TODO: move to internal lookup func
	routes, err := g.opts.Lookup(ctx, req, callOpts)
	if err != nil {
		return nil, errors.InternalServerError("go.micro.client", err.Error())
	}

	// balance the list of nodes
	next, err := callOpts.Selector.Select(routes)
	if err != nil {
		return nil, err
	}

	call := func(i int) (client.Stream, error) {
		// call backoff first. Someone may want an initial start delay
		t, err := callOpts.Backoff(ctx, req, i)
		if err != nil {
			return nil, errors.InternalServerError("go.micro.client", err.Error())
		}

		// only sleep if greater than 0
		if t.Seconds() > 0 {
			time.Sleep(t)
		}

		// get the next node
		node := next()

		// make the call
		stream := &grpcStream{}
		err = gstream(ctx, node, req, stream, callOpts)

		// record the result of the call to inform future routing decisions
		if verr := g.opts.Selector.Record(node, err); verr != nil {
			return nil, verr
		}

		// try and transform the error to a go-micro error
		if verr, ok := err.(*errors.Error); ok {
			return nil, verr
		}

		if rerr := g.opts.Selector.Record(node, err); rerr != nil {
			return nil, rerr
		}

		return stream, err
	}

	type response struct {
		stream client.Stream
		err    error
	}

	ch := make(chan response, callOpts.Retries+1)
	var grr error

	for i := 0; i <= callOpts.Retries; i++ {
		go func(i int) {
			s, err := call(i)
			ch <- response{s, err}
		}(i)

		select {
		case <-ctx.Done():
			return nil, errors.New("go.micro.client", fmt.Sprintf("%v", ctx.Err()), 408)
		case rsp := <-ch:
			// if the call succeeded lets bail early
			if rsp.err == nil {
				return rsp.stream, nil
			}

			retry, rerr := callOpts.Retry(ctx, req, i, grr)
			if rerr != nil {
				return nil, rerr
			}

			if !retry {
				return nil, rsp.err
			}

			grr = rsp.err
		}
	}

	return nil, grr
}

func (g *grpcClient) Publish(ctx context.Context, p client.Message, opts ...client.PublishOption) error {
	var body []byte

	options := client.NewPublishOptions(opts...)

	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		md = metadata.New(2)
	}
	md["Content-Type"] = p.ContentType()
	md["Micro-Topic"] = p.Topic()

	// passed in raw data
	if d, ok := p.Payload().(*codec.Frame); ok {
		body = d.Data
	} else {
		// use codec for payload
		cf, err := g.newCodec(p.ContentType())
		if err != nil {
			return errors.InternalServerError("go.micro.client", err.Error())
		}
		// set the body
		b, err := cf.Marshal(p.Payload())
		if err != nil {
			return errors.InternalServerError("go.micro.client", err.Error())
		}
		body = b
	}

	topic := p.Topic()

	// get the exchange
	if len(options.Exchange) > 0 {
		topic = options.Exchange
	}

	return g.opts.Broker.Publish(metadata.NewOutgoingContext(ctx, md), topic, &broker.Message{
		Header: md,
		Body:   body,
	}, broker.PublishContext(options.Context))
}

func (g *grpcClient) String() string {
	return "grpc"
}

func (g *grpcClient) Name() string {
	return g.opts.Name
}

func (g *grpcClient) getGrpcDialOptions() []grpc.DialOption {
	if g.opts.CallOptions.Context == nil {
		return nil
	}

	v := g.opts.CallOptions.Context.Value(grpcDialOptions{})

	if v == nil {
		return nil
	}

	opts, ok := v.([]grpc.DialOption)

	if !ok {
		return nil
	}

	return opts
}

func (g *grpcClient) getGrpcCallOptions() []grpc.CallOption {
	if g.opts.CallOptions.Context == nil {
		return nil
	}

	v := g.opts.CallOptions.Context.Value(grpcCallOptions{})

	if v == nil {
		return nil
	}

	opts, ok := v.([]grpc.CallOption)

	if !ok {
		return nil
	}

	return opts
}

func NewClient(opts ...client.Option) client.Client {
	options := client.NewOptions(opts...)
	// default content type for grpc
	options.ContentType = defaultContentType

	rc := &grpcClient{
		opts: options,
	}

	rc.pool = newPool(options.PoolSize, options.PoolTTL, rc.poolMaxIdle(), rc.poolMaxStreams())

	c := client.Client(rc)

	// wrap in reverse
	for i := len(options.Wrappers); i > 0; i-- {
		c = options.Wrappers[i-1](c)
	}

	if rc.opts.Context != nil {
		if codecs, ok := rc.opts.Context.Value(codecsKey{}).(map[string]encoding.Codec); ok && codecs != nil {
			for k, v := range codecs {
				rc.opts.Codecs[k] = &wrapGrpcCodec{v}
			}
		}
	}

	for _, k := range options.Codecs {
		encoding.RegisterCodec(&wrapMicroCodec{k})
	}

	return c
}
