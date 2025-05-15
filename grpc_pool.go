package grpc

import (
	"context"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
)

type ConnPool struct {
	conns      map[string]*streamsPool
	size       int
	ttl        int64
	maxStreams int
	maxIdle    int
	mu         sync.Mutex
}

type streamsPool struct {
	//  head of list
	head *PoolConn
	//  busy conns list
	busy *PoolConn
	//  the siza of list
	count int
	//  idle conn
	idle int
}

type PoolConn struct {
	err error
	*grpc.ClientConn
	next    *PoolConn
	pool    *ConnPool
	sp      *streamsPool
	pre     *PoolConn
	addr    string
	streams int
	created int64
	in      bool
}

func NewConnPool(size int, ttl time.Duration, idle int, ms int) *ConnPool {
	if ms <= 0 {
		ms = 1
	}
	if idle < 0 {
		idle = 0
	}
	return &ConnPool{
		size:       size,
		ttl:        int64(ttl.Seconds()),
		maxStreams: ms,
		maxIdle:    idle,
		conns:      make(map[string]*streamsPool),
	}
}

func (p *ConnPool) Get(ctx context.Context, addr string, opts ...grpc.DialOption) (*PoolConn, error) {
	if strings.HasPrefix(addr, "http") {
		addr = addr[strings.Index(addr, ":")+3:]
	}
	now := time.Now().Unix()
	p.mu.Lock()
	sp, ok := p.conns[addr]
	if !ok {
		sp = &streamsPool{head: &PoolConn{}, busy: &PoolConn{}, count: 0, idle: 0}
		p.conns[addr] = sp
	}
	//  while we have conns check streams and then return one
	//  otherwise we'll create a new conn
	conn := sp.head.next
	for conn != nil {
		//  check conn state
		// https://github.com/grpc/grpc/blob/master/doc/connectivity-semantics-and-api.md
		switch conn.GetState() {
		case connectivity.Connecting:
			conn = conn.next
			continue
		case connectivity.Shutdown:
			next := conn.next
			if conn.streams == 0 {
				removeConn(conn)
				sp.idle--
			}
			conn = next
			continue
		case connectivity.TransientFailure:
			next := conn.next
			if conn.streams == 0 {
				removeConn(conn)
				conn.ClientConn.Close()
				sp.idle--
			}
			conn = next
			continue
		case connectivity.Ready:
		case connectivity.Idle:
		}
		//  a old conn
		if now-conn.created > p.ttl {
			next := conn.next
			if conn.streams == 0 {
				removeConn(conn)
				conn.ClientConn.Close()
				sp.idle--
			}
			conn = next
			continue
		}
		//  a busy conn
		if conn.streams >= p.maxStreams {
			next := conn.next
			removeConn(conn)
			addConnAfter(conn, sp.busy)
			conn = next
			continue
		}
		//  a idle conn
		if conn.streams == 0 {
			sp.idle--
		}
		//  a good conn
		conn.streams++
		p.mu.Unlock()
		return conn, nil
	}
	p.mu.Unlock()

	// nolint (TODO need fix)  create new conn)
	cc, err := grpc.DialContext(ctx, addr, opts...)
	if err != nil {
		return nil, err
	}
	conn = &PoolConn{ClientConn: cc, err: nil, addr: addr, pool: p, sp: sp, streams: 1, created: time.Now().Unix(), pre: nil, next: nil, in: false}

	//  add conn to streams pool
	p.mu.Lock()
	if sp.count < p.size {
		addConnAfter(conn, sp.head)
	}
	p.mu.Unlock()

	return conn, nil
}

func (p *ConnPool) Put(conn *PoolConn, err error) {
	p.mu.Lock()
	p, sp, created := conn.pool, conn.sp, conn.created
	//  try to add conn
	if !conn.in && sp.count < p.size {
		addConnAfter(conn, sp.head)
	}
	if !conn.in {
		p.mu.Unlock()
		conn.ClientConn.Close()
		return
	}
	//  a busy conn
	if conn.streams >= p.maxStreams {
		removeConn(conn)
		addConnAfter(conn, sp.head)
	}
	conn.streams--
	//  if streams == 0, we can do something
	if conn.streams == 0 {
		//  1. it has errored
		//  2. too many idle conn or
		//  3. conn is too old
		now := time.Now().Unix()
		if err != nil || sp.idle >= p.maxIdle || now-created > p.ttl {
			removeConn(conn)
			p.mu.Unlock()
			conn.ClientConn.Close()
			return
		}
		sp.idle++
	}
	p.mu.Unlock()
}

func (conn *PoolConn) Close() {
	conn.pool.Put(conn, conn.err)
}

func removeConn(conn *PoolConn) {
	if conn.pre != nil {
		conn.pre.next = conn.next
	}
	if conn.next != nil {
		conn.next.pre = conn.pre
	}
	conn.pre = nil
	conn.next = nil
	conn.in = false
	conn.sp.count--
}

func addConnAfter(conn *PoolConn, after *PoolConn) {
	conn.next = after.next
	conn.pre = after
	if after.next != nil {
		after.next.pre = conn
	}
	after.next = conn
	conn.in = true
	conn.sp.count++
}
