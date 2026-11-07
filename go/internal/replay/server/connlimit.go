// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"net"
	"sync"
	"sync/atomic"
)

// ConnectionsPerRequest is how many open connections the serving listener
// keeps per request slot (--max-concurrent-requests): room for every
// executing request, the twice-as-many outstanding responses, and the
// clients waiting for a slot or idle between requests. A connection past
// that is accepted and closed at once.
const ConnectionsPerRequest = 4

// MaxConnections is the serving listener's connection bound.
func (c *Config) MaxConnections() int { return ConnectionsPerRequest * c.MaxConcurrentRequests }

// limitListener bounds the connections a listener holds open. Execution
// slots bound the work, not the connections: without this each accepted
// connection keeps a goroutine and its buffers however many wait. It is
// golang.org/x/net/netutil.LimitListener's idea, but refusing rather than
// waiting: a connection over the bound is closed instead of left unaccepted
// in the kernel's queue, so a client learns at once (and a waiting accept
// cannot hold up shutdown). It is small enough that the module takes no
// direct dependency on x/net for it.
type limitListener struct {
	net.Listener
	max  int64
	open atomic.Int64
}

func (l *limitListener) Accept() (net.Conn, error) {
	for {
		c, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}
		if l.open.Add(1) > l.max {
			l.open.Add(-1)
			c.Close()
			continue
		}
		return &limitConn{Conn: c, l: l}, nil
	}
}

type limitConn struct {
	net.Conn
	l     *limitListener
	once  sync.Once
	heads headCounter
}

// Read counts the request heads as they arrive (headCounter).
func (c *limitConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	c.heads.feed(p[:n])
	return n, err
}

func (c *limitConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(func() { c.l.open.Add(-1) })
	return err
}
