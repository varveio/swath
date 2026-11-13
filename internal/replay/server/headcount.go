// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"net"
	"net/http"
	"sync"
)

// headCounter measures the request heads a connection carries as they
// arrive on the wire, before net/http parses them: the bytes from the
// start of each head to its blank line, inclusive. net/http drops and
// normalizes header bytes before a handler sees them (Transfer-Encoding,
// Host, whitespace around values), so a head measured from the parsed
// request can be shorter than the bytes the replay wire contract's HTTP
// layer limits (TestRequestHeadLimits). Heads are queued in arrival
// order; the handler takes its own with next, since a connection's
// requests reach the handler one at a time in that order.
//
// Bytes after a head that carries a body are counted as the start of the
// next head; that is harmless here, because a request with a body closes
// its connection after its answer (ignoreBody), so no later request on it
// is served.
type headCounter struct {
	mu     sync.Mutex
	heads  []int
	cur    int
	window uint32
}

// feed counts bytes read from the connection.
func (h *headCounter) feed(p []byte) {
	if len(p) == 0 {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, b := range p {
		// CR and LF before a head starts are not part of it: net/http
		// discards them after a request without a body (up to four,
		// after a POST; elsewhere the request line is malformed and the
		// connection closes), and the contract's server does not count
		// them either.
		if h.cur == 0 && (b == '\r' || b == '\n') {
			continue
		}
		h.cur++
		h.window = h.window<<8 | uint32(b)
		// A head ends at a blank line (CRLF or LF) after a line that
		// ended in CRLF or LF: net/http's line reader accepts all four
		// combinations, so the head ends at LF LF or LF CR LF.
		if h.window&0xffff == 0x0a0a || h.window&0xffffff == 0x0a0d0a {
			h.heads = append(h.heads, h.cur)
			h.cur, h.window = 0, 0
		}
	}
}

// next returns the size of the oldest head not yet taken.
func (h *headCounter) next() (int, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.heads) == 0 {
		return 0, false
	}
	n := h.heads[0]
	h.heads = h.heads[1:]
	return n, true
}

type headCounterKey struct{}

// connContext gives a connection's requests its head counter.
func connContext(ctx context.Context, c net.Conn) context.Context {
	if lc, ok := c.(*limitConn); ok {
		return context.WithValue(ctx, headCounterKey{}, &lc.heads)
	}
	return ctx
}

// rawHeadSize is the size of r's head as sent. counted reports whether
// r's connection counts heads; on such a connection a head it did not
// frame has no size (ok false), which the caller treats as over the limit
// rather than measuring the parsed request instead.
func rawHeadSize(r *http.Request) (size int, ok, counted bool) {
	if h, has := r.Context().Value(headCounterKey{}).(*headCounter); has {
		size, ok = h.next()
		return size, ok, true
	}
	return 0, false, false
}
