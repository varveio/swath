// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package ranged

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptrace"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/varveio/swath/objfile"
)

// errUnwound is the result of a request whose reader unwound (a panic)
// before it ended.
var errUnwound = errors.New("objfile: the read unwound before the request ended")

// report is how one request's end is reported, exactly once: by the
// Observer's done for a request reported here, or, for one an Observe
// transport observes, by completing its last HTTP attempt (the earlier
// ones are completed by the transport as each next one starts). The nil
// report reports nothing.
type report struct {
	mu   sync.Mutex
	done func(objfile.RequestResult) // reported here
	a    *attempt                    // observed by the transport
	over bool
}

// end reports the request's end once every check of it is done and its
// body is closed: the body bytes consumed (for a transport-observed
// request, the transport's own count) and err, a clean end of the body
// (nil or io.EOF) being a success. Only the first end counts.
func (r *report) end(bytes int64, err error) {
	if r == nil {
		return
	}
	r.mu.Lock()
	over := r.over
	r.over = true
	r.mu.Unlock()
	if over {
		return
	}
	if err == io.EOF {
		err = nil
	}
	if r.a != nil {
		r.a.finish(err)
		return
	}
	if r.done != nil {
		r.done(objfile.RequestResult{Bytes: bytes, Err: err})
	}
}

// unwound ends the request as failed when nothing ended it, as a reader
// unwinds from a panic; it is deferred right after begin.
func (r *report) unwound() { r.end(0, errUnwound) }

// begin starts the observation of a request q under ctx: nothing without
// obs; with observed, the request's context is marked for the Observe
// transport, which reports each HTTP attempt; otherwise q is reported here.
// The caller defers the report's unwound at once.
func begin(ctx context.Context, obs objfile.Observer, observed bool, q objfile.Request) (context.Context, *report) {
	switch {
	case obs == nil:
		return ctx, nil
	case observed:
		a := &attempt{obs: obs, q: q}
		return context.WithValue(ctx, attemptKey{}, a), &report{a: a}
	}
	return ctx, &report{done: obs.Begin(q)}
}

type attemptKey struct{}

// attempt is the work of one request of an Object, as the Observe
// transport reports its physical attempts: one for each connection a
// send of it gets, the SDK's retries, a reader's reopens and the Go
// transport's own resends included. q describes the first; each later one
// is a Retry. An attempt is completed when the next one starts, or, the
// last, when the Object's request ends (finish), after every check.
type attempt struct {
	obs objfile.Observer
	q   objfile.Request

	mu      sync.Mutex
	sent    int
	pending *sent // the current attempt, not yet completed
	over    bool  // finished: later attempts complete at once
}

// sent is one physical attempt: its completion, the body bytes read and
// the failure seen of it so far.
type sent struct {
	done   func(objfile.RequestResult)
	bytes  atomic.Int64
	mu     sync.Mutex
	failed error
	body   io.Closer // available to close if an SDK/getter unwinds
	once   sync.Once
}

func (s *sent) fail(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failed == nil {
		s.failed = err
	}
}

// complete reports the attempt's end, with err when it has seen no
// failure of its own.
func (s *sent) complete(err error) {
	s.once.Do(func() {
		s.mu.Lock()
		if s.failed != nil {
			err = s.failed
		}
		body := s.body
		s.mu.Unlock()
		closed := false
		defer func() {
			if !closed && err == nil {
				err = errUnwound
			}
			if s.done != nil {
				s.done(objfile.RequestResult{Bytes: s.bytes.Load(), Err: err})
			}
		}()
		if body != nil {
			if closeErr := body.Close(); err == nil {
				err = closeErr
			}
		}
		closed = true
	})
}

// errResent is the result of an attempt that ended, with no failure of its
// own seen, because another was sent in its place.
var errResent = errors.New("objfile: the request was sent again")

// start begins a physical attempt q (its range as sent), completing the
// one before it.
func (a *attempt) start(q objfile.Request) *sent {
	a.mu.Lock()
	defer a.mu.Unlock()
	q.Retry = q.Retry || a.sent > 0
	a.sent++
	if a.pending != nil {
		a.pending.complete(errResent)
	}
	// Publish only after Begin returns, so finish never sees an attempt whose
	// completion has not been installed. Observer callbacks cannot reenter.
	s := &sent{done: a.obs.Begin(q)}
	if a.over {
		s.complete(errUnwound)
	} else {
		a.pending = s
	}
	return s
}

// current returns the attempt in flight, or nil.
func (a *attempt) current() *sent {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.pending
}

// finish completes the last attempt with err (unless it failed itself).
func (a *attempt) finish(err error) {
	a.mu.Lock()
	last := a.pending
	a.pending, a.over = nil, true
	a.mu.Unlock()
	if last != nil {
		last.complete(err)
	}
}

func attemptOf(ctx context.Context) *attempt {
	a, _ := ctx.Value(attemptKey{}).(*attempt)
	return a
}

// rejectResponse records why a transport refused the response to req on
// its current attempt.
func rejectResponse(req *http.Request, err error) {
	if a := attemptOf(req.Context()); a != nil {
		if s := a.current(); s != nil {
			s.fail(err)
		}
	}
}

// Observe reports every physical attempt of an Object's request (marked by
// the Object, whose store's client Observing names) to the request's
// Observer: each connection a send gets (httptrace's GotConn), so that the
// SDK's retries, a reader's reopens of a failed body and the Go
// transport's own resends on a closed connection are each an attempt, with
// the range its Range header asks for. A request whose path does not end
// in the object's name, such as a credential exchange made with the same
// context, is not one of them. Bodies are counted as they are read; the
// last attempt is completed by the Object once its checks are done, the
// others as the next one starts. It belongs below every transport that may
// refuse a response (Transport), which records the refusal for it.
func Observe(base http.RoundTripper) http.RoundTripper { return observe{base: base} }

type observe struct{ base http.RoundTripper }

func (t observe) RoundTrip(req *http.Request) (*http.Response, error) {
	a := attemptOf(req.Context())
	if a == nil || !strings.HasSuffix(req.URL.Path, "/"+a.q.Name) {
		return t.base.RoundTrip(req)
	}
	q := a.q
	rng := req.Header.Get("x-ms-range")
	if rng == "" {
		rng = req.Header.Get("Range")
	}
	if first, last, ok := requested(rng); ok {
		q.Kind, q.Off, q.Len = objfile.RangeRequest, first, -1
		if last >= 0 {
			q.Len = last - first + 1
		}
	}
	var cur atomic.Pointer[sent]
	cur.Store(a.start(q))
	var connected bool // GotConn calls are sequential within one RoundTrip.
	trace := &httptrace.ClientTrace{
		GotConn: func(httptrace.GotConnInfo) {
			if connected {
				cur.Store(a.start(q))
			}
			connected = true
		},
	}
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), trace))
	res, err := t.base.RoundTrip(req)
	s := cur.Load()
	if err != nil {
		s.fail(err)
		return res, err
	}
	if res.StatusCode < 200 || res.StatusCode > 299 {
		s.fail(fmt.Errorf("the store answered %s", res.Status))
	}
	body := &counted{ReadCloser: res.Body, s: s}
	s.mu.Lock()
	s.body = body
	s.mu.Unlock()
	res.Body = body
	return res, nil
}

// CloseIdleConnections closes the idle connections of the base transport.
func (t observe) CloseIdleConnections() {
	if c, ok := t.base.(interface{ CloseIdleConnections() }); ok {
		c.CloseIdleConnections()
	}
}

// counted is an observed attempt's body: it counts the bytes read and
// keeps the body's failure for the attempt.
type counted struct {
	io.ReadCloser
	s         *sent
	closeOnce sync.Once
	closeErr  error
}

func (b *counted) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	b.s.bytes.Add(int64(n))
	if err != nil && err != io.EOF {
		b.s.fail(err)
	}
	return n, err
}

// Close releases the HTTP body once, whether the SDK closes it normally or
// the operation's deferred completion closes it during panic unwinding.
func (b *counted) Close() error {
	b.closeOnce.Do(func() { b.closeErr = b.ReadCloser.Close() })
	return b.closeErr
}

// observing holds the clients whose transport is an Observe transport, by
// pointer: the store packages ask Observing when they wrap one.
var observing sync.Map

// SetObserving records that client (an SDK client, by pointer) sends every
// request through an Observe transport; on false, it forgets it.
func SetObserving(client any, on bool) {
	if on {
		observing.Store(client, true)
	} else {
		observing.Delete(client)
	}
}

// Observing reports whether SetObserving recorded client.
func Observing(client any) bool {
	_, ok := observing.Load(client)
	return ok
}
