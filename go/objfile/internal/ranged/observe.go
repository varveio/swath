// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package ranged

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/varveio/swath/go/objfile"
)

// report is how one request's end is reported: by the Observer's done
// when the request is observed here, or, for a request an Observe
// transport observes, by marking a failure the transport did not see (a
// response check) on its attempt. The zero report reports nothing.
type report struct {
	done func(objfile.RequestResult)
	a    *attempt
}

// end reports the request's end, before its body is closed: bytes of
// body consumed and err, a clean end of the body (nil or io.EOF) being a
// success.
func (r report) end(bytes int64, err error) {
	if err == io.EOF {
		err = nil
	}
	if r.done != nil {
		r.done(objfile.RequestResult{Bytes: bytes, Err: err})
	}
	if r.a != nil && err != nil {
		r.a.reject(err)
	}
}

// begin starts the observation of a request q under ctx: nothing without
// obs; with observed, the request's context is marked for the Observe
// transport, which reports each HTTP attempt; otherwise q is reported here.
func begin(ctx context.Context, obs objfile.Observer, observed bool, q objfile.Request) (context.Context, report) {
	switch {
	case obs == nil:
		return ctx, report{}
	case observed:
		a := &attempt{obs: obs, q: q}
		return context.WithValue(ctx, attemptKey{}, a), report{a: a}
	}
	return ctx, report{done: obs.Begin(q)}
}

type attemptKey struct{}

// attempt is the work of one request of an Object, as the Observe
// transport reports its HTTP attempts: q describes the first, and each
// one after it (an SDK's retry, a reader's reopen) is a Retry.
type attempt struct {
	obs  objfile.Observer
	q    objfile.Request
	sent atomic.Bool

	mu       sync.Mutex
	rejected error // a check's failure of the current attempt's response
}

func (a *attempt) reject(err error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.rejected == nil {
		a.rejected = err
	}
}

// take returns and clears the current attempt's rejection.
func (a *attempt) take() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	err := a.rejected
	a.rejected = nil
	return err
}

func attemptOf(ctx context.Context) *attempt {
	a, _ := ctx.Value(attemptKey{}).(*attempt)
	return a
}

// rejectResponse records why a transport refused the response to req, for
// the Observe transport below it to report.
func rejectResponse(req *http.Request, err error) {
	if a := attemptOf(req.Context()); a != nil {
		a.reject(err)
	}
}

// Observe reports every HTTP attempt of an Object's request (marked by the
// Object, whose store's client Observing names) to the request's Observer,
// SDK retries and a reader's reopens of a failed body included, each with
// the range its Range header asks for. A request whose path does not end
// in the object's name, such as a credential exchange made with the same
// context, is not one of them. Its responses' bodies are counted as they
// are read; an attempt ends when its body fails, ends or is closed. It
// belongs below every transport that may refuse a response (Transport),
// which records the refusal for it.
func Observe(base http.RoundTripper) http.RoundTripper { return observe{base: base} }

type observe struct{ base http.RoundTripper }

func (t observe) RoundTrip(req *http.Request) (*http.Response, error) {
	a := attemptOf(req.Context())
	if a == nil || !strings.HasSuffix(req.URL.Path, "/"+a.q.Name) {
		return t.base.RoundTrip(req)
	}
	q := a.q
	q.Retry = q.Retry || a.sent.Swap(true)
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
	a.take() // a refusal of an earlier attempt is not this one's
	done := a.obs.Begin(q)
	res, err := t.base.RoundTrip(req)
	if err != nil {
		if done != nil {
			done(objfile.RequestResult{Err: err})
		}
		return res, err
	}
	var failed error
	if res.StatusCode < 200 || res.StatusCode > 299 {
		failed = fmt.Errorf("the store answered %s", res.Status)
	}
	res.Body = &counted{ReadCloser: res.Body, a: a, done: done, failed: failed}
	return res, nil
}

// CloseIdleConnections closes the idle connections of the base transport.
func (t observe) CloseIdleConnections() {
	if c, ok := t.base.(interface{ CloseIdleConnections() }); ok {
		c.CloseIdleConnections()
	}
}

// counted is an observed attempt's body: it counts the bytes read and
// reports the attempt's end once, when the body fails, ends or is closed.
type counted struct {
	io.ReadCloser
	a      *attempt
	done   func(objfile.RequestResult)
	failed error // an error status

	mu    sync.Mutex
	bytes int64
	ended bool
}

func (b *counted) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	b.mu.Lock()
	b.bytes += int64(n)
	b.mu.Unlock()
	if err == io.EOF {
		b.finish(nil)
	} else if err != nil {
		b.finish(err)
	}
	return n, err
}

func (b *counted) Close() error {
	err := b.ReadCloser.Close()
	b.finish(nil)
	return err
}

func (b *counted) finish(err error) {
	b.mu.Lock()
	if b.ended {
		b.mu.Unlock()
		return
	}
	b.ended = true
	bytes := b.bytes
	b.mu.Unlock()
	// A check's refusal recorded so far explains the end best; then an
	// error status; then the body's own failure.
	if rejected := b.a.take(); rejected != nil {
		err = rejected
	} else if b.failed != nil {
		err = b.failed
	}
	if b.done != nil {
		b.done(objfile.RequestResult{Bytes: bytes, Err: err})
	}
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
