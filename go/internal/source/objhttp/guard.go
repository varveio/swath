// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package objhttp

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/varveio/swath/go/internal/source"
)

const (
	defaultMaxBody  = 16 << 20
	defaultMaxError = 64 << 10
)

// Guard is an http.RoundTripper that bounds and validates every response
// before the SDK client above it decodes it. It is SDK-agnostic: the SDK
// sees an ordinary response whose body is already in memory.
//
// A 2xx body is read up to MaxBody and handed to Validate; a longer one or a
// rejection fails the round trip with a *GuardError. Any other status has its
// body truncated to MaxError (silently: the SDK decodes what it gets); a
// failed read of such a body is not a failure either: the answer goes up
// with the bytes read before the failure, so the status still decides its
// class. The original body is closed on every path. Every error the Guard
// itself returns, a failed 2xx body read included, is a *GuardError;
// transport errors from Next pass through untouched.
type Guard struct {
	Next     http.RoundTripper // nil: http.DefaultTransport
	MaxBody  int64             // 2xx bodies; zero is 16 MiB
	MaxError int64             // non-2xx bodies; zero is 64 KiB
	// Validate inspects a fully read 2xx body (any 2xx, an empty 204
	// included). A non-nil error (wrap ErrSyntax for a garbled or
	// wrong-envelope body) fails the round trip with a *GuardError.
	Validate func(req *http.Request, status int, body []byte) error
	// Header, if set, runs first on a clone of the request (a bearer
	// token); its error fails the round trip with a *GuardError.
	Header func(req *http.Request) error
}

// GuardError marks a Guard failure. http.Client wraps it in a *url.Error (a
// net.Error, which Transport would read as a retryable connection failure) and
// SDKs wrap further; Cause recovers the cause.
type GuardError struct{ Err error }

func (e *GuardError) Error() string { return "response guard: " + e.Err.Error() }
func (e *GuardError) Unwrap() error { return e.Err }

// TooLargeError is the cause of a Guard failure on a 2xx body longer than
// MaxBody (a client may answer it by asking for less).
type TooLargeError struct{ Limit int64 }

func (e *TooLargeError) Error() string { return fmt.Sprintf("response larger than %d bytes", e.Limit) }

// Cause returns the cause of the *GuardError err holds (through its Unwrap
// chain), or err unchanged when it holds none. Normalize uses it so the
// url.Error around the failure is not mistaken for a transport failure.
func Cause(err error) error {
	var ge *GuardError
	if errors.As(err, &ge) {
		return ge.Err
	}
	return err
}

// Capture asks the Guard, through the request context (WithCapture), for the
// request URL and delivered response. Status and Header are set only when
// a response is handed upward, after bounding its body and validating a 2xx.
// Replace is what the SDK decodes instead (a minimal envelope). One Capture
// serves one attempt and is never shared
// across goroutines.
type Capture struct {
	URL     *url.URL    // copied before sending, even if no response is delivered
	Status  int         // the response delivered through the Guard, zero on failure
	Header  http.Header // the delivered response headers
	Body    []byte      // set by the Guard: the validated 2xx body
	Replace []byte      // set by Validate's owner: handed upward instead of Body
}

type captureKey struct{}

// WithCapture returns a context that makes a Guard fill c.
func WithCapture(ctx context.Context, c *Capture) context.Context {
	return context.WithValue(ctx, captureKey{}, c)
}

// RoundTrip implements http.RoundTripper. The watch (source.SetWatch) sees each
// round trip, its body read included.
func (g *Guard) RoundTrip(req *http.Request) (*http.Response, error) {
	w := source.Watching()
	if w == nil || w.Exchange == nil {
		return g.roundTrip(req)
	}
	t0 := time.Now()
	resp, err := g.roundTrip(req)
	e := source.Exchange{Method: req.Method, URL: req.URL, Err: err, Latency: time.Since(t0)}
	if resp != nil {
		e.Status = resp.StatusCode
	}
	w.Exchange(e)
	return resp, err
}

func (g *Guard) roundTrip(req *http.Request) (*http.Response, error) {
	c, _ := req.Context().Value(captureKey{}).(*Capture)
	if c != nil {
		u := *req.URL
		c.URL = &u
	}
	if t, _ := req.Context().Value(targetKey{}).(*target); t != nil {
		u := *req.URL
		t.url.Store(&u)
	}
	if g.Header != nil {
		req = req.Clone(req.Context()) // the caller's request is not ours to change
		if err := g.Header(req); err != nil {
			if req.Body != nil {
				_ = req.Body.Close() // the RoundTripper contract
			}
			return nil, &GuardError{Err: err}
		}
	}
	next := g.Next
	if next == nil {
		next = http.DefaultTransport
	}
	resp, err := next.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	ok := resp.StatusCode >= 200 && resp.StatusCode < 300
	limit := orDefault(g.MaxError, defaultMaxError)
	if ok {
		limit = orDefault(g.MaxBody, defaultMaxBody)
	}
	// A declared length within the limit is read into one allocation (the
	// MinRead spare keeps ReadFrom from growing it for the final EOF read);
	// otherwise the buffer grows as io.ReadAll's would.
	size := int64(bytes.MinRead)
	if n := resp.ContentLength; n >= 0 && n <= limit {
		size += n
	}
	buf := bytes.NewBuffer(make([]byte, 0, size))
	_, err = buf.ReadFrom(io.LimitReader(resp.Body, limit+1))
	body := buf.Bytes()
	if err != nil && ok {
		// Wrapped, so Cause strips the url.Error around it: a cut body
		// (io.ErrUnexpectedEOF, a net.Error) is still a transport failure,
		// a malformed one (gzip.ErrHeader) is not.
		return nil, &GuardError{Err: err}
	}
	// A non-2xx body is diagnostic: a failed read keeps what arrived, so
	// the answer (its status, headers and the bytes read so far) is still
	// classified by its status. A cut 403 stays an authorization refusal,
	// a cut 503 a throttle.
	if int64(len(body)) > limit {
		if ok {
			return nil, &GuardError{Err: &TooLargeError{Limit: limit}}
		}
		body = body[:limit]
	}
	if ok {
		if g.Validate != nil {
			if err := g.Validate(req, resp.StatusCode, body); err != nil {
				return nil, &GuardError{Err: err}
			}
		}
		if c != nil {
			c.Body = body
			if c.Replace != nil {
				body = c.Replace
			}
		}
	}

	out := *resp
	out.Body = io.NopCloser(bytes.NewReader(body))
	out.ContentLength = int64(len(body))
	out.Header = resp.Header.Clone()
	if out.Header == nil {
		out.Header = http.Header{}
	}
	out.Header.Set("Content-Length", strconv.Itoa(len(body)))
	out.TransferEncoding = nil
	if c != nil {
		c.Status, c.Header = out.StatusCode, out.Header
	}
	return &out, nil
}

func orDefault(v, def int64) int64 {
	if v <= 0 {
		return def
	}
	return v
}
