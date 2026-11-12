// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package ranged

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
)

type roundTripper func(*http.Request) (*http.Response, error)

func (f roundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// TestTransport: a response to a range request passes only when it is a 206
// of exactly that range of a larger object, of that length, not decompressed
// in transit (varve's rangeTransport rule).
func TestTransport(t *testing.T) {
	type response struct {
		status        int
		contentRange  string
		contentLength int64
		uncompressed  bool
	}
	good := response{http.StatusPartialContent, "bytes 10-17/100", 8, false}
	for _, c := range []struct {
		name     string
		rng      string
		response response
		ok       bool
	}{
		{"exact", "bytes=10-17", good, true},
		{"last byte", "bytes=10-17", response{206, "bytes 10-17/18", 8, false}, true},
		{"no range requested", "", response{200, "", 100, false}, true},
		{"open range ignored", "bytes=10-", response{200, "", 100, false}, false},
		{"open range", "bytes=10-", response{206, "bytes 10-99/100", 90, false}, true},
		{"open range cut short", "bytes=10-", response{206, "bytes 10-17/100", 8, false}, false},
		{"open range shifted", "bytes=10-", response{206, "bytes 11-99/100", 89, false}, false},
		{"error status", "bytes=10-17", response{404, "", 9, false}, true},
		{"range ignored", "bytes=10-17", response{200, "", 100, false}, false},
		{"start shifted", "bytes=10-17", response{206, "bytes 11-18/100", 8, false}, false},
		{"end shifted", "bytes=10-17", response{206, "bytes 10-16/100", 8, false}, false},
		{"past the object", "bytes=10-17", response{206, "bytes 10-17/17", 8, false}, false},
		{"no Content-Range", "bytes=10-17", response{206, "", 8, false}, false},
		{"unknown total", "bytes=10-17", response{206, "bytes 10-17/*", 8, false}, false},
		{"wrong length", "bytes=10-17", response{206, "bytes 10-17/100", 7, false}, false},
		{"unknown length", "bytes=10-17", response{206, "bytes 10-17/100", -1, false}, false},
		{"decompressed", "bytes=10-17", response{206, "bytes 10-17/100", 8, true}, false},
	} {
		closed := false
		base := roundTripper(func(*http.Request) (*http.Response, error) {
			h := http.Header{}
			if c.response.contentRange != "" {
				h.Set("Content-Range", c.response.contentRange)
			}
			body := io.NopCloser(strings.NewReader("body"))
			return &http.Response{StatusCode: c.response.status, Status: http.StatusText(c.response.status), Header: h,
				ContentLength: c.response.contentLength, Uncompressed: c.response.uncompressed,
				Body: closer{body, &closed}}, nil
		})
		req, _ := http.NewRequest(http.MethodGet, "http://user:SECRET@store/bkt/ob%6A?sig=SECRET#SECRET", nil)
		if c.rng != "" {
			req.Header.Set("Range", c.rng)
		}
		res, err := Transport(base).RoundTrip(req)
		if c.ok != (err == nil) {
			t.Errorf("%s: %v", c.name, err)
		}
		if err == nil && res.StatusCode != c.response.status {
			t.Errorf("%s: status %d", c.name, res.StatusCode)
		}
		if err != nil && !strings.Contains(err.Error(), "GET http://store/bkt/ob%6A:") {
			t.Errorf("%s: request method and escaped URL missing: %v", c.name, err)
		}
		if err != nil && strings.Contains(err.Error(), "SECRET") {
			t.Errorf("%s: credential in error: %v", c.name, err)
		}
		if err != nil && !closed {
			t.Errorf("%s: refused body not closed", c.name)
		}
	}
}

type closer struct {
	io.Reader
	closed *bool
}

func (c closer) Close() error { *c.closed = true; return nil }

type failing struct{}

func (failing) Read(p []byte) (int, error) { return copy(p, "ab"), io.ErrUnexpectedEOF }
func (failing) Close() error               { return nil }

// TestTransportEndsFailedBody: a checked body that fails part way ends
// there, so that the SDK does not request the rest itself.
func TestTransportEndsFailedBody(t *testing.T) {
	base := roundTripper(func(*http.Request) (*http.Response, error) {
		h := http.Header{"Content-Range": {"bytes 0-3/10"}}
		return &http.Response{StatusCode: 206, Header: h, ContentLength: 4, Body: failing{}}, nil
	})
	req, _ := http.NewRequest(http.MethodGet, "http://user:SECRET@store/bkt/ob%6A?sig=SECRET#SECRET", nil)
	req.Header.Set("Range", "bytes=0-3")
	res, err := Transport(base).RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := res.Body.Read(make([]byte, 4)); n != 2 || err != io.EOF {
		t.Fatalf("Read = %d, %v; want 2, io.EOF", n, err)
	}
}

// TestReadObjectThroughTransport: a whole read through Transport, which
// ends a failed body as if it ended and keeps the failure: a failure with
// the last bytes, or after them, or a cancellation as the body ends, fails
// ReadObject, with no ranged read after it.
func TestReadObjectThroughTransport(t *testing.T) {
	data := []byte("0123456789")
	broken := errors.New("connection reset")
	for _, c := range []struct {
		name   string
		steps  func(cancel context.CancelFunc) []step
		cancel bool
	}{
		{"error with the last bytes", func(context.CancelFunc) []step { return []step{{b: data[:4]}, {b: data[4:], err: broken}} }, false},
		{"error after the last bytes", func(context.CancelFunc) []step { return []step{{b: data}, {err: broken}} }, false},
		{"cancelled as the body ends", func(cancel context.CancelFunc) []step {
			return []step{{b: data}, {err: context.Canceled, before: cancel}}
		}, true},
		// The body ends cleanly (io.EOF, no failure to keep) just after
		// the context is cancelled: only the context tells.
		{"cancelled before a clean end", func(cancel context.CancelFunc) []step {
			return []step{{b: data}, {err: io.EOF, before: cancel}}
		}, true},
	} {
		ctx, cancel := context.WithCancel(context.Background())
		base := roundTripper(func(r *http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 200, Header: http.Header{"Etag": {`"e"`}}, ContentLength: int64(len(data)),
				Body: &script{steps: c.steps(cancel)}}, nil
		})
		client := &http.Client{Transport: Transport(base)}
		ranged := 0
		get := func(ctx context.Context, off, n int64, pin Pin) (Response, error) {
			if n >= 0 {
				ranged++
				return Response{}, errors.New("no ranged read expected")
			}
			req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://store/bkt/obj", nil)
			res, err := client.Do(req)
			if err != nil {
				return Response{}, err
			}
			return Response{Body: res.Body, Pin: Pin{ETag: res.Header.Get("ETag")}, Start: -1, Total: -1, Length: res.ContentLength}, nil
		}
		b, _, err := ReadObject(ctx, "obj", 100, get)
		if err == nil || ranged != 0 {
			t.Errorf("%s: ReadObject = %q, %v after %d ranged reads; want an error and none", c.name, b, err, ranged)
		}
		if !c.cancel && !errors.Is(err, broken) {
			t.Errorf("%s: %v, want the body's failure", c.name, err)
		}
		if c.cancel && !errors.Is(err, context.Canceled) {
			t.Errorf("%s: %v, want context.Canceled", c.name, err)
		}
		if b != nil {
			t.Errorf("%s: %d bytes returned with the error", c.name, len(b))
		}
		cancel()
	}
}
