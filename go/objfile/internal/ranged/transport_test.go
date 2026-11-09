// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package ranged

import (
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
		{"open range requested", "bytes=10-", response{200, "", 100, false}, true},
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
		req, _ := http.NewRequest(http.MethodGet, "http://store/bkt/obj", nil)
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
