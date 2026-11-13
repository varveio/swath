// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package ranged

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
)

// Transport checks every response to a request for one byte range,
// "Range: bytes=first-last", on its way to the SDK: unless it is a 206 whose
// Content-Range states exactly that range of an object larger than last,
// whose Content-Length is the range's length, and whose body Go's transport
// did not decompress, the request fails. A request for the rest of an object,
// "bytes=first-" (which the GCS SDK sends for the rest of a whole-object read
// that is not marked as below), is checked alike, its range being first to the object's last
// byte. A response cut at a shorter object's end is refused with a
// *StatedTotal, which the Object classifies. The response to a request for
// a whole object that the Object marks is passed with its body ending where
// it fails, so that the SDK does not request the rest itself, and the
// failure kept for the Object. A Getter checks the response it is
// given; Transport also checks the requests an SDK sends on its own.
//
// A body that fails part way ends there instead (io.EOF), so that the SDK
// does not request the rest itself: the GCS reader would, and for a response
// labelled Content-Encoding: gzip it then skips as many bytes of that answer
// as it had read, joining two parts of the object. The Object requests the
// rest of a short body itself, checked as every request is. Responses to
// other requests, and error statuses, pass unchanged.
func Transport(base http.RoundTripper) http.RoundTripper { return transport{base: base} }

type transport struct{ base http.RoundTripper }

func (t transport) RoundTrip(req *http.Request) (*http.Response, error) {
	res, err := t.base.RoundTrip(req)
	if err != nil || res.StatusCode < 200 || res.StatusCode > 299 {
		return res, err
	}
	rng := req.Header.Get("Range")
	if failure := wholeReadOf(req.Context()); rng == "" && failure != nil {
		// A whole-object read: its body ends where it fails, so that the
		// SDK does not request the rest itself, and the failure is kept
		// for ReadObject, which reads the rest (or fails) itself.
		if res.Uncompressed {
			err := fmt.Errorf("objfile: %s %s://%s%s: the response was decompressed in transit", req.Method, req.URL.Scheme, req.URL.Host, req.URL.EscapedPath())
			rejectResponse(req, err)
			res.Body.Close()
			return nil, err
		}
		res.Body = recordFailure{ReadCloser: res.Body, failure: failure}
		return res, nil
	}
	first, last, ok := requested(rng)
	if !ok {
		return res, nil
	}
	if err := served(res, first, last); err != nil {
		rejectResponse(req, err)
		res.Body.Close()
		err = fmt.Errorf("objfile: %s %s://%s%s: %w", req.Method, req.URL.Scheme, req.URL.Host, req.URL.EscapedPath(), err)
		if total, ok := clipped(res, first, last); ok {
			// A range cut at a shorter object's end: refused, as the SDK
			// would read on past it, with the total the Object classifies.
			err = &StatedTotal{Total: total, Err: err}
		}
		return nil, err
	}
	res.Body = endOnFailure{res.Body}
	return res, nil
}

// CloseIdleConnections closes the idle connections of the base transport,
// as http.Client.CloseIdleConnections asks.
func (t transport) CloseIdleConnections() {
	if c, ok := t.base.(interface{ CloseIdleConnections() }); ok {
		c.CloseIdleConnections()
	}
}

// recordFailure is a body whose read failure is its end, the failure kept
// in failure.
type recordFailure struct {
	io.ReadCloser
	failure *bodyFailure
}

func (b recordFailure) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if err != nil && err != io.EOF {
		b.failure.set(err)
		err = io.EOF
	}
	return n, err
}

// clipped reports whether res is a 206 of the range first..last cut at the
// object's end (it states that end and total), and the total it states.
func clipped(res *http.Response, first, last int64) (int64, bool) {
	a, b, total, ok := ContentRange(res.Header.Get("Content-Range"))
	return total, ok && res.StatusCode == http.StatusPartialContent && last >= 0 && a == first &&
		b < last && b == total-1 && res.ContentLength == b-a+1
}

// endOnFailure is a body whose read failure is its end.
type endOnFailure struct{ io.ReadCloser }

func (b endOnFailure) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if err != nil {
		err = io.EOF
	}
	return n, err
}

// requested parses a Range header naming one byte range, closed or to the
// object's end (last -1).
func requested(v string) (first, last int64, ok bool) {
	rng, ok := strings.CutPrefix(v, "bytes=")
	a, b, ok2 := strings.Cut(rng, "-")
	if !ok || !ok2 {
		return 0, 0, false
	}
	first, err1 := strconv.ParseInt(a, 10, 64)
	if b == "" {
		return first, -1, err1 == nil && first >= 0
	}
	last, err2 := strconv.ParseInt(b, 10, 64)
	return first, last, err1 == nil && err2 == nil && first >= 0 && last >= first
}

func served(res *http.Response, first, last int64) error {
	if res.StatusCode != http.StatusPartialContent {
		return fmt.Errorf("the store answered a range request with %s", res.Status)
	}
	cr := res.Header.Get("Content-Range")
	a, b, total, ok := ContentRange(cr)
	if ok && last < 0 && b == total-1 {
		last = b // to the end: the object's last byte
	}
	if !ok || a != first || b != last || total <= last {
		return fmt.Errorf("the store served Content-Range %q for bytes %d-%d", cr, first, last)
	}
	if res.ContentLength != last-first+1 {
		return fmt.Errorf("the store served %d bytes for a range of %d", res.ContentLength, last-first+1)
	}
	if res.Uncompressed {
		return errors.New("the response was decompressed in transit")
	}
	return nil
}
