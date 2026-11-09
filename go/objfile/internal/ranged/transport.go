// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package ranged

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
)

// Transport checks every response to a request for one byte range,
// "Range: bytes=first-last", on its way to the SDK: unless it is a 206 whose
// Content-Range states exactly that range of an object larger than last,
// whose Content-Length is the range's length, and whose body Go's transport
// did not decompress, the request fails. A Getter checks the response it is
// given; Transport also checks the requests an SDK sends on its own, such as
// the GCS reader's request for the rest of a range after a body fails part
// way. Responses to other requests, and error statuses, pass unchanged.
func Transport(base http.RoundTripper) http.RoundTripper { return transport{base: base} }

type transport struct{ base http.RoundTripper }

func (t transport) RoundTrip(req *http.Request) (*http.Response, error) {
	res, err := t.base.RoundTrip(req)
	if err != nil || res.StatusCode < 200 || res.StatusCode > 299 {
		return res, err
	}
	first, last, ok := requested(req.Header.Get("Range"))
	if !ok {
		return res, nil
	}
	if err := served(res, first, last); err != nil {
		res.Body.Close()
		return nil, fmt.Errorf("objfile: %s %s: %w", req.Method, req.URL.Redacted(), err)
	}
	return res, nil
}

// requested parses a Range header naming one closed byte range.
func requested(v string) (first, last int64, ok bool) {
	rng, ok := strings.CutPrefix(v, "bytes=")
	a, b, ok2 := strings.Cut(rng, "-")
	if !ok || !ok2 {
		return 0, 0, false
	}
	first, err1 := strconv.ParseInt(a, 10, 64)
	last, err2 := strconv.ParseInt(b, 10, 64)
	return first, last, err1 == nil && err2 == nil && first >= 0 && last >= first
}

func served(res *http.Response, first, last int64) error {
	if res.StatusCode != http.StatusPartialContent {
		return fmt.Errorf("the store answered a range request with %s", res.Status)
	}
	cr := res.Header.Get("Content-Range")
	a, b, total, ok := ContentRange(cr)
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
