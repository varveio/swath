// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package azure

import (
	"unicode/utf16"

	"github.com/varveio/swath/internal/replay/latency"
)

// Shape is the request's latency class: a delimiter request is a
// structure probe, a delimiter-free request for one result a pivot probe,
// anything else a worker page.
func (r *Request) Shape() latency.Shape {
	return latency.Classify(r.Delimiter != "", int64(r.PageSize))
}

// LatencyKey identifies the request for latency jitter: a hash of prefix,
// delimiter, startFrom, marker and page size (the client request id is
// left out: it changes per call, not per page). It is the Java server's
// key for the same request, so both servers jitter a request alike
// (TestLatencyKeyGolden).
func (r *Request) LatencyKey() int64 {
	h := int32(1)
	for _, s := range []string{r.Prefix, r.Delimiter, r.StartFrom, r.Marker} {
		h = 31*h + stringHash(s)
	}
	return int64(31*h + int32(r.PageSize))
}

// stringHash is the polynomial hash of a string's UTF-16 code units.
func stringHash(s string) int32 {
	var h int32
	for _, u := range utf16.Encode([]rune(s)) {
		h = 31*h + int32(u)
	}
	return h
}
