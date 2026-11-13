// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"fmt"
	"maps"
	"net/http"
	"slices"
	"strings"
)

// response is what a comparison sees of one answer.
type response struct {
	Status int
	Header http.Header
	Body   []byte
}

// diff is a comparison's verdict.
type diff struct {
	OK          bool     `json:"ok"`
	Status      [2]int   `json:"status"`
	HeaderDiffs []string `json:"header_diffs,omitempty"`
	// Offset is the first differing body byte, -1 when the bodies agree.
	Offset     int    `json:"first_diff_offset"`
	RefExcerpt string `json:"ref_excerpt,omitempty"`
	GotExcerpt string `json:"got_excerpt,omitempty"`
	Detail     string `json:"detail,omitempty"`
}

// comparator decides whether two answers are the same. A route that
// cannot promise byte identity (a live service with opaque tokens and
// request ids) supplies a structural one.
type comparator interface {
	compare(ref, got *response) diff
}

// compared reports whether a header takes part in the comparison:
// Content-Type and every x-amz-* and x-swath-replay-* header. Date, Server
// and the framing headers (Content-Length against chunked) are transport,
// not answer.
func compared(name string) bool {
	n := strings.ToLower(name)
	return n == "content-type" || strings.HasPrefix(n, "x-amz-") || strings.HasPrefix(n, "x-swath-replay-")
}

// headerDiffs lists the compared headers whose values differ, as
// "name: ref | got".
func headerDiffs(ref, got http.Header, ignore func(string) bool) []string {
	names := map[string]bool{}
	for _, h := range []http.Header{ref, got} {
		for k := range h {
			if compared(k) && (ignore == nil || !ignore(k)) {
				names[strings.ToLower(k)] = true
			}
		}
	}
	var out []string
	for _, n := range slices.Sorted(maps.Keys(names)) {
		a, b := values(ref, n), values(got, n)
		if a != b {
			out = append(out, fmt.Sprintf("%s: %q | %q", n, a, b))
		}
	}
	return out
}

// values joins a header's values, looked up without regard to case (the
// servers spell names differently on the wire).
func values(h http.Header, name string) string {
	var v []string
	for k, vs := range h {
		if strings.EqualFold(k, name) {
			v = append(v, vs...)
		}
	}
	return strings.Join(v, ",")
}

// bytesComparator requires the same status, the same compared headers and
// the same body bytes: what "identical" means between two replay servers.
type bytesComparator struct{}

func (bytesComparator) compare(ref, got *response) diff {
	d := diff{Status: [2]int{ref.Status, got.Status}, Offset: -1}
	d.HeaderDiffs = headerDiffs(ref.Header, got.Header, nil)
	if !bytes.Equal(ref.Body, got.Body) {
		d.Offset = firstDiff(ref.Body, got.Body)
		d.RefExcerpt, d.GotExcerpt = excerpt(ref.Body, d.Offset), excerpt(got.Body, d.Offset)
	}
	d.OK = ref.Status == got.Status && len(d.HeaderDiffs) == 0 && d.Offset < 0
	return d
}

func firstDiff(a, b []byte) int {
	n := min(len(a), len(b))
	for i := range n {
		if a[i] != b[i] {
			return i
		}
	}
	return n
}

// excerpt is the bytes around offset, quoted so control bytes stay
// readable.
func excerpt(b []byte, offset int) string {
	lo, hi := max(0, offset-80), min(len(b), offset+80)
	return fmt.Sprintf("%q", b[lo:hi])
}
