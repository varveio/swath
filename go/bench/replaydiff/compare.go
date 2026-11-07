// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"io"
	"maps"
	"net/http"
	"regexp"
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

// liveComparator compares the replay server with live S3, structurally:
// the same status; for an error the same <Code>; for a listing the same
// sequence of leaf elements, with continuation tokens (opaque and
// server-specific) masked and the ListBucketResult's formatting ignored.
// Request-id headers are not compared. Live S3 drifts (new response
// elements, changed objects), so a difference here is a lead, not a
// verdict.
type liveComparator struct{}

var tokenRe = regexp.MustCompile(`<(Next)?ContinuationToken>[^<]*</(Next)?ContinuationToken>`)

func (liveComparator) compare(ref, got *response) diff {
	d := diff{Status: [2]int{ref.Status, got.Status}, Offset: -1}
	d.HeaderDiffs = headerDiffs(ref.Header, got.Header, func(n string) bool {
		n = strings.ToLower(n)
		return n != "content-type" // S3's own x-amz-* headers are per request
	})
	if ref.Status != got.Status {
		return d
	}
	a, errA := leaves(tokenRe.ReplaceAll(ref.Body, []byte("<NextContinuationToken>T</NextContinuationToken>")))
	b, errB := leaves(tokenRe.ReplaceAll(got.Body, []byte("<NextContinuationToken>T</NextContinuationToken>")))
	if errA != nil || errB != nil {
		d.Detail = fmt.Sprintf("unparsable body: %v / %v", errA, errB)
		return d
	}
	if ref.Status != 200 {
		a, b = codeOnly(a), codeOnly(b)
	}
	if !slices.Equal(a, b) {
		i := 0
		for i < min(len(a), len(b)) && a[i] == b[i] {
			i++
		}
		d.Offset = i
		d.RefExcerpt, d.GotExcerpt = at(a, i), at(b, i)
		d.Detail = "leaf element sequences differ at element " + fmt.Sprint(i)
	}
	d.OK = d.Offset < 0 && len(d.HeaderDiffs) == 0
	return d
}

func at(s []string, i int) string {
	if i < len(s) {
		return s[i]
	}
	return "<end>"
}

func codeOnly(l []string) []string {
	for _, s := range l {
		if strings.HasPrefix(s, "Error/Code=") {
			return []string{s}
		}
	}
	return nil
}

// leaves flattens an XML document to "path=text" for every leaf element.
func leaves(b []byte) ([]string, error) {
	dec := xml.NewDecoder(bytes.NewReader(b))
	var path []string
	var text strings.Builder
	var out []string
	leaf := false
	for {
		tok, err := dec.Token()
		if err == io.EOF {
			return out, nil
		}
		if err != nil {
			return out, err
		}
		switch t := tok.(type) {
		case xml.StartElement:
			path = append(path, t.Name.Local)
			text.Reset()
			leaf = true
		case xml.CharData:
			text.Write(t)
		case xml.EndElement:
			if leaf {
				out = append(out, strings.Join(path, "/")+"="+text.String())
			}
			leaf = false
			path = path[:len(path)-1]
		}
	}
}
