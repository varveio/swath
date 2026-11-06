// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package gcs is the replay server's Cloud Storage JSON API route:
// objects.list at GET /storage/v1/b/{bucket}/o. It parses the request
// strictly, pages it over a store.Store with the API's range semantics
// (inclusive startOffset, exclusive endOffset, a delimiter rollup whose
// prefixes are emitted even when startOffset lands inside them), issues
// gcs1. page tokens bound to the fixture and the request's scope, and
// renders the objects resource and the JSON error envelope.
//
// The wire contract is the Java replay server's: the same parameters are
// accepted and refused for the same reasons, the same synthetic metadata is
// rendered, and the same state yields the same token bytes, so a token from
// either server resumes on the other (TestTokenGolden, TestRenderGolden).
package gcs

import (
	"bytes"
	"strconv"
	"unicode/utf16"
	"unicode/utf8"
)

// MaxPageSize is the most entries one page holds, objects and prefixes
// together, whatever maxResults asks: the service's own cap.
const MaxPageSize = 1000

// Field caps: a value longer than these is refused as unsupported. An
// object name is at most 1,024 bytes; a page token is the replay's own
// and needs at most 2 + 16 + 1,024 bytes before base64.
const (
	maxFieldBytes = 1024
	maxTokenBytes = 2048
)

// Request is a parsed objects.list request. The byte fields are the
// decoded parameters (valid UTF-8; nil when absent or empty).
type Request struct {
	Bucket      string
	Prefix      []byte
	Delimiter   []byte
	StartOffset []byte
	EndOffset   []byte
	// PageToken is the pageToken parameter, "" when absent or empty.
	PageToken string
	// PageSize is maxResults clamped to MaxPageSize (1,000 when absent);
	// Clamped says the client asked for more.
	PageSize int
	Clamped  bool
	// FullProjection is projection=full: every item carries an empty acl.
	FullProjection bool
}

// Failure is a request the route refuses: Unsupported for a recognized
// feature the replay does not serve (an unsupported feature, a field too
// long), otherwise malformed. Reason is sent in x-swath-replay-error.
type Failure struct {
	Unsupported bool
	Reason      string
}

func (f *Failure) Error() string { return "gcs request refused: " + f.Reason }

func malformed(reason string) *Failure   { return &Failure{Reason: reason} }
func unsupported(reason string) *Failure { return &Failure{Unsupported: true, Reason: reason} }

// allowed are the parameters the route recognizes; any other, or any
// parameter given twice, is refused.
var allowed = map[string]bool{
	"prefix": true, "delimiter": true, "startOffset": true, "endOffset": true, "maxResults": true,
	"pageToken": true, "projection": true, "versions": true, "softDeleted": true,
	"includeFoldersAsPrefixes": true, "includeTrailingDelimiter": true, "matchGlob": true,
	"filter": true, "fields": true, "prettyPrint": true, "alt": true,
}

// Parse parses the raw query string (as sent on the request line) of an
// objects.list request for bucket.
//
// The checks run in the wire contract's order, so a request with several
// faults reports the same reason from either server: unknown or repeated
// names and bad escapes while splitting (left to right), then the boolean
// features (versions, softDeleted, includeFoldersAsPrefixes,
// includeTrailingDelimiter: anything but "false"), then matchGlob, filter
// and fields (anything non-empty), alt (only "json"), prettyPrint ("true"
// or "false", accepted and otherwise ignored: the body is always compact),
// projection ("noAcl" or "full"), maxResults, and the field lengths.
func Parse(bucket, rawQuery string) (*Request, error) {
	params := map[string][]byte{}
	if rawQuery != "" {
		for _, part := range splitAll(requestText(rawQuery), '&') {
			name, value, _ := bytes.Cut(part, []byte{'='})
			n, err := decode(name)
			if err != nil {
				return nil, err
			}
			v, err := decode(value)
			if err != nil {
				return nil, err
			}
			if !allowed[string(n)] {
				return nil, malformed("unknown_or_duplicate_parameter")
			}
			if _, dup := params[string(n)]; dup {
				return nil, malformed("unknown_or_duplicate_parameter")
			}
			params[string(n)] = v
		}
	}
	for _, name := range []string{"versions", "softDeleted", "includeFoldersAsPrefixes", "includeTrailingDelimiter"} {
		if v, ok := params[name]; ok && string(v) != "false" {
			return nil, unsupported("feature_" + name)
		}
	}
	for _, name := range []string{"matchGlob", "filter", "fields"} {
		if v, ok := params[name]; ok && len(v) > 0 {
			return nil, unsupported("feature_" + name)
		}
	}
	if v, ok := params["alt"]; ok && string(v) != "json" {
		return nil, unsupported("alt_format")
	}
	if v, ok := params["prettyPrint"]; ok && string(v) != "true" && string(v) != "false" {
		return nil, malformed("invalid_pretty_print")
	}
	projection := "noAcl"
	if v, ok := params["projection"]; ok {
		projection = string(v)
	}
	if projection != "noAcl" && projection != "full" {
		return nil, malformed("invalid_projection")
	}
	r := &Request{Bucket: bucket, PageSize: MaxPageSize, FullProjection: projection == "full"}
	if v, ok := params["maxResults"]; ok {
		n, ok := parseMaxResults(v)
		if !ok {
			return nil, malformed("invalid_max_results")
		}
		r.PageSize, r.Clamped = int(min(n, MaxPageSize)), n > MaxPageSize
	}
	var err error
	for _, f := range []struct {
		name string
		dst  *[]byte
	}{{"prefix", &r.Prefix}, {"delimiter", &r.Delimiter}, {"startOffset", &r.StartOffset}, {"endOffset", &r.EndOffset}} {
		if *f.dst, err = field(params, f.name, maxFieldBytes); err != nil {
			return nil, err
		}
	}
	tok, err := field(params, "pageToken", maxTokenBytes)
	if err != nil {
		return nil, err
	}
	r.PageToken = string(tok)
	return r, nil
}

// parseMaxResults accepts a positive decimal integer without a sign or
// leading zero that fits in an int64; zero and everything else are
// refused.
func parseMaxResults(v []byte) (int64, bool) {
	if len(v) == 0 || v[0] == '0' {
		return 0, false
	}
	for _, c := range v {
		if c < '0' || c > '9' {
			return 0, false
		}
	}
	n, err := strconv.ParseInt(string(v), 10, 64)
	return n, err == nil
}

// field returns a parameter's value, nil when absent or empty, refusing a
// value over limit bytes as unsupported.
func field(params map[string][]byte, name string, limit int) ([]byte, error) {
	v := params[name]
	if len(v) == 0 {
		return nil, nil
	}
	if len(v) > limit {
		return nil, unsupported("field_too_long_" + name)
	}
	return v, nil
}

func splitAll(s []byte, sep byte) [][]byte {
	return bytes.Split(s, []byte{sep})
}

// decode percent-decodes one query component: %XX with ASCII hex digits
// is a byte, a lone or short % is refused, and the result must be valid
// UTF-8 (overlong forms, surrogates and code points above U+10FFFF are
// refused). A raw '+' stays a '+'.
func decode(raw []byte) ([]byte, error) {
	if bytes.IndexByte(raw, '%') < 0 {
		if !utf8.Valid(raw) {
			return nil, malformed("invalid_utf8")
		}
		return raw, nil
	}
	out := make([]byte, 0, len(raw))
	for i := 0; i < len(raw); i++ {
		c := raw[i]
		if c != '%' {
			out = append(out, c)
			continue
		}
		if i+2 >= len(raw) {
			return nil, malformed("invalid_percent_escape")
		}
		hi, lo := unhex(raw[i+1]), unhex(raw[i+2])
		if hi < 0 || lo < 0 {
			return nil, malformed("invalid_percent_escape")
		}
		out = append(out, byte(hi<<4|lo))
		i += 2
	}
	if !utf8.Valid(out) {
		return nil, malformed("invalid_utf8")
	}
	return out, nil
}

func unhex(c byte) int {
	switch {
	case c >= '0' && c <= '9':
		return int(c - '0')
	case c >= 'a' && c <= 'f':
		return int(c-'a') + 10
	case c >= 'A' && c <= 'F':
		return int(c-'A') + 10
	}
	return -1
}

// requestText is the text the replay wire contract reads a raw query as on
// this route: the request line's bytes decoded as UTF-8, each maximal
// ill-formed subsequence (the WHATWG grouping) becoming one U+FFFD, valid
// sequences kept as they are. A raw é arrives as é; a raw 0xFF byte as
// U+FFFD. Go's own decoders group ill-formed bytes differently (a
// truncated sequence is one replacement per byte), which would change a
// prefix and so the listing (TestRequestTextGrouping).
func requestText(raw string) []byte {
	out := make([]byte, 0, len(raw))
	const replacement = "\uFFFD"
	needed, seen, start := 0, 0, 0
	lower, upper := byte(0x80), byte(0xbf)
	for i := 0; i < len(raw); i++ {
		b := raw[i]
		if needed == 0 {
			switch {
			case b < 0x80:
				out = append(out, b)
			case b >= 0xc2 && b <= 0xdf:
				needed, start = 1, i
			case b >= 0xe0 && b <= 0xef:
				if b == 0xe0 {
					lower = 0xa0
				} else if b == 0xed {
					upper = 0x9f
				}
				needed, start = 2, i
			case b >= 0xf0 && b <= 0xf4:
				if b == 0xf0 {
					lower = 0x90
				} else if b == 0xf4 {
					upper = 0x8f
				}
				needed, start = 3, i
			default:
				out = append(out, replacement...)
			}
			continue
		}
		if b < lower || b > upper {
			// The sequence so far is one replacement; b starts afresh.
			out = append(out, replacement...)
			needed, seen, lower, upper = 0, 0, 0x80, 0xbf
			i--
			continue
		}
		lower, upper = 0x80, 0xbf
		seen++
		if seen == needed {
			out = append(out, raw[start:i+1]...)
			needed, seen = 0, 0
		}
	}
	if needed != 0 {
		out = append(out, replacement...)
	}
	return out
}

// LatencyKey identifies the request for latency jitter: the wire
// contract's hash of prefix, delimiter, startOffset, endOffset, page size
// and token (a polynomial over their UTF-16 text, absent fields 0), so a
// request draws the same jitter from either server and successive pages
// of one walk draw different ones (TestLatencyKeyGolden).
func (r *Request) LatencyKey() int64 {
	h := int32(1)
	for _, s := range [][]byte{r.Prefix, r.Delimiter, r.StartOffset, r.EndOffset} {
		h = 31*h + textHash(s)
	}
	h = 31*h + int32(r.PageSize)
	h = 31*h + textHash([]byte(r.PageToken))
	return int64(h)
}

// textHash is the polynomial hash of a text's UTF-16 code units, 0 for an
// absent (nil or empty) field.
func textHash(b []byte) int32 {
	var h int32
	for _, u := range utf16.Encode([]rune(string(b))) {
		h = 31*h + int32(u)
	}
	return h
}
