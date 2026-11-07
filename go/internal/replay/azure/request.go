// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package azure

import (
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

// The service versions the route answers; any other x-ms-version is
// refused with InvalidHeaderValue.
const (
	Version20260606 = "2026-06-06"
	Version20261006 = "2026-10-06"
)

// Page-size bounds of maxresults.
const (
	DefaultPageSize = 5000
	MaxPageSize     = 5000
)

// Field-length bounds, in UTF-8 bytes.
const (
	maxFieldBytes       = 4096
	maxClientRequestID  = 1024
	maxTimeoutSeconds   = 3600
	defaultMarkerLength = 8192
)

// Request is a parsed flat List Blobs request. An absent or empty string
// field is "" (the route treats an empty value as absent); the Supplied
// flags say whether the parameter was in the query at all, which decides
// the response's echoes.
type Request struct {
	Account, Container string
	Version            string
	Prefix, Delimiter  string
	StartFrom, Marker  string
	PageSize           int
	// MaxResults is the maxresults value as sent, echoed in MaxResults.
	MaxResults                                                       string
	PrefixSupplied, DelimiterSupplied, MarkerSupplied, MaxResultsSet bool
	// ClientRequestID is x-ms-client-request-id when HasClientRequestID.
	ClientRequestID    string
	HasClientRequestID bool
}

// allowed are the query parameters the route reads; any other is refused.
var allowed = map[string]bool{
	"restype": true, "comp": true, "prefix": true, "delimiter": true, "marker": true,
	"maxresults": true, "startFrom": true, "timeout": true, "include": true,
}

// header returns a request header's value (the last one sent) and whether
// it was sent more than once.
func header(h http.Header, name string) (value string, present, duplicate bool) {
	vs := h.Values(name)
	if len(vs) == 0 {
		return "", false, false
	}
	return vs[len(vs)-1], true, len(vs) > 1
}

// Parse validates a request the route has claimed: the path must be
// /{account}/{container} exactly (ContainerNotFound otherwise), HEAD is
// refused, any other method but GET is UnsupportedHttpVerb, then the
// headers and the query are checked in the order the replay wire contract
// checks them, so the first problem found is the one reported
// (TestParseRefusalOrder). path is the request path as sent and rawQuery
// the query as sent.
func Parse(method, path, rawQuery string, h http.Header, account, container string) (*Request, error) {
	if path != "/"+account+"/"+container {
		return nil, fail(NotFound, "container_not_found")
	}
	if method == http.MethodHead {
		return nil, fail(Unsupported, "container_head_unsupported")
	}
	if method != http.MethodGet {
		return nil, fail(WrongMethod, "wrong_method")
	}
	_, _, dupVersion := header(h, "x-ms-version")
	_, _, dupClient := header(h, "x-ms-client-request-id")
	if dupVersion || dupClient {
		return nil, fail(Malformed, "duplicate_header")
	}
	version, _, _ := header(h, "x-ms-version")
	if version != Version20260606 && version != Version20261006 {
		return nil, fail(Unsupported, "invalid_header_value")
	}
	accept, _, _ := header(h, "accept")
	if !acceptsXMLOnly(accept) {
		return nil, fail(Unsupported, "unsupported_accept")
	}
	clientID, hasClientID, _ := header(h, "x-ms-client-request-id")
	if hasClientID && !ValidClientRequestID(clientID) {
		return nil, fail(Malformed, "invalid_client_request_id")
	}
	params := map[string]string{}
	if rawQuery != "" {
		for _, part := range strings.Split(requestLineText(rawQuery), "&") {
			name, value, _ := strings.Cut(part, "=")
			key, err := decodeComponent(name)
			if err != nil {
				return nil, err
			}
			val, err := decodeComponent(value)
			if err != nil {
				return nil, err
			}
			if !allowed[key] {
				return nil, fail(Unsupported, "unsupported_query_parameter")
			}
			if _, dup := params[key]; dup {
				return nil, fail(Malformed, "duplicate_query_parameter")
			}
			params[key] = val
		}
	}
	if params["restype"] != "container" || params["comp"] != "list" {
		return nil, fail(Malformed, "invalid_selector")
	}
	if params["include"] != "" {
		return nil, fail(Unsupported, "unsupported_include")
	}
	if v, ok := params["timeout"]; ok {
		t, ok := decimal(v)
		if !ok || t <= 0 || t > maxTimeoutSeconds {
			return nil, fail(Malformed, "invalid_timeout")
		}
	}
	r := &Request{Account: account, Container: container, Version: version, PageSize: DefaultPageSize,
		ClientRequestID: clientID, HasClientRequestID: hasClientID}
	for _, f := range []struct {
		name string
		dst  *string
		max  int
	}{{"prefix", &r.Prefix, maxFieldBytes}, {"delimiter", &r.Delimiter, maxFieldBytes},
		{"startFrom", &r.StartFrom, maxFieldBytes}, {"marker", &r.Marker, defaultMarkerLength}} {
		if len(params[f.name]) > f.max {
			return nil, fail(Unsupported, "query_field_too_long")
		}
		*f.dst = params[f.name]
	}
	if invalidXMLEcho(r.Prefix) || invalidXMLEcho(r.Delimiter) {
		return nil, fail(Unsupported, "xml_query_echo_unmeasured")
	}
	_, r.PrefixSupplied = params["prefix"]
	_, r.DelimiterSupplied = params["delimiter"]
	_, r.MarkerSupplied = params["marker"]
	if raw, ok := params["maxresults"]; ok {
		r.MaxResults, r.MaxResultsSet = raw, true
		if negativeDecimal.MatchString(raw) {
			return nil, fail(Malformed, "maxresults_out_of_range")
		}
		n, ok := decimal(raw)
		if !ok {
			return nil, fail(Malformed, "invalid_maxresults")
		}
		if n < 1 {
			return nil, fail(Malformed, "maxresults_out_of_range")
		}
		r.PageSize = int(min(n, MaxPageSize))
	}
	return r, nil
}

var negativeDecimal = regexp.MustCompile(`^-[0-9]+$`)

// decimal reads an unsigned ASCII decimal of at most ten digits that fits
// an int32.
func decimal(v string) (int64, bool) {
	if v == "" || len(v) > 10 || strings.Trim(v, "0123456789") != "" {
		return 0, false
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n > 1<<31-1 {
		return 0, false
	}
	return n, true
}

// ValidClientRequestID is the rule for x-ms-client-request-id, applied
// both to refuse a request and to decide whether an error echoes the id:
// at most 1,024 bytes once the header's bytes are read as Latin-1 text and
// written as UTF-8 (each byte at or above 0x80 counts two), and no CR or
// LF. The HTTP layer of the replay wire contract reads header bytes as
// Latin-1, so 513 raw 0xE9 bytes are 1,026 (TestClientRequestIDLength).
func ValidClientRequestID(v string) bool {
	n := len(v)
	for i := 0; i < len(v); i++ {
		if v[i] >= 0x80 {
			n++
		}
	}
	return n <= maxClientRequestID && !strings.ContainsAny(v, "\r\n")
}

// decodeComponent percent-decodes one query component strictly: a '%'
// must be followed by two hex digits, and the decoded bytes must be valid
// UTF-8.
func decodeComponent(raw string) (string, error) {
	out := make([]byte, 0, len(raw))
	for i := 0; i < len(raw); i++ {
		c := raw[i]
		if c != '%' {
			out = append(out, c)
			continue
		}
		if i+2 >= len(raw) {
			return "", fail(Malformed, "invalid_percent_escape")
		}
		hi, lo := unhex(raw[i+1]), unhex(raw[i+2])
		if hi < 0 || lo < 0 {
			return "", fail(Malformed, "invalid_percent_escape")
		}
		out = append(out, byte(hi<<4|lo))
		i += 2
	}
	if !utf8.Valid(out) {
		return "", fail(Malformed, "invalid_utf8")
	}
	return string(out), nil
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

// invalidXMLEcho reports a value the XML echo cannot carry as measured: a
// control character other than TAB and LF, or U+FFFE / U+FFFF.
func invalidXMLEcho(v string) bool {
	for _, r := range v {
		if r < 0x20 && r != '\t' && r != '\n' || r == 0xfffe || r == 0xffff {
			return true
		}
	}
	return false
}

// acceptsXMLOnly is the Accept rule: absent or blank accepts; otherwise
// some media range must be application/xml or */* with a quality above
// zero, no quality may be malformed or outside [0, 1], and the Arrow
// stream type is refused outright.
func acceptsXMLOnly(accept string) bool {
	if strings.TrimFunc(accept, unicode.IsSpace) == "" {
		return true
	}
	xml := false
	for _, part := range splitDroppingTrailingEmpty(accept, ",") {
		pieces := strings.Split(strings.ToLower(trimControl(part)), ";")
		media := trimControl(pieces[0])
		if media == "application/vnd.apache.arrow.stream" {
			return false
		}
		quality := 1.0
		for _, p := range pieces[1:] {
			p = trimControl(p)
			if strings.HasPrefix(p, "q=") {
				q, ok := javaDouble(p[2:])
				if !ok {
					return false
				}
				quality = q
			}
		}
		if quality < 0 || quality > 1 {
			return false
		}
		if (media == "application/xml" || media == "*/*") && quality > 0 {
			xml = true
		}
	}
	return xml
}

// splitDroppingTrailingEmpty splits like strings.Split and then drops
// trailing empty fields, the split the Accept rule is defined with.
func splitDroppingTrailingEmpty(s, sep string) []string {
	parts := strings.Split(s, sep)
	for len(parts) > 0 && parts[len(parts)-1] == "" {
		parts = parts[:len(parts)-1]
	}
	return parts
}

// trimControl trims every byte at or below a space from both ends.
func trimControl(s string) string {
	return strings.TrimFunc(s, func(r rune) bool { return r <= ' ' })
}

// javaDecimal is the decimal grammar a quality value accepts once lower
// cased: an optional sign, digits with an optional fraction (or a bare
// fraction), an optional exponent and an optional d/f suffix.
var javaDecimal = regexp.MustCompile(`^[+-]?(\d+\.?\d*|\.\d+)(e[+-]?\d+)?[df]?$`)

// javaHex is the hexadecimal floating-point grammar (binary exponent
// required).
var javaHex = regexp.MustCompile(`^[+-]?0x([0-9a-f]+\.?[0-9a-f]*|\.[0-9a-f]+)p[+-]?\d+[df]?$`)

func javaDouble(s string) (float64, bool) {
	s = trimControl(s)
	switch {
	case javaDecimal.MatchString(s):
		f, err := strconv.ParseFloat(strings.TrimRight(s, "df"), 64)
		return f, err == nil || isRange(err)
	case javaHex.MatchString(s):
		f, err := strconv.ParseFloat(strings.TrimRight(s, "df"), 64)
		return f, err == nil || isRange(err)
	}
	return 0, false
}

// isRange reports an overflow, which still parses (to an infinity).
func isRange(err error) bool {
	ne, ok := err.(*strconv.NumError)
	return ok && ne.Err == strconv.ErrRange
}
