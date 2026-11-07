// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"strings"
	"unicode/utf16"
)

// MaxPageSize is the most entries one page holds, whatever max-keys asks:
// S3's own cap.
const MaxPageSize = 1000

// Request is a parsed ListObjectsV2 request. Byte parameters keep their raw
// bytes (nil when absent or empty); the response echoes MaxKeys as asked,
// while PageSize is what a page holds.
type Request struct {
	Bucket     string
	Prefix     []byte
	Delimiter  []byte
	StartAfter []byte
	// StartAfterSet says whether start-after was present at all: an empty
	// one bounds nothing, but S3 echoes it as an empty StartAfter.
	StartAfterSet bool
	// Token is the continuation-token parameter as text; TokenSet says
	// whether the parameter was present at all.
	Token       string
	TokenSet    bool
	MaxKeys     int32
	EncodingURL bool
	FetchOwner  bool
}

// PageSize is max-keys clamped to [0, MaxPageSize].
func (r *Request) PageSize() int {
	return int(min(max(r.MaxKeys, 0), MaxPageSize))
}

// HasToken reports whether the request carries a continuation token that
// resumes something. A blank continuation-token= is absent: it neither
// resumes nor suppresses start-after, in the pager or in the response.
func (r *Request) HasToken() bool {
	return r.TokenSet && !isBlank(r.Token)
}

// query is a parsed query string: parameter names are text, values raw
// bytes. A repeated name keeps its last value.
type query map[string][]byte

func parseQuery(raw string) query {
	q := query{}
	if raw == "" {
		return q
	}
	for _, part := range strings.Split(queryText(raw), "&") {
		name, value, _ := strings.Cut(part, "=")
		q[decodeText(PercentDecode(name))] = PercentDecode(value)
	}
	return q
}

func (q query) text(name string) (string, bool) {
	v, ok := q[name]
	if !ok {
		return "", false
	}
	return decodeText(v), true
}

// ParseRequest parses a ListObjectsV2 query (the raw query string, as sent)
// for bucket. It enforces list-type=2 and a non-negative integer max-keys;
// every other parameter is accepted as it is.
func ParseRequest(bucket, rawQuery string) (*Request, error) {
	q := parseQuery(rawQuery)
	if lt, _ := q.text("list-type"); lt != "2" {
		return nil, errListType
	}
	r := &Request{
		Bucket:     bucket,
		Prefix:     nonEmpty(q["prefix"]),
		Delimiter:  nonEmpty(q["delimiter"]),
		StartAfter: nonEmpty(q["start-after"]),
	}
	_, r.StartAfterSet = q["start-after"]
	r.Token, r.TokenSet = q.text("continuation-token")
	mk, err := maxKeys(q)
	if err != nil {
		return nil, err
	}
	r.MaxKeys = mk
	enc, _ := q.text("encoding-type")
	r.EncodingURL = enc == "url"
	owner, _ := q.text("fetch-owner")
	r.FetchOwner = strings.EqualFold(owner, "true")
	return r, nil
}

func nonEmpty(v []byte) []byte {
	if len(v) == 0 {
		return nil
	}
	return v
}

func maxKeys(q query) (int32, error) {
	s, ok := q.text("max-keys")
	if !ok || isBlank(s) {
		return MaxPageSize, nil
	}
	v, ok := parseInt32(s)
	if !ok {
		return 0, errMaxKeysInteger
	}
	if v < 0 {
		return 0, errMaxKeysNegative
	}
	return v, nil
}

// ParseHTTP parses a request on the S3 route: a GET (anything else is 405
// MethodNotAllowed), path-style, whose first path segment is the bucket
// (anything else is 404 NoSuchBucket; later segments are ignored), and a
// ListObjectsV2 query. path is the request path as sent, still
// percent-encoded: a bucket spelled with escapes is a different bucket.
func ParseHTTP(method, path, rawQuery, bucket string) (*Request, error) {
	if method != "GET" {
		return nil, errMethodNotAllowed
	}
	if bucketOf(path) != bucket {
		return nil, errNoSuchBucket
	}
	return ParseRequest(bucket, rawQuery)
}

func bucketOf(path string) string {
	if path == "/" || isBlank(path) {
		return ""
	}
	path = strings.TrimPrefix(path, "/")
	seg, _, _ := strings.Cut(path, "/")
	return seg
}

// LatencyKey identifies the request for latency jitter: a hash of prefix,
// delimiter, start-after, token and max-keys, so the same request always
// draws the same jitter and successive pages of one walk (which differ
// only in their token) draw different ones. It is the Java server's key
// for the same request, so both servers jitter a request alike
// (TestLatencyKeyGolden).
func (r *Request) LatencyKey() int64 {
	h := int64(1125899906842597)
	h = 31*h + int64(bytesHash(r.Prefix))
	h = 31*h + int64(bytesHash(r.Delimiter))
	h = 31*h + int64(bytesHash(r.StartAfter))
	var th int32
	if r.TokenSet {
		for _, u := range utf16.Encode([]rune(decodeText([]byte(r.Token)))) {
			th = 31*th + int32(u)
		}
	}
	h = 31*h + int64(th)
	return 31*h + int64(r.MaxKeys)
}

// bytesHash is the polynomial hash of signed bytes, 0 for nil.
func bytesHash(b []byte) int32 {
	if b == nil {
		return 0
	}
	h := int32(1)
	for _, c := range b {
		h = 31*h + int32(int8(c))
	}
	return h
}
