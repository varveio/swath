// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"strconv"
	"time"

	"github.com/varveio/swath/go/internal/replay/store"
)

// The response body is assembled byte by byte rather than with
// encoding/xml: the element order, the escaping (all five XML
// metacharacters, '\'' as &apos;), ETag quoting as &quot;, millisecond
// LastModified and the encoding-type=url grammar are the replay wire
// contract, which encoding/xml's marshaller does not produce (it escapes
// '"' as &#34; and control bytes as &#xN;). TestRenderListGolden pins the
// bytes.

const (
	xmlDecl     = `<?xml version="1.0" encoding="UTF-8"?>`
	listOpen    = `<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`
	requestID   = "S3LISTINGREPLAY"
	bytesPerRow = 320
)

// EstimateSize is the response size reserved for a page of n entries
// before it is rendered.
func EstimateSize(n int) int { return max(4096, 512+n*bytesPerRow) }

// AppendList appends the ListBucketResult document for res.
func AppendList(dst []byte, res *Result) []byte {
	req := res.Request
	dst = append(dst, xmlDecl...)
	dst = append(dst, listOpen...)
	dst = textElement(dst, "Name", req.Bucket)
	dst = byteElement(dst, "Prefix", req.Prefix, req.EncodingURL)
	// With both a token and a start-after the pager resumed at the token,
	// so the response echoes the token and omits the start-after it
	// ignored.
	if req.HasToken() {
		dst = textElement(dst, "ContinuationToken", req.Token)
	} else if req.StartAfter != nil {
		dst = byteElement(dst, "StartAfter", req.StartAfter, req.EncodingURL)
	}
	if res.NextToken != "" {
		dst = textElement(dst, "NextContinuationToken", res.NextToken)
	}
	dst = intElement(dst, "KeyCount", int64(len(res.Entries)))
	dst = intElement(dst, "MaxKeys", int64(req.MaxKeys))
	if len(req.Delimiter) > 0 {
		dst = byteElement(dst, "Delimiter", req.Delimiter, req.EncodingURL)
	}
	if req.EncodingURL {
		dst = append(dst, "<EncodingType>url</EncodingType>"...)
	}
	if res.Truncated {
		dst = append(dst, "<IsTruncated>true</IsTruncated>"...)
	} else {
		dst = append(dst, "<IsTruncated>false</IsTruncated>"...)
	}
	var ts timestamps
	for _, e := range res.Entries {
		if e.Object != nil {
			dst = contents(dst, e.Object, req, &ts)
		}
	}
	for _, e := range res.Entries {
		if e.Object == nil {
			dst = append(dst, "<CommonPrefixes>"...)
			dst = byteElement(dst, "Prefix", e.CommonPrefix, req.EncodingURL)
			dst = append(dst, "</CommonPrefixes>"...)
		}
	}
	return append(dst, "</ListBucketResult>"...)
}

func contents(dst []byte, o *store.Object, req *Request, ts *timestamps) []byte {
	dst = append(dst, "<Contents>"...)
	dst = byteElement(dst, "Key", o.Key, req.EncodingURL)
	dst = append(dst, "<LastModified>"...)
	var micros int64 // a null time renders as the epoch
	if o.LastModified != nil {
		micros = o.LastModified.UnixMicro()
	}
	dst = ts.appendMillis(dst, micros)
	dst = append(dst, "</LastModified>"...)
	if o.ETag != nil {
		dst = append(dst, "<ETag>&quot;"...)
		dst = appendEscaped(dst, decodeText([]byte(*o.ETag)))
		dst = append(dst, "&quot;</ETag>"...)
	}
	if o.ChecksumAlgorithm != nil {
		dst = textElement(dst, "ChecksumAlgorithm", *o.ChecksumAlgorithm)
	}
	if o.ChecksumType != nil {
		dst = textElement(dst, "ChecksumType", *o.ChecksumType)
	}
	var size int64 // a null size renders as 0
	if o.Size != nil {
		size = *o.Size
	}
	dst = intElement(dst, "Size", size)
	if o.StorageClass != nil {
		dst = textElement(dst, "StorageClass", *o.StorageClass)
	}
	if req.FetchOwner && (o.OwnerID != nil || o.OwnerDisplayName != nil) {
		dst = append(dst, "<Owner>"...)
		if o.OwnerID != nil {
			dst = textElement(dst, "ID", *o.OwnerID)
		}
		if o.OwnerDisplayName != nil {
			dst = textElement(dst, "DisplayName", *o.OwnerDisplayName)
		}
		dst = append(dst, "</Owner>"...)
	}
	return append(dst, "</Contents>"...)
}

// AppendError appends the S3 error document. resource is the request path
// as sent.
func AppendError(dst []byte, e *Error, resource string) []byte {
	dst = append(dst, xmlDecl...)
	dst = append(dst, "<Error>"...)
	dst = textElement(dst, "Code", e.Code)
	dst = textElement(dst, "Message", e.Message)
	dst = textElement(dst, "Resource", resource)
	dst = textElement(dst, "RequestId", requestID)
	return append(dst, "</Error>"...)
}

func open(dst []byte, name string) []byte {
	dst = append(dst, '<')
	dst = append(dst, name...)
	return append(dst, '>')
}

func closeTag(dst []byte, name string) []byte {
	dst = append(dst, "</"...)
	dst = append(dst, name...)
	return append(dst, '>')
}

func textElement(dst []byte, name, value string) []byte {
	dst = open(dst, name)
	dst = appendEscaped(dst, decodeText([]byte(value)))
	return closeTag(dst, name)
}

func intElement(dst []byte, name string, v int64) []byte {
	dst = open(dst, name)
	dst = strconv.AppendInt(dst, v, 10)
	return closeTag(dst, name)
}

// byteElement renders raw key bytes: percent-encoded for encoding-type=url,
// otherwise as text (malformed UTF-8 replaced) with XML escaping.
func byteElement(dst []byte, name string, raw []byte, encoded bool) []byte {
	dst = open(dst, name)
	if encoded {
		dst = AppendPercentEncoded(dst, raw)
	} else {
		dst = appendEscaped(dst, decodeText(raw))
	}
	return closeTag(dst, name)
}

// appendEscaped escapes the five XML metacharacters and copies every other
// byte, control characters included, as it is.
func appendEscaped(dst []byte, s string) []byte {
	for i := 0; i < len(s); i++ {
		switch c := s[i]; c {
		case '&':
			dst = append(dst, "&amp;"...)
		case '<':
			dst = append(dst, "&lt;"...)
		case '>':
			dst = append(dst, "&gt;"...)
		case '"':
			dst = append(dst, "&quot;"...)
		case '\'':
			dst = append(dst, "&apos;"...)
		default:
			dst = append(dst, c)
		}
	}
	return dst
}

// timestamps caches the formatted calendar second: listings hold long runs
// of objects modified in the same second, and formatting the date costs far
// more than appending three millisecond digits.
type timestamps struct {
	second int64
	prefix []byte
}

// appendMillis appends a UTC timestamp at millisecond precision,
// 2006-01-02T15:04:05.000Z, rounding toward the earlier millisecond (so
// one microsecond before the epoch is 1969-12-31T23:59:59.999Z).
func (t *timestamps) appendMillis(dst []byte, epochMicros int64) []byte {
	sec := floorDiv(epochMicros, 1_000_000)
	if t.prefix == nil || sec != t.second {
		t.second = sec
		t.prefix = appendCalendarSecond(t.prefix[:0], sec)
	}
	dst = append(dst, t.prefix...)
	ms := (epochMicros - sec*1_000_000) / 1000
	return append(dst, byte('0'+ms/100), byte('0'+ms/10%10), byte('0'+ms%10), 'Z')
}

// appendCalendarSecond appends the UTC calendar second as
// yyyy-MM-ddTHH:mm:ss. with the replay wire contract's year: the year of
// era (year 0 is 1, year -1 is 2, as before the common era), four digits
// zero-padded, and a '+' before a year of more than four digits
// (+10000-01-01T00:00:00.). Go's layout writes the signed proleptic year
// instead, so it would differ from year 10000 on and at or before year 0
// (TestLastModifiedYears).
func appendCalendarSecond(dst []byte, sec int64) []byte {
	t := time.Unix(sec, 0).UTC()
	year := int64(t.Year())
	if year <= 0 {
		year = 1 - year
	}
	if year > 9999 {
		dst = append(dst, '+')
		dst = strconv.AppendInt(dst, year, 10)
	} else {
		dst = append(dst, byte('0'+year/1000), byte('0'+year/100%10), byte('0'+year/10%10), byte('0'+year%10))
	}
	two := func(dst []byte, v int, sep byte) []byte { return append(dst, sep, byte('0'+v/10), byte('0'+v%10)) }
	dst = two(dst, int(t.Month()), '-')
	dst = two(dst, t.Day(), '-')
	dst = two(dst, t.Hour(), 'T')
	dst = two(dst, t.Minute(), ':')
	dst = two(dst, t.Second(), ':')
	return append(dst, '.')
}

func floorDiv(a, b int64) int64 {
	q := a / b
	if (a%b != 0) && (a < 0) != (b < 0) {
		q--
	}
	return q
}
