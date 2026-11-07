// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/varveio/swath/go/internal/replay/output"
	"github.com/varveio/swath/go/internal/replay/store"
)

// The response body is assembled byte by byte rather than with
// encoding/xml: the element order, the escaping (all five XML
// metacharacters, '\'' as &apos;), ETag quoting as &quot;, millisecond
// LastModified are the replay wire contract and the encoding-type=url
// grammar is live S3's (output.AppendPercentEncoded), which encoding/xml's marshaller does not produce (it escapes
// '"' as &#34; and control bytes as &#xN;). TestRenderListGolden pins the
// bytes.

const (
	xmlDecl     = `<?xml version="1.0" encoding="UTF-8"?>`
	listOpen    = `<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`
	requestID   = "S3LISTINGREPLAY"
	bytesPerRow = 320
)

// EstimateSize is the response size reserved for a page of n entries
// before it is read, and the first chunk's size once it is.
func EstimateSize(n int) int { return max(4096, 512+n*bytesPerRow) }

// AppendList appends the ListBucketResult document for res, rendered
// outside any budget.
func AppendList(dst []byte, res *Result) []byte {
	o := output.Standalone(EstimateSize(len(res.Entries)))
	RenderList(o, res)
	for _, v := range o.Body() {
		dst = append(dst, v...)
	}
	o.Close()
	return dst
}

// RenderList renders the ListBucketResult document for res into o; o.Err
// reports a refusal (the per-response cap or the budget), which stops it
// at the first byte that does not fit. Each append is the same unit the
// replay wire contract's renderer admits at once, so a refusal falls on
// the same byte.
func RenderList(o *output.Output, res *Result) {
	req := res.Request
	o.ASCII(xmlDecl)
	o.ASCII(listOpen)
	textElement(o, "Name", req.Bucket)
	byteElement(o, "Prefix", req.Prefix, req.EncodingURL)
	// With both a token and a start-after the pager resumed at the token,
	// so the response echoes the token and omits the start-after it
	// ignored. A present but empty start-after is echoed empty, as S3
	// does (TestLiveEmptyStartAfterIsEchoed).
	if req.HasToken() {
		textElement(o, "ContinuationToken", req.Token)
	} else if req.StartAfter != nil || req.StartAfterSet {
		byteElement(o, "StartAfter", req.StartAfter, req.EncodingURL)
	}
	if res.NextToken != "" {
		textElement(o, "NextContinuationToken", res.NextToken)
	}
	intElement(o, "KeyCount", int64(len(res.Entries)))
	intElement(o, "MaxKeys", int64(req.MaxKeys))
	if len(req.Delimiter) > 0 {
		byteElement(o, "Delimiter", req.Delimiter, req.EncodingURL)
	}
	if req.EncodingURL {
		enc := req.EncodingType
		if enc == "" {
			enc = "url"
		}
		textElement(o, "EncodingType", enc)
	}
	if res.Truncated {
		fixedElement(o, "IsTruncated", "true")
	} else {
		fixedElement(o, "IsTruncated", "false")
	}
	var ts timestamps
	var row []byte
	for _, e := range res.Entries {
		if o.Err() != nil {
			return
		}
		if e.Object != nil {
			row = contents(o, e.Object, req, &ts, row)
		}
	}
	for _, e := range res.Entries {
		if o.Err() != nil {
			return
		}
		if e.Object == nil {
			o.ASCII("<CommonPrefixes>")
			byteElement(o, "Prefix", e.CommonPrefix, req.EncodingURL)
			o.ASCII("</CommonPrefixes>")
		}
	}
	o.ASCII("</ListBucketResult>")
}

// rowFastPath is off only in the test that compares the two paths.
var rowFastPath = true

// contents renders one Contents element. When the whole element fits in
// the room left in the current chunk it is built in scratch and appended
// at once: every unit would then be admitted without an allocation, so the
// bytes, the chunks and the percent-encoding counts are the same as unit
// by unit (TestRowFastPathMatchesUnits). It returns scratch for reuse.
func contents(o *output.Output, obj *store.Object, req *Request, ts *timestamps, scratch []byte) []byte {
	if room := o.Room(); room > 0 && rowFastPath {
		var onePass, fallback int
		scratch, onePass, fallback = appendContents(scratch[:0], obj, req, ts, room)
		if len(scratch) <= room {
			o.AppendRoom(scratch, onePass, fallback)
			return scratch
		}
	}
	contentUnits(o, obj, req, ts)
	return scratch
}

// appendContents appends the Contents element contentUnits renders, and
// how PercentEncoded would have counted its key with room bytes left in
// the chunk at the element's start.
func appendContents(dst []byte, obj *store.Object, req *Request, ts *timestamps, room int) (row []byte, onePass, fallback int) {
	start := len(dst)
	dst = append(dst, "<Contents><Key>"...)
	if req.EncodingURL {
		if len(obj.Key) <= (room-(len(dst)-start))/3 {
			onePass = 1
		} else {
			fallback = 1
		}
		dst = output.AppendPercentEncoded(dst, obj.Key)
	} else if utf8.Valid(obj.Key) {
		dst = appendEscaped(dst, obj.Key)
	} else {
		dst = appendEscaped(dst, decodeText(obj.Key))
	}
	dst = append(dst, "</Key><LastModified>"...)
	var micros int64
	if obj.LastModified != nil {
		micros = obj.LastModified.UnixMicro()
	}
	dst = ts.appendMillis(dst, micros)
	dst = append(dst, "</LastModified>"...)
	if obj.ETag != nil {
		dst = append(dst, "<ETag>&quot;"...)
		dst = appendEscaped(dst, decodeString(*obj.ETag))
		dst = append(dst, "&quot;</ETag>"...)
	}
	if obj.ChecksumAlgorithm != nil {
		dst = appendTextElement(dst, "ChecksumAlgorithm", *obj.ChecksumAlgorithm)
	}
	if obj.ChecksumType != nil {
		dst = appendTextElement(dst, "ChecksumType", *obj.ChecksumType)
	}
	var size int64
	if obj.Size != nil {
		size = *obj.Size
	}
	dst = append(dst, "<Size>"...)
	dst = strconv.AppendInt(dst, size, 10)
	dst = append(dst, "</Size>"...)
	if obj.StorageClass != nil {
		dst = appendTextElement(dst, "StorageClass", *obj.StorageClass)
	}
	if req.FetchOwner && (obj.OwnerID != nil || obj.OwnerDisplayName != nil) {
		dst = append(dst, "<Owner>"...)
		if obj.OwnerID != nil {
			dst = appendTextElement(dst, "ID", *obj.OwnerID)
		}
		if obj.OwnerDisplayName != nil {
			dst = appendTextElement(dst, "DisplayName", *obj.OwnerDisplayName)
		}
		dst = append(dst, "</Owner>"...)
	}
	return append(dst, "</Contents>"...), onePass, fallback
}

// contentUnits renders one Contents element unit by unit.
func contentUnits(o *output.Output, obj *store.Object, req *Request, ts *timestamps) {
	o.ASCII("<Contents>")
	byteElement(o, "Key", obj.Key, req.EncodingURL)
	o.ASCII("<LastModified>")
	var micros int64 // a null time renders as the epoch
	if obj.LastModified != nil {
		micros = obj.LastModified.UnixMicro()
	}
	ts.render(o, micros)
	o.ASCII("</LastModified>")
	if obj.ETag != nil {
		o.ASCII("<ETag>&quot;")
		o.Escaped(decodeString(*obj.ETag))
		o.ASCII("&quot;</ETag>")
	}
	if obj.ChecksumAlgorithm != nil {
		textElement(o, "ChecksumAlgorithm", *obj.ChecksumAlgorithm)
	}
	if obj.ChecksumType != nil {
		textElement(o, "ChecksumType", *obj.ChecksumType)
	}
	var size int64 // a null size renders as 0
	if obj.Size != nil {
		size = *obj.Size
	}
	intElement(o, "Size", size)
	if obj.StorageClass != nil {
		textElement(o, "StorageClass", *obj.StorageClass)
	}
	if req.FetchOwner && (obj.OwnerID != nil || obj.OwnerDisplayName != nil) {
		o.ASCII("<Owner>")
		if obj.OwnerID != nil {
			textElement(o, "ID", *obj.OwnerID)
		}
		if obj.OwnerDisplayName != nil {
			textElement(o, "DisplayName", *obj.OwnerDisplayName)
		}
		o.ASCII("</Owner>")
	}
	o.ASCII("</Contents>")
}

// AppendError appends the S3 error document. resource is the request path
// as sent.
func AppendError(dst []byte, e *Error, resource string) []byte {
	dst = append(dst, xmlDecl...)
	dst = append(dst, "<Error>"...)
	dst = appendTextElement(dst, "Code", e.Code)
	dst = appendTextElement(dst, "Message", e.Message)
	dst = appendTextElement(dst, "Resource", resource)
	dst = appendTextElement(dst, "RequestId", requestID)
	return append(dst, "</Error>"...)
}

// The element helpers admit the opening tag's three parts, the value and
// the closing tag separately, as the contract's renderer does.

func openTag(o *output.Output, name string) {
	o.Byte('<')
	o.ASCII(name)
	o.Byte('>')
}

func closeTag(o *output.Output, name string) {
	o.ASCII("</")
	o.ASCII(name)
	o.Byte('>')
}

func textElement(o *output.Output, name, value string) {
	openTag(o, name)
	o.Escaped(decodeString(value))
	closeTag(o, name)
}

func fixedElement(o *output.Output, name, value string) {
	openTag(o, name)
	o.ASCII(value)
	closeTag(o, name)
}

func intElement(o *output.Output, name string, v int64) {
	openTag(o, name)
	o.Int(v)
	closeTag(o, name)
}

// byteElement renders raw key bytes: percent-encoded for encoding-type=url,
// otherwise as text (malformed UTF-8 replaced) with XML escaping.
func byteElement(o *output.Output, name string, raw []byte, encoded bool) {
	openTag(o, name)
	if raw != nil {
		if encoded {
			o.PercentEncoded(raw)
		} else if utf8.Valid(raw) {
			o.EscapedBytes(raw)
		} else {
			o.Escaped(decodeText(raw))
		}
	}
	closeTag(o, name)
}

// appendTextElement and appendEscaped build the small error document, which
// is rendered outside the budget.
func appendTextElement(dst []byte, name, value string) []byte {
	dst = append(dst, '<')
	dst = append(dst, name...)
	dst = append(dst, '>')
	dst = appendEscaped(dst, decodeString(value))
	dst = append(dst, "</"...)
	dst = append(dst, name...)
	return append(dst, '>')
}

// appendEscaped escapes the five XML metacharacters and copies every other
// byte, control characters included, as it is, a run of plain bytes at a
// time.
func appendEscaped[T string | []byte](dst []byte, s T) []byte {
	plain := 0
	for i := 0; i < len(s); i++ {
		entity := xmlEntity[s[i]]
		if entity == "" {
			continue
		}
		dst = append(dst, s[plain:i]...)
		dst = append(dst, entity...)
		plain = i + 1
	}
	return append(dst, s[plain:]...)
}

// xmlEntity is the escape of each XML metacharacter, "" for other bytes.
var xmlEntity = [256]string{'&': "&amp;", '<': "&lt;", '>': "&gt;", '"': "&quot;", '\'': "&apos;"}

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
	sec := t.prefixFor(epochMicros)
	dst = append(dst, t.prefix...)
	ms := (epochMicros - sec*1_000_000) / 1000
	return append(dst, byte('0'+ms/100), byte('0'+ms/10%10), byte('0'+ms%10), 'Z')
}

// render writes the same timestamp into o.
func (t *timestamps) render(o *output.Output, epochMicros int64) {
	sec := t.prefixFor(epochMicros)
	o.Write(t.prefix)
	o.ThreeDigits(int((epochMicros - sec*1_000_000) / 1000))
	o.Byte('Z')
}

// prefixFor caches the calendar prefix of epochMicros's second and returns
// the second.
//
// Within the cached second's day only the time of day changes, so it
// rewrites the HH:mm:ss. that ends the prefix; a listing's objects are
// often modified seconds apart.
func (t *timestamps) prefixFor(epochMicros int64) int64 {
	sec := floorDiv(epochMicros, 1_000_000)
	if t.prefix != nil && sec == t.second {
		return sec
	}
	if day := floorDiv(sec, 86_400); t.prefix != nil && day == floorDiv(t.second, 86_400) {
		clock := sec - day*86_400
		tail := t.prefix[len(t.prefix)-9:]
		h, m, s := clock/3600, clock/60%60, clock%60
		tail[0], tail[1], tail[3], tail[4], tail[6], tail[7] = byte('0'+h/10), byte('0'+h%10), byte('0'+m/10), byte('0'+m%10), byte('0'+s/10), byte('0'+s%10)
		t.second = sec
		return sec
	}
	t.second = sec
	t.prefix = appendCalendarSecond(t.prefix[:0], sec)
	return sec
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
