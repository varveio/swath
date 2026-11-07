// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package azure

import (
	"strconv"
	"time"
	"unicode/utf16"
	"unicode/utf8"

	"github.com/varveio/swath/go/internal/replay/output"
	"github.com/varveio/swath/go/internal/replay/store"
)

// The body is assembled byte by byte: the BOM before the declaration, the
// element order, the escaping (all five XML metacharacters, CR in a name as
// &#13;), the synthetic Properties values and the Encoded name grammar are
// the replay wire contract, which encoding/xml does not produce
// (TestRenderListGolden pins the bytes).

// BOM starts every body, success or error: native Azure sends a UTF-8 byte
// order mark before the XML declaration for both service versions.
const BOM = "\xef\xbb\xbf"

const (
	prolog          = BOM + `<?xml version="1.0" encoding="utf-8"?>` + "\n"
	propertiesStart = "<Properties><Last-Modified>"
	afterTime       = "</Last-Modified><Etag>0x000000000000001</Etag><Content-Length>"
	afterSize       = "</Content-Length><Content-Type>"
	// placeholderContentType is the Content-Type of a row with no content
	// type Azure could hold.
	placeholderContentType = "application/octet-stream"
	afterContent           = "<BlobType>BlockBlob</BlobType>" +
		"<AccessTier>Hot</AccessTier><AccessTierInferred>true</AccessTierInferred>" +
		"<LeaseStatus>unlocked</LeaseStatus><LeaseState>available</LeaseState></Properties></Blob>"
	// HTTPTime is the Last-Modified and Date spelling.
	HTTPTime = "Mon, 02 Jan 2006 15:04:05 GMT"
	// maxNameUnits is the longest name Azure accepts, in UTF-16 code
	// units (bytes for an ASCII name).
	maxNameUnits = 1024
)

// The renderable Last-Modified range: years 1 to 9999.
var (
	firstRenderable = time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC).Unix()
	lastRenderable  = time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC).Unix()
)

// EstimateSize is the response size reserved for a page of pageSize
// entries before it is read and rendered.
func EstimateSize(pageSize int) int { return 2048 + pageSize*512 }

// RenderList renders the EnumerationResults document for res into o.
// endpoint is the ServiceEndpoint, http://host:port/account/. A fixture
// row the profile cannot render (a negative size, a time outside years 1
// to 9999, an empty, over-long or invalid-UTF-8 name, a control character)
// is a FixtureIncompatible error naming the reason. Each entry is
// formatted on its own and written, so no more than one entry is held
// outside o; a refusal by o (its cap or budget, o.Err) stops rendering.
// The writes and the checks keep the replay wire contract's order: the
// entry's opening tag is written before its name is checked, and the name
// and the properties' opening before its time is, so under a small cap a
// write the cap refuses is reported (response_too_large) before a fixture
// value the profile refuses further on (TestRenderErrorPrecedence).
func RenderList(o *output.Output, res *Result, endpoint string) error {
	var buf []byte
	flush := func() bool {
		o.Write(buf)
		buf = buf[:0]
		return o.Err() == nil
	}
	r := res.Request
	buf = append(buf, prolog...)
	buf = append(buf, `<EnumerationResults ServiceEndpoint="`...)
	buf = appendEscaped(buf, endpoint)
	buf = append(buf, `" ContainerName="`...)
	buf = appendEscaped(buf, r.Container)
	buf = append(buf, `">`...)
	if r.PrefixSupplied {
		buf = element(buf, "Prefix", r.Prefix)
	}
	if r.MarkerSupplied {
		buf = element(buf, "Marker", r.Marker)
	}
	if r.MaxResultsSet {
		buf = element(buf, "MaxResults", r.MaxResults)
	}
	if r.DelimiterSupplied {
		buf = element(buf, "Delimiter", r.Delimiter)
	}
	buf = append(buf, "<Blobs>"...)
	cachedSecond, cached := int64(0), []byte(nil)
	for _, e := range res.Entries {
		if !flush() {
			return nil
		}
		if e.Object == nil {
			buf = append(buf, "<BlobPrefix>"...)
			if !flush() {
				return nil
			}
			var err error
			if buf, err = appendName(buf, e.Prefix, true, res.onPath); err != nil {
				return err
			}
			buf = append(buf, "</BlobPrefix>"...)
			continue
		}
		o := e.Object
		var size, micros int64 // a null size renders as 0, a null time as the epoch
		if o.Size != nil {
			size = *o.Size
		}
		if o.LastModified != nil {
			micros = o.LastModified.UnixMicro()
		}
		if size < 0 {
			return fail(FixtureIncompatible, "negative_size")
		}
		buf = append(buf, "<Blob>"...)
		if !flush() {
			return nil
		}
		var err error
		if buf, err = appendName(buf, o.Key, false, res.onPath); err != nil {
			return err
		}
		buf = append(buf, propertiesStart...)
		if !flush() {
			return nil
		}
		sec := floorDiv(micros, 1_000_000)
		if sec < firstRenderable || sec > lastRenderable {
			return fail(FixtureIncompatible, "timestamp_year_out_of_range")
		}
		if cached == nil || sec != cachedSecond {
			cachedSecond, cached = sec, time.Unix(sec, 0).UTC().AppendFormat(cached[:0], HTTPTime)
		}
		buf = append(buf, cached...)
		buf = append(buf, afterTime...)
		buf = strconv.AppendInt(buf, size, 10)
		buf = append(buf, afterSize...)
		buf = appendContent(buf, o)
		buf = append(buf, afterContent...)
	}
	buf = append(buf, "</Blobs><NextMarker>"...)
	buf = appendEscaped(buf, res.NextMarker)
	o.Write(append(buf, "</NextMarker></EnumerationResults>"...))
	return nil
}

// appendContent appends a Blob's Content-Type text and its Content-MD5
// element. A GCS recording maps both (Projection.Content): its content
// type, and its MD5, which GCS lists in the form Content-MD5 takes (the
// standard base64 of 16 bytes); every other row has the placeholder type
// and no Content-MD5. A content type Azure could not hold (empty, not
// UTF-8, or with a control character, which a header value cannot carry)
// takes the placeholder, and an MD5 in any other form is none. Native
// listings write Content-MD5 after Content-Type and the content
// properties the profile omits (Content-Encoding, Content-Language,
// Content-CRC64) and before BlobType (server
// TestGCSRecordingOnTheAzureRoute).
func appendContent(dst []byte, o *store.Object) []byte {
	if o.ContentType != nil && holdable(*o.ContentType) {
		dst = appendEscaped(dst, *o.ContentType)
	} else {
		dst = append(dst, placeholderContentType...)
	}
	dst = append(dst, "</Content-Type>"...)
	if o.MD5 != nil && store.IsMD5(*o.MD5) {
		dst = append(dst, "<Content-MD5>"...)
		dst = append(dst, *o.MD5...)
		dst = append(dst, "</Content-MD5>"...)
	}
	return dst
}

// holdable reports whether a content type can be an Azure Content-Type:
// non-empty UTF-8 without a C0 control character or DEL.
func holdable(s string) bool {
	if s == "" || !utf8.ValidString(s) {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < 0x20 || s[i] == 0x7f {
			return false
		}
	}
	return true
}

// AppendList appends what RenderList renders, outside any budget.
func AppendList(dst []byte, res *Result, endpoint string) ([]byte, error) {
	o := output.Standalone(max(4096, 2048+len(res.Entries)*512))
	defer o.Close()
	if err := RenderList(o, res, endpoint); err != nil {
		return nil, err
	}
	for _, v := range o.Body() {
		dst = append(dst, v...)
	}
	return dst, nil
}

// AppendError appends the error document for code.
func AppendError(dst []byte, code string) []byte {
	dst = append(dst, prolog...)
	dst = append(dst, "<Error><Code>"...)
	dst = append(dst, code...)
	dst = append(dst, "</Code><Message>"...)
	dst = append(dst, code...)
	return append(dst, "</Message></Error>"...)
}

func element(dst []byte, name, value string) []byte {
	dst = append(dst, '<')
	dst = append(dst, name...)
	dst = append(dst, '>')
	dst = appendEscaped(dst, value)
	dst = append(dst, "</"...)
	dst = append(dst, name...)
	return append(dst, '>')
}

// appendName appends a Name element. A name holding U+FFFE or U+FFFF,
// which XML cannot carry, is sent Encoded="true" in percent-encoding with
// the RFC 3986 unreserved bytes literal; any other is XML text with CR as
// &#13; so a parser keeps it. Refused: a control character (TAB, LF and CR
// are allowed in a BlobPrefix, which only repeats part of a listed name), a
// name that is not valid UTF-8, and one longer than 1,024 UTF-16 code
// units. There is no path-segment limit: native Azure lists a flat
// container's 255-segment name (evidence capture azure-12a).
func appendName(dst, raw []byte, derivedPrefix bool, onPath func(path, reason string)) ([]byte, error) {
	if len(raw) == 0 {
		return nil, fail(FixtureIncompatible, "empty_name")
	}
	ascii := true
	for _, b := range raw {
		if b >= 0x80 {
			ascii = false
		} else if b < 0x20 && (!derivedPrefix || b != '\t' && b != '\n' && b != '\r') {
			return nil, fail(FixtureIncompatible, "control_character")
		}
	}
	if ascii {
		if len(raw) > maxNameUnits {
			return nil, fail(FixtureIncompatible, "name_too_long")
		}
		dst = append(dst, "<Name>"...)
		dst = appendNameText(dst, raw)
		return append(dst, "</Name>"...), nil
	}
	if !utf8.Valid(raw) {
		return nil, fail(FixtureIncompatible, "invalid_utf8_name")
	}
	units, encoded := 0, false
	for _, r := range string(raw) {
		units += utf16.RuneLen(r)
		if r == 0xfffe || r == 0xffff {
			encoded = true
		}
	}
	if units > maxNameUnits {
		return nil, fail(FixtureIncompatible, "name_too_long")
	}
	if !encoded {
		dst = append(dst, "<Name>"...)
		dst = appendNameText(dst, raw)
		return append(dst, "</Name>"...), nil
	}
	if onPath != nil {
		reason := "blob_xml_forbidden"
		if derivedPrefix {
			reason = "prefix_xml_forbidden"
		}
		onPath("encoded_name", reason)
	}
	dst = append(dst, `<Name Encoded="true">`...)
	for _, b := range raw {
		if b >= 'A' && b <= 'Z' || b >= 'a' && b <= 'z' || b >= '0' && b <= '9' || b == '-' || b == '_' || b == '.' || b == '~' {
			dst = append(dst, b)
		} else {
			dst = append(dst, '%', upperHex[b>>4], upperHex[b&15])
		}
	}
	return append(dst, "</Name>"...), nil
}

const upperHex = "0123456789ABCDEF"

// appendNameText escapes the five XML metacharacters and CR.
func appendNameText(dst, raw []byte) []byte {
	for _, b := range raw {
		if b == '\r' {
			dst = append(dst, "&#13;"...)
			continue
		}
		dst = appendEscapedByte(dst, b)
	}
	return dst
}

// appendEscaped escapes the five XML metacharacters and copies every other
// byte as it is.
func appendEscaped(dst []byte, s string) []byte {
	for i := 0; i < len(s); i++ {
		dst = appendEscapedByte(dst, s[i])
	}
	return dst
}

func appendEscapedByte(dst []byte, b byte) []byte {
	switch b {
	case '&':
		return append(dst, "&amp;"...)
	case '<':
		return append(dst, "&lt;"...)
	case '>':
		return append(dst, "&gt;"...)
	case '"':
		return append(dst, "&quot;"...)
	case '\'':
		return append(dst, "&apos;"...)
	}
	return append(dst, b)
}

func floorDiv(a, b int64) int64 {
	q := a / b
	if a%b != 0 && (a < 0) != (b < 0) {
		q--
	}
	return q
}
