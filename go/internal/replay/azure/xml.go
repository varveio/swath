// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package azure

import (
	"strconv"
	"time"
	"unicode/utf16"
	"unicode/utf8"
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
	afterSize       = "</Content-Length><Content-Type>application/octet-stream</Content-Type><BlobType>BlockBlob</BlobType>" +
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

// AppendList appends the EnumerationResults document for res. endpoint is
// the ServiceEndpoint, http://host:port/account/. A fixture row the
// profile cannot render (a negative size, a time outside years 1 to 9999,
// an empty, over-long or invalid-UTF-8 name, a control character) is a
// FixtureIncompatible error naming the reason.
func AppendList(dst []byte, res *Result, endpoint string) ([]byte, error) {
	r := res.Request
	dst = append(dst, prolog...)
	dst = append(dst, `<EnumerationResults ServiceEndpoint="`...)
	dst = appendEscaped(dst, endpoint)
	dst = append(dst, `" ContainerName="`...)
	dst = appendEscaped(dst, r.Container)
	dst = append(dst, `">`...)
	if r.PrefixSupplied {
		dst = element(dst, "Prefix", r.Prefix)
	}
	if r.MarkerSupplied {
		dst = element(dst, "Marker", r.Marker)
	}
	if r.MaxResultsSet {
		dst = element(dst, "MaxResults", r.MaxResults)
	}
	if r.DelimiterSupplied {
		dst = element(dst, "Delimiter", r.Delimiter)
	}
	dst = append(dst, "<Blobs>"...)
	cachedSecond, cached := int64(0), []byte(nil)
	for _, e := range res.Entries {
		if e.Object == nil {
			dst = append(dst, "<BlobPrefix>"...)
			var err error
			if dst, err = appendName(dst, e.Prefix, true, res.onPath); err != nil {
				return nil, err
			}
			dst = append(dst, "</BlobPrefix>"...)
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
			return nil, fail(FixtureIncompatible, "negative_size")
		}
		dst = append(dst, "<Blob>"...)
		var err error
		if dst, err = appendName(dst, o.Key, false, res.onPath); err != nil {
			return nil, err
		}
		dst = append(dst, propertiesStart...)
		sec := floorDiv(micros, 1_000_000)
		if sec < firstRenderable || sec > lastRenderable {
			return nil, fail(FixtureIncompatible, "timestamp_year_out_of_range")
		}
		if cached == nil || sec != cachedSecond {
			cachedSecond, cached = sec, time.Unix(sec, 0).UTC().AppendFormat(cached[:0], HTTPTime)
		}
		dst = append(dst, cached...)
		dst = append(dst, afterTime...)
		dst = strconv.AppendInt(dst, size, 10)
		dst = append(dst, afterSize...)
	}
	dst = append(dst, "</Blobs><NextMarker>"...)
	dst = appendEscaped(dst, res.NextMarker)
	return append(dst, "</NextMarker></EnumerationResults>"...), nil
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
