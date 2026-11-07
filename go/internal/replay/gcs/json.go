// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package gcs

import (
	"bytes"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/varveio/swath/go/internal/replay/output"
)

// ContentType is the Content-Type of every answer on the route, errors
// included.
const ContentType = "application/json; charset=utf-8"

// The synthetic profile: the metadata a fixture without GCS columns is
// rendered with. Clients read these fields; their values carry no
// information about the recorded bucket.
const (
	syntheticETag         = "swath-gcs-synthetic-v1"
	syntheticGeneration   = "1"
	syntheticStorageClass = "STANDARD"
	syntheticContentType  = "application/octet-stream"
)

// EstimateSize is the response size reserved before a page of n entries
// is read, and, with at least 4 KiB, the first chunk's size once it is:
// the synthetic metadata is roughly 220 bytes per item before its name.
func EstimateSize(n int) int { return 1024 + 384*n }

// FixtureProblem is a fixture row the route cannot render as an object
// resource: a name that is not 1 to 1,024 bytes of UTF-8 without CR or
// LF, a reserved name, a negative size or a time outside years 0 to 9999.
// Reason is sent in x-swath-replay-error.
type FixtureProblem struct{ Reason string }

func (e *FixtureProblem) Error() string { return "fixture incompatible with GCS listing: " + e.Reason }

// RenderPage renders page as the objects resource list into o: kind, then
// items and prefixes when non-empty, then nextPageToken when there is one,
// with no insignificant whitespace (prettyPrint is accepted and ignored).
// The bytes are the wire contract's, field order and escapes included
// (TestRenderGolden). Each item and prefix is formatted on its own and
// then written, so no more than one entry is ever held outside o; a
// refusal by o (its cap or budget, o.Err) stops rendering before the next
// entry, and a fixture row the route cannot render is a *FixtureProblem.
func RenderPage(o *output.Output, r *Request, page *Page) error {
	var buf []byte
	flush := func() bool {
		o.Write(buf)
		buf = buf[:0]
		return o.Err() == nil
	}
	buf = append(buf, `{"kind":"storage#objects"`...)
	if len(page.Items) > 0 {
		buf = append(buf, `,"items":[`...)
		day, date := int64(-1<<63), ""
		for i := range page.Items {
			if !flush() {
				return nil
			}
			it := &page.Items[i]
			if err := validateName(it.Key); err != nil {
				return err
			}
			if it.Size < 0 {
				return &FixtureProblem{"negative_size"}
			}
			sec, frac := floorDiv(it.Updated, 1_000_000)
			if d, _ := floorDiv(sec, 86_400); d != day {
				t := time.Unix(d*86_400, 0).UTC()
				if t.Year() < 0 || t.Year() > 9999 {
					return &FixtureProblem{"timestamp_out_of_range"}
				}
				day, date = d, t.Format("2006-01-02")
			}
			if i > 0 {
				buf = append(buf, ',')
			}
			buf = append(buf, `{"kind":"storage#object","bucket":`...)
			buf = appendString(buf, []byte(r.Bucket))
			buf = append(buf, `,"name":`...)
			buf = appendString(buf, it.Key)
			buf = append(buf, `,"size":"`...)
			buf = strconv.AppendInt(buf, it.Size, 10)
			buf = append(buf, `","updated":"`...)
			buf = appendUpdated(buf, date, sec, frac)
			buf = append(buf, '"')
			buf = appendMetadata(buf, it.Native)
			if r.FullProjection {
				buf = append(buf, `,"acl":[]`...)
			}
			buf = append(buf, '}')
		}
		buf = append(buf, ']')
	}
	if len(page.Prefixes) > 0 {
		buf = append(buf, `,"prefixes":[`...)
		for i, p := range page.Prefixes {
			if !flush() {
				return nil
			}
			if err := validateUTF8(p); err != nil {
				return err
			}
			if i > 0 {
				buf = append(buf, ',')
			}
			buf = appendString(buf, p)
		}
		buf = append(buf, ']')
	}
	if page.Token != "" {
		buf = append(buf, `,"nextPageToken":`...)
		buf = appendString(buf, []byte(page.Token))
	}
	o.Write(append(buf, '}'))
	return nil
}

// AppendPage appends what RenderPage renders, outside any budget.
func AppendPage(dst []byte, r *Request, page *Page) ([]byte, error) {
	o := output.Standalone(max(4096, EstimateSize(len(page.Items)+len(page.Prefixes))))
	defer o.Close()
	if err := RenderPage(o, r, page); err != nil {
		return nil, err
	}
	for _, v := range o.Body() {
		dst = append(dst, v...)
	}
	return dst, nil
}

// appendUpdated writes an RFC 3339 UTC time with six fractional digits:
// the fixture's microseconds exactly.
func appendUpdated(dst []byte, date string, sec, micros int64) []byte {
	dst = append(dst, date...)
	tod := floorMod(sec, 86_400)
	dst = append(dst, 'T')
	dst = append2(dst, tod/3600)
	dst = append(dst, ':')
	dst = append2(dst, tod/60%60)
	dst = append(dst, ':')
	dst = append2(dst, tod%60)
	dst = append(dst, '.')
	s := strconv.FormatInt(micros, 10)
	for range 6 - len(s) {
		dst = append(dst, '0')
	}
	dst = append(dst, s...)
	return append(dst, 'Z')
}

func append2(dst []byte, v int64) []byte {
	return append(dst, byte('0'+v/10), byte('0'+v%10))
}

// appendMetadata writes etag, generation, metageneration, storageClass and
// contentType (and md5Hash and crc32c when the fixture has them): the
// fixture's values for a native row, the synthetic profile otherwise or
// where a native column is null.
func appendMetadata(dst []byte, n *Native) []byte {
	if n == nil {
		return append(dst, `,"etag":"`+syntheticETag+`","generation":"`+syntheticGeneration+`","metageneration":"`+
			syntheticGeneration+`","storageClass":"`+syntheticStorageClass+`","contentType":"`+syntheticContentType+`"`...)
	}
	str := func(dst []byte, name string, v *string, fallback string) []byte {
		if v == nil {
			if fallback == "" {
				return dst
			}
			v = &fallback
		}
		dst = append(dst, `,"`+name+`":`...)
		return appendString(dst, []byte(*v))
	}
	num := func(dst []byte, name string, v *int64) []byte {
		dst = append(dst, `,"`+name+`":"`...)
		if v == nil {
			dst = append(dst, syntheticGeneration...)
		} else {
			dst = strconv.AppendInt(dst, *v, 10)
		}
		return append(dst, '"')
	}
	dst = str(dst, "etag", n.ETag, syntheticETag)
	dst = num(dst, "generation", n.Generation)
	dst = num(dst, "metageneration", n.Metageneration)
	dst = str(dst, "storageClass", n.StorageClass, syntheticStorageClass)
	dst = str(dst, "contentType", n.ContentType, syntheticContentType)
	dst = str(dst, "md5Hash", n.MD5, "")
	return str(dst, "crc32c", n.CRC32C, "")
}

func floorDiv(a, b int64) (q, r int64) {
	q, r = a/b, a%b
	if r < 0 {
		q, r = q-1, r+b
	}
	return q, r
}

func floorMod(a, b int64) int64 {
	_, r := floorDiv(a, b)
	return r
}

const hexDigits = "0123456789ABCDEF"

// appendString writes s as a JSON string with the wire contract's escapes:
// '"' and '\\' backslashed, BS, TAB, LF, FF and CR as \b \t \n \f \r, the
// other C0 controls as \u00XX with upper-case hex, everything else
// (DEL, U+2028, all non-ASCII) as its UTF-8 bytes. An ill-formed UTF-8
// sequence (possible only in a native metadata value; names are validated
// first) is written as U+FFFD so the document stays valid.
func appendString(dst, s []byte) []byte {
	dst = append(dst, '"')
	for len(s) > 0 {
		c := s[0]
		if c >= utf8.RuneSelf {
			r, n := utf8.DecodeRune(s)
			if r == utf8.RuneError && n == 1 {
				dst = append(dst, "\uFFFD"...)
			} else {
				dst = append(dst, s[:n]...)
			}
			s = s[n:]
			continue
		}
		s = s[1:]
		switch c {
		case '"', '\\':
			dst = append(dst, '\\', c)
		case '\b':
			dst = append(dst, '\\', 'b')
		case '\t':
			dst = append(dst, '\\', 't')
		case '\n':
			dst = append(dst, '\\', 'n')
		case '\f':
			dst = append(dst, '\\', 'f')
		case '\r':
			dst = append(dst, '\\', 'r')
		default:
			if c < 0x20 {
				dst = append(dst, '\\', 'u', '0', '0', hexDigits[c>>4], hexDigits[c&0xf])
			} else {
				dst = append(dst, c)
			}
		}
	}
	return append(dst, '"')
}

var acmePrefix = []byte(".well-known/acme-challenge/")

// validateName refuses an object name the API could not hold: see
// validateUTF8, and the reserved names ".", ".." and the ACME challenge
// directory.
func validateName(b []byte) error {
	if err := validateUTF8(b); err != nil {
		return err
	}
	if string(b) == "." || string(b) == ".." || bytes.HasPrefix(b, acmePrefix) {
		return &FixtureProblem{"reserved_name"}
	}
	return nil
}

// validateUTF8 refuses a name or prefix that is empty or over 1,024
// bytes, then reports its first fault from the left: CR or LF, or an
// ill-formed UTF-8 sequence (overlong forms, surrogates and code points
// above U+10FFFF included).
func validateUTF8(b []byte) error {
	if len(b) == 0 || len(b) > maxFieldBytes {
		return &FixtureProblem{"name_length"}
	}
	for len(b) > 0 {
		r, n := utf8.DecodeRune(b)
		switch {
		case r == utf8.RuneError && n == 1:
			return &FixtureProblem{"invalid_utf8_name"}
		case r == '\r' || r == '\n':
			return &FixtureProblem{"name_newline"}
		}
		b = b[n:]
	}
	return nil
}

// DaysRendered names how many UTC days a page's updated times spanned, in
// runs of consecutive items (the renderer formats a date once per run):
// no_objects, single_utc_day or multiple_utc_days, for the provider.path
// meter.
func DaysRendered(page *Page) string {
	runs := 0
	day := int64(-1 << 63)
	for i := range page.Items {
		sec, _ := floorDiv(page.Items[i].Updated, 1_000_000)
		if d, _ := floorDiv(sec, 86_400); d != day {
			day = d
			runs++
		}
	}
	switch runs {
	case 0:
		return "no_objects"
	case 1:
		return "single_utc_day"
	}
	return "multiple_utc_days"
}
