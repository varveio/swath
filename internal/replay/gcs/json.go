// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package gcs

import (
	"bytes"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/varveio/swath/internal/replay/output"
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
// (TestRenderGolden).
//
// Rendering goes through a fixed 8,000-byte staging buffer that is written
// to o only when it fills (and at the end), at the points the wire
// contract's JSON generator flushes its own buffer (jsonWriter), and each
// item and prefix is validated before any of it is staged. So nothing
// larger than the buffer is ever held outside o, whatever the size of a
// name or a native metadata value, o charges every byte before it grows,
// and when an invalid fixture row and o's cap meet, the one reported is
// the contract's: the row's problem unless a flush before it already
// passed the cap (TestRenderOrderMatchesTheContract). A refusal by o (its
// cap or budget, o.Err) stops rendering at that flush; a fixture row the
// route cannot render is a *FixtureProblem.
func RenderPage(o *output.Output, r *Request, page *Page) error {
	w := newJSONWriter(o)
	w.startObject()
	w.field("kind", true)
	w.ascii("storage#objects")
	if len(page.Items) > 0 {
		w.field("items", false)
		w.startArray()
		day, date := int64(-1<<63), ""
		var updated, createdBuf [27]byte
		var timeCreated []byte
		for i := range page.Items {
			if w.failed() {
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
			created := it.Native != nil && it.Native.TimeCreated != nil
			if created {
				var ok bool
				if timeCreated, ok = appendTime(createdBuf[:0], *it.Native.TimeCreated); !ok {
					return &FixtureProblem{"timestamp_out_of_range"}
				}
			}
			if i > 0 {
				w.byte(',')
			}
			w.startObject()
			w.field("kind", true)
			w.ascii("storage#object")
			w.field("bucket", false)
			w.ascii(r.Bucket)
			w.field("name", false)
			w.utf8(it.Key)
			w.field("size", false)
			w.utf8(strconv.AppendInt(updated[:0], it.Size, 10))
			if created {
				// Live GCS lists timeCreated just before updated; it is
				// written only where the recording stored it.
				w.field("timeCreated", false)
				w.utf8(timeCreated)
			}
			w.field("updated", false)
			w.utf8(appendUpdated(updated[:0], date, sec, frac))
			w.metadata(it.Native)
			if r.FullProjection {
				w.field("acl", false)
				w.startArray()
				w.byte(']')
			}
			w.byte('}')
		}
		w.byte(']')
	}
	if len(page.Prefixes) > 0 {
		w.field("prefixes", false)
		w.startArray()
		for i, p := range page.Prefixes {
			if w.failed() {
				return nil
			}
			if err := validateUTF8(p); err != nil {
				return err
			}
			if i > 0 {
				w.byte(',')
			}
			w.utf8(p)
		}
		w.byte(']')
	}
	if page.Token != "" {
		w.field("nextPageToken", false)
		w.ascii(page.Token)
	}
	w.byte('}')
	w.flush()
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

// appendTime writes epoch microseconds as appendUpdated does, or reports
// false for a time outside years 0 to 9999.
func appendTime(dst []byte, micros int64) ([]byte, bool) {
	sec, frac := floorDiv(micros, 1_000_000)
	d, _ := floorDiv(sec, 86_400)
	t := time.Unix(d*86_400, 0).UTC()
	if t.Year() < 0 || t.Year() > 9999 {
		return dst, false
	}
	return appendUpdated(dst, t.Format("2006-01-02"), sec, frac), true
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
