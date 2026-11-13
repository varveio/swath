// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package gcs

import (
	"strconv"
	"unicode/utf8"

	"github.com/varveio/swath/internal/replay/output"
)

// jsonWriter stages the page in a fixed buffer and writes it to the
// response sink when the buffer fills, at the points the wire contract's
// generator (Jackson's UTF-8 generator, with its default 8,000-byte
// output buffer) flushes its own:
//
//   - a single byte (a bracket, a comma, a colon, a quote) flushes first
//     when the buffer is full;
//   - a field name flushes first when it does not fit;
//   - an ASCII string value flushes first when its length would reach the
//     end, one longer than 1,000 characters (a long page token) is written
//     in 1,000-character segments, each flushing first when it does not fit;
//   - a UTF-8 value (a name, a prefix, the size and the time) is written in
//     segments of at most 1,000 bytes; a segment with nothing to escape
//     flushes first when it does not fit, one with an escape when six bytes
//     per input byte might not.
//
// The flush points matter beyond memory: a sink refusal (the response cap
// or the byte budget) surfaces only at a flush, so they decide whether an
// invalid row after them is reported as the row's problem or as the cap,
// and both servers report the same (TestRenderOrderMatchesTheContract,
// whose table the Java generator answered). Nothing larger than the buffer
// is ever held outside the sink.
type jsonWriter struct {
	o    *output.Output
	buf  []byte
	tail int
	err  bool
}

const (
	stagingBytes  = 8000
	maxContiguous = stagingBytes >> 3
)

func newJSONWriter(o *output.Output) *jsonWriter {
	return &jsonWriter{o: o, buf: make([]byte, stagingBytes)}
}

// flush writes the staged bytes to the sink; after a refusal nothing more
// is written.
func (w *jsonWriter) flush() {
	if w.tail > 0 && !w.err {
		w.o.Write(w.buf[:w.tail])
		w.err = w.o.Err() != nil
	}
	w.tail = 0
}

func (w *jsonWriter) failed() bool { return w.err }

func (w *jsonWriter) byte(c byte) {
	if w.tail >= len(w.buf) {
		w.flush()
	}
	w.buf[w.tail] = c
	w.tail++
}

func (w *jsonWriter) copy(b []byte) {
	w.tail += copy(w.buf[w.tail:], b)
}

func (w *jsonWriter) startObject() { w.byte('{') }
func (w *jsonWriter) startArray()  { w.byte('[') }

// field writes a member name and its colon, with a leading comma unless it
// is the object's first.
func (w *jsonWriter) field(name string, first bool) {
	if !first {
		w.byte(',')
	}
	w.byte('"')
	if w.tail+len(name) > len(w.buf) {
		w.flush()
	}
	w.tail += copy(w.buf[w.tail:], name)
	w.byte('"')
	w.byte(':')
}

// ascii writes s, which needs no escapes, as a string value.
func (w *jsonWriter) ascii(s string) {
	if len(s) > maxContiguous {
		w.byte('"')
		for len(s) > 0 {
			n := min(maxContiguous, len(s))
			if w.tail+n > len(w.buf) {
				w.flush()
			}
			w.tail += copy(w.buf[w.tail:], s[:n])
			s = s[n:]
		}
		w.byte('"')
		return
	}
	if w.tail+len(s) >= len(w.buf) {
		w.flush()
	}
	w.buf[w.tail] = '"'
	w.tail++
	w.tail += copy(w.buf[w.tail:], s)
	w.byte('"')
}

// utf8 writes valid UTF-8 b as a string value.
func (w *jsonWriter) utf8(b []byte) {
	w.byte('"')
	for len(b) > 0 {
		n := min(maxContiguous, len(b))
		w.segment(b[:n])
		b = b[n:]
	}
	w.byte('"')
}

func (w *jsonWriter) segment(b []byte) {
	for _, c := range b {
		if c < utf8.RuneSelf && escapes[c] != 0 {
			if w.tail+6*len(b) > len(w.buf) {
				w.flush()
			}
			for _, c := range b {
				w.tail += copy(w.buf[w.tail:], escaped(c))
			}
			return
		}
	}
	if w.tail+len(b) > len(w.buf) {
		w.flush()
	}
	w.copy(b)
}

// text writes a native metadata value, which may be any bytes, as a string
// value: escaped as utf8 does, an ill-formed UTF-8 sequence written as
// U+FFFD so the document stays valid, one character at a time so a value
// of any size passes through the buffer.
func (w *jsonWriter) text(s string) {
	w.byte('"')
	for len(s) > 0 {
		r, n := utf8.DecodeRuneInString(s)
		var enc []byte
		switch {
		case r == utf8.RuneError && n == 1:
			enc = []byte("\uFFFD")
		case n == 1:
			enc = escaped(s[0])
		default:
			enc = []byte(s[:n])
		}
		if w.tail+len(enc) > len(w.buf) {
			w.flush()
		}
		w.copy(enc)
		s = s[n:]
	}
	w.byte('"')
}

// metadata writes etag, generation, metageneration, storageClass and
// contentType (and md5Hash and crc32c when the fixture has them): the
// fixture's values for a native row, the synthetic profile otherwise or
// where a native column is null.
func (w *jsonWriter) metadata(n *Native) {
	str := func(name string, v *string, fallback string) {
		switch {
		case v != nil:
			w.field(name, false)
			w.text(*v)
		case fallback != "":
			w.field(name, false)
			w.ascii(fallback)
		}
	}
	num := func(name string, v *int64) {
		w.field(name, false)
		if v == nil {
			w.ascii(syntheticGeneration)
		} else {
			w.ascii(strconv.FormatInt(*v, 10))
		}
	}
	contentType := ""
	if n == nil {
		n, contentType = &Native{}, syntheticContentType
	}
	str("etag", n.ETag, syntheticETag)
	num("generation", n.Generation)
	num("metageneration", n.Metageneration)
	str("storageClass", n.StorageClass, syntheticStorageClass)
	// An object stored without a content type is listed without the
	// field (TestNativeNullContentTypeIsOmitted, from the live capture
	// replay); only the synthetic profile names one.
	str("contentType", n.ContentType, contentType)
	str("md5Hash", n.MD5, "")
	str("crc32c", n.CRC32C, "")
}

const hexDigits = "0123456789ABCDEF"

// escapes marks the ASCII bytes a string escapes: '"', '\\' and the C0
// controls.
var escapes = func() (t [utf8.RuneSelf]byte) {
	for c := range 0x20 {
		t[c] = 1
	}
	t['"'], t['\\'] = 1, 1
	return t
}()

// escaped is c as the wire contract writes it inside a string: '"' and
// '\\' backslashed, BS, TAB, LF, FF and CR as \b \t \n \f \r, the other C0
// controls as \u00XX with upper-case hex, anything else as itself.
func escaped(c byte) []byte {
	if c >= utf8.RuneSelf || escapes[c] == 0 {
		return []byte{c}
	}
	switch c {
	case '"', '\\':
		return []byte{'\\', c}
	case '\b':
		return []byte(`\b`)
	case '\t':
		return []byte(`\t`)
	case '\n':
		return []byte(`\n`)
	case '\f':
		return []byte(`\f`)
	case '\r':
		return []byte(`\r`)
	}
	return []byte{'\\', 'u', '0', '0', hexDigits[c>>4], hexDigits[c&0xf]}
}
