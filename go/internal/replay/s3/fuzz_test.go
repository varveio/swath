// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"bytes"
	"encoding/base64"
	"encoding/xml"
	"errors"
	"io"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/varveio/swath/go/internal/replay/output"
	"github.com/varveio/swath/go/internal/replay/store"
)

const tokenAlphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"

// FuzzToken holds the token contract (docs/replay.md, "The S3 route"):
// DecodeToken never panics and answers every string that is not a token
// with the one 400 InvalidArgument; an accepted string is the token's only
// spelling (EncodeToken of what it decoded to is the string); a boundary
// round-trips; and every alteration of a token (a character replaced, one
// or four dropped, a decoded byte changed, a character appended) is
// refused, never a different boundary.
func FuzzToken(f *testing.F) {
	for _, b := range []string{"", "a/dir/", "photos0", "logs/alpha/dt=2026-01-01/part-00008.json.gz", "\xff\xfe?", "\x00"} {
		f.Add(b, true, uint(3), byte(1), "")
		f.Add(b, false, uint(0), byte(0x80), EncodeToken([]byte(b), false))
	}
	for _, s := range []string{"AGxvZ3MvYWxwaGEvZHQ9MjAyNi0wMS0wMS9wYXJ0LTAwMDA4Lmpzb24uZ3qV49Nz", "v2:AGE", "ANIC740", "AA", "A\n", "AGEvXBsmCw=="} {
		f.Add("k", false, uint(1), byte(1), s)
	}
	f.Fuzz(func(t *testing.T, boundary string, inclusive bool, pos uint, xor byte, raw string) {
		// Arbitrary text is either refused with the one error or is the
		// only spelling of what it decodes to.
		if tok, ok, err := DecodeToken(raw); err != nil {
			if ok || err != errInvalidToken {
				t.Fatalf("%q: ok=%v err=%v, want the 400 InvalidArgument", raw, ok, err)
			}
		} else if !ok || EncodeToken(tok.Boundary, tok.Inclusive) != raw {
			t.Fatalf("%q decoded to %+v but is not its spelling", raw, tok)
		}

		tokStr := EncodeToken([]byte(boundary), inclusive)
		got, ok, err := DecodeToken(tokStr)
		if err != nil || !ok || got.Inclusive != inclusive || !bytes.Equal(got.Boundary, []byte(boundary)) {
			t.Fatalf("round trip of %q/%v: %+v %v", boundary, inclusive, got, err)
		}
		refused := func(what, s string) {
			t.Helper()
			if s == tokStr {
				return
			}
			if _, _, err := DecodeToken(s); err != errInvalidToken {
				t.Fatalf("%s of %q: %q gave %v, want the 400 InvalidArgument", what, tokStr, s, err)
			}
		}
		at := int(pos % uint(len(tokStr)))
		for i := range len(tokenAlphabet) {
			refused("character replaced", tokStr[:at]+tokenAlphabet[i:i+1]+tokStr[at+1:])
		}
		refused("one dropped", tokStr[:len(tokStr)-1])
		refused("four dropped", tokStr[:max(len(tokStr)-4, 0)])
		refused("one appended", tokStr+"A")
		refused("padded", tokStr+"=")
		payload, err := base64.RawURLEncoding.DecodeString(tokStr)
		if err != nil {
			t.Fatal(err)
		}
		if xor != 0 {
			payload[int(pos%uint(len(payload)))] ^= xor
			refused("byte changed", base64.RawURLEncoding.EncodeToString(payload))
		}
	})
}

// FuzzParseRequest holds that ParseRequest never panics, that every
// refusal is a 400 InvalidArgument (a request of this route's own
// vocabulary: only the method and the bucket give another status, and
// they are not in the query), and that an accepted request is within the
// documented bounds: a page of 0 to 1,000 entries, no NUL where one is
// refused, and a continuation token that decodes.
func FuzzParseRequest(f *testing.F) {
	for _, q := range []string{
		"list-type=2", "list-type=2&prefix=a%2Fb&delimiter=%2F&max-keys=7", "list-type=2&max-keys=-1",
		"list-type=2&max-keys=%D9%A1%D9%A2", "list-type=2&marker=", "list-type=2&encoding-type=URL",
		"list-type=2&encoding-type=", "list-type=2&continuation-token=", "list-type=2&prefix=a%ZZ",
		"list-type=2&prefix=%00", "list-type=2&start-after=%00", "list-type=2&start-after=%FF&fetch-owner=true",
		"list-type=2&prefix=a&prefix=b", "list-type=2&continuation-token=" + EncodeToken([]byte("a/2"), false),
		"list-type=1", "", "list-type=2&%=1&&&=", "list-type=2&prefix=%E2%82", "list-type=2&prefix=a+b%2",
	} {
		f.Add(q)
	}
	f.Fuzz(func(t *testing.T, rawQuery string) {
		r, err := ParseRequest("bkt", rawQuery)
		if err != nil {
			var e *Error
			if !errors.As(err, &e) || e.Status != 400 || e.Code != "InvalidArgument" {
				t.Fatalf("%q: %v, want a 400 InvalidArgument", rawQuery, err)
			}
			// The refusal renders as a document encoding/xml reads, unless it
			// echoes a value XML cannot carry (a NUL, as a decimal reference).
			carriable := true
			for _, d := range e.Details {
				carriable = carriable && representableText(decodeString(d.Value))
			}
			if _, err := xmlTexts(AppendError(nil, e)); err != nil && carriable {
				t.Fatalf("%q: the error document: %v", rawQuery, err)
			}
			return
		}
		if n := r.PageSize(); n < 0 || n > MaxPageSize {
			t.Fatalf("%q: page size %d", rawQuery, n)
		}
		if bytes.IndexByte(r.Prefix, 0) >= 0 || !r.TokenSet && bytes.IndexByte(r.StartAfter, 0) >= 0 {
			t.Fatalf("%q: a NUL in a bound that is used", rawQuery)
		}
		if r.TokenSet {
			if _, ok, err := DecodeToken(r.Token); err != nil || !ok {
				t.Fatalf("%q: accepted a token that does not decode: %v", rawQuery, err)
			}
		}
		if !utf8.Valid(r.Prefix) || !utf8.Valid(r.Delimiter) || !utf8.Valid(r.StartAfter) {
			t.Fatalf("%q: a key parameter that is not UTF-8", rawQuery)
		}
		// Whatever was accepted can be echoed: the listing parses, unless a
		// bound holds text XML cannot carry.
		if _, err := xmlTexts(AppendList(nil, &Result{Request: r})); err != nil &&
			representableText(string(r.Prefix)) && representableText(string(r.Delimiter)) && representableText(string(r.StartAfter)) {
			t.Fatalf("%q: the listing: %v", rawQuery, err)
		}
	})
}

// xmlTexts parses a document with encoding/xml and returns the text of
// each leaf element, in order, keyed "Parent/Name" for repeated names
// kept apart by order of appearance.
func xmlTexts(doc []byte) ([]xmlLeaf, error) {
	d := xml.NewDecoder(bytes.NewReader(doc))
	var out []xmlLeaf
	var stack []string
	for {
		tok, err := d.Token()
		if err == io.EOF {
			return out, nil
		}
		if err != nil {
			return nil, err
		}
		switch t := tok.(type) {
		case xml.StartElement:
			stack = append(stack, t.Name.Local)
			out = append(out, xmlLeaf{Name: strings.Join(stack, "/")})
		case xml.EndElement:
			stack = stack[:len(stack)-1]
		case xml.CharData:
			if len(stack) > 0 {
				// Text belongs to the innermost open element.
				for i := len(out) - 1; i >= 0; i-- {
					if out[i].Name == strings.Join(stack, "/") {
						out[i].Text += string(t)
						break
					}
				}
			}
		}
	}
}

type xmlLeaf struct{ Name, Text string }

func leafText(leaves []xmlLeaf, name string) (string, bool) {
	for _, l := range leaves {
		if l.Name == name {
			return l.Text, true
		}
	}
	return "", false
}

// representableText reports whether encoding/xml can return s from a
// document that carries it as element text: valid UTF-8 of XML 1.0
// characters, without CR (the parser folds CR and CRLF to LF). Control
// characters other than TAB, LF and CR are written as character
// references, which XML 1.0 does not allow either.
func representableText(s string) bool {
	if !utf8.ValidString(s) {
		return false
	}
	for _, r := range s {
		switch {
		case r == '\t' || r == '\n':
		case r < 0x20 || r == '\r' || r == 0xfffe || r == 0xffff:
			return false
		}
	}
	return true
}

// urlDecode reads encoding-type=url text back as S3's table writes it:
// '+' is a space, %XX a byte.
func urlDecode(s string) ([]byte, bool) {
	var out []byte
	for i := 0; i < len(s); i++ {
		switch c := s[i]; c {
		case '+':
			out = append(out, ' ')
		case '%':
			if i+2 >= len(s) {
				return nil, false
			}
			hi, lo := strings.IndexByte("0123456789ABCDEF", s[i+1]), strings.IndexByte("0123456789ABCDEF", s[i+2])
			if hi < 0 || lo < 0 {
				return nil, false
			}
			out = append(out, byte(hi<<4|lo))
			i += 2
		default:
			out = append(out, c)
		}
	}
	return out, true
}

// FuzzListingText holds, for any key bytes, prefix and delimiter, that the
// listing parses with encoding/xml; that with encoding-type=url each
// echoed key decodes back to the bytes (S3's table: '+' for a space);
// and that without it each decodes to the text the key reads as (decodeText:
// malformed UTF-8 replaced), wherever that text is representable.
func FuzzListingText(f *testing.F) {
	for _, k := range []string{"", "a b+c", "k/😀&<>'\" space", "k\xed\xa0\x80z\xff", "k\x00\x01\t\n\r\x1f\x7f/", "a%41%", "\ufffe\uffff", "a\r\nb"} {
		f.Add([]byte(k), []byte(k), []byte("/"))
	}
	f.Fuzz(func(t *testing.T, key, prefix, delimiter []byte) {
		obj := store.Object{Key: key, ETag: sp(string(prefix))}
		for _, enc := range []bool{true, false} {
			req := &Request{Bucket: "b", MaxKeys: 1000, EncodingURL: enc, Prefix: prefix, Delimiter: delimiter, StartAfter: prefix}
			doc := AppendList(nil, &Result{Request: req, Entries: []Entry{{Object: &obj}, {CommonPrefix: delimiter}}})
			leaves, err := xmlTexts(doc)
			text := decodeText(key)
			if err != nil {
				// Only text XML cannot carry may be refused by the parser.
				if representableText(text) && representableText(decodeText(prefix)) && representableText(decodeText(delimiter)) {
					t.Fatalf("url=%v key %q prefix %q delimiter %q: %v\n%s", enc, key, prefix, delimiter, err, doc)
				}
				continue
			}
			for _, c := range []struct {
				path string
				raw  []byte
			}{{"ListBucketResult/Contents/Key", key}, {"ListBucketResult/Prefix", prefix}, {"ListBucketResult/Delimiter", delimiter},
				{"ListBucketResult/StartAfter", prefix}, {"ListBucketResult/CommonPrefixes/Prefix", delimiter}} {
				got, ok := leafText(leaves, c.path)
				if !ok {
					if len(c.raw) == 0 && (c.path == "ListBucketResult/Delimiter") {
						continue
					}
					t.Fatalf("url=%v: no %s in %s", enc, c.path, doc)
				}
				if enc {
					back, ok := urlDecode(got)
					if !ok || !bytes.Equal(back, c.raw) {
						t.Fatalf("url=true %s: %q of %q decodes to %q", c.path, got, c.raw, back)
					}
				} else if want := decodeText(c.raw); representableText(want) && got != want {
					t.Fatalf("url=false %s: %q, want %q (key %q)", c.path, got, want, c.raw)
				}
			}
		}
	})
}

// FuzzErrorText holds that an error document carrying any value parses
// with encoding/xml and returns the value as the error text reads it
// (malformed UTF-8 replaced), wherever that text is representable.
func FuzzErrorText(f *testing.F) {
	for _, v := range []string{"", "a&b<c>d\"e'f", "\x00\x01\x1f", "tab\tlf\ncr\r", "k\xff\xfe", "\U0001F600"} {
		f.Add(v)
	}
	f.Fuzz(func(t *testing.T, value string) {
		e := (&Error{Status: 400, Code: "InvalidArgument", Message: value}).with(Detail{"ArgumentName", "p"}, Detail{"ArgumentValue", value})
		doc := AppendError(nil, e)
		leaves, err := xmlTexts(doc)
		want := decodeString(value)
		if err != nil {
			if representableText(want) {
				t.Fatalf("%q: %v\n%s", value, err, doc)
			}
			return
		}
		for _, p := range []string{"Error/Message", "Error/ArgumentValue"} {
			if got, _ := leafText(leaves, p); representableText(want) && got != want {
				t.Fatalf("%s: %q, want %q", p, got, want)
			}
		}
	})
}

// FuzzRowFastPathMatchesUnits is TestRowFastPathMatchesUnits for any key
// and text values: a page rendered with whole Contents elements appended
// where they fit is the page rendered unit by unit, in bytes, refusal,
// chunks and percent-encoding counts, at any chunk size and cap.
func FuzzRowFastPathMatchesUnits(f *testing.F) {
	for _, k := range []string{"", "logs/alpha/dt=2026-01-01/part-00000.json.gz", "k/😀&<>'\" space", "k\xed\xa0\x80z\xff", "k\x00\x01\t\n\r\x1f\x7f/", strings.Repeat("long key/", 40)} {
		f.Add([]byte(k), "e&<>\"'\x0b\xff", uint16(97), uint16(1000), true, true)
		f.Add([]byte(k), "", uint16(3), uint16(50), false, false)
	}
	f.Fuzz(func(t *testing.T, key []byte, text string, chunk, limit uint16, enc, owner bool) {
		size := int64(len(key))
		obj := store.Object{Key: key, Size: &size, ETag: sp(text), StorageClass: sp(text), OwnerID: sp(text), OwnerDisplayName: sp(text),
			ChecksumAlgorithm: sp(text), ChecksumType: sp(text)}
		entries := []Entry{{Object: &obj}, {Object: &store.Object{Key: key}}, {Object: &obj}}
		type outcome struct {
			body                      string
			err                       error
			length, capacity, charged int
			onePass, fallback, chunks int
		}
		render := func(fast bool) outcome {
			rowFastPath = fast
			defer func() { rowFastPath = true }()
			chunks := 0
			budget := output.NewBudget(1<<30, func(output.Reason, int) { chunks++ })
			c, l := 1+int(chunk)%1500, 1+int(limit)*8
			o, err := output.NewChunked(budget, min(l, 4096), l, c, nil)
			if err != nil {
				t.Fatal(err)
			}
			RenderList(o, &Result{Request: &Request{Bucket: "b", MaxKeys: 1000, EncodingURL: enc, FetchOwner: owner}, Entries: entries})
			var b strings.Builder
			for _, v := range o.Body() {
				b.Write(v)
			}
			one, fall := o.PercentPaths()
			got := outcome{b.String(), o.Err(), o.Len(), o.Capacity(), o.Charged(), one, fall, chunks}
			o.Close()
			return got
		}
		if fast, units := render(true), render(false); fast != units {
			t.Fatalf("key %q text %q chunk %d limit %d:\nfast  %+v\nunits %+v", key, text, chunk, limit, fast, units)
		}
	})
}
