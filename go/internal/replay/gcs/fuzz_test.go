// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package gcs

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"unicode/utf8"
)

const b64Alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"

// FuzzToken holds the page-token contract: decodeToken never panics and
// answers every refusal with errToken; a token decoded under its own
// binding re-encodes to a token that decodes to the same cursor (the
// decoder is lenient about padding and spare bits, so the spelling is not
// unique); an encoded token round-trips, progress byte included; and a
// token under another binding, or with a character of its binding
// altered, is refused.
func FuzzToken(f *testing.F) {
	bind := bytes.Repeat([]byte{7}, bindingBytes)
	for _, b := range []string{"", "a/1", "k\xff", "logs/alpha/dt=2026-01-01/part-00004.json.gz"} {
		f.Add(b, true, byte(0), uint(5), encodeToken(false, []byte(b), bind, 1))
		f.Add(b, false, byte(1), uint(20), "")
	}
	for _, s := range []string{"gcs1.", "gcs1.AAA", "gcs2.AAAA", encodeToken(true, []byte("ab"), bind, 0) + "==", "gcs1.\n"} {
		f.Add("k", false, byte(0), uint(1), s)
	}
	f.Fuzz(func(t *testing.T, boundary string, inclusive bool, progress byte, pos uint, raw string) {
		bind := bytes.Repeat([]byte{7}, bindingBytes)
		if c, err := decodeToken(raw, bind); err != nil {
			if err != errToken {
				t.Fatalf("%q: %v", raw, err)
			}
		} else {
			again, err := decodeToken(encodeToken(c.inclusive, c.boundary, bind, c.progress), bind)
			if err != nil || again.inclusive != c.inclusive || again.progress != c.progress || !bytes.Equal(again.boundary, c.boundary) {
				t.Fatalf("%q decoded to %+v, which does not re-encode to itself: %+v %v", raw, c, again, err)
			}
		}

		progress &= 1
		if len(boundary) > maxFieldBytes {
			boundary = boundary[:maxFieldBytes]
		}
		tok := encodeToken(inclusive, []byte(boundary), bind, progress)
		c, err := decodeToken(tok, bind)
		if err != nil || c.inclusive != inclusive || c.progress != progress || !bytes.Equal(c.boundary, []byte(boundary)) {
			t.Fatalf("round trip of %q/%v/%d: %+v %v", boundary, inclusive, progress, c, err)
		}
		other := bytes.Clone(bind)
		other[int(pos%uint(bindingBytes))] ^= 1 << (pos / bindingBytes % 8)
		if _, err := decodeToken(tok, other); err != errToken {
			t.Fatalf("token accepted under another binding: %v", err)
		}
		// Characters 3 to 23 of the body lie wholly in the binding.
		body := strings.TrimPrefix(tok, tokenPrefix)
		at := 3 + int(pos%21)
		for i := range len(b64Alphabet) {
			if b64Alphabet[i] == body[at] {
				continue
			}
			if _, err := decodeToken(tokenPrefix+body[:at]+b64Alphabet[i:i+1]+body[at+1:], bind); err != errToken {
				t.Fatalf("binding character %d of %q replaced by %q: %v", at, tok, b64Alphabet[i], err)
			}
		}
	})
}

// FuzzTokenScope holds that a token is accepted by a request exactly when
// the request's scope (bucket aside, which the route has already matched,
// the prefix, delimiter and both offsets) and the fixture are the ones it
// was issued under: the same scope with another page size is accepted, any
// other refused through Pager.ValidateToken.
func FuzzTokenScope(f *testing.F) {
	f.Add("a/", "/", "", "", "a/", "/", "", "", "fx", "fx")
	f.Add("a/", "", "a/1", "", "a/", "", "a/2", "", "fx", "fx")
	f.Add("", "", "", "z", "", "", "", "z", "fx", "fy")
	f.Fuzz(func(t *testing.T, p1, d1, s1, e1, p2, d2, s2, e2, fx1, fx2 string) {
		fields := []string{p1, d1, s1, e1, p2, d2, s2, e2}
		for _, v := range fields {
			if !utf8.ValidString(v) || len(v) > maxFieldBytes {
				return
			}
		}
		a := &Request{Bucket: "b", Prefix: []byte(p1), Delimiter: []byte(d1), StartOffset: []byte(s1), EndOffset: []byte(e1)}
		b := &Request{Bucket: "b", Prefix: []byte(p2), Delimiter: []byte(d2), StartOffset: []byte(s2), EndOffset: []byte(e2), PageSize: 7}
		issued := encodeToken(false, []byte("boundary"), tokenBinding(a, fx1), 0)
		b.PageToken = issued
		p := &Pager{fixtureIdentity: fx2}
		err := p.ValidateToken(b)
		same := p1 == p2 && d1 == d2 && s1 == s2 && e1 == e2 && fx1 == fx2
		if same != (err == nil) {
			// Distinct field tuples may not collide: the length prefixes
			// keep ("a","") and ("","a") apart, which is what this checks.
			t.Fatalf("scope %q %q %q %q/%q vs %q %q %q %q/%q: same=%v, err=%v", p1, d1, s1, e1, fx1, p2, d2, s2, e2, fx2, same, err)
		}
		if err != nil && !errors.Is(err, errToken) {
			t.Fatalf("%v", err)
		}
	})
}

// FuzzParse holds that Parse never panics, that every refusal is a
// *Failure, and that an accepted request is within the documented bounds:
// a page of 1 to 1,000, key parameters that are valid UTF-8 and at most
// 1,024 bytes, a token of at most 2,048.
func FuzzParse(f *testing.F) {
	for _, q := range []string{
		"", "prefix=a%2F&delimiter=%2F&maxResults=7", "maxResults=0", "maxResults=99999999999999999999", "maxResults=%2B1",
		"pageToken=gcs1.AAAA", "prefix=%ff", "prefix=a&prefix=b", "versions=true", "alt=media", "projection=full&prettyPrint=true",
		"startOffset=%E2%82", "%zz=1", "fields=", "x=1", "&&&", "prefix=a+b%2",
	} {
		f.Add(q)
	}
	f.Fuzz(func(t *testing.T, rawQuery string) {
		r, err := Parse("bkt", rawQuery)
		if err != nil {
			var fail *Failure
			if !errors.As(err, &fail) || fail.Reason == "" {
				t.Fatalf("%q: %v, want a *Failure with a reason", rawQuery, err)
			}
			return
		}
		if r.PageSize < 1 || r.PageSize > MaxPageSize || r.Clamped && r.PageSize != MaxPageSize {
			t.Fatalf("%q: page size %d clamped=%v", rawQuery, r.PageSize, r.Clamped)
		}
		for _, v := range [][]byte{r.Prefix, r.Delimiter, r.StartOffset, r.EndOffset} {
			if !utf8.Valid(v) || len(v) > maxFieldBytes {
				t.Fatalf("%q: key parameter %q", rawQuery, v)
			}
		}
		if len(r.PageToken) > maxTokenBytes {
			t.Fatalf("%q: token of %d bytes", rawQuery, len(r.PageToken))
		}
	})
}
