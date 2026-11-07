// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package azure

import (
	"bytes"
	"encoding/xml"
	"errors"
	"io"
	"net/url"
	"slices"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/varveio/swath/go/internal/replay/store"
)

const b64Alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"

// FuzzMarker holds the marker contract: decodeMarker never panics and
// answers every refusal with errInvalidMarker; an accepted marker re-encodes
// to one that decodes to the same cursor, progress byte included (the
// progress byte sits outside the binding and every marker issued from one
// carries it on); an encoded marker round-trips; and a marker under
// another binding, or with a character of its binding altered, is refused.
func FuzzMarker(f *testing.F) {
	bind := bytes.Repeat([]byte{7}, bindingBytes)
	for _, b := range []string{"", "a/1", "k\xff", "logs/alpha/dt=2026-01-01/part-00004.json.gz"} {
		f.Add(b, true, byte(0), uint(5), encodeMarker(false, []byte(b), bind, 1))
		f.Add(b, false, byte(1), uint(20), "")
	}
	for _, s := range []string{"az1.", "az1.AAA", "az2.AAAA", encodeMarker(true, []byte("ab"), bind, 0) + "==", "az1.\n", "az1.AA=="} {
		f.Add("k", false, byte(0), uint(1), s)
	}
	f.Fuzz(func(t *testing.T, boundary string, inclusive bool, progress byte, pos uint, raw string) {
		bind := bytes.Repeat([]byte{7}, bindingBytes)
		if m, err := decodeMarker(raw, bind); err != nil {
			if err != errInvalidMarker {
				t.Fatalf("%q: %v", raw, err)
			}
		} else {
			again, err := decodeMarker(encodeMarker(m.inclusive, m.boundary, bind, m.progress), bind)
			if err != nil || again.inclusive != m.inclusive || again.progress != m.progress || !bytes.Equal(again.boundary, m.boundary) {
				t.Fatalf("%q decoded to %+v, which does not re-encode to itself: %+v %v", raw, m, again, err)
			}
		}

		progress &= 1
		if len(boundary) > maxBoundaryBytes {
			boundary = boundary[:maxBoundaryBytes]
		}
		tok := encodeMarker(inclusive, []byte(boundary), bind, progress)
		m, err := decodeMarker(tok, bind)
		if err != nil || m.inclusive != inclusive || m.progress != progress || !bytes.Equal(m.boundary, []byte(boundary)) {
			t.Fatalf("round trip of %q/%v/%d: %+v %v", boundary, inclusive, progress, m, err)
		}
		other := bytes.Clone(bind)
		other[int(pos%uint(bindingBytes))] ^= 1 << (pos / bindingBytes % 8)
		if _, err := decodeMarker(tok, other); err != errInvalidMarker {
			t.Fatalf("marker accepted under another binding: %v", err)
		}
		// Characters 3 to 23 of the body lie wholly in the binding.
		body := strings.TrimPrefix(tok, markerPrefix)
		at := 3 + int(pos%21)
		for i := range len(b64Alphabet) {
			if b64Alphabet[i] == body[at] {
				continue
			}
			if _, err := decodeMarker(markerPrefix+body[:at]+b64Alphabet[i:i+1]+body[at+1:], bind); err != errInvalidMarker {
				t.Fatalf("binding character %d of %q replaced by %q: %v", at, tok, b64Alphabet[i], err)
			}
		}
	})
}

// FuzzMarkerScope holds that a marker is accepted by ValidateMarker
// exactly when the request's scope (fixture identity, account, container,
// service version, prefix, delimiter and startFrom; the page size is not
// bound) is the one it was issued under.
func FuzzMarkerScope(f *testing.F) {
	f.Add("a/", "/", "", "a/", "/", "", "fx", "fx", true)
	f.Add("a/", "", "a/1", "a/", "", "a/2", "fx", "fx", false)
	f.Add("", "", "", "", "", "", "fx", "fy", false)
	f.Fuzz(func(t *testing.T, p1, d1, s1, p2, d2, s2, fx1, fx2 string, otherVersion bool) {
		a := &Request{Account: "replay", Container: "c1", Version: Version20260606, Prefix: p1, Delimiter: d1, StartFrom: s1}
		b := &Request{Account: "replay", Container: "c1", Version: Version20260606, Prefix: p2, Delimiter: d2, StartFrom: s2, PageSize: 7}
		if otherVersion {
			b.Version = Version20261006
		}
		b.Marker = encodeMarker(false, []byte("boundary"), binding(a, fx1), 0)
		b.MarkerSupplied = true
		err := (&Pager{FixtureIdentity: fx2}).ValidateMarker(b)
		same := p1 == p2 && d1 == d2 && s1 == s2 && fx1 == fx2 && !otherVersion
		if same != (err == nil) {
			t.Fatalf("scope %q %q %q/%q vs %q %q %q/%q version-changed=%v: same=%v, err=%v", p1, d1, s1, fx1, p2, d2, s2, fx2, otherVersion, same, err)
		}
		var e *Error
		if err != nil && (!errors.As(err, &e) || e.Kind != Malformed) {
			t.Fatalf("%v", err)
		}
	})
}

// FuzzParse holds that Parse never panics, that every refusal is an
// *Error, and that an accepted request is within the documented bounds.
func FuzzParse(f *testing.F) {
	for _, q := range []string{
		"restype=container&comp=list", "restype=container&comp=list&prefix=a%2F&delimiter=%2F&maxresults=7",
		"restype=container&comp=list&maxresults=0", "restype=container&comp=list&maxresults=-1",
		"restype=container&comp=list&maxresults=99999999999", "restype=container&comp=list&marker=az1.AAAA",
		"restype=container&comp=list&prefix=%00", "restype=container&comp=list&prefix=%ff",
		"restype=container&comp=list&timeout=0", "restype=container&comp=list&include=metadata",
		"restype=container&comp=list&startFrom=%E2%82%AC", "restype=container&comp=list&prefix=a&prefix=b", "", "%",
	} {
		f.Add(q)
	}
	f.Fuzz(func(t *testing.T, rawQuery string) {
		r, err := Parse("GET", "/replay/c1", rawQuery, version(Version20260606), "replay", "c1")
		if err != nil {
			var e *Error
			if !errors.As(err, &e) {
				t.Fatalf("%q: %v, want an *Error", rawQuery, err)
			}
			if s := e.Status(); s < 400 || s > 599 || e.Code() == "" {
				t.Fatalf("%q: status %d code %q", rawQuery, s, e.Code())
			}
			return
		}
		if r.PageSize < 1 || r.PageSize > MaxPageSize {
			t.Fatalf("%q: page size %d", rawQuery, r.PageSize)
		}
		for _, v := range []string{r.Prefix, r.Delimiter, r.StartFrom, r.Marker} {
			if !utf8.ValidString(v) || len(v) > defaultMarkerLength {
				t.Fatalf("%q: parameter %q", rawQuery, v)
			}
		}
		if invalidXMLEcho(r.Prefix) || invalidXMLEcho(r.Delimiter) {
			t.Fatalf("%q: accepted an echo XML cannot carry", rawQuery)
		}
	})
}

// FuzzCompareIsUTF16Order holds that Compare is the comparison of UTF-16
// code units for any two valid UTF-8 names, and a total order for any two
// byte strings.
func FuzzCompareIsUTF16Order(f *testing.F) {
	for _, s := range [][2]string{{"a", "b"}, {"\ue000", "\U00010000"}, {"\uffff", "\U00010000a"}, {"\U0010ffff", "\ud7ff"}, {"", "a"}, {"k\xee", "k\xf0"}} {
		f.Add(s[0], s[1])
	}
	f.Fuzz(func(t *testing.T, a, b string) {
		ab, ba := Compare([]byte(a), []byte(b)), Compare([]byte(b), []byte(a))
		if ab != -ba || (ab == 0) != (a == b) || Compare([]byte(a), []byte(a)) != 0 {
			t.Fatalf("%q vs %q: %d and %d", a, b, ab, ba)
		}
		if utf8.ValidString(a) && utf8.ValidString(b) {
			if want := utf16Less(a, b); ab != want {
				t.Fatalf("%q vs %q: %d, want %d", a, b, ab, want)
			}
		}
	})
}

// FuzzListingOrder holds that a walk of any valid names, flat or under a
// delimiter, from any prefix and startFrom, over any page size and with
// the rollup either native or read as rows, lists them as UTF-16 order
// does: the brute-force expectation of TestProviderOrderMatchesUTF16, whose
// cursor and ranges the names' lead bytes split and merge.
func FuzzListingOrder(f *testing.F) {
	f.Add("a\nb\n\ue000x\n\U00010000y\nz", "", "", "", uint8(2), false)
	f.Add("a/1\na/\U00010000\na/\ue000\nb/\uffff\nb/\U00010000", "a/", "/", "a/\ue000", uint8(1), true)
	f.Add("\ue000\ue000\n\ue000\U00010000\n\U00010000\ue000\n\U00010000\U00010000", "\ue000", "\U00010000", "\ue000\ue000", uint8(3), true)
	f.Fuzz(func(t *testing.T, joined, prefix, delimiter, startFrom string, size uint8, rollup bool) {
		seen := map[string]bool{}
		var names []string
		for _, n := range strings.Split(joined, "\n") {
			if n != "" && utf8.ValidString(n) && len(n) <= 256 && !seen[n] {
				seen[n] = true
				names = append(names, n)
			}
		}
		if len(names) > 40 || !utf8.ValidString(prefix) || !utf8.ValidString(delimiter) || !utf8.ValidString(startFrom) || len(prefix+delimiter+startFrom) > 512 {
			return
		}
		if startFrom != "" && !strings.HasPrefix(startFrom, prefix) {
			startFrom = prefix + startFrom
		}
		p := &Pager{Store: newStore(t, rollup, objects(names...)), FixtureIdentity: "f"}
		r := &Request{Account: "replay", Container: "c1", Version: Version20260606, Prefix: prefix, Delimiter: delimiter,
			StartFrom: startFrom, PageSize: 1 + int(size)%9}
		got, want := walk(t, p, r), expected(names, prefix, delimiter, startFrom)
		if !slices.Equal(got, want) {
			t.Fatalf("names %q prefix %q delimiter %q startFrom %q size %d rollup %v:\n got %q\nwant %q", names, prefix, delimiter, startFrom, r.PageSize, rollup, got, want)
		}
	})
}

// xmlLeaves returns the text of every element of a document by its path.
func xmlLeaves(doc []byte) (map[string][]string, error) {
	d := xml.NewDecoder(bytes.NewReader(bytes.TrimPrefix(doc, []byte(BOM))))
	out := map[string][]string{}
	var stack []string
	var idx []int
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
			path := strings.Join(stack, "/")
			out[path] = append(out[path], "")
			idx = append(idx, len(out[path])-1)
			for _, a := range t.Attr {
				out[path+"@"+a.Name.Local] = append(out[path+"@"+a.Name.Local], a.Value)
			}
		case xml.EndElement:
			stack, idx = stack[:len(stack)-1], idx[:len(idx)-1]
		case xml.CharData:
			if len(stack) > 0 {
				path := strings.Join(stack, "/")
				out[path][idx[len(idx)-1]] += string(t)
			}
		}
	}
}

// FuzzListingXML holds that a listing of any name and echoed values the
// route can render parses with encoding/xml and reads back as the input: a
// name as its text (or percent-decoded where it is sent Encoded="true"),
// the prefix, delimiter, container and endpoint as their text. A name the
// route refuses (control character, invalid UTF-8, over 1,024 UTF-16 code
// units, empty) is a FixtureIncompatible error, never a malformed body.
func FuzzListingXML(f *testing.F) {
	for _, n := range []string{"a", "logs/alpha/part-0.json.gz", "k&<>'\" space", "a\r\nb", "\ufffe", "x\uffffy", "\U00010000", "a\x00", "\xff", strings.Repeat("\U00010000", 600), ""} {
		f.Add(n, n, "/", "image/png")
	}
	f.Fuzz(func(t *testing.T, name, prefix, delimiter, contentType string) {
		if invalidXMLEcho(prefix) || invalidXMLEcho(delimiter) || !utf8.ValidString(prefix) || !utf8.ValidString(delimiter) {
			return
		}
		size := int64(3)
		obj := store.Object{Key: []byte(name), Size: &size, ContentType: &contentType}
		res := &Result{Request: &Request{Container: "c1", Prefix: prefix, PrefixSupplied: true, Delimiter: delimiter, DelimiterSupplied: true},
			Entries: []Entry{{Object: &obj}, {Prefix: []byte(name)}}, NextMarker: "az1." + prefix}
		doc, err := AppendList(nil, res, "http://127.0.0.1:1/replay/")
		if err != nil {
			var e *Error
			if !errors.As(err, &e) || e.Kind != FixtureIncompatible {
				t.Fatalf("name %q: %v", name, err)
			}
			return
		}
		leaves, err := xmlLeaves(doc)
		if err != nil {
			t.Fatalf("name %q prefix %q: %v\n%s", name, prefix, err, doc)
		}
		one := func(path string) string {
			if v := leaves[path]; len(v) > 0 {
				return v[0]
			}
			return ""
		}
		for _, c := range []struct{ path, want string }{
			{"EnumerationResults/Prefix", prefix}, {"EnumerationResults/Delimiter", delimiter},
			{"EnumerationResults@ContainerName", "c1"}, {"EnumerationResults@ServiceEndpoint", "http://127.0.0.1:1/replay/"},
			{"EnumerationResults/NextMarker", "az1." + prefix},
		} {
			if got := one(c.path); got != c.want {
				t.Fatalf("%s: %q, want %q\n%s", c.path, got, c.want, doc)
			}
		}
		// Names: the Blob's and the BlobPrefix's, in order, as the key.
		for _, path := range []string{"EnumerationResults/Blobs/Blob/Name", "EnumerationResults/Blobs/BlobPrefix/Name"} {
			got := leaves[path]
			if len(got) != 1 {
				t.Fatalf("%s: %q\n%s", path, got, doc)
			}
			if len(leaves[path+"@Encoded"]) > 0 {
				dec, err := url.PathUnescape(got[0])
				if err != nil || dec != name {
					t.Fatalf("%s: Encoded %q is %q (%v), want %q", path, got[0], dec, err, name)
				}
			} else if got[0] != name {
				t.Fatalf("%s: %q, want %q", path, got[0], name)
			}
		}
	})
}
