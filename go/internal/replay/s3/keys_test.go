// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"bytes"
	"encoding/hex"
	"math/rand/v2"
	"testing"
)

func TestPercentEncodeMatchesS3(t *testing.T) {
	for raw, want := range map[string]string{
		"a/has space+tilde~.txt": "a/has%20space%2Btilde%7E.txt",
		"test_file(3).png":       "test_file%283%29.png",
		"/ \xff&<>":              "/%20%FF%26%3C%3E",
		"é":                      "%C3%A9",
	} {
		if got := PercentEncode([]byte(raw)); got != want {
			t.Errorf("PercentEncode(%q) = %q, want %q", raw, got, want)
		}
	}
}

func TestPercentDecodeKeepsWhatIsNotAnEscape(t *testing.T) {
	for in, want := range map[string]string{
		"a%2Fb":   "a/b",
		"a%2fb":   "a/b",
		"a+b":     "a+b",
		"a%zz":    "a%zz",
		"a%2":     "a%2",
		"%":       "%",
		"%41%":    "A%",
		"%e2%82":  "\xe2\x82",
		"%FF%00x": "\xff\x00x",
	} {
		if got := PercentDecode(in); string(got) != want {
			t.Errorf("PercentDecode(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestQueryTextMatchesTable pins how raw (unescaped) non-ASCII bytes in the
// query string are read. The expected values are the Java server's answers
// to the same bytes sent as a raw prefix= value.
func TestQueryTextMatchesTable(t *testing.T) {
	for _, c := range []struct{ in, want string }{
		{"41", "A"}, {"80", "?"}, {"c0af", "??"}, {"c2", "?"}, {"c241", "?A"}, {"c2c2", "??"},
		{"e2", "?"}, {"e282", "?"}, {"e28241", "?A"}, {"e282ac", "?"}, {"e08080", "???"},
		{"e0a080", "?"}, {"eda080", "???"}, {"eda0", "??"}, {"ed9fbf", "?"}, {"efbfbf", "?"},
		{"f0", "?"}, {"f09f", "?"}, {"f09f98", "?"}, {"f09f9880", "?"}, {"f09f9841", "?A"},
		{"f08f8080", "????"}, {"f4908080", "????"}, {"f48f8080", "?"}, {"f5808080", "????"},
		{"f8", "?"}, {"ff", "?"}, {"e28241e2", "?A?"}, {"f0418080", "?A??"}, {"f09f4180", "?A?"},
		{"c2e282ac", "??"}, {"61ff62", "a?b"},
	} {
		raw, _ := hex.DecodeString(c.in)
		if got := queryText(string(raw)); got != c.want {
			t.Errorf("queryText(%s) = %q, want %q", c.in, got, c.want)
		}
	}
}

// TestDecodeTextMatchesTable pins the grouping of malformed UTF-8 into
// replacement characters when key bytes are rendered as text. The expected
// values are the Java server's decoding of the same bytes (and agree with
// it on 546,200 generated inputs, exhaustive to four bytes over the
// boundary bytes of every sequence class).
func TestDecodeTextMatchesTable(t *testing.T) {
	for _, c := range []struct{ in, want string }{
		{"41", "41"}, {"80", "efbfbd"}, {"c0af", "efbfbdefbfbd"}, {"c2", "efbfbd"},
		{"c241", "efbfbd41"}, {"c2c2", "efbfbdefbfbd"}, {"e2", "efbfbd"}, {"e282", "efbfbd"},
		{"e28241", "efbfbd41"}, {"e282ac", "e282ac"}, {"e08080", "efbfbdefbfbdefbfbd"},
		{"e0a080", "e0a080"}, {"eda080", "efbfbd"}, {"eda0", "efbfbd"}, {"ed9fbf", "ed9fbf"},
		{"efbfbf", "efbfbf"}, {"f0", "efbfbd"}, {"f09f", "efbfbd"}, {"f09f98", "efbfbd"},
		{"f09f9880", "f09f9880"}, {"f09f9841", "efbfbd41"},
		{"f08f8080", "efbfbdefbfbdefbfbdefbfbd"}, {"f4908080", "efbfbdefbfbdefbfbdefbfbd"},
		{"f48f8080", "f48f8080"}, {"f5808080", "efbfbdefbfbdefbfbdefbfbd"}, {"f8", "efbfbd"},
		{"ff", "efbfbd"}, {"e28241e2", "efbfbd41efbfbd"}, {"f0418080", "efbfbd41efbfbdefbfbd"},
		{"f09f4180", "efbfbd41efbfbd"}, {"c2e282ac", "efbfbde282ac"}, {"61ff62", "61efbfbd62"},
	} {
		raw, _ := hex.DecodeString(c.in)
		if got := hex.EncodeToString([]byte(decodeText(raw))); got != c.want {
			t.Errorf("decodeText(%s) = %s, want %s", c.in, got, c.want)
		}
	}
}

func TestIsBlank(t *testing.T) {
	for _, s := range []string{"", " ", "   ", "\t", "\n", " \t\n ", " ", "\x1c\x1f", " "} {
		if !isBlank(s) {
			t.Errorf("isBlank(%q) = false", s)
		}
	}
	for _, s := range []string{"a", " a ", " ", " ", " ", "�", "\x00"} {
		if isBlank(s) {
			t.Errorf("isBlank(%q) = true", s)
		}
	}
}

func TestParseInt32(t *testing.T) {
	for s, want := range map[string]int32{
		"0": 0, "-0": 0, "+7": 7, "1000": 1000, "2147483647": 2147483647, "-2147483648": -2147483648,
		"١٢": 12, // Arabic-Indic digits
		"９":  9,  // a fullwidth digit
	} {
		if got, ok := parseInt32(s); !ok || got != want {
			t.Errorf("parseInt32(%q) = %d, %v; want %d", s, got, ok, want)
		}
	}
	for _, s := range []string{"", "+", "-", "abc", "1.5", " 5", "5 ", "2147483648", "-2147483649", "99999999999999999999",
		"0x10", "1_000", "𝟗", "𝟙"} { // the last two: mathematical digits outside the BMP
		if _, ok := parseInt32(s); ok {
			t.Errorf("parseInt32(%q) accepted", s)
		}
	}
}

func TestPrefixUpperAndSuccessor(t *testing.T) {
	if u, open := PrefixUpper([]byte("photos/")); open || string(u) != "photos0" {
		t.Errorf("PrefixUpper(photos/) = %q, %v", u, open)
	}
	for _, p := range [][]byte{nil, {}, {0xff, 0xff}} {
		if _, open := PrefixUpper(p); !open {
			t.Errorf("PrefixUpper(%x) is bounded, want open", p)
		}
		if _, end := Successor(p); !end {
			t.Errorf("Successor(%x) has a key, want end of keyspace", p)
		}
	}
	if s, end := Successor([]byte("photos/")); end || string(s) != "photos0" {
		t.Errorf("Successor(photos/) = %q, %v", s, end)
	}
	if s, _ := Successor([]byte{'a', 'b', 0xff, 0xff}); string(s) != "ac" {
		t.Errorf("Successor(ab\\xff\\xff) = %q, want ac", s)
	}
	if s, _ := Successor([]byte{'a', 0xff}); string(s) != "b" {
		t.Errorf("Successor(a\\xff) = %q, want b", s)
	}
}

func randomBytes(r *rand.Rand, minLen, maxLen int) []byte {
	b := make([]byte, minLen+r.IntN(maxLen-minLen+1))
	for i := range b {
		// Bias toward the carry's edge bytes.
		switch r.IntN(4) {
		case 0:
			b[i] = 0xff
		case 1:
			b[i] = 0
		default:
			b[i] = byte(r.IntN(256))
		}
	}
	return b
}

// The prefix range law: for a prefix P with a successor, t starts with P
// iff P <= t < Successor(P); PrefixUpper(P) is the same bytes; every
// P||suffix is below the successor, and the successor itself is outside P.
func TestPrefixRangeLaw(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	for range 20000 {
		p := randomBytes(r, 1, 6)
		next, end := Successor(p)
		if end {
			if !bytes.Equal(p, bytes.Repeat([]byte{0xff}, len(p))) {
				t.Fatalf("Successor(%x) ended, but the prefix is not all 0xFF", p)
			}
			if _, open := PrefixUpper(p); !open {
				t.Fatalf("PrefixUpper(%x) bounded for an all-0xFF prefix", p)
			}
			continue
		}
		if u, open := PrefixUpper(p); open || !bytes.Equal(u, next) {
			t.Fatalf("PrefixUpper(%x) = %x, %v; Successor %x", p, u, open, next)
		}
		k := randomBytes(r, 0, 6)
		in := bytes.Compare(p, k) <= 0 && bytes.Compare(k, next) < 0
		if bytes.HasPrefix(k, p) != in {
			t.Fatalf("prefix %x key %x: HasPrefix %v, in [P, succ) %v", p, k, !in, in)
		}
		pk := append(bytes.Clone(p), k...)
		if bytes.Compare(pk, next) >= 0 || bytes.HasPrefix(next, p) {
			t.Fatalf("prefix %x: %x not below successor %x, or successor inside the prefix", p, pk, next)
		}
	}
}
