// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"math/rand/v2"
	"net/url"
	"testing"

	"github.com/varveio/swath/internal/replay/store/memstore"
)

// TestTokenGolden pins the wire bytes. They are this server's own: the
// Java server's token for the first boundary was
// v2:AGxvZ3MvYWxwaGEvZHQ9MjAyNi0wMS0wMS9wYXJ0LTAwMDA4Lmpzb24uZ3o, the same
// flag and boundary without the checksum.
func TestTokenGolden(t *testing.T) {
	for _, c := range []struct {
		boundary  string
		inclusive bool
		want      string
	}{
		{"logs/alpha/dt=2026-01-01/part-00008.json.gz", false, "AGxvZ3MvYWxwaGEvZHQ9MjAyNi0wMS0wMS9wYXJ0LTAwMDA4Lmpzb24uZ3qV49Nz"},
		{"photos0", true, "AXBob3RvczA_OMM4"},
		{"", false, "ANIC740"},
		{"\xff\xfe?", false, "AP_-P6OFCv8"},
	} {
		if got := EncodeToken([]byte(c.boundary), c.inclusive); got != c.want {
			t.Errorf("EncodeToken(%q, %v) = %q, want %q", c.boundary, c.inclusive, got, c.want)
		}
		payload := append([]byte{0}, c.boundary...)
		if c.inclusive {
			payload[0] = 1
		}
		payload = binary.BigEndian.AppendUint32(payload, crc32.ChecksumIEEE(payload))
		if want := base64.RawURLEncoding.EncodeToString(payload); want != c.want {
			t.Errorf("%q: the golden %q is not base64url(flag || boundary || crc32) %q", c.boundary, c.want, want)
		}
	}
}

func TestTokenRoundTripsAndRefusesUncheckedForms(t *testing.T) {
	tok, ok, err := DecodeToken(EncodeToken([]byte("a/dir/"), false))
	if err != nil || !ok || string(tok.Boundary) != "a/dir/" || tok.Inclusive {
		t.Errorf("exclusive round trip: %+v %v %v", tok, ok, err)
	}
	tok, ok, err = DecodeToken(EncodeToken([]byte("photos0"), true))
	if err != nil || !ok || string(tok.Boundary) != "photos0" || !tok.Inclusive {
		t.Errorf("inclusive round trip: %+v %v %v", tok, ok, err)
	}
	// The Java server's unchecked forms resume nothing here.
	for _, s := range []string{"v1:" + base64.RawURLEncoding.EncodeToString([]byte("a/2")), "v2:AGE", "v2:AGE=", "v2:AXBob3RvczA"} {
		if _, _, err := DecodeToken(s); err != errInvalidToken {
			t.Errorf("DecodeToken(%q) = %v, want the 400 InvalidArgument", s, err)
		}
	}
}

func TestBlankTokenIsRefused(t *testing.T) {
	for _, s := range []string{"", "   ", "\t", "\u2003"} {
		if _, _, err := DecodeToken(s); err != errInvalidToken {
			t.Errorf("DecodeToken(%q) = %v; want the 400 InvalidArgument", s, err)
		}
	}
}

func TestMalformedTokensAreRejected(t *testing.T) {
	good := EncodeToken([]byte("a/"), false) // "AGEvXBsmCw" style: 10 characters
	sum := func(p []byte) []byte { return binary.BigEndian.AppendUint32(p, crc32.ChecksumIEEE(p)) }
	flag7f := base64.RawURLEncoding.EncodeToString(sum([]byte{0x7f, 'a', '/'}))
	short := base64.RawURLEncoding.EncodeToString([]byte{0, 1, 2, 3})
	for _, s := range []string{"not-a-token", "*** not base64 ***", flag7f, short, good + "=", good + "==", good[:len(good)-1],
		good[:4] + "\n" + good[4:], good[:4] + "+" + good[5:], good[:4] + "/" + good[5:], "é", "A"} {
		_, _, err := DecodeToken(s)
		var e *Error
		if !errors.As(err, &e) || e != errInvalidToken {
			t.Errorf("DecodeToken(%q) = %v, want the 400 InvalidArgument", s, err)
		}
	}
}

// Every alteration of a token is refused: one character replaced by any
// other in the alphabet, the last character dropped or four of them, and
// one decoded byte changed, over random boundaries of the full byte range.
func TestTokenProperties(t *testing.T) {
	const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
	r := rand.New(rand.NewPCG(3, 4))
	for range 5000 {
		boundary := randomBytes(r, 1, 40)
		inclusive := r.IntN(2) == 1
		tok := EncodeToken(boundary, inclusive)
		got, ok, err := DecodeToken(tok)
		if err != nil || !ok || !bytes.Equal(got.Boundary, boundary) || got.Inclusive != inclusive {
			t.Fatalf("round trip of %x/%v: %+v %v", boundary, inclusive, got, err)
		}
		body := []byte(tok)
		pos := r.IntN(len(body))
		if c := alphabet[r.IntN(64)]; c != body[pos] {
			body[pos] = c
			if _, _, err := DecodeToken(string(body)); err != errInvalidToken {
				t.Fatalf("token %q with %q at %d: %v, want 400 InvalidArgument", tok, c, pos, err)
			}
		}
		for _, cut := range []int{1, 4} {
			if _, _, err := DecodeToken(tok[:len(tok)-cut]); err != errInvalidToken {
				t.Fatalf("token %q less %d characters: %v, want 400 InvalidArgument", tok, cut, err)
			}
		}
		payload, _ := base64.RawURLEncoding.DecodeString(tok)
		payload[r.IntN(len(payload))] ^= byte(1 + r.IntN(255))
		if _, _, err := DecodeToken(base64.RawURLEncoding.EncodeToString(payload)); err != errInvalidToken {
			t.Fatalf("token %q with a changed byte: %v, want 400 InvalidArgument", tok, err)
		}
	}
}

// isBadToken is the contract's answer to a malformed token: an S3 error,
// 400 InvalidArgument, never another error type.
func isBadToken(err error) bool {
	var e *Error
	return errors.As(err, &e) && e.Status == 400 && e.Code == "InvalidArgument"
}

// TestTokenIsNotBound pins what the checksum does not establish: a
// token issued over one fixture resumes a pager over another at the
// same boundary, and a token built by a client for a boundary of its
// choosing is accepted, one past every key giving an empty page.
func TestTokenIsNotBound(t *testing.T) {
	list := func(keys []string, query string) *Result {
		t.Helper()
		req, err := ParseRequest("bkt", query)
		if err != nil {
			t.Fatalf("%s: %v", query, err)
		}
		res, err := (&Pager{Store: memstore.Keys(memstore.Options{}, keys...)}).List(context.Background(), req)
		if err != nil {
			t.Fatalf("%s: %v", query, err)
		}
		return res
	}
	a := list([]string{"a/1", "a/2", "a/3"}, "list-type=2&max-keys=2")
	if a.NextToken == "" {
		t.Fatal("no token")
	}
	other := []string{"a/1", "a/2x", "a/3", "b/9"}
	if got := render(list(other, "list-type=2&continuation-token="+url.QueryEscape(a.NextToken))); got != "O:a/2x O:a/3 O:b/9" {
		t.Errorf("another fixture, the issued token: %s", got)
	}
	built := EncodeToken([]byte("a/3"), false)
	if got := render(list(other, "list-type=2&continuation-token="+url.QueryEscape(built))); got != "O:b/9" {
		t.Errorf("a built token: %s", got)
	}
	past := list(other, "list-type=2&continuation-token="+url.QueryEscape(EncodeToken([]byte("zzz"), false)))
	if len(past.Entries) != 0 || past.NextToken != "" {
		t.Errorf("a built token past every key: %s", render(past))
	}
}
