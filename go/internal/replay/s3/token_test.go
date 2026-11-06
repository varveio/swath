// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"bytes"
	"encoding/base64"
	"errors"
	"math/rand/v2"
	"testing"
)

// TestTokenGolden pins the wire bytes: the first was answered by the Java
// server for the same boundary, so a client walking either server sees the
// same tokens.
func TestTokenGolden(t *testing.T) {
	for _, c := range []struct {
		boundary  string
		inclusive bool
		want      string
	}{
		{"logs/alpha/dt=2026-01-01/part-00008.json.gz", false, "v2:AGxvZ3MvYWxwaGEvZHQ9MjAyNi0wMS0wMS9wYXJ0LTAwMDA4Lmpzb24uZ3o"},
		{"photos0", true, "v2:AXBob3RvczA"},
		{"", false, "v2:AA"},
		{"\xff\xfe?", false, "v2:AP_-Pw"},
	} {
		if got := EncodeToken([]byte(c.boundary), c.inclusive); got != c.want {
			t.Errorf("EncodeToken(%q, %v) = %q, want %q", c.boundary, c.inclusive, got, c.want)
		}
	}
}

func TestTokenRoundTripsAndLegacyV1(t *testing.T) {
	tok, ok, err := DecodeToken(EncodeToken([]byte("a/dir/"), false))
	if err != nil || !ok || string(tok.Boundary) != "a/dir/" || tok.Inclusive {
		t.Errorf("exclusive round trip: %+v %v %v", tok, ok, err)
	}
	tok, ok, err = DecodeToken(EncodeToken([]byte("photos0"), true))
	if err != nil || !ok || string(tok.Boundary) != "photos0" || !tok.Inclusive {
		t.Errorf("inclusive round trip: %+v %v %v", tok, ok, err)
	}
	tok, ok, err = DecodeToken("v1:" + base64.RawURLEncoding.EncodeToString([]byte("a/2")))
	if err != nil || !ok || string(tok.Boundary) != "a/2" || tok.Inclusive {
		t.Errorf("v1: %+v %v %v", tok, ok, err)
	}
	// Padding is accepted when complete, and unused trailing bits need not
	// be zero.
	for _, s := range []string{"v2:AGE=", "v2:AGE", "v2:AGF"} {
		if tok, _, err := DecodeToken(s); err != nil || string(tok.Boundary) != "a" {
			t.Errorf("DecodeToken(%q) = %+v, %v", s, tok, err)
		}
	}
}

func TestBlankTokenIsAbsent(t *testing.T) {
	for _, s := range []string{"", "   ", "\t", " "} {
		if _, ok, err := DecodeToken(s); ok || err != nil {
			t.Errorf("DecodeToken(%q) = %v, %v; want absent", s, ok, err)
		}
	}
}

func TestMalformedTokensAreRejected(t *testing.T) {
	flag7f := "v2:" + base64.RawURLEncoding.EncodeToString([]byte{0x7f, 'a', '/'})
	for _, s := range []string{"not-a-token", "v2:*** not base64 ***", "v2:", flag7f, "v3:AA", "V2:AA",
		"v2:AGE==", "v2:AG=", "v2:A", "v2:AG\nE", "v2:AG+E", "v2:AG/E", "v2:é", "v1:*"} {
		_, _, err := DecodeToken(s)
		var e *Error
		if !errors.As(err, &e) || e.Status != 400 || e.Message != "invalid continuation-token" {
			t.Errorf("DecodeToken(%q) = %v, want the 400 InvalidArgument", s, err)
		}
	}
}

// The token properties, on random boundaries over the full byte range.
func TestTokenProperties(t *testing.T) {
	const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
	r := rand.New(rand.NewPCG(3, 4))
	var rejected, different int
	for range 5000 {
		boundary := randomBytes(r, 1, 40)
		inclusive := r.IntN(2) == 1
		tok := EncodeToken(boundary, inclusive)
		got, ok, err := DecodeToken(tok)
		if err != nil || !ok || !bytes.Equal(got.Boundary, boundary) || got.Inclusive != inclusive {
			t.Fatalf("round trip of %x/%v: %+v %v", boundary, inclusive, got, err)
		}
		v1 := "v1:" + base64.RawURLEncoding.EncodeToString(boundary)
		if got, _, err := DecodeToken(v1); err != nil || !bytes.Equal(got.Boundary, boundary) || got.Inclusive {
			t.Fatalf("v1 of %x: %+v %v", boundary, got, err)
		}
		if _, _, err := DecodeToken(tok + "*"); err == nil {
			t.Fatalf("%q with a non-alphabet byte accepted", tok+"*")
		}
		payload := append([]byte{byte(2 + r.IntN(254))}, boundary...)
		if _, _, err := DecodeToken("v2:" + base64.RawURLEncoding.EncodeToString(payload)); err == nil {
			t.Fatalf("flag byte %#x accepted", payload[0])
		}
		// One mutated body character is rejected, decodes to a different
		// cursor, or is a pure base64 alias of the same payload.
		body := []byte(tok[3:])
		pos := r.IntN(len(body))
		c := alphabet[r.IntN(64)]
		if c == body[pos] {
			continue
		}
		orig, _ := base64.RawURLEncoding.DecodeString(string(body))
		body[pos] = c
		mut, aliasErr := base64.RawURLEncoding.DecodeString(string(body))
		if aliasErr == nil && bytes.Equal(orig, mut) {
			continue
		}
		got, _, err = DecodeToken("v2:" + string(body))
		switch {
		case err != nil:
			rejected++
		case bytes.Equal(got.Boundary, boundary) && got.Inclusive == inclusive:
			t.Fatalf("mutated token %q decodes to the original cursor", body)
		default:
			different++
		}
	}
	if rejected == 0 || different == 0 {
		t.Errorf("mutations: %d rejected, %d decoded differently; want both", rejected, different)
	}
}

// HasToken and DecodeToken classify every string the same way: a request
// never drops its start-after for a token that resolves to nothing.
func TestHasTokenAgreesWithDecode(t *testing.T) {
	r := rand.New(rand.NewPCG(5, 6))
	inputs := []string{"", " ", "   ", "\t", "\n", " \t\n "}
	for range 500 {
		b := make([]byte, r.IntN(20))
		for i := range b {
			b[i] = byte(r.IntN(128))
		}
		inputs = append(inputs, string(b), EncodeToken(randomBytes(r, 1, 10), true))
	}
	for _, s := range inputs {
		_, ok, err := DecodeToken(s)
		req := &Request{Token: s, TokenSet: true}
		if req.HasToken() != (ok || err != nil) {
			t.Errorf("token %q: HasToken %v, decode ok=%v err=%v", s, req.HasToken(), ok, err)
		}
	}
}
