// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package gcs

import (
	"errors"
	"strings"
	"testing"
)

// TestParseRefusals holds each refusal to the reason and kind the Java
// replay server answered for the same query (probed on its GCS route).
func TestParseRefusals(t *testing.T) {
	for _, c := range []struct {
		query, reason string
		unsupported   bool
	}{
		{"maxResults=0", "invalid_max_results", false},
		{"maxResults=01", "invalid_max_results", false},
		{"maxResults=+5", "invalid_max_results", false},
		{"maxResults=-1", "invalid_max_results", false},
		{"maxResults=", "invalid_max_results", false},
		{"maxResults=99999999999999999999", "invalid_max_results", false},
		{"foo=1", "unknown_or_duplicate_parameter", false},
		{"Prefix=a", "unknown_or_duplicate_parameter", false},
		{"=x", "unknown_or_duplicate_parameter", false},
		{"&", "unknown_or_duplicate_parameter", false},
		{"maxResults=1&maxResults=2", "unknown_or_duplicate_parameter", false},
		{"prefix=a&pre%66ix=b", "unknown_or_duplicate_parameter", false},
		{"prefix=%ZZ", "invalid_percent_escape", false},
		{"prefix=%2", "invalid_percent_escape", false},
		{"prefix=%", "invalid_percent_escape", false},
		{"prefix=%C3", "invalid_utf8", false},
		{"prefix=%E9", "invalid_utf8", false},
		{"prefix=%ED%A0%80", "invalid_utf8", false},
		{"prefix=%C0%80", "invalid_utf8", false},
		{"prefix=%F4%90%80%80", "invalid_utf8", false},
		{"versions=true", "feature_versions", true},
		{"versions=False", "feature_versions", true},
		{"softDeleted=", "feature_softDeleted", true},
		{"includeFoldersAsPrefixes=true", "feature_includeFoldersAsPrefixes", true},
		{"includeTrailingDelimiter=true", "feature_includeTrailingDelimiter", true},
		{"matchGlob=**", "feature_matchGlob", true},
		{"filter=x", "feature_filter", true},
		{"fields=items", "feature_fields", true},
		{"alt=xml", "alt_format", true},
		{"alt=", "alt_format", true},
		{"prettyPrint=1", "invalid_pretty_print", false},
		{"projection=FULL", "invalid_projection", false},
		{"projection=", "invalid_projection", false},
		{"prefix=" + strings.Repeat("a", 1025), "field_too_long_prefix", true},
		{"endOffset=" + strings.Repeat("%C3%A9", 513), "field_too_long_endOffset", true},
		{"pageToken=" + strings.Repeat("a", 2049), "field_too_long_pageToken", true},
	} {
		_, err := Parse("bkt", c.query)
		var f *Failure
		if !errors.As(err, &f) || f.Reason != c.reason || f.Unsupported != c.unsupported {
			t.Errorf("%.60q: %v, want %s (unsupported %v)", c.query, err, c.reason, c.unsupported)
		}
	}
}

func TestParseValues(t *testing.T) {
	r, err := Parse("bkt", "prefix=a%2Fb+c&delimiter=%2F&startOffset=a/k&endOffset=a/z&maxResults=5000&pageToken=t&projection=full"+
		"&versions=false&softDeleted=false&includeFoldersAsPrefixes=false&includeTrailingDelimiter=false&matchGlob=&filter=&fields=&prettyPrint=true&alt=json")
	if err != nil {
		t.Fatal(err)
	}
	if string(r.Prefix) != "a/b+c" || string(r.Delimiter) != "/" || string(r.StartOffset) != "a/k" || string(r.EndOffset) != "a/z" ||
		r.PageSize != 1000 || !r.Clamped || r.PageToken != "t" || !r.FullProjection {
		t.Errorf("%+v", r)
	}
	r, err = Parse("bkt", "prefix=&delimiter=&pageToken=&maxResults=1000&prefix")
	if err == nil {
		t.Fatalf("a repeated empty prefix was accepted: %+v", r)
	}
	r, err = Parse("bkt", "prefix&delimiter=&pageToken=&maxResults=1000")
	if err != nil || r.Prefix != nil || r.Delimiter != nil || r.PageToken != "" || r.PageSize != 1000 || r.Clamped || r.FullProjection {
		t.Errorf("%+v %v", r, err)
	}
	if r, err := Parse("bkt", ""); err != nil || r.PageSize != MaxPageSize {
		t.Errorf("%+v %v", r, err)
	}
	if r, err := Parse("bkt", "maxResults=9223372036854775807"); err != nil || r.PageSize != MaxPageSize || !r.Clamped {
		t.Errorf("%+v %v", r, err)
	}
	if r, err := Parse("bkt", "prefix="+strings.Repeat("%C3%A9", 512)); err != nil || len(r.Prefix) != 1024 {
		t.Errorf("a 1,024-byte prefix: %v", err)
	}
}

// TestRequestTextGrouping holds the raw-byte decoding of the request line
// to the Java replay server's: a raw é is é, and each maximal ill-formed
// subsequence is one U+FFFD (probed against the fixture keys ".../u/é"
// and ".../u/U+FFFD": a raw E1 80 matched the one-replacement key, E0 80
// and ED A0 80 matched neither, F0 9F 98 matched it).
func TestRequestTextGrouping(t *testing.T) {
	for raw, want := range map[string]string{
		"a\u00e9b":         "a\u00e9b",
		"\xe1\x80":         "\uFFFD",
		"\xe0\x80":         "\uFFFD\uFFFD",
		"\xed\xa0\x80":     "\uFFFD\uFFFD\uFFFD",
		"\xf0\x9f\x98":     "\uFFFD",
		"\xf0\x9f\x98x":    "\uFFFDx",
		"\xff":             "\uFFFD",
		"\xc3\xa9\xc3":     "é\uFFFD",
		"\xf4\x90\x80\x80": "\uFFFD\uFFFD\uFFFD\uFFFD",
		"\U0001F600":       "\U0001F600",
	} {
		if got := string(requestText(raw)); got != want {
			t.Errorf("%q: %q, want %q", raw, got, want)
		}
	}
	r, err := Parse("bkt", "prefix=a\xff")
	if err != nil || string(r.Prefix) != "a\uFFFD" {
		t.Errorf("%+v %v", r, err)
	}
}

// TestLatencyKeyGolden holds the jitter key to the Java server's for the
// same requests (java.util.Objects.hash over the same fields, evaluated
// by the JDK the oracle runs on).
func TestLatencyKeyGolden(t *testing.T) {
	r := &Request{Prefix: []byte("a/"), Delimiter: []byte("/"), PageSize: 1000}
	if got := r.LatencyKey(); got != -1829945894 {
		t.Errorf("got %d", got)
	}
	r = &Request{Prefix: []byte("é\U0001F600"), StartOffset: []byte("s"), EndOffset: []byte("e"), PageSize: 7, PageToken: "gcs1.AAAA"}
	if got := r.LatencyKey(); got != 1594918676 {
		t.Errorf("got %d", got)
	}
}

// TestCompetingFeaturesUseTheFixedOrder: with several refused features
// the reason is the first in the fixed order. Across the two groups that
// is also the Java server's (the boolean group is checked first); within
// a group the Java server's choice varies by JVM launch.
func TestCompetingFeaturesUseTheFixedOrder(t *testing.T) {
	for query, want := range map[string]string{
		"softDeleted=true&versions=true":                           "feature_versions",
		"includeTrailingDelimiter=true&includeFoldersAsPrefixes=x": "feature_includeFoldersAsPrefixes",
		"fields=x&filter=x&matchGlob=x":                            "feature_matchGlob",
		"fields=x&filter=x":                                        "feature_filter",
		"fields=x&includeTrailingDelimiter=true":                   "feature_includeTrailingDelimiter",
		"alt=xml&fields=x":                                         "feature_fields",
		"versions=false&softDeleted=true&maxResults=0":             "feature_softDeleted",
	} {
		_, err := Parse("bkt", query)
		var f *Failure
		if !errors.As(err, &f) || f.Reason != want || !f.Unsupported {
			t.Errorf("%s: %v, want %s", query, err, want)
		}
	}
}

// TestMaxResultsBounds: the page size is checked as an int64 before it is
// narrowed. 1 to 1,000 are taken as asked; 1,001, 2^31, 2^31-1 and
// 2^63-1 are clamped to 1,000 (the wire contract parses a long and
// clamps); 2^63 and beyond, zero and negatives are refused.
func TestMaxResultsBounds(t *testing.T) {
	for v, want := range map[string]struct {
		size    int
		clamped bool
	}{
		"1": {1, false}, "999": {999, false}, "1000": {1000, false}, "1001": {1000, true},
		"2147483647": {1000, true}, "2147483648": {1000, true}, "9223372036854775807": {1000, true},
	} {
		r, err := Parse("bkt", "maxResults="+v)
		if err != nil || r.PageSize != want.size || r.Clamped != want.clamped {
			t.Errorf("%s: %+v %v", v, r, err)
		}
	}
	for _, v := range []string{"9223372036854775808", "18446744073709551616", "0", "-1", "-9223372036854775808"} {
		if _, err := Parse("bkt", "maxResults="+v); err == nil || err.(*Failure).Reason != "invalid_max_results" {
			t.Errorf("%s: %v", v, err)
		}
	}
}
