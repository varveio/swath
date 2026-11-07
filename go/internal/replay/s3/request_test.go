// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"errors"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/replay/latency"
)

func TestParseRequest(t *testing.T) {
	r, err := ParseRequest("bkt", "list-type=2&prefix=a%2F&delimiter=%2F&start-after=a%2F1&max-keys=5000&encoding-type=url&fetch-owner=TRUE&continuation-token=")
	if err != nil {
		t.Fatal(err)
	}
	if string(r.Prefix) != "a/" || string(r.Delimiter) != "/" || string(r.StartAfter) != "a/1" || r.MaxKeys != 5000 ||
		r.PageSize() != 1000 || !r.EncodingURL || !r.FetchOwner || !r.TokenSet || r.HasToken() {
		t.Errorf("parsed %+v", r)
	}
	r, err = ParseRequest("bkt", "list-type=2&prefix=&max-keys=&encoding-type=URL&pre%66ix=q&prefix=a+b%2&fetch-owner=yes")
	if err != nil {
		t.Fatal(err)
	}
	// Empty values are absent, a later duplicate wins, names are
	// percent-decoded, '+' and a broken escape stay as they are, and only
	// "url" (any case not) selects URL encoding.
	if string(r.Prefix) != "a+b%2" || r.MaxKeys != 1000 || r.EncodingURL || r.FetchOwner || r.TokenSet {
		t.Errorf("parsed %+v", r)
	}
	r, _ = ParseRequest("bkt", "list-type=2&max-keys=%D9%A1%D9%A2&max-keys=+3")
	if r.MaxKeys != 3 {
		t.Errorf("max-keys=+3 parsed as %d", r.MaxKeys)
	}
	r, _ = ParseRequest("bkt", "list-type=2&max-keys=%D9%A1%D9%A2")
	if r.MaxKeys != 12 {
		t.Errorf("Arabic-Indic max-keys parsed as %d", r.MaxKeys)
	}
}

func TestParseRequestErrors(t *testing.T) {
	for q, want := range map[string]string{
		"":                                  "only ListObjectsV2 is supported",
		"max-keys=1":                        "only ListObjectsV2 is supported",
		"list-type=1":                       "only ListObjectsV2 is supported",
		"list-type=2&max-keys=-1":           "max-keys must be non-negative",
		"list-type=2&max-keys=abc":          "max-keys must be an integer",
		"list-type=2&max-keys=2147483648":   "max-keys must be an integer",
		"list-type=2&max-keys=1.0":          "max-keys must be an integer",
		"list-type=abc&max-keys=abc":        "only ListObjectsV2 is supported",
		"list-type=2&max-keys=-2147483648":  "max-keys must be non-negative",
		"list-type=2&max-keys=%2B%2B1":      "max-keys must be an integer",
		"list-type=2&max-keys=-0x1":         "max-keys must be an integer",
		"list-type=2&max-keys=%201":         "max-keys must be an integer",
		"list-type=%32&max-keys=%2D%31":     "max-keys must be non-negative",
		"list%2Dtype=2&max-keys=-5":         "max-keys must be non-negative",
		"list-type=2&max-keys=-1&max-keys=": "",
		// A mathematical digit one, outside the BMP: two UTF-16 code units, neither a digit.
		"list-type=2&max-keys=%F0%9D%9F%99":    "max-keys must be an integer",
		"list-type=2&max-keys=%EF%BC%91%D9%A0": "",
	} {
		_, err := ParseRequest("bkt", q)
		if want == "" {
			if err != nil {
				t.Errorf("%q: %v", q, err)
			}
			continue
		}
		var e *Error
		if !errors.As(err, &e) || e.Status != 400 || e.Code != "InvalidArgument" || e.Message != want {
			t.Errorf("%q: %v, want 400 InvalidArgument %q", q, err, want)
		}
	}
}

func TestParseHTTPRoutesByFirstSegment(t *testing.T) {
	for _, c := range []struct {
		method, path string
		status       int
	}{
		{"GET", "/bkt", 0},
		{"GET", "/bkt/trailing/segments", 0},
		{"GET", "/bkt/../other", 0},
		{"GET", "/b%6Bt", 404},
		{"GET", "/", 404},
		{"GET", "", 404},
		{"GET", "/other", 404},
		{"GET", "/bkt;x", 404},
		{"GET", "/x/../bkt", 404},
		{"HEAD", "/bkt", 405},
		{"POST", "/other", 405},
	} {
		_, err := ParseHTTP(c.method, c.path, "list-type=2", "bkt")
		var e *Error
		switch {
		case c.status == 0 && err != nil:
			t.Errorf("%s %s: %v", c.method, c.path, err)
		case c.status != 0 && (!errors.As(err, &e) || e.Status != c.status):
			t.Errorf("%s %s: %v, want %d", c.method, c.path, err, c.status)
		}
	}
}

// TestLatencyKeyGolden checks the jitter key end to end: the delays are the
// Java server's for the same requests under
// worker_page=223ms,pivot_probe=121ms,structure_probe=223ms+55ms/cp with
// jitter 0.15.
func TestLatencyKeyGolden(t *testing.T) {
	p, err := latency.Parse("worker_page=223ms,pivot_probe=121ms,structure_probe=223ms+55ms/cp", 0.15, 1)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		prefix, delim, startAfter string
		token                     *string
		maxKeys                   int32
		want                      time.Duration
	}{
		{"", "", "", nil, 1000, 243283920},
		{"a/", "", "", nil, 1000, 195661324},
		{"a/", "/", "", nil, 1000, 220312875},
		{"", "", "x", nil, 1, 127292513},
		{"", "", "", ptr("v2:AGxvZ3M"), 1000, 240201115},
		{"", "", "", ptr(""), 1000, 243283920},
		{"é\x00", "", "", ptr("tokén"), 7, 238277199},
		{"", "/", "", nil, 0, 198548590},
	} {
		r := &Request{Prefix: b(c.prefix), Delimiter: b(c.delim), StartAfter: b(c.startAfter), MaxKeys: c.maxKeys}
		if c.token != nil {
			r.Token, r.TokenSet = *c.token, true
		}
		shape := latency.Classify(len(r.Delimiter) > 0, int64(r.MaxKeys))
		if got := p.Delay(shape, 0, r.LatencyKey()); got != c.want {
			t.Errorf("%+v: %d, want %d", c, got, c.want)
		}
	}
}

func ptr(s string) *string { return &s }
