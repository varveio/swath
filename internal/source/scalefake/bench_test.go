// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package scalefake

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

// BenchmarkS3Page is the fake's cost of one full ListObjectsV2 page
// (seek by start-after, 1,000 keys, XML) on the 1B-key campaign shape,
// natural and 1 KiB keys: what the server spends per request, so a run
// can tell whether the fake is its bottleneck.
func BenchmarkS3Page(b *testing.B) {
	for _, c := range []struct {
		name string
		s    Shape
	}{
		{"deep", Shape{Keys: 1_000_000_000, Fanout: []int{32, 32, 32, 8}, Skew: 1, Seed: 1}},
		{"deep-1KiB", Shape{Keys: 1_000_000_000, Fanout: []int{32, 32, 32, 8}, Skew: 1, KeyLen: 1024, Seed: 1}},
	} {
		b.Run(c.name, func(b *testing.B) {
			k, err := New(c.s)
			if err != nil {
				b.Fatal(err)
			}
			srv := NewServer(k, Options{})
			after := k.Key(123_456_789)
			r := httptest.NewRequest(http.MethodGet, "/bkt?list-type=2&encoding-type=url&start-after="+after, nil)
			b.ReportAllocs()
			for b.Loop() {
				w := httptest.NewRecorder()
				srv.ServeHTTP(w, r)
				if w.Code != 200 {
					b.Fatal(w.Code)
				}
				_, _ = io.Copy(io.Discard, w.Body)
			}
		})
	}
}

// TestAppendURL: the encoding decodes back to every byte string.
func TestAppendURL(t *testing.T) {
	for _, s := range []string{"", "a/b", "a b+c%/é\x00\xff~._-", "日本/x"} {
		var b bytes.Buffer
		appendURL(&b, s)
		got, err := url.PathUnescape(b.String())
		if err != nil || got != s || strings.ContainsAny(b.String(), " +") {
			t.Fatalf("%q -> %q -> %q (%v)", s, b.String(), got, err)
		}
	}
}
