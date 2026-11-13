// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package objfiletest_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/varveio/swath/go/objfile/azblob/azfake"
	"github.com/varveio/swath/go/objfile/gcs/gcsfake"
	"github.com/varveio/swath/go/objfile/s3/s3fake"
)

// TestFakesRefuseNegativeRanges: each fake answers a Range whose first byte
// is negative with 416, not a panic.
func TestFakesRefuseNegativeRanges(t *testing.T) {
	g := gcsfake.New("bkt")
	g.Put("obj", []byte("0123456789"))
	s := s3fake.New("bkt")
	s.Put("obj", []byte("0123456789"))
	a := azfake.New("acct", "ctr")
	a.Put("obj", []byte("0123456789"))
	for _, c := range []struct {
		name, path, header string
		h                  http.Handler
	}{
		{"gcsfake", "/bkt/obj", "Range", g},
		{"s3fake", "/bkt/obj", "Range", s},
		{"azfake", "/acct/ctr/obj", "x-ms-range", a},
	} {
		for _, rng := range []string{"bytes=-1-1", "bytes=-5-"} {
			if c.name != "gcsfake" && rng == "bytes=-5-" {
				continue // only gcsfake serves an open range
			}
			r := httptest.NewRequest(http.MethodGet, c.path, nil)
			r.Header.Set(c.header, rng)
			w := httptest.NewRecorder()
			c.h.ServeHTTP(w, r)
			if w.Code != http.StatusRequestedRangeNotSatisfiable {
				t.Errorf("%s, %s: status %d, want 416", c.name, rng, w.Code)
			}
		}
	}
}
