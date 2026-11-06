// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package objhttp

import "testing"

func TestCanonicalRoute(t *testing.T) {
	for in, want := range map[string]string{
		"HTTP://Host:1/storage/v1/b/x/o": "http://host:1/storage/v1/b/x/o",
		"https://B.S3.Amazonaws.com/":    "https://b.s3.amazonaws.com/",
		"http://h/Base//K":               "http://h/Base//K", // path bytes kept
		"http://H":                       "http://h",
		"no-scheme":                      "no-scheme",
	} {
		if got := CanonicalRoute(in); got != want {
			t.Errorf("%q: %q, want %q", in, got, want)
		}
	}
}
