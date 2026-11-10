// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package objhttp

import "testing"

func TestParseEndpoint(t *testing.T) {
	const form = "http(s)://host[:port][/path]"
	for _, raw := range []string{
		"https://host/?sig=SECRET", "https://host/#SECRET", "https://host?", "https://host#",
		"https://user:SECRET@host", "host/?sig=SECRET", "https://host/%SECRET",
		"ftp://host/SECRET", "https:///SECRET", "SECRET",
		"https://host/%3Fsig=SECRET", "https://host/%23SECRET", "https://host/a%0ASECRET", "https://host/%253Fsig=SECRET%25",
	} {
		t.Run(raw, func(t *testing.T) {
			u, err := ParseEndpoint(raw, form)
			want := "endpoint: want " + form + " without user information, a query or a fragment"
			if u != nil || err == nil || err.Error() != want {
				t.Fatalf("URL %v, error %v; want %q", u, err, want)
			}
		})
	}
	for _, raw := range []string{"https://host", "http://host:9000/a%2Fb/../c", "https://host/a%20b%7B"} {
		u, err := ParseEndpoint(raw, form)
		if err != nil || u.String() != raw {
			t.Fatalf("%q: URL %v, error %v", raw, u, err)
		}
	}
}
