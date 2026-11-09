// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package registry

import (
	"slices"
	"testing"
)

func TestRedactEndpointArgv(t *testing.T) {
	for _, flag := range []string{"endpoint", "gcs-endpoint", "azblob-endpoint"} {
		for _, endpoint := range []string{
			"https://user:password@host/base/", "https://user@host/",
			"https://user:password@[broken", "https://user:pass%ZZ@host/",
		} {
			for _, argv := range [][]string{
				{"list", "--" + flag, endpoint, "--anonymous"},
				{"list", "--" + flag + "=" + endpoint, "--anonymous"},
			} {
				original := slices.Clone(argv)
				want := []string{"list", "--" + flag + "=[redacted]", "--anonymous"}
				got := RedactArgv(argv)
				if !slices.Equal(got, want) || !slices.Equal(argv, original) || !slices.Equal(RedactArgv(got), want) {
					t.Errorf("%q: got %q, want %q; input %q", original, got, want, argv)
				}
			}
		}
		// Valid endpoints are resume identity inputs: preserve them exactly.
		for _, argv := range [][]string{
			{"list", "--" + flag, "https://host/proxy%2Ftenant/"},
			{"list", "--" + flag + "=https://host/proxy%2Ftenant/"},
		} {
			if got := RedactArgv(argv); !slices.Equal(got, argv) {
				t.Errorf("clean endpoint changed: %q -> %q", argv, got)
			}
		}
	}
}
