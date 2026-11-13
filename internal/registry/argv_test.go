// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package registry

import (
	"slices"
	"testing"

	"github.com/varveio/swath/internal/source"
)

// TestRedactEndpointArgv: the URL flags of the registered providers'
// shared groups (source.FlagGroup.URLs, the store group's --endpoint-url) are
// redacted when they carry userinfo, a query or fragment, or do not parse; kept as given
// otherwise.
func TestRedactEndpointArgv(t *testing.T) {
	if _, err := Lookup("argvtest://x"); err != nil { // registered once per process (-count)
		Register(Factory{Scheme: "argvtest", Shared: []*source.FlagGroup{source.StoreGroup}})
	}
	if len(source.StoreGroup.URLs) == 0 {
		t.Fatal("the store group declares no URL flag")
	}
	for _, flag := range source.StoreGroup.URLs {
		for _, endpoint := range []string{
			"https://user:password@host/base/", "https://user@host/",
			"https://user:password@[broken", "https://user:pass%ZZ@host/",
			"https://host/?sig=SECRET", "https://host/#SECRET", "https://host?", "https://host#", "https://host/%3Fsig=SECRET%0A", "https://host/%253Fsig=SECRET%25",
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

// TestRedactOutputSecretArgv: an output's secret flag (OutputSecretFlags)
// is hidden whatever its value, in either spelling.
func TestRedactOutputSecretArgv(t *testing.T) {
	for _, flag := range OutputSecretFlags {
		for _, argv := range [][]string{
			{"list", "--" + flag, "sv=1&sig=SECRET", "--anonymous"},
			{"list", "--" + flag + "=sv=1&sig=SECRET", "--anonymous"},
		} {
			want := []string{"list", "--" + flag + "=[redacted]", "--anonymous"}
			if got := RedactArgv(argv); !slices.Equal(got, want) {
				t.Errorf("%q: got %q, want %q", argv, got, want)
			}
		}
	}
}
