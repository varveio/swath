// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package buildinfo

import (
	"strings"
	"testing"
)

// TestRevision: a revision set at link time (build.sh, CI) is the build's,
// whatever the toolchain's VCS stamp says.
func TestRevision(t *testing.T) {
	old := Revision
	defer func() { Revision = old }()
	Revision = "0123abcd-dirty"
	if b := Read(); b.Revision != Revision || b.Tool != Tool {
		t.Fatalf("build %+v", b)
	}
}

// TestVersion: a version set at link time (build.sh, from $SWATH_VERSION)
// is the build's and its User-Agent's; unset, a source build is devel.
func TestVersion(t *testing.T) {
	old := Version
	defer func() { Version = old }()
	Version = ""
	if b := Read(); b.Version != "devel" {
		t.Fatalf("unstamped build %+v, want version devel", b)
	}
	Version = "0.4.0-rc.1"
	if b := Read(); b.Version != Version || b.Tool != Tool {
		t.Fatalf("stamped build %+v, want version %s", b, Version)
	}
	if got, want := UserAgent(), "swath-go/0.4.0-rc.1"; got != want {
		t.Fatalf("UserAgent() = %q, want %q", got, want)
	}
}

func TestUserAgent(t *testing.T) {
	for _, tc := range []struct {
		name  string
		build Build
		want  string
	}{
		{"unknown", Build{}, "swath-go/devel"},
		{"development", Build{Version: "devel"}, "swath-go/devel"},
		{"revision", Build{Version: "devel", Revision: "0123456789abcdef-dirty"}, "swath-go/devel+01234567"},
		{"unknown with revision", Build{Revision: "0123456789abcdef"}, "swath-go/devel+01234567"},
		{"short revision", Build{Version: "devel", Revision: "abc"}, "swath-go/devel+abc"},
		{"release", Build{Version: "v0.2.1", Revision: "0123456789abcdef"}, "swath-go/v0.2.1"},
		{"release without revision", Build{Version: "v0.2.1"}, "swath-go/v0.2.1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.build.userAgent()
			if got != tc.want || len(got) >= 24 || strings.ContainsAny(got, " \t\r\n") {
				t.Fatalf("User-Agent %q, want %q (under 24 bytes, no whitespace)", got, tc.want)
			}
		})
	}
	if got, want := UserAgent(), Read().userAgent(); got != want {
		t.Fatalf("UserAgent() = %q, want %q", got, want)
	}
}
