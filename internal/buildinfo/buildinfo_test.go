// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package buildinfo

import (
	"runtime/debug"
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
	if got, want := UserAgent(), "swath/0.4.0-rc.1"; got != want {
		t.Fatalf("UserAgent() = %q, want %q", got, want)
	}
}

func TestUserAgent(t *testing.T) {
	for _, tc := range []struct {
		name  string
		build Build
		want  string
	}{
		{"unknown", Build{}, "swath/devel"},
		{"development", Build{Version: "devel"}, "swath/devel"},
		{"revision", Build{Version: "devel", Revision: "0123456789abcdef-dirty"}, "swath/devel+01234567"},
		{"unknown with revision", Build{Revision: "0123456789abcdef"}, "swath/devel+01234567"},
		{"short revision", Build{Version: "devel", Revision: "abc"}, "swath/devel+abc"},
		{"release", Build{Version: "v0.2.1", Revision: "0123456789abcdef"}, "swath/v0.2.1"},
		{"release without revision", Build{Version: "v0.2.1"}, "swath/v0.2.1"},
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

// TestReadHost: in a program that embeds swath, the build is swath's
// dependency version, never the host's version or VCS revision; in the
// swath binary, the main module's.
func TestReadHost(t *testing.T) {
	oldV, oldR := Version, Revision
	defer func() { Version, Revision = oldV, oldR }()
	Version, Revision = "", ""
	stamp := []debug.BuildSetting{{Key: "vcs.revision", Value: "0123abcd"}, {Key: "vcs.modified", Value: "true"}}
	host := func(dep *debug.Module) *debug.BuildInfo {
		info := &debug.BuildInfo{Main: debug.Module{Path: "example.com/host", Version: "v9.9.9"}, Settings: stamp}
		if dep != nil {
			info.Deps = []*debug.Module{{Path: "example.com/other", Version: "v1.0.0"}, dep}
		}
		return info
	}
	for _, tc := range []struct {
		name string
		info *debug.BuildInfo
		want Build
	}{
		{"swath", &debug.BuildInfo{Main: debug.Module{Path: Module, Version: "v0.5.0"}, Settings: stamp}, Build{Tool, "0.5.0", "0123abcd-dirty"}},
		{"host", host(&debug.Module{Path: Module, Version: "v0.4.2"}), Build{Tool, "0.4.2", ""}},
		{"host replacing swath with a release", host(&debug.Module{Path: Module, Version: "v0.4.2", Replace: &debug.Module{Path: "example.com/fork", Version: "v0.4.3"}}), Build{Tool, "0.4.3", ""}},
		{"host replacing swath with a directory", host(&debug.Module{Path: Module, Version: "v0.4.2", Replace: &debug.Module{Path: "../swath"}}), Build{Tool, "devel", ""}},
		{"host in a workspace", host(&debug.Module{Path: Module, Version: "(devel)"}), Build{Tool, "devel", ""}},
		{"no build info", nil, Build{Tool, "devel", ""}},
	} {
		if got := read(tc.info); got != tc.want {
			t.Errorf("%s: %+v, want %+v", tc.name, got, tc.want)
		}
	}
}
