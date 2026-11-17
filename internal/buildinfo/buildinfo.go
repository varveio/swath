// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package buildinfo is the binary's identity: the tool's name, its version
// and the VCS revision it was built from. The run report, --version, the
// resume identity and the Parquet footer's created_by all read it here.
package buildinfo

import (
	"runtime/debug"
	"strings"
)

// Tool is the tool's name in every identity (report, journal, footer,
// User-Agent).
const Tool = "swath"

// Version is the release version (X.Y.Z or X.Y.Z-rc.N), set at link time
// by build.sh from $SWATH_VERSION (the release workflow passes its tag's
// version); empty, the Go module version is used without its leading v
// (go install's v0.4.0 is 0.4.0), devel for a build from a source checkout.
var Version string

// Revision is the commit the binary was built from, set at link time by
// build.sh and CI (-ldflags -X .../internal/buildinfo.Revision=...);
// empty, the Go toolchain's VCS stamp is used, which a build in a git
// worktree may lack.
var Revision string

// Build is a binary's name, version and VCS revision.
type Build struct {
	Tool     string `json:"tool"` // Tool
	Version  string `json:"version"`
	Revision string `json:"revision,omitempty"` // VCS revision, "-dirty" if modified
}

// Module is swath's module path. In a program that embeds swath (package
// command), the main module is the host's, and swath's version is that of
// its entry among the dependencies.
const Module = "github.com/varveio/swath"

// Read is this binary's build: swath's, also in a host that embeds it.
func Read() Build {
	info, _ := debug.ReadBuildInfo() // nil without module support
	return read(info)
}

// read is the build info describes (nil: none). The toolchain's version
// and VCS stamp are the main module's, so in a host only swath's
// dependency version is used: its version, devel for a local replacement
// or a workspace module, and no revision unless one was set at link time.
func read(info *debug.BuildInfo) Build {
	b := Build{Tool: Tool, Version: Version, Revision: Revision}
	if b.Version == "" {
		b.Version = "devel"
	}
	if info == nil {
		return b
	}
	mod, host := &info.Main, info.Main.Path != Module
	if host {
		mod = nil
		for _, d := range info.Deps {
			if d.Path == Module {
				mod = d
				if d.Replace != nil {
					mod = d.Replace
				}
				break
			}
		}
	}
	if mod != nil && Version == "" && mod.Version != "" && mod.Version != "(devel)" {
		b.Version = strings.TrimPrefix(mod.Version, "v") // the release's X.Y.Z, as build.sh stamps it
	}
	if b.Revision != "" || host {
		return b
	}
	dirty := false
	for _, s := range info.Settings {
		switch s.Key {
		case "vcs.revision":
			b.Revision = s.Value
		case "vcs.modified":
			dirty = s.Value == "true"
		}
	}
	if b.Revision != "" && dirty {
		b.Revision += "-dirty"
	}
	return b
}

// UserAgent is the HTTP product token for this binary.
func UserAgent() string { return Read().userAgent() }

func (b Build) userAgent() string {
	v := b.Version
	if v == "" {
		v = "devel"
	}
	if v == "devel" && b.Revision != "" {
		// Eight revision characters keep a development token under 24 bytes.
		v += "+" + b.Revision[:min(len(b.Revision), 8)]
	}
	return Tool + "/" + v
}
