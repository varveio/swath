// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package buildinfo is the binary's identity: the tool's name, its module
// version and the VCS revision it was built from. The run report, --version,
// the resume identity and the Parquet footer's created_by all read it here.
package buildinfo

import "runtime/debug"

// Tool is the tool's name in every identity (report, journal, footer,
// User-Agent).
const Tool = "swath-go"

// Revision is the commit the binary was built from, set at link time by
// go/build.sh and CI (-ldflags -X .../internal/buildinfo.Revision=...);
// empty, the Go toolchain's VCS stamp is used, which a build in a git
// worktree may lack.
var Revision string

// Build is a binary's name, module version and VCS revision.
type Build struct {
	Tool     string `json:"tool"` // Tool
	Version  string `json:"version"`
	Revision string `json:"revision,omitempty"` // VCS revision, "-dirty" if modified
}

// Read is this binary's build.
func Read() Build {
	b := Build{Tool: Tool, Version: "devel", Revision: Revision}
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return b
	}
	if v := info.Main.Version; v != "" && v != "(devel)" {
		b.Version = v
	}
	if b.Revision != "" {
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
