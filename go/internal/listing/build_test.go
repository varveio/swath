// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import "testing"

// TestBuildRevision: a revision set at link time (build.sh, CI) is the
// report's and --version's, whatever the toolchain's VCS stamp says.
func TestBuildRevision(t *testing.T) {
	old := revision
	defer func() { revision = old }()
	revision = "0123abcd-dirty"
	if b := BuildInfo(); b.Revision != revision || b.Tool != "swath-go" {
		t.Fatalf("build %+v", b)
	}
}
