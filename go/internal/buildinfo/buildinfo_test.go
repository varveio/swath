// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package buildinfo

import "testing"

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
