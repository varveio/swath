// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"context"
	"testing"
)

// suppliedVersions is a supplied client that can list objects and
// versions; it is asked nothing here.
type suppliedVersions struct {
	API
	VersionsAPI
}

// TestRestartRecordsBoth: the provider's Restart records the resume in
// its own options and in the engine's (internal/source
// TestRegisteredRestart sees only the engine's, through the listing), for
// the current objects and the versions.
func TestRestartRecordsBoth(t *testing.T) {
	opt := Options{Bucket: "bkt", Client: suppliedVersions{}}
	p, err := New(context.Background(), opt)
	if err != nil {
		t.Fatal(err)
	}
	restartRecords(t, "objects", p)
	opt.AllVersions = true
	v, err := NewVersions(context.Background(), opt)
	if err != nil {
		t.Fatal(err)
	}
	restartRecords(t, "versions", v)
}

func restartRecords[P any](t *testing.T, name string, p *provider[P]) {
	t.Helper()
	const after = "k/after"
	if err := p.Restart(after); err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	if r := p.opt.Resume; r == nil || r.After != after {
		t.Errorf("%s: the provider's options record resume %+v, want after %q", name, r, after)
	}
	if r := p.Options().Resume; r == nil || r.After != after {
		t.Errorf("%s: the engine's options record resume %+v, want after %q", name, r, after)
	}
}
