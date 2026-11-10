// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package azblob

import (
	"context"
	"testing"

	"github.com/varveio/swath/go/internal/source/azblob/azblobtest"
)

// TestRestartRecordsBoth: the provider's Restart records the resume in
// its own options and in the engine's (internal/source
// TestRegisteredRestart sees only the engine's, through the listing).
func TestRestartRecordsBoth(t *testing.T) {
	p, err := New(context.Background(), Options{Account: "acct", Container: "c", Client: azblobtest.NewStore(nil)})
	if err != nil {
		t.Fatal(err)
	}
	const after = "k/after"
	if err := p.Restart(after); err != nil {
		t.Fatal(err)
	}
	if r := p.opt.Resume; r == nil || r.After != after {
		t.Errorf("the provider's options record resume %+v, want after %q", r, after)
	}
	if r := p.Options().Resume; r == nil || r.After != after {
		t.Errorf("the engine's options record resume %+v, want after %q", r, after)
	}
}
