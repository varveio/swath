// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package datasettest holds test helpers shared by the packages that write
// datasets.
package datasettest

import (
	"testing"

	"github.com/varveio/swath/go/internal/dataset"
)

// RequireJournal skips t where a journaled run is refused
// (listing.ErrUnenforced): on Windows, directory entries cannot be synced,
// so journaled output and resume are not offered there.
func RequireJournal(t testing.TB) {
	t.Helper()
	if !dataset.DirSyncEnforced {
		t.Skip("journaled output is refused on Windows: directory entries cannot be synced")
	}
}

// NoJournal reports whether a test that needs only a dataset (no resume,
// no crash recovery) should list without the journal: where a journaled
// run is refused (Windows) that is the supported way to list an object
// store; elsewhere false, so the test keeps its journaled run.
func NoJournal() bool { return !dataset.DirSyncEnforced }
