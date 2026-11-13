// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"runtime"

	seekparquet "github.com/varveio/seekparquet/go"

	"github.com/varveio/swath/go/internal/replay/legacy"
)

// DefaultListIndexMaxBytes is the list index's retained-storage budget when
// ListIndex.MaxBytes is zero, the seekparquet module's own default.
const DefaultListIndexMaxBytes int64 = 64 << 20

// DefaultListIndexWorkers is the number of row groups the list index build
// decodes at once when ListIndex.Workers is zero: GOMAXPROCS, at most 8.
func DefaultListIndexWorkers() int { return max(1, min(runtime.GOMAXPROCS(0), 8)) }

// ListIndex configures the "/" listing index Open builds over a SeekParquet
// dataset (seekparquet's Dataset.BuildListIndex): each key page decoded once
// at open, recording where the directory runs under '/' start, so a
// delimited listing skips a wide common prefix without decoding a key page
// to find its end. The zero value builds it with the defaults. A Java-era
// fixture has no such index.
type ListIndex struct {
	// Off serves without the index.
	Off bool
	// MaxBytes bounds the index's retained storage; groups past the budget
	// are read without it. Zero selects DefaultListIndexMaxBytes.
	MaxBytes int64
	// Workers bounds the row groups decoded at once while building. Zero
	// selects DefaultListIndexWorkers().
	Workers int
}

// ListIndexState is the outcome of the list index build at open.
type ListIndexState string

const (
	// ListIndexBuilt: the build completed; its stats say what it covers
	// (a dataset with no '/' runs retains nothing).
	ListIndexBuilt ListIndexState = "built"
	// ListIndexOff: ListIndex.Off.
	ListIndexOff ListIndexState = "off"
	// ListIndexUnsupported: the fixture is not a SeekParquet dataset.
	ListIndexUnsupported ListIndexState = "unsupported"
	// ListIndexFailed: the build failed (ListIndexBuild.Err) and the
	// store serves without the index.
	ListIndexFailed ListIndexState = "failed"
)

// ListIndexBuild reports the list index build at open, for the startup
// line and the meters.
type ListIndexBuild struct {
	State ListIndexState
	// MaxBytes and Workers are the options of a build that ran: the
	// module's effective ones (ListIndexStats.Options) once it completed,
	// the resolved request otherwise.
	MaxBytes int64
	Workers  int
	// Stats are the module's build stats: groups indexed, skipped and
	// failed, parts failed, key pages decoded, entries and bytes retained,
	// budget exhaustion, duration, the effective options and whether any
	// group is indexed (HasIndex). The store builds once per dataset, so
	// OptionsMismatch stays false.
	Stats seekparquet.ListIndexStats
	// Err is a failed build's error.
	Err error
}

// ListIndexReads counts one rollup's use of the list index; all zero
// without one.
type ListIndexReads struct {
	// Skips counts common prefixes skipped through the index.
	Skips int64
	// MissingGroup, DensePage and UnsafeRun count the skips the index
	// declined, by reason: a row group it omits (over budget, flat or
	// unreadable), a key page with more directory changes than it
	// records, and a run whose successor it cannot establish exactly. A
	// declined skip decodes key pages as it would without the index. (The
	// module's fourth reason, another delimiter, cannot occur: the store
	// declines every delimiter but '/' before reading.)
	MissingGroup, DensePage, UnsafeRun int64
}

// RollupStats is one skip-scan's work: the reader's page counts and, over a
// SeekParquet dataset, its use of the list index.
type RollupStats struct {
	legacy.Stats
	ListIndex ListIndexReads
}

// buildListIndex builds the dataset's list index with opt. A failure other
// than cancellation is recorded in the result, not returned: the dataset
// serves without the index.
func (r *seekReader) buildListIndex(ctx context.Context, opt ListIndex) (ListIndexBuild, error) {
	if opt.Off {
		return ListIndexBuild{State: ListIndexOff}, nil
	}
	b := ListIndexBuild{MaxBytes: opt.MaxBytes, Workers: opt.Workers}
	if b.MaxBytes == 0 {
		b.MaxBytes = DefaultListIndexMaxBytes
	}
	if b.Workers == 0 {
		b.Workers = DefaultListIndexWorkers()
	}
	b.Stats, b.Err = r.d.BuildListIndex(ctx, seekparquet.ListIndexOptions{MaxBytes: b.MaxBytes, Workers: b.Workers})
	switch {
	case b.Err == nil:
		b.State = ListIndexBuilt
		b.MaxBytes, b.Workers = b.Stats.Options.MaxBytes, b.Stats.Options.Workers
	case ctx.Err() != nil:
		return ListIndexBuild{}, b.Err
	default:
		// A storage failure or a resource limit fails the whole attempt
		// (data refusals and unsupported encodings only omit their groups).
		b.State = ListIndexFailed
	}
	return b, nil
}

// countIndex adds the module's list index counts of one request to idx.
func countIndex(idx *ListIndexReads, st *seekparquet.ReadStats) {
	idx.Skips += st.ListIndexSkips
	idx.MissingGroup += st.ListIndexMissingGroup
	idx.DensePage += st.ListIndexDensePage
	idx.UnsafeRun += st.ListIndexUnsafeRun
}

// legacyReader is the legacy core's reader under the store's rollup stats,
// which it fills with its page counts only.
type legacyReader struct{ *legacy.Reader }

// Scan is the legacy core's Scan, handing sink the legacy core's page
// values (legacy.ColumnSink).
func (r legacyReader) Scan(ctx context.Context, rng legacy.Range, stats *legacy.Stats, sink columnSink) error {
	return r.Reader.Scan(ctx, rng, stats, sink)
}

func (r legacyReader) RollupRead(ctx context.Context, rl legacy.Rollup, stats *RollupStats) ([]legacy.Entry, error) {
	return r.Reader.RollupRead(ctx, rl, &stats.Stats)
}
