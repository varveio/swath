// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package dataset owns the on-disk layout of a sorted listing dataset:
// output-directory safety, the private workspace, the manifest and the
// commit order. The manifest is the commit record: a dataset exists once
// manifest.json is installed, and not before.
package dataset

import (
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
)

// Format identifies the dataset format in the manifest and the footers: a
// SeekParquet dataset (format 1, draft revision 1, stamped format_version
// 0), keyed by KeyColumn. swath's own metadata (what was listed, how much
// of it, the parts' MD5 digests) is the manifest's Extension.
const (
	Format        = "seekparquet"
	FormatVersion = 0
	DraftRevision = 1
	KeyColumn     = "key"
	Extension     = "swath"
)

// LegacyFormat is the swath-dataset format 2 earlier builds wrote: still
// read where a committed dataset is read (show, --seed-from, Managed), and
// served by the replay server; never written.
const (
	LegacyFormat        = "swath-dataset"
	LegacyFormatVersion = 2
)

// File and directory names inside a dataset.
const (
	DataDir      = "data"
	ManifestName = "manifest.json"
	// ReportName is the run report, operational, written after the commit.
	ReportName = "run.json"
	// ErrorName is the outcome of a run that did not complete (class, exit
	// code, cause, resumable), written by the command.
	ErrorName = "error.json"
	// Workspace is the run's private directory: the lock, the journal,
	// unpublished parts and spill. Readers ignore it.
	Workspace = ".swath"
	// JournalDir is the workspace (the journal lives there).
	JournalDir = Workspace
	// LockName is the output lock, inside the workspace. Replacing a
	// previous run never removes it (see Prepare).
	LockName = "lock"
	// PartsDir holds parts until they are published into data/.
	PartsDir = "parts"
	// SuspectDir, inside the workspace, keeps the temporary part of a
	// journal record that reads as torn: the record may have been
	// acknowledged and damaged since (see listing's recovery).
	SuspectDir = "suspect"
)

// Footer key/value stamps on every part (SeekParquet STAMP-1): they repeat
// the manifest's version, schema id and key declaration. Order within a
// part is the standard sorting_columns; order across parts, bounds and
// completeness are the manifest's.
const (
	StampFormatVersion = "seekparquet.format_version" // FormatVersion
	StampDraftRevision = "seekparquet.draft_revision" // DraftRevision
	StampSchema        = "seekparquet.schema_id"
	StampKeyColumn     = "seekparquet.key_column" // KeyColumn
	StampKeyUnique     = "seekparquet.key_unique" // "true", "false" with repeated keys
	// StampIndexBound is swath's own: the ColumnIndex min/max truncation
	// of the value columns in bytes. The key column's bounds are exact.
	StampIndexBound = "swath.index_bound_bytes"
)

// PartName is the published name of part ordinal i: dense, zero-based,
// five digits (%05d), so a dataset holds at most MaxParts parts and name
// order is ordinal order, which is key order. Code still compares ordinals
// (PartOrdinal), and readers take the order from the manifest's explicit
// part list.
func PartName(i int) string { return fmt.Sprintf("part-%05d.parquet", i) }

// MaxParts is the most parts a dataset holds: ordinals 0 to 99,999, the
// five digits of PartName. The router refuses to open another
// (ErrPartLimit).
const MaxParts = 100_000

// ErrPartLimit: the run would open part MaxParts, past the five-digit
// ordinals a dataset has. Every earlier part is published first. The part
// size and row cap are frozen in a resume's identity, so a resume would
// reach the limit again: the run is not resumable (exit 1, class fatal,
// cause part_limit), and starts over with a larger --part-size or without
// --part-rotation-max-rows.
var ErrPartLimit = errors.New("the dataset reached its limit of 100,000 parts")

// PartOrdinal is the ordinal of a name PartName makes (ok false for any
// other name: another shape, a sign, extra leading zeros, an ordinal past
// MaxParts).
func PartOrdinal(name string) (i int, ok bool) {
	digits, found := strings.CutPrefix(name, "part-")
	if digits, found = strings.CutSuffix(digits, ".parquet"); !found || digits == "" {
		return 0, false
	}
	for _, c := range digits {
		if c < '0' || c > '9' {
			return 0, false
		}
	}
	i, err := strconv.Atoi(digits)
	if err != nil || i >= MaxParts || PartName(i) != name {
		return 0, false
	}
	return i, true
}

// TempPath is where part i is written before publication.
func TempPath(out string, i int) string {
	return filepath.Join(out, Workspace, PartsDir, PartName(i))
}

// ClaimSuffix marks the one part, in PartsDir, whose journal record is being
// appended: the ordered committer renames part i from TempPath to ClaimPath
// just before the append, and into data/ after it.
const ClaimSuffix = ".claimed"

// ClaimPath is where part i waits while its record is appended (TempPath
// with ClaimSuffix). A part there may be named by a record that was
// acknowledged and lost since; any other temporary part was never named by
// a record.
func ClaimPath(out string, i int) string { return TempPath(out, i) + ClaimSuffix }

// ownEntries are the root entries a dataset owns; overwrite removes these and
// their temporary files (TempOf, and earlier builds' .tmp siblings), nothing
// else. The workspace is emptied except for the
// lock. Earlier layouts' entries (_SUCCESS, symlink.txt, .swath.lock, the
// _swath_*summary.json reports and
// Swath's .swath-state.json) are removed too, so an old dataset can be
// replaced.
// commitEntries are the entries that make a directory a dataset (the
// manifest; _SUCCESS for earlier layouts): overwrite removes them first.
var commitEntries = []string{ManifestName, legacySuccess}

var ownEntries = []string{DataDir, ManifestName, ReportName, ErrorName, legacyLocalSummary, legacySummary, legacySuccess, legacySymlink, legacyLock, ".swath-state.json"}

const (
	legacyLocalSummary = "_swath_local_summary.json"
	legacySummary      = "_swath_summary.json"
	legacySuccess      = "_SUCCESS"
	legacySymlink      = "symlink.txt"
	legacyLock         = ".swath.lock"
)
