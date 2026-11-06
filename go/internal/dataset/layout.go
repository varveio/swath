// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package dataset owns the on-disk layout of a sorted listing dataset:
// output-directory safety, the private workspace, the manifest and the
// commit order. The manifest is the commit record: a dataset exists once
// manifest.json is installed, and not before.
package dataset

import (
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
)

// Format identifies the dataset format in the manifest and the footers.
const (
	Format        = "swath-dataset"
	FormatVersion = 2
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

// Footer key/value stamps on every part. Order within a part is the
// standard sorting_columns; order across parts, bounds and completeness
// are the manifest's.
const (
	StampProfile        = "swath.profile"         // Format
	StampProfileVersion = "swath.profile_version" // FormatVersion
	StampSchema         = "swath.schema_id"
	// StampIndexBound is the ColumnIndex min/max truncation in bytes: bounds
	// of longer values are truncated, so readers treat them as conservative.
	StampIndexBound = "swath.index_bound_bytes"
)

// PartName is the published name of part ordinal i: dense, zero-based, at
// least five digits (%05d). Ordinal order is key order; name order is not
// past 99,999 (part-100000 sorts before part-99999), so code compares
// ordinals (PartOrdinal), and readers take the order from the manifest's
// explicit part list.
func PartName(i int) string { return fmt.Sprintf("part-%05d.parquet", i) }

// PartOrdinal is the ordinal of a name PartName makes (ok false for any
// other name: another shape, a sign, extra leading zeros).
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
	if err != nil || PartName(i) != name {
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
