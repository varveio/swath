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

// PartName is the published name of part ordinal i: dense, zero-based, %05d,
// so lexical name order is key order.
func PartName(i int) string { return fmt.Sprintf("part-%05d.parquet", i) }

// TempPath is where part i is written before publication.
func TempPath(out string, i int) string {
	return filepath.Join(out, Workspace, PartsDir, PartName(i))
}

// ownEntries are the root entries a dataset owns; overwrite removes these and
// their .tmp siblings, nothing else. The workspace is emptied except for the
// lock. Earlier layouts' entries (_SUCCESS, symlink.txt, .swath.lock, the
// _swath_*summary.json reports and
// Swath's .swath-state.json) are removed too, so an old dataset can be
// replaced.
var ownEntries = []string{DataDir, ManifestName, ReportName, ErrorName, legacyLocalSummary, legacySummary, legacySuccess, legacySymlink, legacyLock, ".swath-state.json"}

const (
	legacyLocalSummary = "_swath_local_summary.json"
	legacySummary      = "_swath_summary.json"
	legacySuccess      = "_SUCCESS"
	legacySymlink      = "symlink.txt"
	legacyLock         = ".swath.lock"
)
