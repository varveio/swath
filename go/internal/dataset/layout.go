// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package dataset owns the on-disk layout of a sorted listing dataset (the
// layout of Swath's sorted managed Parquet output): output-directory safety,
// the publication gate, the manifest and the commit order.
package dataset

import (
	"fmt"
	"path/filepath"
)

// File and directory names inside a dataset.
const (
	DataDir      = "data"
	ManifestName = "manifest.json"
	SuccessName  = "_SUCCESS"
	SymlinkName  = "symlink.txt"
	// SummaryName is the run report of filesystem datasets. It avoids Swath's reserved
	// _swath_summary.json, whose schema readers such as Varve validate.
	SummaryName = "_swath_local_summary.json"
)

// Footer key/value stamps on every sorted part (Swath's SortedParquetWriter).
const (
	StampOrder     = "swath.sort.order"
	StampMode      = "swath.sort.mode"
	StampFormat    = "swath.sort.format_version"
	StampFileIndex = "swath.sort.file_index" // 1-based
	StampFileFinal = "swath.sort.file_final" // "true", last part only
)

// PartName is the published name of part ordinal i: dense, zero-based, %05d,
// so lexical name order is key order.
func PartName(i int) string { return fmt.Sprintf("part-%05d.parquet", i) }

// TempPath is where part i is written before publication.
func TempPath(out string, i int) string {
	return filepath.Join(out, DataDir, PartName(i)+".tmp")
}

// ownEntries are the root entries a dataset owns; overwrite removes these and
// their .tmp siblings, nothing else. .swath-state.json is Swath's resume
// identity file, which this tool does not write but older outputs may hold.
var ownEntries = []string{DataDir, ManifestName, SuccessName, SummaryName, SymlinkName, SwathSummaryName, ".swath-state.json"}
