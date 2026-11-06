// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package dataset

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

// Reserved reports whether name, an entry directly in an output directory,
// is one the dataset owns (its data, manifest, reports, workspace and the
// earlier layouts' entries an overwrite removes). The comparison ignores
// case: on a case-insensitive filesystem MANIFEST.JSON is the manifest.
func Reserved(name string) bool {
	return strings.EqualFold(name, Workspace) || slices.ContainsFunc(ownEntries, func(e string) bool { return strings.EqualFold(e, name) })
}

// Managed reports whether dir holds this tool's output, by a heuristic
// that errs toward an unrelated directory: a workspace with this tool's
// lock or journal (an interrupted or running run), or a manifest.json that
// parses as this tool's (format swath-dataset and a coverage object: a
// committed dataset). A manifest of anything else, an earlier layout's
// marks (a schemaId manifest, _SUCCESS beside data/) and a data/ directory
// alone are not this tool's.
func Managed(dir string) bool {
	for _, mark := range []string{filepath.Join(Workspace, LockName), filepath.Join(JournalDir, "journal")} {
		if _, err := os.Lstat(filepath.Join(dir, mark)); err == nil {
			return true
		}
	}
	b, err := os.ReadFile(filepath.Join(dir, ManifestName))
	if err != nil {
		return false
	}
	var m struct {
		Format   string          `json:"format"`
		Coverage json.RawMessage `json:"coverage"`
	}
	return json.Unmarshal(b, &m) == nil && m.Format == Format && len(m.Coverage) > 0 && m.Coverage[0] == '{'
}
