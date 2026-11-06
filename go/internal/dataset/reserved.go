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

// Managed reports whether dir holds this tool's output: a workspace with a
// lock or a journal, a manifest of this tool (a dataset of any format), or
// an earlier layout's _SUCCESS beside data/. A manifest.json of something
// else is not one.
func Managed(dir string) bool {
	for _, mark := range []string{filepath.Join(Workspace, LockName), filepath.Join(JournalDir, "journal")} {
		if _, err := os.Lstat(filepath.Join(dir, mark)); err == nil {
			return true
		}
	}
	if b, err := os.ReadFile(filepath.Join(dir, ManifestName)); err == nil {
		var m struct {
			Format   string `json:"format"`
			SchemaID string `json:"schema_id"`
			Legacy   string `json:"schemaId"`
		}
		if json.Unmarshal(b, &m) == nil && (m.Format == Format || m.SchemaID != "" || m.Legacy != "") {
			return true
		}
	}
	_, e1 := os.Lstat(filepath.Join(dir, legacySuccess))
	fi, e2 := os.Stat(filepath.Join(dir, DataDir))
	return e1 == nil && e2 == nil && fi.IsDir()
}
