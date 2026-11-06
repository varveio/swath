// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package dataset

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/varveio/swath/go/internal/faults"
)

// Durability is what a replacement (Replace) promises after a crash.
type Durability int

const (
	// Atomic: a reader sees the old file or the new one, never a torn one;
	// a power loss may still lose the replacement (no sync). For files
	// rewritten as they go or read only by this invocation's supervisor:
	// progress, the outcome file, the report.
	Atomic Durability = iota
	// Durable: Atomic, and once Replace returns the new file survives a
	// power loss (the file is synced before the rename, its directory
	// after). For commits and checkpoints: the manifest, run.json.
	Durable
)

// Replace replaces path with data through a temporary file of its own
// beside it (TempOf), so concurrent writers of one path never share or
// remove each other's temporary file, and a failed write leaves the old
// file and no temporary one. Writes and syncs go through faults.
func Replace(path string, data []byte, d Durability) error {
	name := filepath.Base(path)
	f, err := os.CreateTemp(filepath.Dir(path), "."+name+".*.tmp")
	if err != nil {
		return fmt.Errorf("write %s: %w", name, err)
	}
	tmp := f.Name()
	_, err = faults.Write(f, data)
	if err == nil && d == Durable {
		err = faults.Sync(f)
	}
	if err = errors.Join(err, f.Chmod(0o644), f.Close()); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("write %s: %w", name, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("commit %s: %w", name, err)
	}
	if d == Durable {
		if err := syncDir(filepath.Dir(path)); err != nil {
			return fmt.Errorf("sync after %s: %w", name, err)
		}
	}
	return nil
}

// ReplaceJSON is Replace with v as indented JSON (HTML characters kept).
func ReplaceJSON(path string, v any, d Durability) error {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		return fmt.Errorf("encode %s: %w", filepath.Base(path), err)
	}
	return Replace(path, b.Bytes(), d)
}

// TempOf reports whether base (a file name) is a temporary file of
// Replace for the file named name.
func TempOf(name, base string) bool {
	return strings.HasPrefix(base, "."+name+".") && strings.HasSuffix(base, ".tmp")
}
