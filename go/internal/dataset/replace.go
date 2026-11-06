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
	p, err := NewPending(path)
	if err != nil {
		return err
	}
	if _, err := p.Write(data); err != nil {
		p.Abort()
		return err
	}
	return p.Commit(d)
}

// Pending is a replacement written as it goes (a streamed text output):
// the temporary file beside the target (TempOf) takes the writes; Commit
// installs it as Replace does, Abort removes it and leaves the target as
// it was.
type Pending struct {
	path, tmp string
	f         *os.File
}

// NewPending creates the temporary file for a replacement of path.
func NewPending(path string) (*Pending, error) {
	name := filepath.Base(path)
	f, err := os.CreateTemp(filepath.Dir(path), "."+name+".*.tmp")
	if err != nil {
		return nil, fmt.Errorf("write %s: %w", name, err)
	}
	return &Pending{path: path, tmp: f.Name(), f: f}, nil
}

// Write writes to the temporary file (through faults).
func (p *Pending) Write(b []byte) (int, error) {
	n, err := faults.Write(p.f, b)
	if err != nil {
		err = fmt.Errorf("write %s: %w", filepath.Base(p.path), err)
	}
	return n, err
}

// Commit installs the temporary file as the target, with durability d.
func (p *Pending) Commit(d Durability) error {
	name := filepath.Base(p.path)
	var err error
	if d == Durable {
		err = faults.Sync(p.f)
	}
	if err = errors.Join(err, p.f.Chmod(0o644), p.f.Close()); err != nil {
		os.Remove(p.tmp)
		return fmt.Errorf("write %s: %w", name, err)
	}
	if err := os.Rename(p.tmp, p.path); err != nil {
		os.Remove(p.tmp)
		return fmt.Errorf("commit %s: %w", name, err)
	}
	if d == Durable {
		if err := syncDir(filepath.Dir(p.path)); err != nil {
			return fmt.Errorf("sync after %s: %w", name, err)
		}
	}
	return nil
}

// Abort removes the temporary file; the target stays as it was.
func (p *Pending) Abort() {
	p.f.Close()
	os.Remove(p.tmp)
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
