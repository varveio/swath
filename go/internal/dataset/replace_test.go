// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package dataset

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/varveio/swath/go/internal/faults"
)

// TestReplace: a replacement at either durability installs the new bytes;
// a failed write or sync leaves the old file and no temporary file. A
// directory sync that fails after the rename is an error with the new,
// complete file in place and no temporary file: visible, not proven
// durable; the caller takes the replacement as failed (Pending.Commit).
func TestReplace(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "f.json")
	for _, d := range []Durability{Atomic, Durable} {
		if err := Replace(path, []byte("new"), d); err != nil {
			t.Fatal(err)
		}
		if b, _ := os.ReadFile(path); string(b) != "new" {
			t.Fatalf("durability %d: %q", d, b)
		}
	}
	for _, op := range []string{"write", "sync"} {
		restore := faults.SetIO(func(o, name string, b []byte) (int, error) {
			if o == op && TempOf("f.json", filepath.Base(name)) {
				return len(b) / 2, errors.New("injected")
			}
			return 0, nil
		})
		err := Replace(path, []byte("lost"), Durable)
		restore()
		if err == nil {
			t.Fatalf("%s fault: no error", op)
		}
		if b, _ := os.ReadFile(path); string(b) != "new" {
			t.Fatalf("%s fault: the file became %q", op, b)
		}
		if ents, _ := os.ReadDir(dir); len(ents) != 1 {
			t.Fatalf("%s fault: %d entries left", op, len(ents))
		}
	}
	if !DirSyncEnforced {
		return
	}
	restore := faults.SetIO(func(o, name string, b []byte) (int, error) {
		if o == "sync" && name == dir {
			return 0, errors.New("injected")
		}
		return 0, nil
	})
	err := Replace(path, []byte("visible"), Durable)
	restore()
	if err == nil {
		t.Fatal("directory sync fault: no error")
	}
	if b, _ := os.ReadFile(path); string(b) != "visible" {
		t.Fatalf("directory sync fault: the file is %q, want the new one", b)
	}
	if ents, _ := os.ReadDir(dir); len(ents) != 1 {
		t.Fatalf("directory sync fault: %d entries left", len(ents))
	}
}
