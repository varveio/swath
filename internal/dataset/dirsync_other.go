// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build !windows

package dataset

import (
	"errors"
	"os"

	"github.com/varveio/swath/internal/faults"
)

// DirSyncEnforced reports whether syncDir makes a directory's entries
// durable here.
var DirSyncEnforced = true // a variable for tests

// syncDir fsyncs dir, through faults.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	return errors.Join(faults.Sync(d), d.Close())
}

// SyncDirIn fsyncs directory name, opened through root (recovery acts
// relative to the locked output, never by a pathname that may name another
// directory by then).
func SyncDirIn(root *os.Root, name string) error {
	d, err := root.Open(name)
	if err != nil {
		return err
	}
	return errors.Join(faults.Sync(d), d.Close())
}
