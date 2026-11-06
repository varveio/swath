// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build !windows

package dataset

import (
	"errors"
	"os"
)

// DirSyncEnforced reports whether syncDir makes a directory's entries
// durable here.
var DirSyncEnforced = true // a variable for tests

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	return errors.Join(d.Sync(), d.Close())
}
