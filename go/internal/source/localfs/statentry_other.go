// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build !darwin

package localfs

import (
	"io/fs"
	"os"
)

// statEntry fills r from d.Info: on Windows the directory enumeration
// already carries it, with no call per entry.
func statEntry(_ *os.File, d fs.DirEntry, r *Entry) error {
	fi, err := d.Info()
	if err != nil {
		return err
	}
	fromFileInfo(r, fi)
	return nil
}
