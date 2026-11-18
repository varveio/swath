// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build !linux && !darwin

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

// readlinkIn reads the target of the link at path.
func readlinkIn(_ *os.File, path, _ string) (string, error) { return os.Readlink(path) }

// tooLong: no path is refused by its length here (Go gives Windows the
// extended-length form of a long path), so nothing below is reached.
func tooLong(error) bool { return false }

func statLong(string, Fields, *Entry, *Stats) error { return nil }

func readlinkLong(string, *Stats) (string, error) { return "", nil }
