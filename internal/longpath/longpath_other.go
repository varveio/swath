// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build !linux && !darwin

package longpath

import (
	"io/fs"
	"os"
)

// TooLong: no path is refused by its length here (Go gives Windows the
// extended-length form of a long path).
func TooLong(error) bool { return false }

// Open is os.Open.
func Open(path string) (*os.File, error) { return os.Open(path) }

// Stat is os.Stat.
func Stat(path string) (fs.FileInfo, error) { return os.Stat(path) }
