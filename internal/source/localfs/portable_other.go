// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build !linux

package localfs

import (
	"context"
	"os"

	"github.com/varveio/swath/internal/source"
)

// No native engine yet: getattrlistbulk(2) on macOS and directory
// enumeration with FindFirstFileEx on Windows (both return sizes and times
// with the names) are the natural next ones.
const nativeEngine = false

type chunk struct{}

func (p *Provider) expandNative(ctx context.Context, r Range, e source.Emitter[Range, Entry], st *Stats) error {
	return p.expandPortable(ctx, r, e, st)
}

func disposeChunk(Range) {}

// statPath fills r from an lstat of path, as the portable engine reads an
// entry.
func statPath(path string, _ Fields, r *Entry) error {
	fi, err := os.Lstat(path)
	if err != nil {
		return err
	}
	fromFileInfo(r, fi)
	return nil
}
