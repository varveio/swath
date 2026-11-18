// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package localfs

import (
	"context"
	"io"
	"os"
)

// readLevel calls add with each name the directory f holds and whether it
// is a directory. os.File.ReadDir types an entry the filesystem leaves
// untyped by a stat relative to f, never by a path, and a failure of that
// stat fails the read: APFS and HFS+ type every entry.
func readLevel(ctx context.Context, f *os.File, _ int, add func(name string, dir bool)) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		ents, err := f.ReadDir(levelBatch)
		for _, e := range ents {
			add(e.Name(), e.IsDir())
		}
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return readError(err)
		}
	}
}
