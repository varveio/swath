// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Command capture writes a small unsorted capture: one Parquet part of the
// 13-column canonical schema, N current objects in descending key order and
// no sortedness stamps, the shape of a listing written without sorting. The
// replay server refuses it as not servable sorted, and `swath-replay
// sort-fixture` turns it into a fixture, which is what the container smoke's
// sorting fallback needs in CI without Java or a recording.
//
//	go run ./bench/replay/capture -rows 2000 -o /tmp/capture
//
// The rows are legacytest.Object rows (synthetic; the writer is
// parquet-go, not either lister).
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/varveio/swath/internal/replay/legacy/legacytest"
)

func main() {
	rows := flag.Int("rows", 2000, "objects to write")
	out := flag.String("o", "", "output directory (created; must not hold a part already)")
	flag.Parse()
	if *out == "" || *rows < 1 || flag.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "usage: capture -o DIR [-rows N]")
		os.Exit(2)
	}
	if err := write(*out, *rows); err != nil {
		fmt.Fprintln(os.Stderr, "capture:", err)
		os.Exit(1)
	}
}

// write writes n objects to dir/part-00000.parquet, last key first.
func write(dir string, n int) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	path := filepath.Join(dir, "part-00000.parquet")
	if _, err := os.Lstat(path); err == nil {
		return fmt.Errorf("%s exists", path)
	}
	part := legacytest.Part{Rows: make([]legacytest.Row, 0, n), PageBytes: 64 << 10}
	for i := n - 1; i >= 0; i-- {
		part.Rows = append(part.Rows, legacytest.Object(fmt.Sprintf("logs/p%02d/obj-%06d.json", i%37, i), i))
	}
	return legacytest.Write(path, part)
}
