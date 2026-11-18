// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package view_test

import (
	"context"
	"fmt"
	"log"

	"github.com/varveio/swath/reader/listing"
	"github.com/varveio/swath/reader/view"
)

// Answer the questions from a local tree's inventory: the root's children,
// with the files under each child directory summarized, what is at a path,
// and the files under a directory.
func Example() {
	ctx := context.Background()
	inv, err := view.OpenDir(ctx, "../testdata/photos")
	if err != nil {
		log.Fatal(err)
	}
	defer inv.Close()

	files := listing.SummaryOptions{Kinds: []listing.Kind{listing.KindFile}}
	page, err := inv.Page(ctx, listing.PageRequest{Limit: 10})
	if err != nil {
		log.Fatal(err)
	}
	var dirs []listing.Target
	for _, e := range page.Entries {
		if e.Kind == listing.KindDir {
			dirs = append(dirs, listing.Target{Key: e.Key, Subtree: true})
		}
	}
	totals, _, err := inv.Summarize(ctx, dirs, files)
	if err != nil {
		log.Fatal(err)
	}
	for _, e := range page.Entries {
		if e.Kind == listing.KindDir {
			t := totals[0]
			totals = totals[1:]
			fmt.Printf("%-4s %-10s %d files, %d bytes\n", e.Kind, e.Key, t.Entries, *t.Bytes)
		} else {
			fmt.Printf("%-4s %-10s %d bytes\n", e.Kind, e.Key, *e.Size)
		}
	}

	found, err := inv.Lookup(ctx, []byte("2025/summer"))
	if err != nil {
		log.Fatal(err)
	}
	missing, err := inv.Lookup(ctx, []byte("2025/winter"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(found.State, string(found.Entry.Key), "|", missing.State, "as of", missing.Evidence.Finished.Format("2006-01-02"))

	sums, _, err := inv.Summarize(ctx, []listing.Target{{Key: []byte("2025/"), Subtree: true}}, files)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("2025/:", sums[0].Entries, "files,", *sums[0].Bytes, "bytes, newest", sums[0].Newest != nil)
	// Output:
	// dir  2025/      2 files, 2000 bytes
	// dir  2026/      1 files, 500 bytes
	// file notes.txt  6 bytes
	// present 2025/summer/ | absent as of 2026-11-12
	// 2025/: 2 files, 2000 bytes, newest true
}
