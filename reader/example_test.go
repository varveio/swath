// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package reader_test

import (
	"context"
	"fmt"
	"log"

	seekparquet "github.com/varveio/seekparquet/go"

	"github.com/varveio/swath/reader"
)

// testdata/photos is a dataset swath wrote with `swath list photos -o DIR`
// from a tree of four files in three directories; its manifest's source
// and times were then set to fixed values.
const photos = "testdata/photos"

// wholeSource reports whether m records a complete, unfiltered listing of
// all of want, with nothing skipped or excluded. A member the manifest
// lacks is nil, never zero or empty, and counts as unknown: not whole.
func wholeSource(m reader.Metadata, want string) bool {
	src, sc, c := m.Source, m.Scope, m.Coverage
	return src != nil && src.URI != nil && *src.URI == want &&
		sc != nil && sc.Kind != nil && *sc.Kind == "whole" &&
		c != nil && c.Complete != nil && *c.Complete &&
		c.StopReason != nil && *c.StopReason == "completed" &&
		c.Unreadable != nil && *c.Unreadable == 0 &&
		c.Filters != nil && len(c.Filters) == 0 &&
		c.Exclusions != nil && len(c.Exclusions) == 0
}

// Open a dataset, decide from its metadata whether it holds the whole
// source the application expects, check its parts against the schema, and
// read every entry. The judgement is the application's: the reader hands
// the metadata on.
func Example() {
	ctx := context.Background()
	d, err := reader.OpenDir(ctx, photos, seekparquet.OpenOptions{})
	if err != nil {
		log.Fatal(err)
	}
	defer d.Close()

	m := d.Metadata()
	fmt.Println(m.SchemaID, "whole source:", wholeSource(m, "/data/photos"))

	if err := d.CheckSchema(ctx); err != nil {
		log.Fatal(err)
	}
	spec, _ := reader.Schema(m.SchemaID)
	err = d.SeekParquet().Scan(ctx, seekparquet.Query{Columns: spec.ColumnNames()}, nil, func(b *seekparquet.Batch) error {
		for i := range b.Len() {
			e, err := reader.DecodeLocalEntry(b.Row(i))
			if err != nil {
				return err
			}
			fmt.Printf("%-4s %5d %s\n", e.Kind, *e.Size, e.Key)
		}
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
	// Output:
	// swath/local-entries/v1 whole source: true
	// DIR   4096 2025/
	// DIR   4096 2025/summer/
	// FILE  1200 2025/summer/beach.jpg
	// FILE   800 2025/summer/hike.jpg
	// DIR   4096 2026/
	// FILE   500 2026/snow.jpg
	// FILE     6 notes.txt
}

// List the top level of a dataset by delimiter, two entries a page: a
// common prefix stands for every key below it (here a directory's own row
// and its contents), and each page continues from the
// cursor of the last entry of the one before.
func ExampleDataset_SeekParquet() {
	ctx := context.Background()
	d, err := reader.OpenDir(ctx, photos, seekparquet.OpenOptions{})
	if err != nil {
		log.Fatal(err)
	}
	defer d.Close()

	req := seekparquet.ListQuery{Delimiter: '/', Limit: 2}
	for {
		page, err := d.SeekParquet().List(ctx, req, nil)
		if err != nil {
			log.Fatal(err)
		}
		if len(page) == 0 {
			break
		}
		fmt.Println("page")
		for _, e := range page {
			if e.CommonPrefix != nil {
				fmt.Printf("prefix %s\n", e.CommonPrefix)
			} else {
				fmt.Printf("entry  %s\n", e.Row.Key)
			}
		}
		req.Cursor = page[len(page)-1].Cursor()
	}
	// Output:
	// page
	// prefix 2025/
	// prefix 2026/
	// page
	// entry  notes.txt
}
