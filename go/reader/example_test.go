// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package reader_test

import (
	"context"
	"fmt"
	"log"

	"github.com/varveio/swath/go/reader"
)

// testdata/photos is a dataset swath wrote with `swath list photos -o DIR`
// from a tree of four files in three directories; its manifest's source
// and times were then set to fixed values.
const photos = "testdata/photos"

// Open a dataset, decide from its coverage whether it holds the whole
// source, check its parts against the schema, and read every entry. The
// coverage is the application's to judge: the reader hands it on.
func Example() {
	ctx := context.Background()
	d, err := reader.OpenDir(ctx, photos, reader.Options{})
	if err != nil {
		log.Fatal(err)
	}
	defer d.Close()

	m := d.Metadata()
	c := m.Coverage
	whole := *c.Complete && *c.StopReason == "completed" && *c.SkippedEntries == 0 && len(c.Filters) == 0
	fmt.Println(m.SchemaID, *m.Source.URI, "whole source:", whole)

	if err := d.CheckSchema(ctx); err != nil {
		log.Fatal(err)
	}
	err = d.StreamLocal(ctx, func(e reader.LocalEntry) error {
		fmt.Printf("%-4s %5d %s\n", e.Kind, *e.Size, e.Key)
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
	// Output:
	// swath/local-entries/v1 /data/photos whole source: true
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
func ExampleDataset_List() {
	ctx := context.Background()
	d, err := reader.OpenDir(ctx, photos, reader.Options{})
	if err != nil {
		log.Fatal(err)
	}
	defer d.Close()

	req := reader.ListRequest{Delimiter: '/', Limit: 2}
	for {
		page, err := d.List(ctx, req)
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
				fmt.Printf("entry  %s\n", e.Local.Key)
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
