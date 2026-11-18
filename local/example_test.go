// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build linux || darwin

package local_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"syscall"

	"github.com/varveio/swath/local"
	"github.com/varveio/swath/reader/listing"
)

// Open a directory without following a symbolic link, page one level of
// it and close the source, then the directory. The source never resolves
// the directory by its path, and opens nothing below it by one.
func Example() {
	ctx := context.Background()
	root, err := os.OpenFile("/srv/data", os.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		log.Fatal(err)
	}
	defer root.Close() // after the source's Close
	src, err := local.Open(root)
	if err != nil {
		log.Fatal(err)
	}
	defer src.Close()

	req := listing.PageRequest{Prefix: []byte("logs/"), Limit: 100}
	for {
		page, err := src.Page(ctx, req)
		if err != nil {
			log.Fatal(err)
		}
		for _, e := range page.Entries {
			fmt.Println(e.Kind, string(e.Key))
		}
		if page.Next == "" {
			break
		}
		req.Token = page.Next
	}
}
