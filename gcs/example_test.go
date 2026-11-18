// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package gcs_test

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/varveio/swath/gcs"
	"github.com/varveio/swath/reader/listing"
)

// Open a bucket, page one level of it, look up one key and close it. A
// page's token continues the level; a lookup that cannot tell is unknown,
// never absent.
func Example() {
	ctx := context.Background()
	src, err := gcs.Open(ctx, "example-bucket", gcs.Options{})
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
		if page.Self != nil {
			fmt.Printf("an object at exactly %s\n", page.Self.Key)
		}
		for _, e := range page.Entries {
			fmt.Println(e.Kind, string(e.Key))
		}
		if page.Next == "" {
			break
		}
		req.Token = page.Next
	}

	r, err := src.Lookup(ctx, []byte("logs/2026-11-17.gz"))
	if err != nil {
		log.Fatal(err)
	}
	switch {
	case r.State == listing.LookupPresent:
		fmt.Println("present, read at", r.Evidence.Finished)
	case r.State == listing.LookupAbsent:
		fmt.Println("absent")
	case errors.Is(r.Reason, listing.ErrAuth):
		fmt.Println("unknown: not authorized to tell")
	default:
		fmt.Println("unknown:", r.Reason)
	}
}
