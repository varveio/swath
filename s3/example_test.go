// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/varveio/swath/reader/listing"
	"github.com/varveio/swath/s3"
)

// Open a bucket, page one level of it, look up one key and close it. A
// page's token continues the level; a lookup that cannot tell is unknown,
// never absent.
func Example() {
	ctx := context.Background()
	src, err := s3.Open(ctx, "example-bucket", s3.Options{Region: "eu-west-1"})
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

// Walk everything under a prefix, in parallel, for at most a minute:
// count the objects and their bytes. A walk sets no limit of its own; the
// caller stops it by cancelling its context, and the summary says whether
// it finished.
func Example_walk() {
	ctx := context.Background()
	src, err := s3.Open(ctx, "example-bucket", s3.Options{Region: "eu-west-1"})
	if err != nil {
		log.Fatal(err)
	}
	defer src.Close()

	walkCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	var objects, size int64
	res, err := src.Walk(walkCtx, listing.WalkRequest{Prefix: []byte("logs/")}, func(b listing.WalkBatch) error {
		for _, e := range b.Entries {
			objects++
			size += *e.Size
		}
		return nil
	})
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		fmt.Printf("not finished in a minute: at least %d objects, %d bytes\n", res.Entries, size)
	case err != nil:
		log.Fatal(err)
	default:
		fmt.Printf("%d objects, %d bytes\n", objects, size)
	}
}
