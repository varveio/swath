// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3_test

import (
	"bytes"
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

// Walk everything under a prefix, in parallel, within a minute: count the
// objects and their bytes, and keep the key intervals the walk did not
// read when the minute ran out.
func Example_walk() {
	ctx := context.Background()
	src, err := s3.Open(ctx, "example-bucket", s3.Options{Region: "eu-west-1"})
	if err != nil {
		log.Fatal(err)
	}
	defer src.Close()

	var objects, size int64
	var unread []listing.Unread
	req := listing.WalkRequest{Prefix: []byte("logs/"), Budget: listing.Budget{MaxDuration: time.Minute}}
	res, err := src.Walk(ctx, req, func(b listing.WalkBatch) error {
		for _, e := range b.Entries {
			objects++
			size += *e.Size
		}
		for _, u := range b.Unread {
			unread = append(unread, listing.Unread{Key: bytes.Clone(u.Key), Hi: bytes.Clone(u.Hi), Scope: u.Scope, Class: u.Class})
		}
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%d objects, %d bytes\n", objects, size)
	if res.Evidence.Stop != listing.StopCompleted {
		fmt.Printf("stopped at the %s: %d key intervals not read\n", res.Evidence.Stop, len(unread))
	}
}

// Summarize a prefix, a prefix nested in it and one key: the nested one is
// counted in full by the same walk, and the key is a lookup.
func Example_summarize() {
	ctx := context.Background()
	src, err := s3.Open(ctx, "example-bucket", s3.Options{Region: "eu-west-1"})
	if err != nil {
		log.Fatal(err)
	}
	defer src.Close()

	targets := []listing.Target{
		{Key: []byte("logs/"), Subtree: true},
		{Key: []byte("logs/2026/"), Subtree: true}, // nested: counted in full, in the same walk
		{Key: []byte("README")},                    // exact: a lookup
	}
	sums, ev, err := src.Summarize(ctx, targets, listing.SummaryOptions{Budget: listing.Budget{MaxDuration: time.Minute}})
	if err != nil {
		log.Fatal(err)
	}
	for i, s := range sums {
		fmt.Printf("%s: %s, %d objects", targets[i].Key, s.State, s.Entries)
		if s.Bytes != nil {
			fmt.Printf(", %d bytes", *s.Bytes)
		}
		if len(s.Unread) > 0 {
			fmt.Print(", at least: not all of it was read")
		}
		fmt.Println()
	}
	fmt.Println("the call:", ev.Stop)
}
