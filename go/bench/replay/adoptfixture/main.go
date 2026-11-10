// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Command adoptfixture converts the study's sorted Java-era replay
// fixtures (swath format 1 sorted parts: the 13-column canonical schema,
// footer-stamped, no manifest) into SeekParquet datasets of
// swath/s3-objects/v1 without rewriting a data page. It exists only for
// that one conversion and is deleted with the legacy readers in #353.
//
//	go run ./bench/replay/adoptfixture -input DIR -output DIR -bucket NAME \
//	  [-region R] [-prefix P] [-fixture ID]
//
// Each part is copied byte for byte up to the start of its footer; only
// the footer is replaced, by the same Thrift FileMetaData with additions
// spliced in (rewriteFooter): the key's STRING annotation (every key is
// checked to be UTF-8, and so is every key page bound), sorting_columns on
// each row group, the SeekParquet stamps after the existing key-value
// entries, and column_orders when absent. Every other footer field,
// including the row groups' column chunk offsets, statistics and page
// index offsets, is copied byte for byte, and a field-by-field comparison
// of the two footers (checkFooterChange) refuses any other difference.
// Then manifest.json is published: exact first and last keys decoded from
// the data, sizes, SHA-256, extensions.swath (source, scope, publication
// time) and extensions["swath-replay"].adopted_from (each input part's
// name, size, SHA-256 and shared prefix length).
//
// A part is refused, and nothing is published, when a row is not a
// current object (row_type statistics that do not prove OBJECT, a
// version_id, a delete marker), a key is empty, not UTF-8 or over 1,024
// bytes, keys are not strictly ascending across the parts in file_index
// order, or the key statistics are not each row group's exact first and
// last key.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	var o Options
	flag.StringVar(&o.Input, "input", "", "the sorted Java-era fixture: a directory of parts, or one part")
	flag.StringVar(&o.Output, "output", "", "the dataset directory to create (new or empty)")
	flag.StringVar(&o.Bucket, "bucket", "", "the S3 bucket the fixture lists")
	flag.StringVar(&o.Region, "region", "", "the bucket's region (informative)")
	flag.StringVar(&o.Prefix, "prefix", "", "the prefix the fixture lists; empty for the whole bucket")
	flag.StringVar(&o.Fixture, "fixture", "", "the input's identity, recorded in the manifest (e.g. the study's fixture id)")
	flag.Parse()
	if flag.NArg() != 0 || o.Input == "" || o.Output == "" || o.Bucket == "" {
		fmt.Fprintln(os.Stderr, "usage: adoptfixture -input DIR -output DIR -bucket NAME [-region R] [-prefix P] [-fixture ID]")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if _, err := Run(ctx, o, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
