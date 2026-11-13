// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package objfile_test

import (
	"fmt"

	"github.com/varveio/swath/go/objfile"
)

// A dataset root URI parses into its store, container and literal prefix;
// the prefix is never decoded, and a URI with a query, a fragment or user
// information is refused without quoting it.
func ExampleParseLocation() {
	for _, uri := range []string{
		"s3://bucket/inventories/2026-11",
		"az://account/container",
		"gs://bucket/a%20b/",
		"s3://bucket/x?versionId=1",
	} {
		l, err := objfile.ParseLocation(uri)
		if err != nil {
			fmt.Println("refused:", err)
			continue
		}
		fmt.Printf("%s account=%q container=%q prefix=%q\n", l.Scheme, l.Account, l.Container, l.Prefix)
	}
	// Output:
	// s3 account="" container="bucket" prefix="inventories/2026-11/"
	// az account="account" container="container" prefix=""
	// gs account="" container="bucket" prefix="a%20b/"
	// refused: objfile: location "[redacted]": has a query or a fragment
}

// Object names below a root are literal relative paths.
func ExampleCheckPath() {
	for _, name := range []string{"data/part-00000.parquet", "../manifest.json", "a//b", "/abs"} {
		fmt.Println(name, objfile.CheckPath(name) == nil)
	}
	// Output:
	// data/part-00000.parquet true
	// ../manifest.json false
	// a//b false
	// /abs false
}
