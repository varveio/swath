// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Command swath lists a local directory tree or an S3 bucket and writes a globally
// key-sorted Parquet dataset in Swath's sorted dataset layout. The command
// line is package command's.
package main

import (
	"context"
	"os"

	"github.com/varveio/swath/command"
)

func main() {
	command.Exit(command.Main(context.Background(), os.Args[1:], os.Stdin, os.Stdout, os.Stderr))
}
