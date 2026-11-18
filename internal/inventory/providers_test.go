// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package inventory

import (
	"os"

	"github.com/varveio/swath/internal/source/gcs"
	"github.com/varveio/swath/internal/source/localfs"
	"github.com/varveio/swath/internal/source/s3"

	// The command's composition registers the providers by scheme.
	_ "github.com/varveio/swath/internal/cli"
)

// The tests open the providers by scheme as the command does and reach
// their options and stats through these helpers.

// S3Config is the S3 provider's options value (Config.Source).
type S3Config = s3.Options

// GCSConfig is the GCS provider's options value.
type GCSConfig = gcs.Options

func s3Stats(sum Report) *s3.Stats {
	if r := sum.SourceReport(); r != nil {
		st, _ := r.Stats.(*s3.Stats)
		return st
	}
	return nil
}

func fsStats(sum Report) *localfs.Stats {
	if r := sum.SourceReport(); r != nil {
		st, _ := r.Stats.(*localfs.Stats)
		return st
	}
	return nil
}

func fsOptions(portable bool) *localfs.Options {
	return &localfs.Options{LinkTarget: true, Portable: portable}
}

func readFile(t interface {
	Helper()
	Fatal(...any)
}, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
