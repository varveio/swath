// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"encoding/json"
	"os"

	"github.com/varveio/swath/go/internal/source/fs"
	"github.com/varveio/swath/go/internal/source/s3"
)

// The tests register both providers (the command does it the same way)
// and reach their options and stats through these helpers.

// S3Config is the S3 provider's options value (Config.Source).
type S3Config = s3.Options

func s3Stats(sum Report) *s3.Stats {
	if r := sum.SourceReport(); r != nil {
		st, _ := r.Stats.(*s3.Stats)
		return st
	}
	return nil
}

func fsStats(sum Report) *fs.Stats {
	if r := sum.SourceReport(); r != nil {
		st, _ := r.Stats.(*fs.Stats)
		return st
	}
	return nil
}

func fsOptions(portable bool) *fs.Options { return &fs.Options{LinkTarget: true, Portable: portable} }

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

func mustJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return string(b)
}
