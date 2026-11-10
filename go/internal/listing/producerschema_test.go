// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/varveio/swath/go/internal/schema"
	"github.com/varveio/swath/go/internal/source/gcs"
	"github.com/varveio/swath/go/internal/source/localfs"
	"github.com/varveio/swath/go/internal/source/s3"
)

// typeNames spell each column type with the Parquet types the writer
// gives it (parquetout's nodeOf).
var typeNames = map[schema.Type]string{
	schema.String: "BYTE_ARRAY STRING", schema.Int32: "INT32 INT(32,true)", schema.Int64: "INT64 INT(64,true)", schema.TimestampMicros: "INT64 TIMESTAMP(MICROS,UTC)",
	schema.Bool: "BOOLEAN", schema.Uint32: "INT32 INT(32,false)", schema.Uint64: "INT64 INT(64,false)", schema.Binary: "BYTE_ARRAY",
}

// describeSchema is a producer schema as the writer sees it: its Info, then
// every column (the key first) in order with its type, nullability,
// encoding, value cap and accessor.
func describeSchema[P any](s *schema.Schema[P]) string {
	var b strings.Builder
	fmt.Fprintf(&b, "schema_id %s\nmessage %s\nkey_unique %v\nmax_key_bytes %d\n", s.ID, s.Name, !s.Groups, s.MaxKeyBytes)
	fmt.Fprintf(&b, "column key %s required delta\n", typeNames[s.Key])
	for _, c := range s.Columns {
		nullable, delta, accessor := "optional", "", "str"
		if c.Required {
			nullable = "required"
		}
		if c.Delta {
			delta = " delta"
		}
		switch {
		case c.Int != nil:
			accessor = "int"
		case c.Bool != nil:
			accessor = "bool"
		}
		fmt.Fprintf(&b, "column %s %s %s%s max_bytes %d accessor %s\n", c.Name, typeNames[c.Type], nullable, delta, c.MaxBytes, accessor)
	}
	return b.String()
}

// TestProducerSchemasGolden: the five producer schemas are testdata's
// goldens, recorded from the producers before their specs moved to the
// reader module; a change to what swath writes shows in review as a diff
// of those files. Run with UPDATE=1 to rewrite them.
func TestProducerSchemasGolden(t *testing.T) {
	for name, got := range map[string]string{
		"s3-objects":      describeSchema(&s3.Schema),
		"s3-versions":     describeSchema(&s3.VersionsSchema),
		"gcs-objects":     describeSchema(&gcs.Schema),
		"gcs-generations": describeSchema(&gcs.GenerationSchema),
		"local":           describeSchema(&localfs.Schema),
	} {
		golden := filepath.Join("testdata", "producer-schema-"+name+".golden")
		if os.Getenv("UPDATE") != "" {
			if err := os.WriteFile(golden, []byte(got), 0o644); err != nil {
				t.Fatal(err)
			}
			continue
		}
		want, err := os.ReadFile(golden)
		if err != nil {
			t.Fatal(err)
		}
		if string(want) != got {
			t.Errorf("%s: the producer schema changed (a new schema id, or UPDATE=1 go test ./internal/listing -run TestProducerSchemasGolden for a compatible change)\ngot:\n%s\nwant:\n%s", name, got, want)
		}
	}
}
