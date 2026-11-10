// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package textout_test

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/schema"
	"github.com/varveio/swath/go/internal/source/gcs"
	"github.com/varveio/swath/go/internal/source/s3"
	"github.com/varveio/swath/go/internal/textout"
	"github.com/varveio/swath/go/reader"
)

// text writes rows through a sink bound to s on cfg (to a buffer) and
// returns the output.
func text[P any](t *testing.T, cfg textout.Config, s *schema.Schema[P], rows []schema.Row[P]) string {
	t.Helper()
	var buf bytes.Buffer
	cfg.Stdout = &buf
	if cfg.Format == textout.Table && cfg.Loc == nil {
		cfg.Loc = time.UTC
	}
	o, err := textout.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	sink, err := textout.NewSink(o, s)
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.Write(context.Background(), [][]schema.Row[P]{rows}); err != nil {
		t.Fatal(err)
	}
	if err := o.Commit(); err != nil {
		t.Fatal(err)
	}
	return buf.String()
}

var (
	s3versions = []s3.VersionRow{
		{Key: "k", Data: s3.Version{Object: s3.Object{Size: 1, ETag: "e", Mtime: 1_700_000_000_000_000}, VersionID: "new", IsLatest: true}},
		{Key: "k", Data: s3.Version{Object: s3.Object{Mtime: 1_700_000_000_000_000}, VersionID: "null", DeleteMarker: true}},
	}
	gcsGenerations = []gcs.GenerationRow{
		{Key: "k", Data: gcs.Generation{Object: gcs.Object{Size: 1, Generation: 1700000000000001, Mtime: 5}, TimeDeleted: 10}},
		{Key: "k", Data: gcs.Generation{Object: gcs.Object{Size: 2, Generation: 1700000000000002, ContentType: "text/plain"}}},
	}
)

// TestVersionsRows: a versions schema's rows keep its columns and order
// under its own names: S3's delete marker with a null size, GCS's
// generation as its integer and time_deleted null on the live one, with
// no column named after another provider's.
func TestVersionsRows(t *testing.T) {
	for _, c := range []struct {
		name, got, want string
	}{
		{"S3 TSV", text(t, textout.Config{Format: textout.TSV}, &s3.VersionsSchema, s3versions),
			"key\tsize\tlast_modified\tetag\tstorage_class\towner_id\towner_display_name\tchecksum_algorithm\tchecksum_type\tversion_id\tis_latest\tis_delete_marker\n" +
				"k\t1\t2023-11-14T22:13:20.000000Z\te\t\\N\t\\N\t\\N\t\\N\t\\N\tnew\ttrue\tfalse\n" +
				"k\t\\N\t2023-11-14T22:13:20.000000Z\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\tnull\tfalse\ttrue\n"},
		{"S3 JSONL", text(t, textout.Config{Format: textout.JSONL}, &s3.VersionsSchema, s3versions),
			`{"key":"k","size":1,"last_modified":"2023-11-14T22:13:20.000000Z","etag":"e","storage_class":null,"owner_id":null,"owner_display_name":null,"checksum_algorithm":null,"checksum_type":null,"version_id":"new","is_latest":true,"is_delete_marker":false}` + "\n" +
				`{"key":"k","size":null,"last_modified":"2023-11-14T22:13:20.000000Z","etag":null,"storage_class":null,"owner_id":null,"owner_display_name":null,"checksum_algorithm":null,"checksum_type":null,"version_id":"null","is_latest":false,"is_delete_marker":true}` + "\n"},
		{"GCS TSV", text(t, textout.Config{Format: textout.TSV}, &gcs.GenerationSchema, gcsGenerations),
			"key\tsize\tlast_modified\tetag\tgeneration\tmetageneration\tstorage_class\tmd5\tcrc32c\tcontent_type\ttime_created\ttime_deleted\n" +
				"k\t1\t1970-01-01T00:00:00.000005Z\t\\N\t1700000000000001\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t1970-01-01T00:00:00.000010Z\n" +
				"k\t2\t\\N\t\\N\t1700000000000002\t\\N\t\\N\t\\N\t\\N\ttext/plain\t\\N\t\\N\n"},
	} {
		if c.got != c.want {
			t.Errorf("%s:\n%q\nwant\n%q", c.name, c.got, c.want)
		}
	}
}

// TestVersionsTable: the table shows a versions schema's roles from its
// declaration: the version ID column under its own name, whether the row
// is the latest, and a delete marker where the schema has them; a delete
// marker's size is blank.
func TestVersionsTable(t *testing.T) {
	s3want := "last_modified (local)      size  version_id  latest  delete_marker  key\n" +
		"2023-11-14 22:13:20         1 B  new         true    false          k\n" +
		"2023-11-14 22:13:20              null        false   true           k\n"
	if got := text(t, textout.Config{Format: textout.Table}, &s3.VersionsSchema, s3versions); got != s3want {
		t.Errorf("S3:\n%q\nwant\n%q", got, s3want)
	}
	gcswant := "last_modified (local)      size  generation  latest  key\n" +
		"1970-01-01 00:00:00         1 B  1700000000000001  false   k\n" +
		"                            2 B  1700000000000002  true    k\n"
	if got := text(t, textout.Config{Format: textout.Table}, &gcs.GenerationSchema, gcsGenerations); got != gcswant {
		t.Errorf("GCS:\n%q\nwant\n%q", got, gcswant)
	}
}

// TestVersionsRolesBindTheSink: roles that do not define the rows refuse
// the sink; grouped rows without roles are written by their columns.
func TestVersionsRolesBindTheSink(t *testing.T) {
	plain := s3.VersionsSchema
	plain.Versions = reader.VersionSpec{}
	if got := text(t, textout.Config{Format: textout.Table}, &plain, s3versions[:1]); strings.Contains(got, "latest") {
		t.Errorf("grouped rows without roles: %q", got)
	}
	bad := s3.VersionsSchema
	bad.Versions.Latest = "size"
	o, err := textout.Open(textout.Config{Format: textout.JSONL, Stdout: &bytes.Buffer{}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := textout.NewSink(o, &bad); err == nil || !strings.Contains(err.Error(), "text output: schema swath/s3-versions/v1: version roles: the latest column size is not a required boolean") {
		t.Fatalf("malformed roles: %v", err)
	}
}
