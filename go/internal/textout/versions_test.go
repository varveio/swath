// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package textout_test

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/varveio/swath/go/internal/schema"
	"github.com/varveio/swath/go/internal/source/gcs"
	"github.com/varveio/swath/go/internal/source/s3"
	"github.com/varveio/swath/go/internal/textout"
	"github.com/varveio/swath/go/reader"
)

// TestVersionsTableIncludesBooleans: The S3 versions table renders both is_latest
// and is_delete_marker.
func TestVersionsTableIncludesBooleans(t *testing.T) {
	var buf bytes.Buffer
	o, err := textout.Open(textout.Config{Format: textout.Table, Stdout: &buf})
	if err != nil {
		t.Fatal(err)
	}
	k, err := textout.NewSink(o, &s3.VersionsSchema)
	if err != nil {
		t.Fatal(err)
	}
	rows := [][]s3.VersionRow{{{Key: "k", Data: s3.Version{Object: s3.Object{Size: 1}, VersionID: "vid", IsLatest: true}}}}
	if err := k.Write(context.Background(), rows); err != nil {
		t.Fatal(err)
	}
	if err := o.Commit(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), "true") || !strings.Contains(buf.String(), "false") {
		t.Fatalf("table omitted IsLatest/IsDeleteMarker: %q", buf.String())
	}
}

func TestVersionsTableFixtures(t *testing.T) {
	t.Run("S3", func(t *testing.T) {
		var buf bytes.Buffer
		o, err := textout.Open(textout.Config{Format: textout.Table, Stdout: &buf})
		if err != nil {
			t.Fatal(err)
		}
		sink, err := textout.NewSink(o, &s3.VersionsSchema)
		if err != nil {
			t.Fatal(err)
		}
		rows := []s3.VersionRow{
			{Key: "k", Data: s3.Version{Object: s3.Object{Size: 0}, VersionID: "new", IsLatest: true}},
			{Key: "k", Data: s3.Version{VersionID: "null", DeleteMarker: true}},
			{Key: "k", Data: s3.Version{Object: s3.Object{Size: 3}, VersionID: "old"}},
		}
		if err := sink.Write(context.Background(), [][]s3.VersionRow{rows}); err != nil {
			t.Fatal(err)
		}
		if err := o.Commit(); err != nil {
			t.Fatal(err)
		}
		want := "1970-01-01 00:00:00          0 k new true false\n1970-01-01 00:00:00            k null false true\n1970-01-01 00:00:00          3 k old false false\n"
		if buf.String() != want {
			t.Fatalf("got %q want %q", buf.String(), want)
		}
	})
	t.Run("GCS", func(t *testing.T) {
		var buf bytes.Buffer
		o, err := textout.Open(textout.Config{Format: textout.Table, Stdout: &buf})
		if err != nil {
			t.Fatal(err)
		}
		sink, err := textout.NewSink(o, &gcs.GenerationSchema)
		if err != nil {
			t.Fatal(err)
		}
		rows := []gcs.GenerationRow{
			{Key: "k", Data: gcs.Generation{Object: gcs.Object{Size: 1, Generation: 1}, TimeDeleted: 10}},
			{Key: "k", Data: gcs.Generation{Object: gcs.Object{Size: 2, Generation: 2}}},
		}
		if err := sink.Write(context.Background(), [][]gcs.GenerationRow{rows}); err != nil {
			t.Fatal(err)
		}
		if err := o.Commit(); err != nil {
			t.Fatal(err)
		}
		want := "                             1 k 1 false\n                             2 k 2 true\n"
		if buf.String() != want {
			t.Fatalf("got %q want %q", buf.String(), want)
		}
	})
}

// versionsText writes rows through a sink bound to s in format f.
func versionsText[P any](t *testing.T, f string, s *schema.Schema[P], rows []schema.Row[P]) string {
	t.Helper()
	var buf bytes.Buffer
	o, err := textout.Open(textout.Config{Format: f, Stdout: &buf})
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

// TestVersionsJSONLAndTSV: the versions fields in JSONL and TSV: S3's
// version ID verbatim (a delete marker without a size), IsLatest and
// IsDeleteMarker; a GCS generation's in decimal and IsLatest from a null
// time_deleted, with no IsDeleteMarker.
func TestVersionsJSONLAndTSV(t *testing.T) {
	s3rows := []s3.VersionRow{
		{Key: "k", Data: s3.Version{Object: s3.Object{Size: 1, ETag: "e"}, VersionID: "new", IsLatest: true}},
		{Key: "k", Data: s3.Version{VersionID: "null", DeleteMarker: true}},
	}
	gcsrows := []gcs.GenerationRow{
		{Key: "k", Data: gcs.Generation{Object: gcs.Object{Size: 1, Generation: 1700000000000001}, TimeDeleted: 10}},
		{Key: "k", Data: gcs.Generation{Object: gcs.Object{Size: 2, Generation: 1700000000000002}}},
	}
	for _, c := range []struct {
		name, got, want string
	}{
		{"S3 JSONL", versionsText(t, textout.JSONL, &s3.VersionsSchema, s3rows),
			`{"Key":"k","VersionId":"new","IsLatest":true,"IsDeleteMarker":false,"LastModified":"1970-01-01T00:00:00+00:00","ETag":"\"e\"","Size":1}` + "\n" +
				`{"Key":"k","VersionId":"null","IsLatest":false,"IsDeleteMarker":true,"LastModified":"1970-01-01T00:00:00+00:00"}` + "\n"},
		{"S3 TSV", versionsText(t, textout.TSV, &s3.VersionsSchema, s3rows),
			"Key\tVersionId\tIsLatest\tIsDeleteMarker\tLastModified\tETag\tChecksumAlgorithm\tChecksumType\tSize\tStorageClass\tOwner.DisplayName\tOwner.ID\n" +
				"k\tnew\ttrue\tfalse\t1970-01-01T00:00:00+00:00\t\"e\"\t\t\t1\t\t\t\n" +
				"k\tnull\tfalse\ttrue\t1970-01-01T00:00:00+00:00\t\t\t\t\t\t\t\n"},
		{"GCS JSONL", versionsText(t, textout.JSONL, &gcs.GenerationSchema, gcsrows),
			`{"Key":"k","VersionId":"1700000000000001","IsLatest":false,"Size":1}` + "\n" +
				`{"Key":"k","VersionId":"1700000000000002","IsLatest":true,"Size":2}` + "\n"},
		{"GCS TSV", versionsText(t, textout.TSV, &gcs.GenerationSchema, gcsrows),
			"Key\tVersionId\tIsLatest\tLastModified\tETag\tSize\tStorageClass\n" +
				"k\t1700000000000001\tfalse\t\t\t1\t\n" +
				"k\t1700000000000002\ttrue\t\t\t2\t\n"},
	} {
		if c.got != c.want {
			t.Errorf("%s:\n%q\nwant\n%q", c.name, c.got, c.want)
		}
	}
}

// TestVersionsFieldsFromRoles: the versions fields come from the schema's
// version roles, not its grouping: grouped rows without roles are written
// as objects, and roles that do not define the rows refuse the sink.
func TestVersionsFieldsFromRoles(t *testing.T) {
	rows := []s3.VersionRow{{Key: "k", Data: s3.Version{Object: s3.Object{Size: 1}, VersionID: "v", IsLatest: true}}}
	plain := s3.VersionsSchema
	plain.Versions = reader.VersionSpec{}
	if got, want := versionsText(t, textout.JSONL, &plain, rows), `{"Key":"k","LastModified":"1970-01-01T00:00:00+00:00","Size":1}`+"\n"; got != want {
		t.Errorf("grouped rows without roles: %q, want %q", got, want)
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
