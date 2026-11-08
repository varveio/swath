// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package textout_test

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/varveio/swath/go/internal/source/gcs"
	"github.com/varveio/swath/go/internal/source/s3"
	"github.com/varveio/swath/go/internal/textout"
)

func TestReviewTableVersionBooleans(t *testing.T) {
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
