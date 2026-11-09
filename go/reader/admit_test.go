// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package reader

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	seekparquet "github.com/varveio/seekparquet/go"
)

var bucketB = WholeSource{Kind: "s3", Container: "b"}

// admitDir writes a dataset of three parts.
func admitDir(t *testing.T) string {
	t.Helper()
	return writeParts(t, s3Spec(), 2, 3, cut(s3Rows(12), 0, 4, 8, 12)...)
}

// TestAdmitWhole: the dataset is admitted open, with the SHA-256 of its
// manifest file and its publication time, through a Source and a
// directory alike.
func TestAdmitWhole(t *testing.T) {
	dir := admitDir(t)
	raw, err := os.ReadFile(filepath.Join(dir, seekparquet.ManifestName))
	if err != nil {
		t.Fatal(err)
	}
	want := Identity{ManifestSHA256: sha256.Sum256(raw), PublishedAt: time.Date(2026, 11, 9, 10, 0, 0, 0, time.UTC)}
	src := newCountingSource(t, dir)
	d, id, err := AdmitWhole(context.Background(), src, bucketB, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if id != want || id.PublishedAt.Location() != time.UTC {
		t.Fatalf("identity %+v, want %+v", id, want)
	}
	if objects, err := stream(t, d); err != nil || len(objects) != 12 {
		t.Fatalf("streamed %d objects, %v", len(objects), err)
	}
	if err := d.Close(); err != nil || src.open() != 0 {
		t.Fatalf("close: %v, %d files open", err, src.open())
	}
	d, id, err = AdmitWholeDir(context.Background(), dir, bucketB, Options{})
	if err != nil || id != want {
		t.Fatalf("dir: %+v, %v", id, err)
	}
	_ = d.Close()
}

// TestAdmitWholeEmpty: a dataset of no parts is admitted, its digests
// validated with none to check.
func TestAdmitWholeEmpty(t *testing.T) {
	dir := writeDataset[s3Row](t, s3Spec(), nil)
	res, err := ValidateDigestsDir(context.Background(), dir, Options{})
	if err != nil || res.CheckedParts != 0 {
		t.Fatalf("%+v, %v", res, err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, seekparquet.ManifestName))
	if err != nil {
		t.Fatal(err)
	}
	src := newCountingSource(t, dir)
	d, id, err := AdmitWhole(context.Background(), src, bucketB, Options{})
	if err != nil || id.ManifestSHA256 != sha256.Sum256(raw) || id.PublishedAt.IsZero() {
		t.Fatalf("%+v, %v", id, err)
	}
	if objects, err := stream(t, d); err != nil || len(objects) != 0 {
		t.Fatalf("streamed %d objects, %v", len(objects), err)
	}
	if err := d.Close(); err != nil || src.open() != 0 {
		t.Fatalf("close: %v, %d files open", err, src.open())
	}
}

// TestAdmitWholeRefusals: each step refuses in order, the Dataset is
// closed and the Source stays usable.
func TestAdmitWholeRefusals(t *testing.T) {
	corrupt := func(t *testing.T, dir string) {
		part := filepath.Join(dir, "data", "part-00001.parquet")
		raw, err := os.ReadFile(part)
		if err != nil {
			t.Fatal(err)
		}
		raw[len(raw)/2] ^= 0xff
		if err := os.WriteFile(part, raw, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	versions := s3Spec()
	versions.id, versions.key.Unique = S3VersionsV1, false
	unpublished := s3Spec()
	delete(unpublished.ext, "published_at")
	for _, tc := range []struct {
		name string
		dir  func(t *testing.T) string
		want WholeSource
		is   func(error) bool
	}{
		{"corrupt part", func(t *testing.T) string { dir := admitDir(t); corrupt(t, dir); return dir }, bucketB, func(err error) bool {
			return seekparquet.CodeOf(err) == seekparquet.CodeDigestMismatch
		}},
		{"missing digest", func(t *testing.T) string {
			dir := admitDir(t)
			editManifest(t, dir, func(m *seekparquet.Manifest) { m.Parts[2].SHA256 = "" })
			return dir
		}, bucketB, func(err error) bool { return errors.Is(err, ErrMissingDigest) }},
		{"schema after parts opened", func(t *testing.T) string {
			dir := writeDataset(t, s3Spec(), s3Rows(5))
			appendPart(t, dir, writeDataset(t, s3Spec(), []s3RowNoETag{{Key: "z1"}, {Key: "z2"}}))
			return dir
		}, bucketB, func(err error) bool {
			var se *SemanticError
			return errors.As(err, &se) && se.Kind == KindSchema && se.Column == "etag"
		}},
		{"wrong kind", admitDir, WholeSource{Kind: "gcs", Container: "b"}, func(err error) bool {
			var se *SemanticError
			return errors.As(err, &se) && se.Kind == KindCoverage && se.Column == "source.kind"
		}},
		{"wrong container", admitDir, WholeSource{Kind: "s3", Container: "c"}, func(err error) bool {
			var se *SemanticError
			return errors.As(err, &se) && se.Kind == KindCoverage && se.Column == "source.container"
		}},
		{"versions", func(t *testing.T) string { return writeDataset[s3Row](t, versions, nil) }, bucketB, func(err error) bool {
			var se *SemanticError
			return errors.As(err, &se) && se.Kind == KindSchema && se.Column == ""
		}},
		{"unpublished", func(t *testing.T) string { return writeParts(t, unpublished, 2, 3, cut(s3Rows(12), 0, 4, 8, 12)...) }, bucketB, func(err error) bool {
			var se *SemanticError
			return errors.As(err, &se) && se.Kind == KindCoverage && se.Column == "published_at"
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := tc.dir(t)
			src := newCountingSource(t, dir)
			d, id, err := AdmitWhole(context.Background(), src, tc.want, Options{})
			if !tc.is(err) || !IsRefusal(err) || d != nil || id != (Identity{}) {
				t.Fatalf("%v, %v, %+v", d, err, id)
			}
			if n := src.open(); n != 0 {
				t.Fatalf("%d files left open", n)
			}
			if opens, _ := src.taken(); tc.name == "schema after parts opened" && !slices.Contains(opens, "data/part-00001.parquet") {
				t.Fatalf("opened %q, want every part", opens)
			}
			if _, err := src.Open(context.Background(), seekparquet.ManifestName); err != nil {
				t.Fatalf("source closed: %v", err)
			}
			if _, _, err := AdmitWholeDir(context.Background(), dir, tc.want, Options{}); !tc.is(err) {
				t.Fatalf("dir: %v", err)
			}
		})
	}
}

// TestReopenWhole: a reopen opens the manifest alone and accepts the
// identity admitted; another identity or source is refused and closed.
func TestReopenWhole(t *testing.T) {
	dir := admitDir(t)
	d, id, err := AdmitWholeDir(context.Background(), dir, bucketB, Options{})
	if err != nil {
		t.Fatal(err)
	}
	_ = d.Close()

	src := newCountingSource(t, dir)
	d, err = ReopenWhole(context.Background(), src, bucketB, id, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if opens, _ := src.taken(); len(opens) != 1 {
		t.Fatalf("opened %q, want the manifest alone", opens)
	}
	_ = d.Close()
	if d, err = ReopenWholeDir(context.Background(), dir, bucketB, id, Options{}); err != nil {
		t.Fatal(err)
	}
	_ = d.Close()

	otherManifest, otherTime := id, id
	otherManifest.ManifestSHA256[0] ^= 1
	otherTime.PublishedAt = otherTime.PublishedAt.Add(time.Second)
	for _, tc := range []struct {
		name string
		want WholeSource
		id   Identity
		is   func(error) bool
	}{
		{"manifest", bucketB, otherManifest, func(err error) bool { return errors.Is(err, ErrIdentityMismatch) }},
		{"published_at", bucketB, otherTime, func(err error) bool { return errors.Is(err, ErrIdentityMismatch) }},
		{"container", WholeSource{Kind: "s3", Container: "c"}, id, func(err error) bool {
			var se *SemanticError
			return errors.As(err, &se) && se.Column == "source.container"
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src := newCountingSource(t, dir)
			d, err := ReopenWhole(context.Background(), src, tc.want, tc.id, Options{})
			if !tc.is(err) || !IsRefusal(err) || d != nil {
				t.Fatalf("%v, %v", d, err)
			}
			if n := src.open(); n != 0 {
				t.Fatalf("%d files left open", n)
			}
		})
	}
}

func TestIsRefusal(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want bool
	}{
		{semantic(KindRow, "size", nil, "x"), true},
		{fmt.Errorf("wrapped: %w", semantic(KindSchema, "", nil, "x")), true},
		{&RefusalError{Code: seekparquet.CodeDigestMismatch}, true},
		{fmt.Errorf("%w: part p", ErrMissingDigest), true},
		{fmt.Errorf("%w: manifest", ErrIdentityMismatch), true},
		{errors.Join(semantic(KindCoverage, "x", nil, "x"), nil), true},
		{&UnsupportedError{Code: "x"}, false},
		{ErrClosed, false},
		{fmt.Errorf("%w: limit", ErrInvalidRequest), false},
		{ErrLimitExceeded, false},
		{context.Canceled, false},
		{context.DeadlineExceeded, false},
		{fs.ErrNotExist, false},
		{fs.ErrPermission, false},
		{errors.New("storage"), false},
		{nil, false},
	} {
		if got := IsRefusal(tc.err); got != tc.want {
			t.Errorf("IsRefusal(%v) = %t", tc.err, got)
		}
	}
}
