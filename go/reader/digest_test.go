// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package reader

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	seekparquet "github.com/varveio/seekparquet/go"
)

// digestDir writes a dataset of two parts.
func digestDir(t *testing.T) string {
	t.Helper()
	return writeParts(t, s3Spec(), 2, 3, cut(s3Rows(10), 0, 5, 10)...)
}

func TestValidateDigests(t *testing.T) {
	dir := digestDir(t)
	got, err := ValidateDigestsDir(context.Background(), dir, Options{})
	if err != nil || got.CheckedParts != 2 {
		t.Fatalf("dir: %+v, %v", got, err)
	}
	got, err = ValidateDigests(context.Background(), newCountingSource(t, dir), Options{})
	if err != nil || got.CheckedParts != 2 {
		t.Fatalf("source: %+v, %v", got, err)
	}
}

func TestValidateDigestsEmpty(t *testing.T) {
	got, err := ValidateDigestsDir(context.Background(), writeDataset[s3Row](t, s3Spec(), nil), Options{})
	if err != nil || got.CheckedParts != 0 {
		t.Fatalf("%+v, %v", got, err)
	}
}

// TestValidateDigestsCorruptPart: the module reports the mismatch in a
// failed report and no error; it is the refusal here.
func TestValidateDigestsCorruptPart(t *testing.T) {
	dir := digestDir(t)
	part := filepath.Join(dir, "data", "part-00001.parquet")
	raw, err := os.ReadFile(part)
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)/2] ^= 0xff
	if err := os.WriteFile(part, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	got, err := ValidateDigestsDir(context.Background(), dir, Options{})
	var refusal *RefusalError
	if !errors.As(err, &refusal) || refusal.Code != seekparquet.CodeDigestMismatch || refusal.Part != "data/part-00001.parquet" || got != (DigestResult{}) {
		t.Fatalf("%+v, %v", got, err)
	}
}

func TestValidateDigestsMissingDigest(t *testing.T) {
	dir := digestDir(t)
	editManifest(t, dir, func(m *seekparquet.Manifest) { m.Parts[1].SHA256 = "" })
	src := newCountingSource(t, dir)
	_, err := ValidateDigests(context.Background(), src, Options{})
	if !errors.Is(err, ErrMissingDigest) || !strings.Contains(err.Error(), "data/part-00001.parquet") {
		t.Fatalf("%v", err)
	}
	if opens, _ := src.taken(); len(opens) != 1 {
		t.Fatalf("opened %q, want the manifest alone", opens)
	}
}

// TestValidateDigestsIdentity: a dataset swath did not write is refused
// before a part is read.
func TestValidateDigestsIdentity(t *testing.T) {
	noExt, keyRepeats, unknown := s3Spec(), s3Spec(), s3Spec()
	noExt.ext, keyRepeats.key.Unique, unknown.id = nil, false, "swath/canonical"
	for _, tc := range []struct {
		name string
		s    spec
		want error
	}{{"no extension", noExt, ErrCoverage}, {"keys repeat", keyRepeats, ErrSchema}, {"unknown schema id", unknown, ErrSchema}} {
		t.Run(tc.name, func(t *testing.T) {
			src := newCountingSource(t, writeDataset(t, tc.s, s3Rows(3)))
			if _, err := ValidateDigests(context.Background(), src, Options{}); !errors.Is(err, tc.want) {
				t.Fatalf("%v, want %v", err, tc.want)
			}
			if opens, _ := src.taken(); len(opens) != 1 {
				t.Fatalf("opened %q, want the manifest alone", opens)
			}
		})
	}
}

// partSource serves a directory and calls at when it opens a part.
type partSource struct {
	*countingSource
	at func() error
}

func (s partSource) Open(ctx context.Context, path string) (seekparquet.SourceFile, error) {
	if path != seekparquet.ManifestName {
		if err := s.at(); err != nil {
			return nil, err
		}
	}
	return s.countingSource.Open(ctx, path)
}

// TestValidateDigestsIncomplete: a storage error leaves the module's
// level incomplete with no error, and a context ended mid-way stops it,
// each at the first part's open and at the second's, after the first
// part's digest compared: both are errors here, never success.
func TestValidateDigestsIncomplete(t *testing.T) {
	dir := digestDir(t)
	for _, at := range []int{1, 2} {
		t.Run(fmt.Sprintf("part %d", at), func(t *testing.T) {
			failure := errors.New("storage down")
			n := 0
			src := partSource{newCountingSource(t, dir), func() error {
				if n++; n == at {
					return failure
				}
				return nil
			}}
			got, err := ValidateDigests(context.Background(), src, Options{})
			if !errors.Is(err, failure) || got != (DigestResult{}) || !strings.Contains(err.Error(), fmt.Sprintf("data/part-%05d.parquet", at-1)) {
				t.Fatalf("storage error: %+v, %v", got, err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			n = 0
			src = partSource{newCountingSource(t, dir), func() error {
				if n++; n == at {
					cancel()
				}
				return nil
			}}
			if got, err := ValidateDigests(ctx, src, Options{}); !errors.Is(err, context.Canceled) || got != (DigestResult{}) {
				t.Fatalf("cancelled: %+v, %v", got, err)
			}
		})
	}
}

// TestValidateDigestsCancelledAtLastRead: a context ended during the last
// part's last read, which returns every byte, leaves the module's report
// complete; it is the context's error here.
func TestValidateDigestsCancelledAtLastRead(t *testing.T) {
	dir := digestDir(t)
	src := newCountingSource(t, dir)
	if _, err := ValidateDigests(context.Background(), src, Options{}); err != nil {
		t.Fatal(err)
	}
	_, reads := src.taken()
	if last := reads[len(reads)-1]; last.path != "data/part-00001.parquet" {
		t.Fatalf("last read %+v, want the last part's", last)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	src = newCountingSource(t, dir)
	src.cancelAt, src.cancel = len(reads), cancel
	if got, err := ValidateDigests(ctx, src, Options{}); !errors.Is(err, context.Canceled) || got != (DigestResult{}) {
		t.Fatalf("%+v, %v", got, err)
	}
	if _, again := src.taken(); len(again) != len(reads) {
		t.Fatalf("%d reads, want %d: the cancellation was not at the last read", len(again), len(reads))
	}
}

// TestDigestResultCountsEveryPart: a complete report that compared fewer
// digests than the manifest has parts is not success.
func TestDigestResultCountsEveryPart(t *testing.T) {
	report := &seekparquet.Report{Levels: []seekparquet.LevelReport{{Level: seekparquet.LevelDigest, Result: seekparquet.Complete,
		Checked: seekparquet.Checked{Parts: 1, Digests: 1}}}}
	if got, err := digestResult(context.Background(), report, nil, 2); err == nil || got != (DigestResult{}) {
		t.Fatalf("%+v, %v", got, err)
	}
	if got, err := digestResult(context.Background(), report, nil, 1); err != nil || got.CheckedParts != 1 {
		t.Fatalf("%+v, %v", got, err)
	}
}
