// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package reader

import (
	"context"
	"errors"
	"fmt"

	seekparquet "github.com/varveio/seekparquet/go"
)

// ErrMissingDigest is a part whose manifest entry declares no SHA-256,
// which ValidateDigests requires of every part.
var ErrMissingDigest = errors.New("swath reader: missing SHA-256")

// DigestResult is a digest validation that succeeded: CheckedParts is the
// number of parts whose SHA-256 was compared, every part of the manifest.
type DigestResult struct {
	CheckedParts int64
}

// ValidateDigests checks, for a reader importing or attaching the dataset
// at src, that it is a swath dataset and that every part's bytes are those
// its manifest declares. It opens the dataset as Open does (a schema id
// swath writes, extensions.swath well-formed) and requires
// extensions.swath (KindCoverage naming it) and the manifest's key column
// and key uniqueness to be the spec's (KindSchema), as CheckSchema
// requires them; it opens no part to do so. Every part must declare a
// SHA-256: one that does not fails with an error wrapping ErrMissingDigest
// naming its path, before any part is read. Then the module validates the
// dataset at its digest level (seekparquet.Validate, LevelDigest), reading
// every part as bytes.
//
// A digest that differs, or a part that is missing, is the module's
// *RefusalError, returned unchanged. A validation that could not check
// every part (a storage error, a limit, an unsupported feature: an
// incomplete report) is an error wrapping its cause, and an ended context
// is the context's error: neither is ever success. An empty dataset
// succeeds with no part checked.
//
// It does not check the parts as Parquet (Open's queries and CheckSchema
// do). The caller must validate, then open, the same bytes: a Source whose
// files are pinned to one stored object each, used for both, or a local
// directory that does not change in between.
func ValidateDigests(ctx context.Context, src Source, opt Options) (DigestResult, error) {
	parts, err := digestParts(Open(ctx, src, opt))
	if err != nil {
		return DigestResult{}, err
	}
	report, err := seekparquet.Validate(ctx, src, seekparquet.ValidateOptions{Level: seekparquet.LevelDigest, Limits: opt.Limits})
	return digestResult(ctx, report, err, parts)
}

// ValidateDigestsDir validates the dataset in the local directory at path,
// as ValidateDigests does, through OpenDir and seekparquet.ValidateDir.
func ValidateDigestsDir(ctx context.Context, path string, opt Options) (DigestResult, error) {
	parts, err := digestParts(OpenDir(ctx, path, opt))
	if err != nil {
		return DigestResult{}, err
	}
	report, err := seekparquet.ValidateDir(ctx, path, seekparquet.ValidateOptions{Level: seekparquet.LevelDigest, Limits: opt.Limits})
	return digestResult(ctx, report, err, parts)
}

// digestParts checks an opened dataset's identity, closes it, and returns
// its number of parts, each of which declares a SHA-256.
func digestParts(d *Dataset, err error) (int, error) {
	if err != nil {
		return 0, err
	}
	parts := d.meta.Manifest.Parts
	err = d.checkSwath()
	if err == nil {
		err = d.checkKey()
	}
	if err := errors.Join(err, d.Close()); err != nil {
		return 0, err
	}
	for _, p := range parts {
		if p.SHA256 == "" {
			return 0, fmt.Errorf("%w: part %s", ErrMissingDigest, p.Path)
		}
	}
	return len(parts), nil
}

// digestResult turns a digest validation of a dataset of parts parts into
// a result, or an error unless it compared every part's digest. A context
// that ended is its error even with a complete report: the module does
// not check it after the last read.
func digestResult(ctx context.Context, report *seekparquet.Report, err error, parts int) (DigestResult, error) {
	if err == nil {
		err = ctx.Err()
	}
	if err != nil {
		return DigestResult{}, err
	}
	lr, _ := report.Level(seekparquet.LevelDigest)
	switch {
	case lr.Result == seekparquet.Failed:
		return DigestResult{}, lr.Failure
	case lr.Result != seekparquet.Complete && lr.Err != nil:
		return DigestResult{}, fmt.Errorf("swath reader: digest validation incomplete (%s): %w", lr.At, lr.Err)
	case lr.Result != seekparquet.Complete:
		return DigestResult{}, fmt.Errorf("swath reader: digest validation %s: %s", lr.Result, lr.Reason)
	case lr.Checked.Digests != int64(parts):
		return DigestResult{}, fmt.Errorf("swath reader: digest validation checked %d parts of %d", lr.Checked.Digests, parts)
	}
	return DigestResult{CheckedParts: lr.Checked.Digests}, nil
}

// checkSwath requires the manifest's extensions.swath.
func (d *Dataset) checkSwath() error {
	if !d.meta.Swath {
		return semantic(KindCoverage, "extensions."+Extension, nil, "absent: the manifest is not a swath listing's")
	}
	return nil
}
