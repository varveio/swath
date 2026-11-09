// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package reader

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"time"
)

// Identity is what a whole-source dataset was admitted as: the SHA-256 of
// the manifest bytes the admitted Dataset opened, and its published_at,
// in UTC. ReopenWhole compares it.
type Identity struct {
	ManifestSHA256 [32]byte
	PublishedAt    time.Time
}

// ErrIdentityMismatch is a dataset ReopenWhole opened that is not the one
// admitted: its manifest or its publication time differs.
var ErrIdentityMismatch = errors.New("swath reader: not the dataset admitted")

// AdmitWhole admits the dataset at src as a complete listing of the
// current objects of the source want names, and returns it open with its
// Identity. In order, each on the same bytes: ValidateDigests; Open;
// CheckSchema; AcceptWholeSource(want); a schema id of current objects
// (a versions schema is a SemanticError of KindSchema); and a
// published_at (else a SemanticError of KindCoverage naming it). The
// first failure is returned as that step returns it, and the Dataset is
// closed; src is the caller's and stays open.
//
// src must serve the same bytes to every open: files pinned to one stored
// object each (objfile), so that what was validated is what is read.
func AdmitWhole(ctx context.Context, src Source, want WholeSource, opt Options) (*Dataset, Identity, error) {
	if _, err := ValidateDigests(ctx, src, opt); err != nil {
		return nil, Identity{}, err
	}
	d, err := Open(ctx, src, opt)
	return admitWhole(ctx, d, err, want)
}

// AdmitWholeDir admits the dataset in the local directory at path, as
// AdmitWhole does, through ValidateDigestsDir and OpenDir. The directory
// must not change while it is admitted.
func AdmitWholeDir(ctx context.Context, path string, want WholeSource, opt Options) (*Dataset, Identity, error) {
	if _, err := ValidateDigestsDir(ctx, path, opt); err != nil {
		return nil, Identity{}, err
	}
	d, err := OpenDir(ctx, path, opt)
	return admitWhole(ctx, d, err, want)
}

func admitWhole(ctx context.Context, d *Dataset, err error, want WholeSource) (*Dataset, Identity, error) {
	if err != nil {
		return nil, Identity{}, err
	}
	var id Identity
	if err = d.CheckSchema(ctx); err == nil {
		id, err = d.acceptWhole(want)
	}
	if err != nil {
		return nil, Identity{}, errors.Join(err, d.Close())
	}
	return d, id, nil
}

// ReopenWhole opens a dataset AdmitWhole admitted as id, without
// validating its digests again: Open, the acceptance AdmitWhole applies
// after CheckSchema (AcceptWholeSource(want), current objects, a
// published_at), and the Identity, which must equal id, else an error
// wrapping ErrIdentityMismatch naming the member that differs. It opens no
// part. On a failure the Dataset is closed; src stays open.
//
// It proves nothing about the parts: the source must be sealed to the
// objects admitted (objfile pins restored from the admission) or be local
// files that have not changed since. A local directory that may have
// changed is admitted again instead.
func ReopenWhole(ctx context.Context, src Source, want WholeSource, id Identity, opt Options) (*Dataset, error) {
	d, err := Open(ctx, src, opt)
	return reopenWhole(d, err, want, id)
}

// ReopenWholeDir reopens the dataset in the local directory at path, as
// ReopenWhole does, through OpenDir.
func ReopenWholeDir(ctx context.Context, path string, want WholeSource, id Identity, opt Options) (*Dataset, error) {
	d, err := OpenDir(ctx, path, opt)
	return reopenWhole(d, err, want, id)
}

func reopenWhole(d *Dataset, err error, want WholeSource, id Identity) (*Dataset, error) {
	if err != nil {
		return nil, err
	}
	got, err := d.acceptWhole(want)
	switch {
	case err != nil:
	case got.ManifestSHA256 != id.ManifestSHA256:
		err = fmt.Errorf("%w: manifest SHA-256 %x, admitted %x", ErrIdentityMismatch, got.ManifestSHA256, id.ManifestSHA256)
	case !got.PublishedAt.Equal(id.PublishedAt):
		err = fmt.Errorf("%w: published_at %s, admitted %s", ErrIdentityMismatch, got.PublishedAt.Format(time.RFC3339Nano), id.PublishedAt.Format(time.RFC3339Nano))
	}
	if err != nil {
		return nil, errors.Join(err, d.Close())
	}
	return d, nil
}

// acceptWhole accepts the opened dataset as a whole-source listing of
// current objects with a publication time, and returns its Identity.
func (d *Dataset) acceptWhole(want WholeSource) (Identity, error) {
	if err := d.AcceptWholeSource(want); err != nil {
		return Identity{}, err
	}
	if objectColumns(d.spec.ID) == nil {
		return Identity{}, notCurrent(d.spec.ID)
	}
	if d.meta.PublishedAt == nil {
		return Identity{}, missing("published_at")
	}
	return Identity{ManifestSHA256: sha256.Sum256(d.meta.Manifest.Manifest), PublishedAt: d.meta.PublishedAt.UTC()}, nil
}
