// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package reader

import (
	"context"
	"errors"

	seekparquet "github.com/varveio/seekparquet/go"
)

// Dataset is a swath dataset opened through the SeekParquet module: its
// metadata and its schema id's spec. It is safe for concurrent use.
type Dataset struct {
	sp   *seekparquet.Dataset
	meta Metadata
	spec SchemaSpec
}

// Open opens the dataset at src through the SeekParquet module, which
// reads and checks the manifest and opens no part (seekparquet.OpenDataset),
// parses its extensions.swath (ParseMetadata) and resolves the spec of its
// schema id. A schema id swath does not write is a SemanticError of
// KindSchema; the module's refusals and errors are returned unchanged. The
// Source must stay usable until Close.
func Open(ctx context.Context, src seekparquet.Source, opt seekparquet.OpenOptions) (*Dataset, error) {
	sp, err := seekparquet.OpenDataset(ctx, src, opt)
	if err != nil {
		return nil, err
	}
	return open(sp)
}

// OpenDir opens the dataset in the local directory at path, as Open does,
// through seekparquet.OpenDir. The directory stays open until Close.
func OpenDir(ctx context.Context, path string, opt seekparquet.OpenOptions) (*Dataset, error) {
	sp, err := seekparquet.OpenDir(ctx, path, opt)
	if err != nil {
		return nil, err
	}
	return open(sp)
}

func open(sp *seekparquet.Dataset) (*Dataset, error) {
	d := &Dataset{sp: sp}
	meta, err := ParseMetadata(sp.Metadata())
	if err == nil {
		spec, ok := Schema(meta.SchemaID)
		if !ok {
			err = semantic(KindSchema, "", nil, "schema id %q is not one swath writes", meta.SchemaID)
		}
		d.meta, d.spec = meta, spec
	}
	if err != nil {
		return nil, errors.Join(err, d.Close())
	}
	return d, nil
}

// Metadata returns the dataset's metadata, parsed at Open: a copy the
// caller may keep and change.
func (d *Dataset) Metadata() Metadata {
	m, _ := ParseMetadata(d.sp.Metadata())
	return m
}

// SeekParquet returns the underlying dataset for reads, plans, listings,
// aggregations and validation. It is borrowed:
// the Dataset owns it and closes it.
func (d *Dataset) SeekParquet() *seekparquet.Dataset { return d.sp }

// Close closes the module's dataset.
func (d *Dataset) Close() error { return d.sp.Close() }
