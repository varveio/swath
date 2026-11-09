// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package reader

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sync"

	seekparquet "github.com/varveio/seekparquet/go"
)

// Dataset is a swath dataset opened through the SeekParquet module: its
// metadata, its schema id's spec, and the reads and checks swath's readers
// apply on top of the module's. It is safe for concurrent use.
type Dataset struct {
	sp   *seekparquet.Dataset
	meta Metadata
	spec SchemaSpec
	// src opens the dataset's files for CheckSchema's footer reads: the
	// caller's Source, or one over the directory OpenDir opened.
	src  seekparquet.Source
	root *os.Root

	statsMu sync.Mutex
	stats   StreamStats
}

// Open opens the dataset at src through the SeekParquet module, which
// reads and checks the manifest and opens no part (seekparquet.OpenDataset),
// parses its extensions.swath (ParseMetadata) and resolves the spec of its
// schema id. A schema id swath does not write is a SemanticError of
// KindSchema; the module's refusals and errors are returned unchanged. The
// Source must stay usable until Close.
func Open(ctx context.Context, src seekparquet.Source, opt seekparquet.Options) (*Dataset, error) {
	sp, err := seekparquet.OpenDataset(ctx, src, opt)
	if err != nil {
		return nil, err
	}
	return open(sp, src, nil)
}

// OpenDir opens the dataset in the local directory at path, as Open does,
// through seekparquet.OpenDir. The directory stays open until Close.
func OpenDir(ctx context.Context, path string, opt seekparquet.Options) (*Dataset, error) {
	root, err := os.OpenRoot(path)
	if err != nil {
		return nil, err
	}
	sp, err := seekparquet.OpenDir(ctx, path, opt)
	if err != nil {
		return nil, errors.Join(err, root.Close())
	}
	return open(sp, rootSource{root}, root)
}

func open(sp *seekparquet.Dataset, src seekparquet.Source, root *os.Root) (*Dataset, error) {
	d := &Dataset{sp: sp, src: src, root: root}
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

// SeekParquet returns the module's dataset, for the queries this package
// does not make (List, Range over other columns, Verify). It is borrowed:
// the Dataset owns it and closes it.
func (d *Dataset) SeekParquet() *seekparquet.Dataset { return d.sp }

// Close closes the module's dataset, and the directory OpenDir opened.
func (d *Dataset) Close() error {
	err := d.sp.Close()
	if d.root != nil {
		err = errors.Join(err, d.root.Close())
	}
	return err
}

// rootSource opens files below an os.Root, for CheckSchema's footer reads
// after Verify has proven every part a regular file of its manifest size.
type rootSource struct{ root *os.Root }

func (s rootSource) Open(ctx context.Context, path string) (seekparquet.SourceFile, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f, err := s.root.Open(filepath.FromSlash(path))
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err == nil && !info.Mode().IsRegular() {
		err = &fs.PathError{Op: "open", Path: path, Err: fs.ErrNotExist}
	}
	if err != nil {
		return nil, errors.Join(err, f.Close())
	}
	return rootFile{f, info.Size()}, nil
}

type rootFile struct {
	*os.File
	size int64
}

func (f rootFile) Size() int64 { return f.size }
