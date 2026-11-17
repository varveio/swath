// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build !linux && !darwin

package localfs

import (
	"context"
	"errors"
	"io/fs"
	"os"
)

// dirFD is a directory the portable engine reads, opened by its path
// (openChecked): no subdirectory is opened relative to it here.
type dirFD struct{ f *os.File }

// release closes d; a nil d holds nothing.
func (d *dirFD) release() {
	if d != nil {
		d.f.Close()
	}
}

// child: a subdirectory is opened by its path here.
func (d *dirFD) child() (*dirFD, int) { return nil, 0 }

// keepLimit: no directory descriptor is kept for a subdirectory here.
func keepLimit() int64 { return 0 }

// openPortable opens r's directory for the portable engine (openChecked).
func (p *Provider) openPortable(_ context.Context, r Range, st *Stats) (*dirFD, bool, error) {
	f, ok := p.openChecked(r, st)
	if !ok {
		return nil, false, nil
	}
	return &dirFD{f: f}, true, nil
}

// openChecked opens a directory by path. Below the root it then checks that
// the path is still a real directory (Lstat: not a symlink) and the same
// object as the opened file (fstat): best-effort protection against a
// directory swapped for a symlink between listing and open. A directory it
// does not open is accounted for.
func (p *Provider) openChecked(r Range, st *Stats) (*os.File, bool) {
	f, err := os.Open(r.path)
	if err != nil {
		st.note(&st.OpenErrors, r.path, err)
		p.unreadableDir(r)
		return nil, false
	}
	if r.prefix == "" {
		return f, true
	}
	ffi, err1 := f.Stat()
	lfi, err2 := os.Lstat(r.path)
	if err1 != nil || err2 != nil || !ffi.IsDir() || !lfi.IsDir() || lfi.Mode()&fs.ModeSymlink != 0 || !os.SameFile(ffi, lfi) {
		f.Close()
		st.note(&st.Unsafe, r.path, errors.New("directory changed before open"))
		p.unreadableDir(r)
		return nil, false
	}
	return f, true
}
