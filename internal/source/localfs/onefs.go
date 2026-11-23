// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package localfs

import (
	"encoding/base64"
	"errors"
	"fmt"
	"io/fs"
	"slices"

	"github.com/varveio/swath/reader"
)

// Replaced by tests to exercise volume boundaries on either Unix platform.
var readFilesystemID = filesystemID

var (
	errOtherVolume        = errors.New("on another volume than the root (same stat device)")
	errFilesystemIdentity = errors.New("cannot read filesystem identity")
)

func (p *Provider) checkVolume(fd int) error {
	id, err := readFilesystemID(fd)
	if err != nil {
		return fmt.Errorf("%w: %w", errFilesystemIdentity, err)
	}
	if id != p.fsid {
		return errOtherVolume
	}
	return nil
}

func (p *Provider) checkFilesystem(fd int, dev int64) error {
	if dev != p.dev {
		return errOtherDevice
	}
	return p.checkVolume(fd)
}

const (
	maxBoundaryKeys  = 4096
	maxBoundaryBytes = 1 << 20
)

// noteBoundary records the traversal decision, independently of whether a
// row filter keeps the boundary's own row. Keys are copied out of row arenas.
func (p *Provider) noteBoundary(key string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.boundaryCount++
	n := base64.StdEncoding.EncodedLen(len(key))
	if len(p.boundaryKeys) < maxBoundaryKeys && p.boundaryBytes+n <= maxBoundaryBytes {
		p.boundaryKeys = append(p.boundaryKeys, base64.StdEncoding.EncodeToString([]byte(key)))
		p.boundaryBytes += n
	}
}

func (p *Provider) filesystemBoundaries() *reader.FilesystemBoundaries {
	if !p.opt.OneFilesystem {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	keys := append([]string{}, p.boundaryKeys...)
	slices.Sort(keys)
	return &reader.FilesystemBoundaries{KeysBase64: keys, TotalCount: p.boundaryCount,
		Complete: int64(len(keys)) == p.boundaryCount}
}

// rootDevice is the device of the root fi describes, which
// Options.OneFilesystem compares each directory's with. It needs each
// directory's device: a stat tier, and a platform whose stat gives it.
func rootDevice(fi fs.FileInfo, f Fields) (int64, error) {
	if f == Names {
		return 0, errors.New("staying on one filesystem needs each directory's device: not with the names metadata tier")
	}
	var e Entry
	fromFileInfo(&e, fi)
	if !e.Has(HasDev) {
		return 0, errors.New("staying on one filesystem needs each directory's device, which this platform's engine does not read")
	}
	return e.Dev, nil
}

// otherDevice reports a directory row on another device than the root's,
// with Options.OneFilesystem: it is listed but not walked.
func (p *Provider) otherDevice(row *Row) bool {
	return p.opt.OneFilesystem && row.Data.Has(HasDev) && row.Data.Dev != p.dev
}
