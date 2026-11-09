// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package localfs

import (
	"errors"
	"io/fs"
)

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
