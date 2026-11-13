// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package localfs

import (
	"math"
	"math/bits"
	"time"

	"github.com/varveio/swath/go/internal/source"
)

// filesystemSpace is the space of the filesystem that holds the root,
// measured the first time it is asked for (at the end of the walk, by
// Report) and the same afterwards, a failed measurement included. A
// failure leaves the dataset without it; it never fails the listing.
func (p *Provider) filesystemSpace() *source.FilesystemSpace {
	p.spaceOnce.Do(func() {
		p.space = measureSpace(p.root, p.file)
	})
	return p.space
}

// blockSpace is the space of a filesystem that counts blocks of unit
// bytes: nil when the unit is not positive or a product does not fit an
// int64.
func blockSpace(total, free, avail uint64, unit int64, at time.Time) *source.FilesystemSpace {
	if unit <= 0 {
		return nil
	}
	var b [3]uint64
	for i, n := range [3]uint64{total, free, avail} {
		hi, lo := bits.Mul64(n, uint64(unit))
		if hi != 0 {
			return nil
		}
		b[i] = lo
	}
	return byteSpace(b[0], b[1], b[2], at)
}

// byteSpace is a filesystem's space from byte counts: nil when the total
// is 0 (a pseudo filesystem, which has no capacity to report) or a count
// does not fit an int64. Free may exceed total where the platform measures
// them differently (a Windows quota bounds the total, not the free space).
func byteSpace(total, free, avail uint64, at time.Time) *source.FilesystemSpace {
	if total == 0 || total > math.MaxInt64 || free > math.MaxInt64 || avail > math.MaxInt64 {
		return nil
	}
	return &source.FilesystemSpace{TotalBytes: int64(total), FreeBytes: int64(free), AvailableBytes: int64(avail), MeasuredAt: at.UTC()}
}
