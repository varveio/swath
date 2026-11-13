// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package order

import (
	"github.com/varveio/swath/internal/schema"
)

// SpillRoundTrip writes frames (one per element) to a spill file under dir,
// in the run's default format (delta) or plain, compressed or not, then
// replays them through the reader: the external tests' window on the frame
// layer, for each producer's schema (order cannot import them).
func SpillRoundTrip[P any](s *schema.Schema[P], dir string, plain, compress bool, frames [][]schema.Row[P]) ([][]schema.Row[P], error) {
	out, _, err := SpillRoundTripBytes(s, dir, plain, compress, frames, true)
	return out, err
}

// SpillRoundTripBytes is SpillRoundTrip that also returns the bytes
// written; scribble false leaves the read buffers alone (benchmarks).
func SpillRoundTripBytes[P any](s *schema.Schema[P], dir string, plain, compress bool, frames [][]schema.Row[P], scribble bool) (out [][]schema.Row[P], written int64, err error) {
	sp, err := newSpiller(s, dir, plain, compress, 0, 0)
	if err != nil {
		return nil, 0, err
	}
	defer sp.close()
	f := sp.file()
	for _, rows := range frames {
		if err := sp.write(f, rows, false); err != nil {
			return nil, 0, err
		}
	}
	if err := sp.write(f, nil, true); err != nil {
		return nil, 0, err
	}
	written = sp.bytes.Load()
	for {
		rows, err := sp.next(f)
		if err != nil || rows == nil {
			return out, written, err
		}
		out = append(out, rows)
		if !scribble {
			continue
		}
		// The reader's buffers are reused by the next frame: scribble on
		// them now, so a row that still views them is caught.
		for _, b := range [][]byte{sp.rbuf[:cap(sp.rbuf)], sp.zbuf[:cap(sp.zbuf)]} {
			for j := range b {
				b[j] = 0xa5
			}
		}
	}
}
