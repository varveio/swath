// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package registry

import (
	"context"
	"sync/atomic"

	"github.com/varveio/swath/go/internal/order"
	"github.com/varveio/swath/go/internal/schema"
)

// VersionCounts describes only this attempt's rows handed on by order
// (or counted), after filtering. It never includes retained resume rows.
// Enabled and S3 are set before listing; read counters after Run returns.
type VersionCounts struct {
	Enabled, S3                               bool
	Versions, DeleteMarkers, Noncurrent, Live atomic.Int64
}

type versionSink[P any] struct {
	order.Sink[P]
	counts         *VersionCounts
	latest, marker func(*P) (bool, bool)
	deleted        func(*P) (int64, bool)
}

func (v *versionSink[P]) bind(s *schema.Schema[P]) {
	for _, c := range s.Columns {
		switch c.Name {
		case "is_latest":
			v.latest = c.Bool
		case "is_delete_marker":
			v.marker = c.Bool
		case "time_deleted":
			v.deleted = c.Int
		}
	}
	v.counts.Enabled = v.latest != nil || v.deleted != nil
	v.counts.S3 = v.marker != nil
}

func (v versionSink[P]) Write(ctx context.Context, segs [][]schema.Row[P]) error {
	var versions, markers, noncurrent, live int64
	for _, seg := range segs {
		for i := range seg {
			p := &seg[i].Data
			marker, latest := false, false
			if v.marker != nil {
				marker, _ = v.marker(p)
			}
			if marker {
				markers++
			} else {
				versions++
			}
			if v.latest != nil {
				latest, _ = v.latest(p)
			} else {
				_, gone := v.deleted(p)
				latest = !gone
				if gone {
					noncurrent++
				}
			}
			if latest && !marker {
				live++
			}
		}
	}
	v.counts.Versions.Add(versions)
	v.counts.DeleteMarkers.Add(markers)
	v.counts.Noncurrent.Add(noncurrent)
	v.counts.Live.Add(live)
	return v.Sink.Write(ctx, segs)
}
