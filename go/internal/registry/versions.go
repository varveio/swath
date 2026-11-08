// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package registry

import (
	"context"
	"sync/atomic"

	"github.com/varveio/swath/go/internal/dataset"
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
	v.count(segs)
	return v.Sink.Write(ctx, segs)
}

// count classifies rows handed on.
func (v versionSink[P]) count(segs [][]schema.Row[P]) {
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
}

// versionSegmenter is versionSink over a Segmenter (the Parquet router):
// the order stage writes the tail's segments through it as well, and each
// segment's rows are counted as Write's are.
type versionSegmenter[P any] struct {
	versionSink[P]
	sg order.Segmenter[P]
}

func (v versionSegmenter[P]) Plan() (dataset.SegmentPlan, bool) { return v.sg.Plan() }

func (v versionSegmenter[P]) BeginSegments() (dataset.SegmentPlan, bool) {
	return v.sg.BeginSegments()
}

func (v versionSegmenter[P]) AbandonSegments() { v.sg.AbandonSegments() }

func (v versionSegmenter[P]) OpenSegment(ctx context.Context, continueOpen bool) (order.SegmentSink[P], error) {
	seg, err := v.sg.OpenSegment(ctx, continueOpen)
	if err != nil {
		return nil, err
	}
	return versionPart[P]{seg, v.versionSink}, nil
}

type versionPart[P any] struct {
	order.SegmentSink[P]
	v versionSink[P]
}

func (p versionPart[P]) Write(ctx context.Context, segs [][]schema.Row[P]) error {
	p.v.count(segs)
	return p.SegmentSink.Write(ctx, segs)
}
