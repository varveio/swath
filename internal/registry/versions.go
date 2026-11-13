// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package registry

import (
	"context"
	"sync/atomic"

	"github.com/varveio/swath/internal/dataset"
	"github.com/varveio/swath/internal/order"
	"github.com/varveio/swath/internal/schema"
)

// VersionCounts describes only this attempt's rows handed on by order
// (or counted), after filtering. It never includes retained resume rows.
// Enabled and HasDeleteMarkers are set before listing, from the schema's
// version roles (schema.Info.Versions); read counters after Run returns.
// Versions counts the rows that are not delete markers, Noncurrent those
// neither latest nor markers, Live those latest and not markers. Every
// counter is counted for every versions schema, but the report shows
// DeleteMarkers only for a schema with delete markers and Noncurrent only
// for one without (listing's report).
type VersionCounts struct {
	Enabled, HasDeleteMarkers                 bool
	Versions, DeleteMarkers, Noncurrent, Live atomic.Int64
}

type versionSink[P any] struct {
	order.Sink[P]
	counts *VersionCounts
	v      *schema.Versions[P]
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
			switch {
			case v.v.Marker(p):
				markers++
			case v.v.Latest(p):
				versions++
				live++
			default:
				versions++
				noncurrent++
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
