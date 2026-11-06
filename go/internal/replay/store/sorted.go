// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"bytes"
	"context"
	"math"
	"sync/atomic"
	"time"

	"github.com/parquet-go/parquet-go"
	"golang.org/x/sync/semaphore"

	"github.com/varveio/swath/go/sortedparquet"
)

// Counters are the store's work counters, plain atomics read by whoever
// exports them.
type Counters struct {
	// Requests counts Rows and DelimitedRollup calls that read.
	Requests atomic.Int64
	// PageDecodes counts data pages decoded (key and value columns).
	PageDecodes atomic.Int64
	// RowsDecoded counts key values decoded.
	RowsDecoded atomic.Int64
	// InFlightReaders is the number of reads holding a reader slot now;
	// PeakReaders the most at once.
	InFlightReaders atomic.Int64
	PeakReaders     atomic.Int64
	// DelimiterEngagements counts rollups answered by the skip-scan;
	// DelimiterDeclines those declined (another delimiter).
	DelimiterEngagements atomic.Int64
	DelimiterDeclines    atomic.Int64
	// GroupShortcuts counts row groups a rollup skipped without a read.
	GroupShortcuts atomic.Int64
	// Reseeks counts key cursor moves through the page index past at
	// least one undecoded page.
	Reseeks atomic.Int64
}

// Sorted is the Store over a sorted-Parquet fixture. Besides the Store
// methods, ReadRows and RollupRows return the reader's rows with any
// columns a caller names, so a route with its own projection (another
// provider's object fields) reads through the same cursor and reader
// slots without the Object projection.
type Sorted struct {
	reader   *sortedparquet.Reader
	sem      *semaphore.Weighted
	schemaID string
	objects  objectProjection
	counters Counters
}

var _ Store = (*Sorted)(nil)

// Counters returns the store's counters.
func (s *Sorted) Counters() *Counters { return &s.counters }

// SchemaID returns the fixture's declared schema id.
func (s *Sorted) SchemaID() string { return s.schemaID }

// Columns returns the value columns the fixture's parts hold.
func (s *Sorted) Columns() []string { return s.reader.Columns() }

// acquire takes a reader slot: Options.Readers bounds the reads decoding
// pages at once.
func (s *Sorted) acquire(ctx context.Context) (func(), error) {
	if err := s.sem.Acquire(ctx, 1); err != nil {
		return nil, err
	}
	s.counters.Requests.Add(1)
	n := s.counters.InFlightReaders.Add(1)
	for peak := s.counters.PeakReaders.Load(); n > peak && !s.counters.PeakReaders.CompareAndSwap(peak, n); peak = s.counters.PeakReaders.Load() {
	}
	return func() {
		s.counters.InFlightReaders.Add(-1)
		s.sem.Release(1)
	}, nil
}

func (s *Sorted) record(stats *sortedparquet.Stats) {
	s.counters.PageDecodes.Add(stats.PagesDecoded)
	s.counters.RowsDecoded.Add(stats.KeysDecoded)
	s.counters.Reseeks.Add(stats.Reseeks)
	s.counters.GroupShortcuts.Add(stats.GroupShortcuts)
}

// ReadRows returns up to limit rows with key in [from, toExclusive), as
// Rows, decoding only the named value columns.
func (s *Sorted) ReadRows(ctx context.Context, from []byte, fromInclusive bool, toExclusive []byte, limit int, columns []string) ([]sortedparquet.Row, error) {
	if limit <= 0 {
		return nil, nil
	}
	release, err := s.acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	var stats sortedparquet.Stats
	defer s.record(&stats)
	return s.reader.Read(ctx, sortedparquet.Range{
		From: bytes.Clone(from), FromInclusive: fromInclusive, To: bytes.Clone(toExclusive),
		Limit: limit, Columns: columns,
	}, &stats)
}

// RollupRows is DelimitedRollup returning the reader's entries with the
// named value columns; ok=false declines.
func (s *Sorted) RollupRows(ctx context.Context, from []byte, fromInclusive bool, toExclusive []byte, prefix, delimiter []byte, limit int, columns []string, suppressPrefixAtOrBeforeFloor bool) ([]sortedparquet.Entry, bool, error) {
	if len(delimiter) != 1 || delimiter[0] != '/' {
		s.counters.DelimiterDeclines.Add(1)
		return nil, false, nil
	}
	if limit <= 0 {
		return nil, true, nil
	}
	release, err := s.acquire(ctx)
	if err != nil {
		return nil, true, err
	}
	defer release()
	s.counters.DelimiterEngagements.Add(1)
	var stats sortedparquet.Stats
	defer s.record(&stats)
	entries, err := s.reader.RollupRead(ctx, sortedparquet.Rollup{
		From: bytes.Clone(from), FromInclusive: fromInclusive, To: bytes.Clone(toExclusive),
		Prefix: bytes.Clone(prefix), Delimiter: '/', Limit: int(min(int64(limit)+1, math.MaxInt)),
		SuppressAtOrBeforeFrom: suppressPrefixAtOrBeforeFloor, Columns: columns,
	}, &stats)
	return entries, true, err
}

// Rows implements Store with the Object projection.
func (s *Sorted) Rows(ctx context.Context, from []byte, fromInclusive bool, toExclusive []byte, limit int, proj Projection) ([]Object, error) {
	rows, err := s.ReadRows(ctx, from, fromInclusive, toExclusive, limit, s.objects.columns(proj))
	if err != nil {
		return nil, err
	}
	out := make([]Object, len(rows))
	for i := range rows {
		out[i] = s.objects.object(&rows[i])
	}
	return out, nil
}

// DelimitedRollup implements Store with the Object projection. It answers
// only the "/" delimiter and declines every other.
func (s *Sorted) DelimitedRollup(ctx context.Context, from []byte, fromInclusive bool, toExclusive []byte, prefix, delimiter []byte, limit int, proj Projection, suppressPrefixAtOrBeforeFloor bool) ([]DelimitedEntry, bool, error) {
	entries, ok, err := s.RollupRows(ctx, from, fromInclusive, toExclusive, prefix, delimiter, limit, s.objects.columns(proj), suppressPrefixAtOrBeforeFloor)
	if !ok || err != nil {
		return nil, ok, err
	}
	out := make([]DelimitedEntry, len(entries))
	for i, entry := range entries {
		if entry.Row == nil {
			out[i] = DelimitedEntry{CommonPrefix: entry.CommonPrefix}
			continue
		}
		object := s.objects.object(entry.Row)
		out[i] = DelimitedEntry{Object: &object}
	}
	return out, true, nil
}

// Close closes the fixture's files after reads in progress.
func (s *Sorted) Close() error { return s.reader.Close() }

// objectProjection names, per Object field, the column of a schema that
// holds it; "" leaves the field nil.
type objectProjection struct {
	size, lastModified, etag, storageClass                     string
	ownerID, ownerDisplayName, checksumAlgorithm, checksumType string
}

// s3Objects is the projection of the S3-shaped schemas: the Java-era
// canonical schema, the S3 current-object schema, and the replay-owned
// schema sort-fixture writes (the same columns under its own id).
var s3Objects = objectProjection{
	size: "size", lastModified: "last_modified", etag: "etag", storageClass: "storage_class",
	ownerID: "owner_id", ownerDisplayName: "owner_display_name",
	checksumAlgorithm: "checksum_algorithm", checksumType: "checksum_type",
}

// objectProjections maps declared schema ids to the Object projection.
// A schema id not listed projects keys only.
var objectProjections = map[string]objectProjection{
	"swath/canonical":         s3Objects,
	"swath/s3-objects/v1":     s3Objects,
	"swath-replay/objects/v1": s3Objects,
	"swath/gcs-objects/v1":    {size: "size", lastModified: "last_modified", etag: "etag", storageClass: "storage_class"},
	"swath-local/v1":          {size: "size", lastModified: "last_modified"},
}

func objectProjectionFor(schemaID string) objectProjection {
	return objectProjections[schemaID]
}

// columns returns the columns proj needs decoded: owner columns only with
// Owner.
func (p objectProjection) columns(proj Projection) []string {
	var names []string
	for _, name := range []string{p.size, p.lastModified, p.etag, p.storageClass, p.checksumAlgorithm, p.checksumType} {
		if name != "" {
			names = append(names, name)
		}
	}
	if proj.Owner {
		for _, name := range []string{p.ownerID, p.ownerDisplayName} {
			if name != "" {
				names = append(names, name)
			}
		}
	}
	return names
}

func (p objectProjection) object(row *sortedparquet.Row) Object {
	o := Object{Key: row.Key}
	if v, ok := value(row, p.size); ok {
		size := v.Int64()
		o.Size = &size
	}
	if v, ok := value(row, p.lastModified); ok {
		t := time.UnixMicro(v.Int64()).UTC()
		o.LastModified = &t
	}
	o.ETag = text(row, p.etag)
	o.StorageClass = text(row, p.storageClass)
	o.OwnerID = text(row, p.ownerID)
	o.OwnerDisplayName = text(row, p.ownerDisplayName)
	o.ChecksumAlgorithm = text(row, p.checksumAlgorithm)
	o.ChecksumType = text(row, p.checksumType)
	return o
}

// value returns a projected, non-null value.
func value(row *sortedparquet.Row, name string) (parquet.Value, bool) {
	if name == "" {
		return parquet.Value{}, false
	}
	v, ok := row.Value(name)
	return v, ok && !v.IsNull()
}

func text(row *sortedparquet.Row, name string) *string {
	v, ok := value(row, name)
	if !ok {
		return nil
	}
	s := string(v.ByteArray())
	return &s
}
