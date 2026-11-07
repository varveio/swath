// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
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
	// DisorderRefusals counts reads that failed because a row group's
	// keys do not ascend strictly (Refusal reports row_group_disorder).
	DisorderRefusals atomic.Int64
}

// Sorted is the Store over a sorted-Parquet fixture. Besides the Store
// methods, ReadRows and RollupRows return the reader's rows with any
// columns a caller names, so a route with its own projection (another
// provider's object fields) reads through the same cursor and reader
// slots without the Object projection.
type Sorted struct {
	reader   *sortedparquet.Reader
	sem      *semaphore.Weighted
	readers  int
	schemaID string
	objects  objectProjection
	counters Counters
	files    *sortedparquet.FileCache
	meters   Meters
	prefetch *prefetcher
	// arenaLimit bounds a packed read's arenas; zero is maxPackedArena.
	// Tests lower it to reach the overflow path without gigabytes.
	arenaLimit int
}

// Meters receives a Sorted store's per-read measurements, for the server's
// meters. A nil func is skipped.
type Meters struct {
	// SlotHeld is how long a read held its reader slot: the decode
	// service time, slot wait excluded.
	SlotHeld func(time.Duration)
	// Query is one ReadRows or RollupRows call that read, slot wait
	// included: its duration, the rows or entries it returned, and
	// whether it failed.
	Query func(d time.Duration, rows int, failed bool)
	// Rollup is one skip-scan's work.
	Rollup func(sortedparquet.Stats)
	// Refused is a read that failed for a fixture reason
	// (row_group_disorder).
	Refused func(reason string)
	// Prefetch receives the window cache's events.
	Prefetch PrefetchMeters
}

// SetMeters installs m. It is called before the store serves its first
// read.
func (s *Sorted) SetMeters(m Meters) {
	s.meters = m
	if s.prefetch != nil {
		s.prefetch.meters = m.Prefetch
	}
}

// PrefetchStats returns the window cache's live counts, and false when the
// cache is off.
func (s *Sorted) PrefetchStats() (PrefetchStats, bool) {
	if s.prefetch == nil {
		return PrefetchStats{}, false
	}
	return s.prefetch.stats(), true
}

// Readers returns the reader-slot bound (Options.Readers resolved).
func (s *Sorted) Readers() int { return s.readers }

var _ Store = (*Sorted)(nil)

// Counters returns the store's counters.
func (s *Sorted) Counters() *Counters { return &s.counters }

// Files returns the descriptor cache the fixture's parts are read through,
// for its counts.
func (s *Sorted) Files() *sortedparquet.FileCache { return s.files }

// FirstKeys returns the first key of every non-empty row group, in key
// order: the routing keys.
func (s *Sorted) FirstKeys() [][]byte { return s.reader.FirstKeys() }

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
	held := time.Now()
	return func() {
		if f := s.meters.SlotHeld; f != nil {
			f(time.Since(held))
		}
		s.counters.InFlightReaders.Add(-1)
		s.sem.Release(1)
	}, nil
}

// record adds a read's stats to the counters; a rollup's also go to
// Meters.Rollup.
func (s *Sorted) record(stats *sortedparquet.Stats, rollup bool) {
	if f := s.meters.Rollup; f != nil && rollup {
		f(*stats)
	}
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
	started := time.Now()
	release, err := s.acquire(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := func() ([]sortedparquet.Row, error) {
		defer release()
		var stats sortedparquet.Stats
		defer s.record(&stats, false)
		return s.reader.Read(ctx, sortedparquet.Range{
			From: bytes.Clone(from), FromInclusive: fromInclusive, To: bytes.Clone(toExclusive),
			Limit: limit, Columns: columns,
		}, &stats)
	}()
	return rows, s.finished(started, len(rows), err)
}

// packedRows reads what objectRows reads, packed for the window cache
// straight from the decoded pages.
func (s *Sorted) packedRows(ctx context.Context, from []byte, fromInclusive bool, toExclusive []byte, limit int, proj Projection) (packed, error) {
	if limit <= 0 {
		return packed{}, nil
	}
	started := time.Now()
	release, err := s.acquire(ctx)
	if err != nil {
		return packed{}, err
	}
	// An arena overflow ends the scan: its rows are read again unpacked.
	ctx, stop := context.WithCancel(ctx)
	defer stop()
	var sink *packer
	err = func() error {
		defer release()
		columns := s.objects.columns(proj)
		// Room for a window up front, never more: a read of a huge
		// limit over a short range must not allocate for rows it will
		// not find, and a longer read grows as it goes.
		sink = s.objects.packer(columns, proj, min(limit, s.windowRows()), s.arenaLimit)
		sink.stop = stop
		var stats sortedparquet.Stats
		defer s.record(&stats, false)
		return s.reader.Scan(ctx, sortedparquet.Range{
			From: bytes.Clone(from), FromInclusive: fromInclusive, To: bytes.Clone(toExclusive),
			Limit: limit, Columns: columns,
		}, &stats, sink)
	}()
	if sink.overflow {
		// Not a failed read: the caller reads the range unpacked.
		_ = s.finished(started, sink.out.n, nil)
		return packed{}, errPackedOverflow
	}
	if err := s.finished(started, sink.out.n, err); err != nil {
		return packed{}, err
	}
	return sink.packed()
}

// windowRows is the window cache's window size, which bounds a packed
// read's initial capacity.
func (s *Sorted) windowRows() int {
	if s.prefetch != nil {
		return s.prefetch.windowRows
	}
	return DefaultPrefetchWindowRows
}

// finished meters a store call that read and counts an error the replay
// server classifies, and returns the error.
func (s *Sorted) finished(started time.Time, rows int, err error) error {
	if f := s.meters.Query; f != nil {
		f(time.Since(started), rows, err != nil)
	}
	if reason := sortedparquet.ReasonOf(err); reason == sortedparquet.ReasonRowGroupDisorder {
		s.counters.DisorderRefusals.Add(1)
		if f := s.meters.Refused; f != nil {
			f(reason)
		}
	}
	return err
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
	started := time.Now()
	release, err := s.acquire(ctx)
	if err != nil {
		return nil, true, err
	}
	s.counters.DelimiterEngagements.Add(1)
	entries, err := func() ([]sortedparquet.Entry, error) {
		defer release()
		var stats sortedparquet.Stats
		defer s.record(&stats, true)
		return s.reader.RollupRead(ctx, sortedparquet.Rollup{
			From: bytes.Clone(from), FromInclusive: fromInclusive, To: bytes.Clone(toExclusive),
			Prefix: bytes.Clone(prefix), Delimiter: '/', Limit: saturatingIncrement(limit),
			SuppressAtOrBeforeFrom: suppressPrefixAtOrBeforeFloor, Columns: columns,
		}, &stats)
	}()
	return entries, true, s.finished(started, len(entries), err)
}

// Rows implements Store with the Object projection, through the window
// cache when it is on.
func (s *Sorted) Rows(ctx context.Context, from []byte, fromInclusive bool, toExclusive []byte, limit int, proj Projection) ([]Object, error) {
	proj = s.objects.projection(proj)
	if s.prefetch != nil && limit > 0 {
		return s.prefetch.serve(ctx, from, fromInclusive, toExclusive, limit, proj)
	}
	return s.objectRows(ctx, from, fromInclusive, toExclusive, limit, proj)
}

// objectRows reads Rows from the fixture.
func (s *Sorted) objectRows(ctx context.Context, from []byte, fromInclusive bool, toExclusive []byte, limit int, proj Projection) ([]Object, error) {
	rows, err := s.ReadRows(ctx, from, fromInclusive, toExclusive, limit, s.objects.columns(proj))
	if err != nil {
		return nil, err
	}
	out := make([]Object, len(rows))
	for i := range rows {
		out[i] = s.objects.object(&rows[i], proj)
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
		object := s.objects.object(entry.Row, proj)
		out[i] = DelimitedEntry{Object: &object}
	}
	return out, true, nil
}

// Close stops the window cache's read-aheads and closes the fixture's
// files after reads in progress.
func (s *Sorted) Close() error {
	if s.prefetch != nil {
		s.prefetch.close()
	}
	return s.reader.Close()
}

// objectProjection names, per Object field, the column of a schema that
// holds it; "" leaves the field nil. The Object is the S3 route's view of
// a row (the other routes read only its key, size and time, or their own
// columns).
type objectProjection struct {
	size, lastModified, etag, storageClass                     string
	ownerID, ownerDisplayName, checksumAlgorithm, checksumType string
	// md5 names the column of a GCS recording's MD5 (base64). When set,
	// the ETag is not the etag column's value but gcsETag of the two, and
	// the MD5 field (with Projection.Content) is the column's value.
	md5 string
	// contentType names the content type column, read with
	// Projection.Content.
	contentType string
	// fixedETag and fixedStorageClass, when set, are the placeholder
	// every row answers with, for a schema with no column the field maps
	// from.
	fixedETag, fixedStorageClass string
}

// s3Objects is the projection of the S3-shaped schemas: the Java-era
// canonical schema, the S3 current-object schema, and the replay-owned
// schema sort-fixture writes (the same columns under its own id).
var s3Objects = objectProjection{
	size: "size", lastModified: "last_modified", etag: "etag", storageClass: "storage_class",
	ownerID: "owner_id", ownerDisplayName: "owner_display_name",
	checksumAlgorithm: "checksum_algorithm", checksumType: "checksum_type",
}

// gcsObjects is the S3 view of the GCS recording schema, the answer Cloud
// Storage's own S3-compatible XML API gives for the same object (probed
// 2026-11-07, server TestLiveGCSXMLAPIContents): the ETag from the MD5 or
// the etag (gcsETag), and no StorageClass, Owner or checksum element,
// which that API's listing never carries, whatever the object's class.
var gcsObjects = objectProjection{size: "size", lastModified: "last_modified", etag: "etag", md5: "md5", contentType: "content_type"}

// objectProjections maps declared schema ids to the Object projection.
// A schema id not listed projects keys only.
var objectProjections = map[string]objectProjection{
	"swath/canonical":         s3Objects,
	"swath/s3-objects/v1":     s3Objects,
	"swath-replay/objects/v1": s3Objects,
	"swath/gcs-objects/v1":    gcsObjects,
	"swath-local/v1":          localObjects,
}

// localObjects is the S3 view of a local tree's recording, which holds
// nothing S3's ETag or StorageClass maps from. S3 lists every object with
// both, so each takes a placeholder: syntheticETag, and STANDARD, the
// class S3 stores an object in unless the upload names another. Owner
// stays absent under fetch-owner=true: an S3 owner id names an account,
// which nothing in a local tree does (its uid names a user of one host),
// so a placeholder would invent one; live S3 drops an owner field it does
// not return (DisplayName, probed 2026-11-07) rather than filling it, and
// the route answers fetch-owner=true without an Owner for every recording
// that holds none (server TestLocalRecordingOnTheS3Route).
var localObjects = objectProjection{size: "size", lastModified: "last_modified", fixedETag: syntheticETag, fixedStorageClass: "STANDARD"}

// syntheticETag is the S3 ETag of a row with no source value for one: a
// GCS recording's row that holds neither an MD5 nor an etag (Cloud
// Storage lists every object with an etag, so only a recording made
// otherwise lacks both), and every row of a local recording. Neither 32
// hexadecimal digits (an MD5, S3's or the XML API's) with an optional
// "-N" part count (an S3 multipart upload), nor "-" and base64 (the XML
// API's composite object), it cannot be taken for a real ETag.
const syntheticETag = "swath-s3-synthetic-v1"

// gcsETag appends the S3 ETag of a GCS object, as the XML API forms it:
// the MD5 in lower-case hexadecimal when the object has one; otherwise
// (a composite object) "-" and the JSON API's etag; otherwise
// syntheticETag. An md5 value that is not the standard base64 of 16
// bytes is not an MD5, and the etag answers. Empty (a null) is no value.
func gcsETag(dst []byte, md5, etag string) []byte {
	if sum, ok := decodeMD5(md5); ok {
		return hex.AppendEncode(dst, sum[:])
	}
	if etag != "" {
		return append(append(dst, '-'), etag...)
	}
	return append(dst, syntheticETag...)
}

// decodeMD5 decodes the standard base64 of 16 bytes, the form GCS lists
// an MD5 in and Azure's Content-MD5 takes; anything else is no MD5.
func decodeMD5(s string) (sum [16]byte, ok bool) {
	if len(s) != base64.StdEncoding.EncodedLen(16) {
		return sum, false
	}
	var buf [18]byte // DecodedLen(24)
	n, err := base64.StdEncoding.Strict().Decode(buf[:], []byte(s))
	copy(sum[:], buf[:])
	return sum, err == nil && n == 16
}

// IsMD5 reports whether s is an MD5 in the standard base64 of 16 bytes.
func IsMD5(s string) bool {
	_, ok := decodeMD5(s)
	return ok
}

func objectProjectionFor(schemaID string) objectProjection {
	return objectProjections[schemaID]
}

// projection is proj as this schema reads it: Content dropped when the
// schema has no column it reads, so such reads key the window cache as
// reads without it do.
func (p objectProjection) projection(proj Projection) Projection {
	if p.contentType == "" && p.md5 == "" {
		proj.Content = false
	}
	return proj
}

// columns returns the columns proj needs decoded: owner columns only with
// Owner, the content type only with Content.
func (p objectProjection) columns(proj Projection) []string {
	var names []string
	for _, name := range []string{p.size, p.lastModified, p.etag, p.storageClass, p.checksumAlgorithm, p.checksumType, p.md5} {
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
	if proj.Content && p.contentType != "" {
		names = append(names, p.contentType)
	}
	return names
}

func (p objectProjection) object(row *sortedparquet.Row, proj Projection) Object {
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
	if p.md5 != "" {
		var md5, stored string
		if v := text(row, p.md5); v != nil {
			md5 = *v
		}
		if o.ETag != nil {
			stored = *o.ETag
		}
		etag := string(gcsETag(nil, md5, stored))
		o.ETag = &etag
	}
	if p.fixedETag != "" {
		etag := p.fixedETag
		o.ETag = &etag
	}
	o.StorageClass = text(row, p.storageClass)
	if p.fixedStorageClass != "" {
		class := p.fixedStorageClass
		o.StorageClass = &class
	}
	o.OwnerID = text(row, p.ownerID)
	o.OwnerDisplayName = text(row, p.ownerDisplayName)
	o.ChecksumAlgorithm = text(row, p.checksumAlgorithm)
	o.ChecksumType = text(row, p.checksumType)
	if proj.Content {
		o.ContentType = text(row, p.contentType)
		o.MD5 = text(row, p.md5)
	}
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

// saturatingIncrement returns n+1, or n at math.MaxInt: the rollup reads
// one entry past the limit to tell the caller the listing is truncated.
func saturatingIncrement(n int) int {
	if n < math.MaxInt {
		return n + 1
	}
	return n
}
