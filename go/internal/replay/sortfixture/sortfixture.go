// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package sortfixture turns an unsorted Java-era capture into a sorted
// SeekParquet dataset the replay server can serve sorted.
//
// The command line is
//
//	swath-replay sort-fixture --capture PATH --output DIR [--key-limit N]
//
// --capture is one capture part or a directory of *.parquet parts (not
// recursed) in the 13-column canonical schema; --output is a new or empty
// directory the dataset is published into; --key-limit, at most 4,096,
// raises the key cap from 1,024 bytes (see Options.KeyLimit).
//
// The dataset's schema id is always SchemaID: the nine columns of the S3
// current-object schema under an id of its own, since a converted capture
// is not a listing by the Go lister.
//
// The capture is read whole into memory, its objects into one array sized
// from their count (row_type statistics, or a pass over the row_type
// column), which is sorted in place and written from in batches. On a
// 64-bit platform that costs, besides the writer's buffers:
//
//   - per OBJECT row, 136 bytes plus its key and each non-null string value
//     (each its own allocation, rounded up to the allocator's size class);
//   - per other row (COMMON_PREFIX, DELETE_MARKER), a 16-byte string header
//     plus its key, kept only for the duplicate rule.
//
// Measured with go/bench/replay/make-fixtures.sh's synthetic 1M-row capture
// (44-byte keys, a 32-byte etag and a storage class per row; linux/amd64,
// GOMAXPROCS 4, 4 vCPU): 309 MiB peak resident, about 300 bytes per row,
// 3 s; TestSortFixtureMemory repeats it and an all-COMMON_PREFIX capture of
// as many rows. A few million rows is the intended scope. The Go lister writes sorted datasets directly; this
// command exists for captures recorded before it.
package sortfixture

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"
	"unsafe"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"

	"github.com/varveio/swath/go/internal/dataset"
	"github.com/varveio/swath/go/internal/faults"
	"github.com/varveio/swath/go/internal/journal"
	"github.com/varveio/swath/go/internal/parquetout"
	"github.com/varveio/swath/go/internal/replay/legacy/javaera"
	"github.com/varveio/swath/go/internal/schema"
)

// SchemaID is the schema id of every dataset Run writes: key, size,
// last_modified, etag, storage_class, owner_id, owner_display_name,
// checksum_algorithm and checksum_type, as the S3 current-object schema,
// with a key cap of DefaultKeyLimit or Options.KeyLimit. Azure blob names
// reach 1,024 characters, up to about 2,000 UTF-8 bytes, which a 1,024-byte
// cap cannot hold.
const SchemaID = "swath-replay/objects/v1"

// DefaultKeyLimit is the key cap without Options.KeyLimit: the S3 limit.
const DefaultKeyLimit = 1024

// MaxKeyLimit is the largest Options.KeyLimit.
const MaxKeyLimit = 4096

// partSize is the encoded target per part.
const partSize = 128 << 20

// Options configures Run.
type Options struct {
	// Capture is one capture part or a directory of *.parquet parts.
	Capture string
	// Output is the dataset directory; it must not exist or be empty.
	Output string
	// KeyLimit, when positive (at most MaxKeyLimit), is the key cap in
	// bytes; zero is DefaultKeyLimit.
	KeyLimit int
}

// Stats summarises a run.
type Stats struct {
	CaptureRows int64 // rows read
	Objects     int64 // OBJECT rows written
	Skipped     int64 // rows of other types left out
	Parts       int
	Bytes       int64
	SchemaID    string
	Duration    time.Duration
}

// ErrDuplicateKey reports two capture rows with the same key.
var ErrDuplicateKey = errors.New("sort-fixture: duplicate key")

// ErrVersioned reports a capture row with a version id: only current
// listings can be served sorted.
var ErrVersioned = errors.New("sort-fixture: versioned capture")

// ErrKeyTooLong reports an object key over the allowed length.
var ErrKeyTooLong = errors.New("sort-fixture: key too long")

// ErrRowType reports a capture row whose row_type is null or not one of
// rowTypes: the canonical schema's values, spelled as the Java tool writes
// them and parses them back, strictly.
var ErrRowType = errors.New("sort-fixture: invalid row_type")

// ErrNoParts reports a capture directory without *.parquet parts. A part
// that holds no rows is a capture of an empty listing; no part at all is a
// wrong path.
var ErrNoParts = errors.New("sort-fixture: no *.parquet parts")

// ErrMalformedCapture reports a capture part whose footer does not parse,
// or whose row counts are negative, disagree with its row groups, or
// exceed what can be held.
var ErrMalformedCapture = errors.New("sort-fixture: malformed capture footer")

// rowTypes are the canonical row_type values; only objectRowType rows are
// written.
var rowTypes = [...]string{objectRowType, "COMMON_PREFIX", "DELETE_MARKER"}

const objectRowType = "OBJECT"

// captured is a capture held for sorting: its objects, which are written,
// and the keys of its other rows, which only the duplicate rule reads.
type captured struct {
	objects []schema.Row[Object]
	others  []string
}

// Run reads the capture, keeps its OBJECT rows, refuses a capture with no
// parts (ErrNoParts), a row whose row_type is not OBJECT, COMMON_PREFIX or
// DELETE_MARKER (ErrRowType), a versioned row (ErrVersioned) or two rows of
// any type with one key (ErrDuplicateKey), sorts the objects by unsigned key bytes and writes
// them as a SeekParquet dataset of SchemaID, nulls intact. It writes one
// summary line to log.
func Run(ctx context.Context, opts Options, log io.Writer) (Stats, error) {
	start := time.Now()
	stats := Stats{}
	if opts.KeyLimit < 0 || opts.KeyLimit > MaxKeyLimit {
		return stats, fmt.Errorf("sort-fixture: key limit %d outside 0..%d", opts.KeyLimit, MaxKeyLimit)
	}
	if opts.Capture == "" || opts.Output == "" {
		return stats, errors.New("sort-fixture: a capture and an output are required")
	}
	// Ownership first, as the lister takes it: an output that is not new
	// or empty is refused before anything is created there, then the
	// output lock is held from before the capture is read until Run
	// returns; Prepare repeats the check under it.
	if _, err := dataset.Usable(opts.Output, "", SchemaID, false); err != nil {
		return stats, fmt.Errorf("sort-fixture: output: %w", err)
	}
	lock, err := journal.TakeLock(opts.Output)
	if err != nil {
		return stats, fmt.Errorf("sort-fixture: %w", err)
	}
	defer lock.Release()
	if err := lock.Verify(); err != nil {
		return stats, fmt.Errorf("sort-fixture: %w", err)
	}
	files, err := javaera.Resolve(opts.Capture)
	if err != nil {
		return stats, fmt.Errorf("sort-fixture: capture: %w", err)
	}
	if len(files) == 0 {
		return stats, fmt.Errorf("%w in %s", ErrNoParts, opts.Capture)
	}
	// One array of the objects, sized from their count: they are sorted in
	// place and written from it in batches, never copied whole.
	objectRows, otherRows, err := countRows(ctx, files)
	if err != nil {
		return stats, fmt.Errorf("sort-fixture: %w", err)
	}
	c := captured{
		objects: make([]schema.Row[Object], 0, min(objectRows, maxPrealloc)),
		others:  make([]string, 0, min(otherRows, maxPrealloc)),
	}
	for _, file := range files {
		if err := readCapture(ctx, file, &c); err != nil {
			return stats, fmt.Errorf("sort-fixture: %s: %w", file, err)
		}
	}
	if err := faults.At("captured"); err != nil { // tests: the output replaced during the read
		return stats, err
	}
	objects := c.objects
	stats.CaptureRows = int64(len(objects) + len(c.others))
	stats.Objects, stats.Skipped = int64(len(objects)), int64(len(c.others))
	slices.SortFunc(objects, func(a, b schema.Row[Object]) int { return strings.Compare(a.Key, b.Key) })
	slices.Sort(c.others)
	if key, ok := duplicate(objects, c.others); ok {
		return stats, fmt.Errorf("%w '%s'", ErrDuplicateKey, key)
	}
	c.others = nil
	longest := 0
	for i := range objects {
		longest = max(longest, len(objects[i].Key))
	}

	limit := cmp.Or(opts.KeyLimit, DefaultKeyLimit)
	if longest > limit {
		return stats, fmt.Errorf("%w: an object key is %d bytes (limit %d; --key-limit raises it up to %d)", ErrKeyTooLong, longest, limit, MaxKeyLimit)
	}
	sch := newSchema(limit)
	stats.SchemaID = sch.ID
	parts, err := write(ctx, opts, lock, sch, objects, start)
	if err != nil {
		return stats, err
	}
	stats.Parts = len(parts)
	for _, p := range parts {
		stats.Bytes += p.Bytes
	}
	stats.Duration = time.Since(start)
	if log != nil {
		abs, _ := filepath.Abs(opts.Output)
		fmt.Fprintf(log, "sort_fixture rows=%d objects=%d skipped=%d parts=%d bytes=%d schema=%s wall_ms=%d output=%s\n",
			stats.CaptureRows, stats.Objects, stats.Skipped, stats.Parts, stats.Bytes, stats.SchemaID, stats.Duration.Milliseconds(), abs)
	}
	return stats, nil
}

// write publishes objects, sorted, through the dataset writer, under lock.
// It owns the output as the lister does: the router checks the output is
// still the directory locked before it creates each part, and creates it
// through that directory (os.Root), not its pathname; each removal, and
// the manifest's installation, follow the same check; the lock file stays
// in the workspace, as the lister leaves it. An output another process
// owns was refused (journal.ErrLocked) by Run without a change.
func write(ctx context.Context, opts Options, lock *journal.Lock, sch *schema.Schema[Object], objects []schema.Row[Object], start time.Time) ([]dataset.Part, error) {
	// Prepare creates data/ and the parts directory: only in the directory
	// still locked, which reading and sorting the capture may have outlasted.
	if err := lock.Verify(); err != nil {
		return nil, fmt.Errorf("sort-fixture: %w", err)
	}
	if err := dataset.Prepare(opts.Output, "", sch.ID, false); err != nil {
		return nil, fmt.Errorf("sort-fixture: output: %w", err)
	}
	// Unpublished parts go when the output is still the one locked.
	cleanup := func() {
		if lock.Verify() == nil {
			dataset.Cleanup(opts.Output)
		}
	}
	root, err := os.OpenRoot(opts.Output)
	if err != nil {
		return nil, fmt.Errorf("sort-fixture: output: %w", err)
	}
	defer func() { _ = root.Close() }()
	if err := lock.Verify(); err != nil { // the root is the directory locked
		return nil, fmt.Errorf("sort-fixture: %w", err)
	}
	router, err := parquetout.NewRouter(parquetout.Options{
		Out: opts.Output, PartSize: partSize, Encoders: 1,
		Verify: lock.Verify,
		Open: func(path string) (*os.File, error) {
			rel, err := filepath.Rel(opts.Output, path)
			if err != nil {
				return nil, err
			}
			return root.OpenFile(rel, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
		},
	}, sch)
	if err != nil {
		cleanup()
		return nil, fmt.Errorf("sort-fixture: %w", err)
	}
	const batch = 4096
	for i := 0; i < len(objects); i += batch {
		if err := router.Write(ctx, [][]schema.Row[Object]{objects[i:min(i+batch, len(objects))]}); err != nil {
			router.Abort()
			cleanup()
			return nil, fmt.Errorf("sort-fixture: write: %w", err)
		}
	}
	parts, err := router.Close()
	if err != nil {
		cleanup()
		return nil, fmt.Errorf("sort-fixture: write: %w", err)
	}
	if err := lock.Verify(); err != nil {
		return nil, fmt.Errorf("sort-fixture: %w", err)
	}
	capture, _ := filepath.Abs(opts.Capture)
	meta := dataset.Meta{
		SchemaID: sch.ID,
		Source:   dataset.Source{Kind: "capture", URI: filepath.ToSlash(capture)},
		Scope:    dataset.Scope{Kind: "whole"},
		Coverage: dataset.Coverage{Complete: true, StopReason: "completed", ListingStartedAt: start.UTC(), ListingFinishedAt: time.Now().UTC()},
	}
	// The manifest is the commit: it is installed only while the output is
	// still the directory locked.
	commit := func(install func() error) error {
		if err := lock.Verify(); err != nil {
			return err
		}
		return install()
	}
	if err := dataset.Publish(opts.Output, meta, parts, commit); err != nil {
		cleanup()
		return nil, fmt.Errorf("sort-fixture: publish: %w", err)
	}
	cleanup()
	return parts, nil
}

// countRows returns the OBJECT rows and the other rows of the parts,
// refusing a footer that does not parse or whose counts are negative,
// disagree with its row groups or add up to more rows than one array of
// them can hold (ErrMalformedCapture). A row group's row_type statistics
// decide it when they prove it all OBJECT or all another type; otherwise
// its row_type column is read and counted. The counts size the arrays the
// capture is read into; readCapture validates every row.
func countRows(ctx context.Context, files []string) (objects, others int64, err error) {
	var total int64
	for _, name := range files {
		o, n, err := countPart(ctx, name)
		if err != nil {
			return 0, 0, fmt.Errorf("%s: %w", name, err)
		}
		if total > maxRows-n {
			return 0, 0, fmt.Errorf("%w: the parts declare more than %d rows", ErrMalformedCapture, maxRows)
		}
		total += n
		objects += o
	}
	// Each group's objects are at most its rows, so this holds; it is
	// checked here because the counts size allocations.
	if objects < 0 || objects > total {
		return 0, 0, fmt.Errorf("%w: %d OBJECT rows counted among %d", ErrMalformedCapture, objects, total)
	}
	return objects, total - objects, nil
}

// countPart returns one part's OBJECT rows and all its rows.
func countPart(ctx context.Context, name string) (objects, rows int64, err error) {
	f, err := os.Open(name)
	if err != nil {
		return 0, 0, err
	}
	defer func() { _ = f.Close() }()
	info, err := f.Stat()
	if err != nil {
		return 0, 0, err
	}
	file, err := parquet.OpenFile(f, info.Size(), parquet.SkipPageIndex(true), parquet.SkipBloomFilters(true))
	if err != nil {
		return 0, 0, fmt.Errorf("%w: %v", ErrMalformedCapture, err)
	}
	if rows, err = footerRows(file.Metadata()); err != nil {
		return 0, 0, err
	}
	leaf, ok := file.Schema().Lookup(captureColumns[cRowType])
	if !ok {
		return 0, rows, nil // readCapture refuses the part
	}
	if leaf.MaxRepetitionLevel > 0 {
		// Values would not be rows: refuse before counting them.
		return 0, 0, fmt.Errorf("%w: the row_type column is repeated", ErrMalformedCapture)
	}
	for i, rg := range file.Metadata().RowGroups {
		switch rowTypes := rg.Columns[leaf.ColumnIndex].MetaData.Statistics; {
		case rg.NumRows == 0:
		case provenAll(rowTypes, true):
			objects += rg.NumRows
		case provenAll(rowTypes, false):
		default:
			n, err := countObjects(ctx, file.RowGroups()[i].ColumnChunks()[leaf.ColumnIndex])
			if err != nil {
				return 0, 0, fmt.Errorf("row group %d: count row types: %w", i, err)
			}
			if n > rg.NumRows {
				return 0, 0, fmt.Errorf("%w: row group %d declares %d rows and holds %d OBJECT row types", ErrMalformedCapture, i, rg.NumRows, n)
			}
			objects += n
		}
	}
	return objects, rows, nil
}

// provenAll reports whether a row group's row_type statistics prove every
// value OBJECT (object) or every value another type: a minimum and maximum
// both OBJECT, or a range that excludes it.
func provenAll(st format.Statistics, object bool) bool {
	lo, hi := st.MinValue, st.MaxValue
	if lo == nil && hi == nil {
		lo, hi = st.Min, st.Max
	}
	if lo == nil || hi == nil {
		return false
	}
	if object {
		return string(lo) == objectRowType && string(hi) == objectRowType
	}
	return string(hi) < objectRowType || string(lo) > objectRowType
}

// countObjects reads a row_type column chunk and counts its OBJECT values.
func countObjects(ctx context.Context, chunk parquet.ColumnChunk) (int64, error) {
	pages := chunk.Pages()
	defer func() { _ = pages.Close() }()
	values := make([]parquet.Value, 1024)
	var n int64
	for {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		page, err := pages.ReadPage()
		if errors.Is(err, io.EOF) {
			return n, nil
		}
		if err != nil {
			return 0, err
		}
		reader := page.Values()
		for {
			k, err := reader.ReadValues(values)
			for _, v := range values[:k] {
				if !v.IsNull() && string(v.ByteArray()) == objectRowType {
					n++
				}
			}
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				parquet.Release(page)
				return 0, err
			}
			if k == 0 {
				break
			}
		}
		parquet.Release(page)
	}
}

// maxRows bounds the rows of one capture: an array of them must be
// representable.
const maxRows = math.MaxInt / int64(unsafe.Sizeof(schema.Row[Object]{}))

// maxPrealloc bounds the array allocated from the footers' counts before
// a row is read: 2^24 rows (2.2 GiB) on 64-bit platforms, a quarter of the
// address space on 32-bit ones. A capture within it, the intended scope,
// is held in exactly one array; a larger one grows it as rows are read, so
// a forged count cannot reserve more than this.
const maxPrealloc = min(1<<24, maxRows/4)

// footerRows returns a footer's row count once it agrees with its row
// groups, each non-negative, summed without overflow.
func footerRows(meta *format.FileMetaData) (int64, error) {
	if meta.NumRows < 0 {
		return 0, fmt.Errorf("%w: the footer declares %d rows", ErrMalformedCapture, meta.NumRows)
	}
	var sum int64
	for i, rg := range meta.RowGroups {
		if rg.NumRows < 0 || sum > math.MaxInt64-rg.NumRows {
			return 0, fmt.Errorf("%w: row group %d declares %d rows", ErrMalformedCapture, i, rg.NumRows)
		}
		sum += rg.NumRows
	}
	if sum != meta.NumRows {
		return 0, fmt.Errorf("%w: the footer declares %d rows, its row groups %d", ErrMalformedCapture, meta.NumRows, sum)
	}
	return sum, nil
}

// duplicate returns a key two rows share: adjacent sorted objects, adjacent
// sorted other rows, or an object and another row.
func duplicate(objects []schema.Row[Object], others []string) (string, bool) {
	for i := 1; i < len(objects); i++ {
		if objects[i].Key == objects[i-1].Key {
			return objects[i].Key, true
		}
	}
	for i := 1; i < len(others); i++ {
		if others[i] == others[i-1] {
			return others[i], true
		}
	}
	for i, j := 0, 0; i < len(objects) && j < len(others); {
		switch order := strings.Compare(objects[i].Key, others[j]); {
		case order == 0:
			return others[j], true
		case order < 0:
			i++
		default:
			j++
		}
	}
	return "", false
}

// captureColumns are the canonical columns a capture row is read from;
// only key and row_type are required.
var captureColumns = []string{"key", "size", "last_modified", "etag", "storage_class", "version_id",
	"owner_id", "owner_display_name", "checksum_algorithm", "checksum_type", "row_type"}

const (
	cKey = iota
	cSize
	cModified
	cETag
	cStorageClass
	cVersion
	cOwnerID
	cOwnerDisplayName
	cChecksumAlgorithm
	cChecksumType
	cRowType
)

// readCapture adds the rows of one capture part to c.
func readCapture(ctx context.Context, name string, c *captured) error {
	f, err := os.Open(name)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	info, err := f.Stat()
	if err != nil {
		return err
	}
	file, err := parquet.OpenFile(f, info.Size())
	if err != nil {
		return err
	}
	index := make([]int, len(captureColumns))
	for i, column := range captureColumns {
		index[i] = -1
		if leaf, ok := file.Schema().Lookup(column); ok {
			index[i] = leaf.ColumnIndex
		}
	}
	if index[cKey] < 0 || index[cRowType] < 0 {
		return errors.New("not a capture part: key and row_type columns are required")
	}
	byColumn := make(map[int]int, len(index))
	for i, column := range index {
		if column >= 0 {
			byColumn[column] = i
		}
	}
	reader := parquet.NewReader(file)
	defer func() { _ = reader.Close() }()
	buffer := make([]parquet.Row, 256)
	values := make([]parquet.Value, len(captureColumns))
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		n, readErr := reader.ReadRows(buffer)
		for _, r := range buffer[:n] {
			for i := range values {
				values[i] = parquet.NullValue()
			}
			for _, v := range r {
				if i, ok := byColumn[v.Column()]; ok {
					values[i] = v
				}
			}
			key := values[cKey].ByteArray()
			if !values[cVersion].IsNull() {
				return fmt.Errorf("%w: key '%s' carries version_id '%s'", ErrVersioned, key, values[cVersion].ByteArray())
			}
			rowType := values[cRowType]
			if rowType.IsNull() || !slices.Contains(rowTypes[:], string(rowType.ByteArray())) {
				return fmt.Errorf("%w: key '%s' has row_type %s", ErrRowType, key, describeValue(rowType))
			}
			if string(rowType.ByteArray()) != objectRowType {
				c.others = append(c.others, string(key))
				continue
			}
			c.objects = append(c.objects, schema.Row[Object]{Key: string(key), Data: Object{
				Size: values[cSize].Int64(), HasSize: !values[cSize].IsNull(),
				Mtime: values[cModified].Int64(), HasMtime: !values[cModified].IsNull(),
				Present:           present(values),
				ETag:              text(values[cETag]),
				StorageClass:      text(values[cStorageClass]),
				OwnerID:           text(values[cOwnerID]),
				OwnerDisplayName:  text(values[cOwnerDisplayName]),
				ChecksumAlgorithm: text(values[cChecksumAlgorithm]),
				ChecksumType:      text(values[cChecksumType]),
			}})
		}
		if errors.Is(readErr, io.EOF) || n == 0 && readErr == nil {
			return nil
		}
		if readErr != nil {
			return readErr
		}
	}
}

// describeValue spells a string value for an error: quoted, or null.
func describeValue(v parquet.Value) string {
	if v.IsNull() {
		return "null"
	}
	return fmt.Sprintf("%q", v.ByteArray())
}

// text returns a string column's value, "" when null (Present tells the
// two apart).
// present returns the string columns of a capture row that are not null,
// an empty string included.
func present(values []parquet.Value) Strings {
	var set Strings
	for _, c := range stringColumns {
		if !values[c.column].IsNull() {
			set |= c.bit
		}
	}
	return set
}

// stringColumns pairs the capture's string columns with their bits.
var stringColumns = [...]struct {
	column int
	bit    Strings
}{{cETag, HasETag}, {cStorageClass, HasStorageClass}, {cOwnerID, HasOwnerID},
	{cOwnerDisplayName, HasOwnerDisplayName}, {cChecksumAlgorithm, HasChecksumAlgorithm}, {cChecksumType, HasChecksumType}}

func text(v parquet.Value) string {
	if v.IsNull() {
		return ""
	}
	return string(v.ByteArray())
}
