// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/encoding/thrift"
	"github.com/parquet-go/parquet-go/format"
	seekparquet "github.com/varveio/seekparquet/go"

	"github.com/varveio/swath/go/internal/replay/legacy/javaera"
)

// SchemaID is the schema id every converted dataset declares: the parts
// are S3 current-object listings swath captured from S3, holding the
// swath/s3-objects/v1 columns (as format 1 wrote them, which upgrade.md
// maps onto that schema unchanged) plus the four format-1 columns
// version_id, is_latest, is_delete_marker and row_type, which the
// profile's readers ignore (dataset.md, "Keys and order").
const SchemaID = "swath/s3-objects/v1"

// MaxKeyBytes is swath/s3-objects/v1's longest key.
const MaxKeyBytes = 1024

// ExtensionNamespace holds the conversion's own record in the manifest's
// extensions: what each part was before its footer was replaced.
const ExtensionNamespace = "swath-replay"

// Refusals. Each names a rule a part breaks; nothing is published.
var (
	ErrNotJavaEra    = errors.New("adopt-fixture: not a sorted Java-era fixture")
	ErrSchema        = errors.New("adopt-fixture: not the 13-column format-1 schema")
	ErrNotObjects    = errors.New("adopt-fixture: a row is not a current object")
	ErrKey           = errors.New("adopt-fixture: a key breaks swath/s3-objects/v1")
	ErrOrder         = errors.New("adopt-fixture: keys are not strictly ascending")
	ErrStatistics    = errors.New("adopt-fixture: key statistics are not the row group's exact first and last keys")
	ErrFooter        = errors.New("adopt-fixture: unsupported footer")
	ErrOutputNotNew  = errors.New("adopt-fixture: the output exists and is not empty")
	errFooterChanged = errors.New("adopt-fixture: the rewritten footer changes more than it adds")
)

// Options configures Run.
type Options struct {
	Input  string // a directory of sorted Java-era parts, or one part
	Output string // a new or empty directory
	Bucket string // the S3 bucket the fixture lists
	Region string // the bucket's region, informative; may be empty
	Prefix string // the listed prefix; empty for the whole bucket
	// Fixture is a free-form identity of the input (the study's fixture
	// id), recorded with the input digests; may be empty.
	Fixture string
	Now     func() time.Time // the publication time; nil is time.Now
}

// Part is one converted part.
type Part struct {
	Input       string `json:"input"`
	InputBytes  int64  `json:"input_bytes"`
	InputSHA256 string `json:"input_sha256"`
	// DataBytes is the length of the prefix both files share, byte for
	// byte: everything before the input's footer.
	DataBytes int64  `json:"data_bytes"`
	Path      string `json:"-"`
	Rows      int64  `json:"-"`
	Bytes     int64  `json:"-"`
	SHA256    string `json:"-"`
	MinKey    []byte `json:"-"`
	MaxKey    []byte `json:"-"`
}

// Run converts the fixture at opts.Input into a SeekParquet dataset at
// opts.Output, part by part in file_index order, then publishes the
// manifest. Memory is bounded by one part's footer and one page per
// column read, whatever the number of rows.
func Run(ctx context.Context, opts Options, log io.Writer) ([]Part, error) {
	if opts.Input == "" || opts.Output == "" || opts.Bucket == "" {
		return nil, errors.New("adopt-fixture: an input, an output and a bucket are required")
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	files, err := javaera.Resolve(opts.Input)
	if err != nil {
		return nil, fmt.Errorf("adopt-fixture: input: %w", err)
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("%w: %s holds no *.parquet parts", ErrNotJavaEra, opts.Input)
	}
	files, err = orderByStamp(files)
	if err != nil {
		return nil, err
	}
	if err := prepareOutput(opts.Output); err != nil {
		return nil, err
	}
	var parts []Part
	var last []byte
	for i, name := range files {
		p, err := convertPart(ctx, name, opts.Output, i, last)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		last = p.MaxKey
		parts = append(parts, p)
		if log != nil {
			fmt.Fprintf(log, "adopt_fixture part=%s rows=%d bytes=%d data_bytes=%d input_sha256=%s sha256=%s\n",
				p.Path, p.Rows, p.Bytes, p.DataBytes, p.InputSHA256, p.SHA256)
		}
	}
	if err := publish(opts, parts, now().UTC()); err != nil {
		return nil, err
	}
	return parts, nil
}

// orderByStamp reads every part's sortedness stamp, refuses a set that is
// not a complete objects-mode output, and returns the parts in file_index
// order.
func orderByStamp(files []string) ([]string, error) {
	stamps := make([]javaera.Stamp, len(files))
	for i, name := range files {
		err := withFile(name, func(f *os.File, file *parquet.File) error {
			stamp, ok := javaera.ReadStamp(file)
			switch {
			case !ok:
				return fmt.Errorf("%w: %s carries no recognised sortedness stamp", ErrNotJavaEra, name)
			case stamp.Mode != javaera.ModeObjects:
				return fmt.Errorf("%w: %s is stamped mode=%s, not objects", ErrNotJavaEra, name, stamp.Mode)
			case stamp.FormatVersion != javaera.FormatVersion:
				return fmt.Errorf("%w: %s is stamped format_version=%d", ErrNotJavaEra, name, stamp.FormatVersion)
			}
			stamps[i] = stamp
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	if v := javaera.CompletenessViolation(stamps); v != "" {
		return nil, fmt.Errorf("%w: %s", ErrNotJavaEra, v)
	}
	ordered := make([]string, len(files))
	for i, s := range stamps {
		ordered[s.FileIndex-1] = files[i]
	}
	return ordered, nil
}

// withFile opens name as Parquet, footer only (no page index, no bloom
// filters), for fn.
func withFile(name string, fn func(*os.File, *parquet.File) error) error {
	f, err := os.Open(name)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	info, err := f.Stat()
	if err != nil {
		return err
	}
	file, err := parquet.OpenFile(f, info.Size(), parquet.SkipPageIndex(true), parquet.SkipBloomFilters(true))
	if err != nil {
		return fmt.Errorf("%w: %v", ErrNotJavaEra, err)
	}
	return fn(f, file)
}

// prepareOutput creates the output and its data directory, refusing an
// output that exists and is not an empty directory.
func prepareOutput(out string) error {
	entries, err := os.ReadDir(out)
	switch {
	case errors.Is(err, os.ErrNotExist):
	case err != nil:
		return fmt.Errorf("adopt-fixture: output: %w", err)
	case len(entries) > 0:
		return fmt.Errorf("%w: %s", ErrOutputNotNew, out)
	}
	return os.MkdirAll(filepath.Join(out, "data"), 0o755)
}

// column is one format-1 column as the Java tool writes it.
type column struct {
	name     string
	typ      format.Type
	rep      format.FieldRepetitionType
	logical  string // "", "string" or "timestamp"
	optional bool   // the annotation may be absent (key) or an INT(64,true) (size)
}

// javaColumns are the format-1 canonical columns, in order.
var javaColumns = []column{
	{name: "key", typ: format.ByteArray, rep: format.Required, logical: "string", optional: true},
	{name: "size", typ: format.Int64, rep: format.Optional, logical: "int64", optional: true},
	{name: "last_modified", typ: format.Int64, rep: format.Optional, logical: "timestamp"},
	{name: "etag", typ: format.ByteArray, rep: format.Optional, logical: "string"},
	{name: "storage_class", typ: format.ByteArray, rep: format.Optional, logical: "string"},
	{name: "version_id", typ: format.ByteArray, rep: format.Optional, logical: "string"},
	{name: "is_latest", typ: format.Boolean, rep: format.Optional},
	{name: "is_delete_marker", typ: format.Boolean, rep: format.Required},
	{name: "owner_id", typ: format.ByteArray, rep: format.Optional, logical: "string"},
	{name: "owner_display_name", typ: format.ByteArray, rep: format.Optional, logical: "string"},
	{name: "checksum_algorithm", typ: format.ByteArray, rep: format.Optional, logical: "string"},
	{name: "checksum_type", typ: format.ByteArray, rep: format.Optional, logical: "string"},
	{name: "row_type", typ: format.ByteArray, rep: format.Required, logical: "string"},
}

// Leaf indexes of the columns the scan reads.
const (
	colKey            = 0
	colVersionID      = 5
	colIsDeleteMarker = 7
)

// checkSchema refuses a footer whose schema is not exactly the format-1
// columns, flat, in order, and reports whether the key column is
// unannotated (the Java writer's form) rather than already STRING.
func checkSchema(meta *format.FileMetaData) (keyAnnotated bool, err error) {
	s := meta.Schema
	if len(s) != 1+len(javaColumns) {
		return false, fmt.Errorf("%w: %d schema elements", ErrSchema, len(s))
	}
	if n, _ := s[0].NumChildren.Get(); int(n) != len(javaColumns) {
		return false, fmt.Errorf("%w: the root has %d children", ErrSchema, n)
	}
	for i, c := range javaColumns {
		e := s[1+i]
		typ, hasType := e.Type.Get()
		rep, _ := e.RepetitionType.Get()
		children, _ := e.NumChildren.Get()
		if e.Name != c.name || !hasType || typ != c.typ || rep != c.rep || children != 0 {
			return false, fmt.Errorf("%w: column %d is %q %v %v", ErrSchema, i, e.Name, typ, rep)
		}
		got := logicalOf(&e)
		switch {
		case got == c.logical:
			if i == colKey {
				keyAnnotated = true
			}
		case got == "" && c.optional:
		default:
			return false, fmt.Errorf("%w: column %q is annotated %q, not %q", ErrSchema, e.Name, got, c.logical)
		}
	}
	return keyAnnotated, nil
}

// logicalOf names a schema element's annotation as checkSchema compares
// it: "string" (STRING, or UTF8 alone), "timestamp" (TIMESTAMP(MICROS,
// UTC)), "int64" (INT(64, signed)), "" for none, or a description of any
// other.
func logicalOf(e *format.SchemaElement) string {
	ct, hasCT := e.ConvertedType.Get()
	switch lt := e.LogicalType.Value.(type) {
	case nil:
		if !hasCT {
			return ""
		}
		if ct == 0 { // UTF8
			return "string"
		}
		return fmt.Sprintf("converted %d", ct)
	case *format.StringType:
		if hasCT && ct != 0 {
			return fmt.Sprintf("string with converted %d", ct)
		}
		return "string"
	case *format.TimestampType:
		if _, micros := lt.Unit.Value.(*format.MicroSeconds); micros && lt.IsAdjustedToUTC {
			return "timestamp"
		}
	case *format.IntType:
		if lt.BitWidth == 64 && lt.IsSigned {
			return "int64"
		}
	}
	return e.LogicalType.String()
}

// convertPart checks one part and writes it, footer replaced, as
// data/part-NNNNN.parquet under out. prev is the previous part's last key.
func convertPart(ctx context.Context, name, out string, index int, prev []byte) (Part, error) {
	p := Part{Input: filepath.Base(name), Path: fmt.Sprintf("data/part-%05d.parquet", index)}
	in, err := os.Open(name)
	if err != nil {
		return p, err
	}
	defer func() { _ = in.Close() }()
	info, err := in.Stat()
	if err != nil {
		return p, err
	}
	size := info.Size()
	p.InputBytes = size
	rawFooter, footerStart, err := readFooter(in, size)
	if err != nil {
		return p, err
	}
	p.DataBytes = footerStart
	file, err := parquet.OpenFile(in, size, parquet.SkipPageIndex(true), parquet.SkipBloomFilters(true))
	if err != nil {
		return p, fmt.Errorf("%w: %v", ErrNotJavaEra, err)
	}
	meta := file.Metadata()
	keyAnnotated, err := checkSchema(meta)
	if err != nil {
		return p, err
	}
	if err := checkFooter(meta); err != nil {
		return p, err
	}
	if !keyAnnotated {
		if err := checkBoundsUTF8(file); err != nil {
			return p, err
		}
	}
	scan, err := scanPart(ctx, file, prev)
	if err != nil {
		return p, err
	}
	p.Rows, p.MinKey, p.MaxKey = scan.rows, scan.first, scan.last
	newFooter, err := rewriteFooter(rawFooter, len(meta.RowGroups), len(javaColumns), !keyAnnotated, len(meta.ColumnOrders) == 0)
	if err != nil {
		return p, err
	}
	if err := checkFooterChange(rawFooter, newFooter, len(meta.KeyValueMetadata), len(meta.ColumnOrders) == 0); err != nil {
		return p, err
	}
	p.InputSHA256, p.SHA256, p.Bytes, err = writePart(in, footerStart, rawFooter, newFooter, filepath.Join(out, filepath.FromSlash(p.Path)))
	return p, err
}

// readFooter returns a part's raw footer and the offset where it starts,
// checking both magic numbers.
func readFooter(f io.ReaderAt, size int64) ([]byte, int64, error) {
	if size < 12 {
		return nil, 0, fmt.Errorf("%w: %d bytes", ErrNotJavaEra, size)
	}
	var head, tail [8]byte
	if _, err := f.ReadAt(head[:4], 0); err != nil {
		return nil, 0, err
	}
	if _, err := f.ReadAt(tail[:], size-8); err != nil {
		return nil, 0, err
	}
	if string(head[:4]) != "PAR1" || string(tail[4:]) != "PAR1" {
		return nil, 0, fmt.Errorf("%w: no PAR1 magic", ErrNotJavaEra)
	}
	n := int64(binary.LittleEndian.Uint32(tail[:4]))
	if n > size-12 {
		return nil, 0, fmt.Errorf("%w: footer length %d", ErrNotJavaEra, n)
	}
	raw := make([]byte, n)
	if _, err := f.ReadAt(raw, size-8-n); err != nil {
		return nil, 0, err
	}
	return raw, size - 8 - n, nil
}

// checkFooter refuses what the rewrite does not handle: encryption, column
// chunks in other files, footer stamps already present, sorting columns
// other than the key ascending, and column orders that are not one
// TYPE_ORDER per leaf. The row counts must add up.
func checkFooter(meta *format.FileMetaData) error {
	if meta.EncryptionAlgorithm.Value != nil || len(meta.FooterSigningKeyMetadata) > 0 {
		return fmt.Errorf("%w: encrypted", ErrFooter)
	}
	for _, kv := range meta.KeyValueMetadata {
		if len(kv.Key) >= len("seekparquet.") && kv.Key[:len("seekparquet.")] == "seekparquet." {
			return fmt.Errorf("%w: it already carries %s", ErrFooter, kv.Key)
		}
	}
	var rows int64
	for g, rg := range meta.RowGroups {
		if len(rg.SortingColumns) > 0 {
			return fmt.Errorf("%w: row group %d already declares sorting_columns", ErrFooter, g)
		}
		if len(rg.Columns) != len(javaColumns) {
			return fmt.Errorf("%w: row group %d has %d column chunks", ErrFooter, g, len(rg.Columns))
		}
		for _, c := range rg.Columns {
			if c.FilePath != "" {
				return fmt.Errorf("%w: a column chunk is in %q", ErrFooter, c.FilePath)
			}
		}
		if rg.NumRows < 0 {
			return fmt.Errorf("%w: row group %d declares %d rows", ErrFooter, g, rg.NumRows)
		}
		rows += rg.NumRows
	}
	if rows != meta.NumRows {
		return fmt.Errorf("%w: the row groups hold %d rows, the footer declares %d", ErrFooter, rows, meta.NumRows)
	}
	if n := len(meta.ColumnOrders); n != 0 {
		if n != len(javaColumns) {
			return fmt.Errorf("%w: %d column orders for %d columns", ErrFooter, n, len(javaColumns))
		}
		for i, o := range meta.ColumnOrders {
			if _, ok := o.Value.(*format.TypeDefinedOrder); !ok {
				return fmt.Errorf("%w: column order %d is not TYPE_ORDER", ErrFooter, i)
			}
		}
	}
	return nil
}

// checkBoundsUTF8 refuses a part whose key ColumnIndex bounds are not all
// valid UTF-8: annotating the key STRING requires it of every page bound
// (SeekParquet KEY-6), and the Java writer cut bounds of an unannotated
// column.
func checkBoundsUTF8(file *parquet.File) error {
	indexes, _, err := file.ReadPageIndex()
	if err != nil {
		return fmt.Errorf("%w: page index: %v", ErrFooter, err)
	}
	for g := range file.Metadata().RowGroups {
		i := g*len(javaColumns) + colKey
		if i >= len(indexes) {
			continue // no page index: the validator refuses it (RG-2)
		}
		for j, b := range append(slices.Clone(indexes[i].MinValues), indexes[i].MaxValues...) {
			if !utf8.Valid(b) {
				return fmt.Errorf("%w: row group %d key page bound %d is not UTF-8", ErrKey, g, j)
			}
		}
	}
	return nil
}

type scanResult struct {
	rows        int64
	first, last []byte
}

// scanPart reads the key, version_id and is_delete_marker columns page by
// page and refuses a part with a row that is not a current object
// (row_type statistics that do not prove OBJECT, as the Java-era replay
// profile requires; a non-null version_id; a true or null delete marker),
// a key that is empty, not UTF-8 or longer than MaxKeyBytes, keys that are
// not strictly ascending from prev on, or key statistics that are not each
// row group's exact first and last key, which the replay store routes by.
func scanPart(ctx context.Context, file *parquet.File, prev []byte) (scanResult, error) {
	var r scanResult
	last := slices.Clone(prev)
	meta := file.Metadata()
	for g := range meta.RowGroups {
		rg := &meta.RowGroups[g]
		if rg.NumRows == 0 {
			continue
		}
		if !javaera.PureObjects(file, rg) {
			return r, fmt.Errorf("%w: row group %d's row_type statistics do not prove OBJECT..OBJECT", ErrNotObjects, g)
		}
		chunks := file.RowGroups()[g].ColumnChunks()
		var first []byte
		n, err := eachValue(ctx, chunks[colKey], func(v parquet.Value) error {
			k := v.ByteArray()
			switch {
			case v.IsNull() || len(k) == 0:
				return fmt.Errorf("%w: an empty key", ErrKey)
			case len(k) > MaxKeyBytes:
				return fmt.Errorf("%w: a key of %d bytes (at most %d)", ErrKey, len(k), MaxKeyBytes)
			case !utf8.Valid(k):
				return fmt.Errorf("%w: key %q is not UTF-8", ErrKey, k)
			case last != nil && bytes.Compare(k, last) <= 0:
				return fmt.Errorf("%w: %q follows %q", ErrOrder, k, last)
			}
			if first == nil {
				first = slices.Clone(k)
			}
			last = append(last[:0], k...)
			return nil
		})
		if err != nil {
			return r, fmt.Errorf("row group %d: %w", g, err)
		}
		if n != rg.NumRows {
			return r, fmt.Errorf("%w: row group %d holds %d keys, declares %d rows", ErrFooter, g, n, rg.NumRows)
		}
		st := rg.Columns[colKey].MetaData.Statistics
		if !bytes.Equal(st.MinValue, first) || !bytes.Equal(st.MaxValue, last) {
			return r, fmt.Errorf("%w: row group %d", ErrStatistics, g)
		}
		if _, err := eachValue(ctx, chunks[colVersionID], func(v parquet.Value) error {
			if !v.IsNull() {
				return fmt.Errorf("%w: a row carries version_id %q", ErrNotObjects, v.ByteArray())
			}
			return nil
		}); err != nil {
			return r, fmt.Errorf("row group %d: %w", g, err)
		}
		if _, err := eachValue(ctx, chunks[colIsDeleteMarker], func(v parquet.Value) error {
			if v.IsNull() || v.Boolean() {
				return fmt.Errorf("%w: a delete marker", ErrNotObjects)
			}
			return nil
		}); err != nil {
			return r, fmt.Errorf("row group %d: %w", g, err)
		}
		if r.first == nil {
			r.first = first
		}
		r.rows += n
	}
	if r.rows == 0 {
		return r, fmt.Errorf("%w: the part holds no rows (SeekParquet PART-2)", ErrFooter)
	}
	r.last = last
	return r, nil
}

// eachValue calls fn with every value of a column chunk, a page at a time,
// and returns how many there were. Values are valid only during the call.
func eachValue(ctx context.Context, chunk parquet.ColumnChunk, fn func(parquet.Value) error) (int64, error) {
	pages := chunk.Pages()
	defer func() { _ = pages.Close() }()
	buf := make([]parquet.Value, 1024)
	var n int64
	for {
		if err := ctx.Err(); err != nil {
			return n, err
		}
		page, err := pages.ReadPage()
		if errors.Is(err, io.EOF) {
			return n, nil
		}
		if err != nil {
			return n, err
		}
		values := page.Values()
		for {
			k, err := values.ReadValues(buf)
			for _, v := range buf[:k] {
				if ferr := fn(v); ferr != nil {
					parquet.Release(page)
					return n, ferr
				}
				n++
			}
			if errors.Is(err, io.EOF) || (err == nil && k == 0) {
				break
			}
			if err != nil {
				parquet.Release(page)
				return n, err
			}
		}
		parquet.Release(page)
	}
}

// encode encodes v with parquet-go's thrift compact codec.
func encode(v any) []byte {
	b, err := thrift.Marshal(new(thrift.CompactProtocol), v)
	if err != nil {
		panic(err) // the values are fixed and always encode
	}
	return b
}

// stamps are the SeekParquet footer stamps (STAMP-1) of a converted part.
func stamps() []format.KeyValue {
	return []format.KeyValue{
		{Key: seekparquet.StampFormatVersion, Value: strconv.FormatInt(seekparquet.FormatVersion, 10)},
		{Key: seekparquet.StampDraftRevision, Value: strconv.FormatInt(seekparquet.DraftRevision, 10)},
		{Key: seekparquet.StampSchemaID, Value: SchemaID},
		{Key: seekparquet.StampKeyColumn, Value: javaColumns[colKey].name},
		{Key: seekparquet.StampKeyUnique, Value: "true"},
	}
}

// FileMetaData, SchemaElement and RowGroup field ids (parquet.thrift).
const (
	fmSchema       = 2
	fmRowGroups    = 4
	fmKeyValue     = 5
	fmColumnOrders = 7
	seConverted    = 6
	seLogical      = 10
	rgSorting      = 4
)

// rewriteFooter splices the SeekParquet additions into a raw format-1
// footer, copying every other field byte for byte:
//
//   - the key's schema element gains converted_type UTF8 and logicalType
//     STRING (KEY-1), when annotate;
//   - every row group gains sorting_columns [key ascending] (RG-1);
//   - key_value_metadata gains the five stamps after the existing entries
//     (STAMP-1; the swath.sort.* stamps stay);
//   - column_orders, when the footer has none, becomes TYPE_ORDER for each
//     leaf (KEY-5).
func rewriteFooter(raw []byte, rowGroups, leaves int, annotate, addOrders bool) ([]byte, error) {
	top, n, err := splitStruct(raw, 0)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrFooter, err)
	}
	if n != len(raw) {
		return nil, fmt.Errorf("%w: %d bytes after the footer struct", ErrFooter, len(raw)-n)
	}
	for i := range top {
		f := &top[i]
		switch f.id {
		case fmSchema:
			if !annotate {
				continue
			}
			et, elems, _, err := splitList(f.val, 1)
			if err != nil || et != tStruct || len(elems) < 2 {
				return nil, fmt.Errorf("%w: schema list", ErrFooter)
			}
			key, _, err := splitStruct(elems[1+colKey], 2)
			if err != nil {
				return nil, fmt.Errorf("%w: key schema element: %v", ErrFooter, err)
			}
			if key, err = insert(key, field{id: seConverted, typ: tI32, val: []byte{0}}); err != nil { // UTF8 = 0
				return nil, fmt.Errorf("%w: key schema element: %v", ErrFooter, err)
			}
			if key, err = insert(key, field{id: seLogical, typ: tStruct, val: encode(&format.LogicalType{Value: &format.StringType{}})}); err != nil {
				return nil, fmt.Errorf("%w: key schema element: %v", ErrFooter, err)
			}
			elems = slices.Clone(elems)
			elems[1+colKey] = joinStruct(key)
			f.val = joinList(et, elems)
		case fmRowGroups:
			et, elems, _, err := splitList(f.val, 1)
			if err != nil || et != tStruct || len(elems) != rowGroups {
				return nil, fmt.Errorf("%w: row group list", ErrFooter)
			}
			sorting := joinList(tStruct, [][]byte{encode(&format.SortingColumn{ColumnIdx: colKey})})
			out := make([][]byte, len(elems))
			for g, e := range elems {
				rg, _, err := splitStruct(e, 2)
				if err == nil {
					rg, err = setSorting(rg, sorting)
				}
				if err != nil {
					return nil, fmt.Errorf("%w: row group %d: %v", ErrFooter, g, err)
				}
				out[g] = joinStruct(rg)
			}
			f.val = joinList(et, out)
		case fmKeyValue:
			et, elems, _, err := splitList(f.val, 1)
			if err != nil || et != tStruct {
				return nil, fmt.Errorf("%w: key-value list", ErrFooter)
			}
			elems = slices.Clone(elems)
			for _, kv := range stamps() {
				elems = append(elems, encode(&kv))
			}
			f.val = joinList(et, elems)
		}
	}
	if _, ok := lookup(top, fmKeyValue); !ok {
		var elems [][]byte
		for _, kv := range stamps() {
			elems = append(elems, encode(&kv))
		}
		if top, err = insert(top, field{id: fmKeyValue, typ: tList, val: joinList(tStruct, elems)}); err != nil {
			return nil, err
		}
	}
	if addOrders {
		orders := make([][]byte, leaves)
		for i := range orders {
			orders[i] = encode(&format.ColumnOrder{Value: &format.TypeDefinedOrder{}})
		}
		if top, err = insert(top, field{id: fmColumnOrders, typ: tList, val: joinList(tStruct, orders)}); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrFooter, err)
		}
	}
	return joinStruct(top), nil
}

// setSorting adds sorting_columns to a row group's fields, or fills an
// empty list a writer left (parquet-go writes one).
func setSorting(rg []field, sorting []byte) ([]field, error) {
	for i, f := range rg {
		if f.id != rgSorting {
			continue
		}
		if _, elems, _, err := splitList(f.val, 3); err != nil || f.typ != tList || len(elems) != 0 {
			return nil, errors.New("sorting_columns is already declared")
		}
		rg = slices.Clone(rg)
		rg[i].val = sorting
		return rg, nil
	}
	return insert(rg, field{id: rgSorting, typ: tList, val: sorting})
}

// checkFooterChange compares the two footers field by field and refuses a
// rewrite that removes or changes any value of the old one (besides the
// key-value list's length) or adds anything outside the key's annotation,
// the row groups' sorting_columns, the appended stamps and, when added,
// the column orders. It is a check of rewriteFooter by other means.
func checkFooterChange(old, new []byte, keyValues int, addedOrders bool) error {
	changed, added, err := footerDiff(old, new)
	if err != nil {
		return err
	}
	for _, path := range changed {
		var g int
		emptySorting := sscan(path, fmt.Sprintf(".%d[%%d].%d.len", fmRowGroups, rgSorting), &g) && path == fmt.Sprintf(".%d[%d].%d.len", fmRowGroups, g, rgSorting)
		if path != fmt.Sprintf(".%d.len", fmKeyValue) && !(emptySorting && emptyList(old, path)) {
			return fmt.Errorf("%w: %s changed", errFooterChanged, path)
		}
	}
	allowed := []string{
		fmt.Sprintf(".%d[%d].%d", fmSchema, 1+colKey, seConverted),
		fmt.Sprintf(".%d[%d].%d.", fmSchema, 1+colKey, seLogical),
		fmt.Sprintf(".%d[%d].%d{}", fmSchema, 1+colKey, seLogical),
	}
	for _, path := range added {
		var g, k int
		switch {
		case hasAnyPrefix(path, allowed):
		case sscan(path, fmt.Sprintf(".%d[%%d].%d", fmRowGroups, rgSorting), &g):
		case sscan(path, fmt.Sprintf(".%d[%%d]", fmKeyValue), &k) && k >= keyValues:
		case path == fmt.Sprintf(".%d.len", fmKeyValue) && keyValues == 0:
		case addedOrders && hasAnyPrefix(path, []string{fmt.Sprintf(".%d", fmColumnOrders)}):
		default:
			return fmt.Errorf("%w: %s added", errFooterChanged, path)
		}
	}
	return nil
}

// emptyList reports whether the footer old holds an empty list at path
// (a ".len" path).
func emptyList(old []byte, path string) bool {
	f, err := flatten(old)
	return err == nil && strings.HasPrefix(f[path], "0/")
}

// sscan reports whether path begins with pattern's text, its one %d
// scanned into n.
func sscan(path, pattern string, n *int) bool {
	_, err := fmt.Sscanf(path, pattern, n)
	if err != nil {
		return false
	}
	prefix := fmt.Sprintf(pattern, *n)
	return len(path) >= len(prefix) && path[:len(prefix)] == prefix
}

// writePart writes dst: in's first dataBytes bytes, unchanged, then
// newFooter, its length and the magic, through a temporary file renamed
// into place after a sync. It returns the SHA-256 of in (its prefix and
// its old footer) and of dst, and dst's size.
func writePart(in *os.File, dataBytes int64, oldFooter, newFooter []byte, dst string) (inSum, outSum string, size int64, err error) {
	tmp, err := os.CreateTemp(filepath.Dir(dst), ".adopt-*.tmp")
	if err != nil {
		return "", "", 0, err
	}
	defer func() {
		if err != nil {
			_ = tmp.Close()
			_ = os.Remove(tmp.Name())
		}
	}()
	hin, hout := sha256.New(), sha256.New()
	if _, err = io.CopyBuffer(io.MultiWriter(tmp, hin, hout), io.NewSectionReader(in, 0, dataBytes), make([]byte, 4<<20)); err != nil {
		return "", "", 0, err
	}
	writeTail := func(w io.Writer, footer []byte) error {
		b := binary.LittleEndian.AppendUint32(slices.Clone(footer), uint32(len(footer)))
		_, err := w.Write(append(b, "PAR1"...))
		return err
	}
	_ = writeTail(hin, oldFooter)
	if err = writeTail(io.MultiWriter(tmp, hout), newFooter); err != nil {
		return "", "", 0, err
	}
	if err = tmp.Sync(); err != nil {
		return "", "", 0, err
	}
	if err = tmp.Close(); err != nil {
		return "", "", 0, err
	}
	if err = os.Rename(tmp.Name(), dst); err != nil {
		return "", "", 0, err
	}
	return sum(hin), sum(hout), dataBytes + int64(len(newFooter)) + 8, nil
}

func sum(h hash.Hash) string { return hex.EncodeToString(h.Sum(nil)) }

// swathExtension is extensions.swath (go/docs/dataset.md): version 1, the
// publication time, the source and the scope. coverage is left out: a
// format-1 fixture's parts cannot show what the listing covered, so the
// dataset identifies as swath's and whole-source acceptance refuses it.
type swathExtension struct {
	Version     int       `json:"version"`
	PublishedAt time.Time `json:"published_at"`
	Source      struct {
		Kind      string `json:"kind"`
		URI       string `json:"uri"`
		Container string `json:"container"`
		Region    string `json:"region,omitempty"`
	} `json:"source"`
	Scope struct {
		Kind   string `json:"kind"`
		Prefix string `json:"prefix,omitempty"`
	} `json:"scope"`
}

// adoptExtension is extensions["swath-replay"]: what the parts were
// before their footers were replaced.
type adoptExtension struct {
	AdoptedFrom struct {
		Layout  string `json:"layout"`
		Fixture string `json:"fixture,omitempty"`
		Parts   []Part `json:"parts"`
	} `json:"adopted_from"`
}

// publish writes manifest.json (atomically: a temporary file, a sync, a
// rename and a sync of the directory) listing parts.
func publish(opts Options, parts []Part, at time.Time) error {
	var sx swathExtension
	sx.Version, sx.PublishedAt = 1, at
	sx.Source.Kind, sx.Source.Container, sx.Source.Region = "s3", opts.Bucket, opts.Region
	sx.Source.URI = "s3://" + opts.Bucket
	sx.Scope.Kind = "whole"
	if opts.Prefix != "" {
		sx.Source.URI += "/" + opts.Prefix
		sx.Scope.Kind, sx.Scope.Prefix = "prefix", opts.Prefix
	}
	var ax adoptExtension
	ax.AdoptedFrom.Layout = "swath format 1 sorted parts (swath.sort.format_version 1, mode objects)"
	ax.AdoptedFrom.Fixture = opts.Fixture
	ax.AdoptedFrom.Parts = parts
	ext, err := json.Marshal(map[string]any{"swath": sx, ExtensionNamespace: ax})
	if err != nil {
		return err
	}
	m := seekparquet.Manifest{
		Key:      seekparquet.Key{Column: javaColumns[colKey].name, Unique: true},
		Version:  seekparquet.Version{FormatVersion: seekparquet.FormatVersion, DraftRevision: seekparquet.DraftRevision},
		SchemaID: SchemaID, Extensions: ext,
	}
	for _, p := range parts {
		m.Parts = append(m.Parts, seekparquet.PartInfo{Path: p.Path, Rows: p.Rows, Bytes: p.Bytes, MinKey: p.MinKey, MaxKey: p.MaxKey, SHA256: p.SHA256})
	}
	raw, err := seekparquet.MarshalManifest(m)
	if err != nil {
		return fmt.Errorf("adopt-fixture: manifest: %w", err)
	}
	if err := syncDir(filepath.Join(opts.Output, "data")); err != nil {
		return err
	}
	tmp := filepath.Join(opts.Output, ".manifest.json.tmp")
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err = f.Write(raw); err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Rename(tmp, filepath.Join(opts.Output, seekparquet.ManifestName))
	}
	if err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return syncDir(opts.Output)
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer func() { _ = d.Close() }()
	return d.Sync()
}
