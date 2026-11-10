// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package reader

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/encoding"
	"github.com/parquet-go/parquet-go/format"
	seekparquet "github.com/varveio/seekparquet/go"
	"github.com/varveio/seekparquet/go/local"
	"github.com/varveio/seekparquet/go/write"
)

// fixtureRow is the swath/s3-objects/v1 Parquet schema as the lister's
// writer builds it (internal/parquetout nodeOf over internal/source/s3
// Schema; message swath_listing): every column optional but the key, the
// key and etag DELTA_BYTE_ARRAY, the other strings the writer's default
// (RLE_DICTIONARY, PLAIN past its dictionary limit) and the integers
// DELTA_BINARY_PACKED. TestFixtureFooter checks the footer says so.
type fixtureRow struct {
	Key               string  `parquet:"key,delta"`
	Size              *int64  `parquet:"size,optional"`
	LastModified      *int64  `parquet:"last_modified,optional,timestamp(microsecond:utc)"`
	ETag              *string `parquet:"etag,optional"` // DELTA_BYTE_ARRAY: fixtureSchema
	StorageClass      *string `parquet:"storage_class,optional"`
	OwnerID           *string `parquet:"owner_id,optional"`
	OwnerDisplayName  *string `parquet:"owner_display_name,optional"`
	ChecksumAlgorithm *string `parquet:"checksum_algorithm,optional"`
	ChecksumType      *string `parquet:"checksum_type,optional"`
}

// fixtureSchema is fixtureRow's schema with etag DELTA_BYTE_ARRAY, which
// a struct tag cannot set on an optional string: the override nodeOf
// applies for schema.Column.Delta.
var fixtureSchema = parquet.NewSchema("swath_listing", deltaFields{parquet.SchemaOf(new(fixtureRow)), "etag"})

// deltaFields sets field name's encoding to DELTA_BYTE_ARRAY.
type deltaFields struct {
	parquet.Node
	name string
}

func (r deltaFields) Fields() []parquet.Field {
	fields := slices.Clone(r.Node.Fields())
	for i, f := range fields {
		if f.Name() == r.name {
			fields[i] = deltaField{f}
		}
	}
	return fields
}

type deltaField struct{ parquet.Field }

func (deltaField) Encoding() encoding.Encoding { return &parquet.DeltaByteArray }

// The fixture's leaf columns, in the schema's order.
const (
	fxKey = iota
	fxSize
	fxMtime
	fxETag
	fxClass
	fxOwnerID
	fxOwnerName
	fxChecksumAlg
	fxChecksumType
	fxColumns
)

// fixtureTops are the top-level prefixes, in key order.
var fixtureTops = []string{
	"analytics/daily_rollups/region=eu-west-1",
	"events/clickstream/v2/source=web",
	"logs/application/web-frontend",
	"raw/ingest/partner-feeds/acme",
}

// fixtureClasses are the storage classes and their weights in 1,000. The
// remainder (5) is null, as an S3-compatible store that omits the class
// writes it.
var fixtureClasses = []struct {
	name   string
	weight int
}{
	{"STANDARD", 850}, {"STANDARD_IA", 80}, {"INTELLIGENT_TIERING", 40}, {"GLACIER_IR", 15}, {"DEEP_ARCHIVE", 10},
}

var fixtureExts = []string{"parquet", "json.gz", "csv", "avro.snappy"}

// fixtureGen streams the rows of a generated s3-objects/v1 listing in key
// order (a parquet.RowReader), holding one batch at a time. Keys are a
// date-partitioned tree: top/yyyy/mm/dd/shard=NNN/part-NNNNN-TAG.EXT, with
// 1 to 16 shard directories a day, 1 to 600 objects each, and an S3 folder
// object (key ending in "/", size 0) opening one in eight. Sizes are
// log-normal around 1 MiB; times millisecond precision, over seven years
// in key order with an hour's jitter; ETags 32 random hex digits, 5%
// multipart ("-N"); checksums on the last 40% of objects but folders,
// null before; storage classes STANDARD-heavy with 0.5% null; owner
// columns null (no --s3-fetch-owner). The sequence is a
// function of rows and seed alone.
type fixtureGen struct {
	rows, emitted int64
	rng           *rand.Rand
	daysPerTop    int

	top, day, shard, shards int
	file, files             int
	marker                  bool // the folder object of the current shard is next
	mtime                   int64
}

// The fixture's times: from fixtureEpoch over fixtureSpan.
var (
	fixtureEpoch = time.Date(2019, 1, 1, 0, 0, 0, 0, time.UTC)
	fixtureSpan  = 7 * 365 * 24 * time.Hour
)

func newFixtureGen(rows int64, seed uint64) *fixtureGen {
	// About 8.5 shard directories of 300 objects a day.
	perTop := rows/int64(len(fixtureTops)) + 1
	g := &fixtureGen{rows: rows, rng: rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15)), daysPerTop: int(perTop/2550) + 1}
	g.newDay()
	return g
}

func (g *fixtureGen) newDay() {
	g.shards, g.shard = 1+g.rng.IntN(16), -1
	g.newShard()
}

func (g *fixtureGen) newShard() {
	g.shard++
	g.file, g.files = 0, 1+g.rng.IntN(600)
	g.marker = g.rng.IntN(8) == 0
}

// advance moves to the next object's directory when the current one is
// exhausted: the next shard, day or top (the last top takes every day
// after its share).
func (g *fixtureGen) advance() {
	if g.file < g.files {
		return
	}
	if g.shard+1 < g.shards {
		g.newShard()
		return
	}
	g.day++
	if g.day >= g.daysPerTop && g.top+1 < len(fixtureTops) {
		g.top, g.day = g.top+1, 0
	}
	g.newDay()
}

func (g *fixtureGen) ReadRows(out []parquet.Row) (int, error) {
	n := int(min(int64(len(out)), g.rows-g.emitted))
	for i := range n {
		g.advance()
		day := fixtureEpoch.AddDate(0, 0, g.day)
		// Each row's byte values are its own: the batch is all that is
		// held, and the part writer copies what it keeps.
		key := make([]byte, 0, 128)
		key = append(key, fixtureTops[g.top]...)
		key = day.AppendFormat(key, "/2006/01/02/shard=")
		key = appendPad(key, g.shard, 3)
		key = append(key, '/')
		folder := g.marker
		if !folder {
			key = append(key, "part-"...)
			key = appendPad(key, g.file, 5)
			key = append(key, '-')
			var tag [18]byte
			for j := range tag {
				tag[j] = byte(g.rng.Uint32())
			}
			if g.rng.IntN(2) == 0 {
				key = hex.AppendEncode(key, tag[:4])
			} else {
				key = hex.AppendEncode(key, tag[:16])
			}
			key = append(key, '.')
			key = append(key, fixtureExts[(g.top+g.shard)%len(fixtureExts)]...)
			g.file++
		}
		g.marker = false
		// Spread over fixtureSpan in row order, with up to an hour of
		// jitter, in milliseconds as S3 reports them.
		at := fixtureEpoch.UnixMicro() + fixtureSpan.Microseconds()/g.rows*(g.emitted+int64(i))
		g.mtime = (at + g.rng.Int64N(time.Hour.Microseconds())) / 1000 * 1000
		row := out[i][:0]
		row = append(row, parquet.ByteArrayValue(key).Level(0, 0, fxKey))
		if folder {
			row = append(row, parquet.Int64Value(0).Level(0, 1, fxSize))
		} else {
			size := int64(math.Exp(13.8 + 2.2*g.rng.NormFloat64()))
			row = append(row, parquet.Int64Value(size).Level(0, 1, fxSize))
		}
		row = append(row, parquet.Int64Value(g.mtime).Level(0, 1, fxMtime))
		var sum [16]byte
		for j := range sum {
			sum[j] = byte(g.rng.Uint32())
		}
		etag := hex.AppendEncode(make([]byte, 0, 40), sum[:])
		if g.rng.IntN(20) == 0 {
			etag = strconv.AppendInt(append(etag, '-'), 2+g.rng.Int64N(200), 10)
		}
		row = append(row, parquet.ByteArrayValue(etag).Level(0, 1, fxETag))
		row = append(row, nullable(fixtureClass(g.rng.IntN(1000)), fxClass))
		row = append(row, parquet.NullValue().Level(0, 0, fxOwnerID), parquet.NullValue().Level(0, 0, fxOwnerName))
		if (g.emitted+int64(i))*10 >= g.rows*6 && !folder {
			row = append(row, parquet.ByteArrayValue([]byte("CRC64NVME")).Level(0, 1, fxChecksumAlg),
				parquet.ByteArrayValue([]byte("FULL_OBJECT")).Level(0, 1, fxChecksumType))
		} else {
			row = append(row, parquet.NullValue().Level(0, 0, fxChecksumAlg), parquet.NullValue().Level(0, 0, fxChecksumType))
		}
		out[i] = row
	}
	g.emitted += int64(n)
	if g.emitted == g.rows {
		return n, io.EOF
	}
	return n, nil
}

func fixtureClass(draw int) string {
	for _, c := range fixtureClasses {
		if draw < c.weight {
			return c.name
		}
		draw -= c.weight
	}
	return ""
}

func nullable(s string, col int) parquet.Value {
	if s == "" {
		return parquet.NullValue().Level(0, 0, col)
	}
	return parquet.ByteArrayValue([]byte(s)).Level(0, 1, col)
}

func appendPad(b []byte, v, width int) []byte {
	s := strconv.Itoa(v)
	for range width - len(s) {
		b = append(b, '0')
	}
	return append(b, s...)
}

// writeFixture writes a generated listing of rows objects as a new
// dataset at dir with the lister's geometry: the module writer's defaults,
// which internal/parquetout sets explicitly (1,024-row pages, 8 MiB or
// 1,000,000-row row groups, ZSTD) and 128 MiB parts. Its memory is one
// batch and the part being written, whatever rows is.
func writeFixture(ctx context.Context, dir string, rows int64, seed uint64) error {
	ext, err := json.Marshal(map[string]any{Extension: swathExtension()})
	if err != nil {
		return err
	}
	return local.WriteDir(ctx, dir, fixtureSchema, newFixtureGen(rows, seed), local.DatasetOptions{
		Part: write.PartOptions{Key: seekparquet.Key{Column: "key", Unique: true}, SchemaID: string(S3ObjectsV1),
			PageRows: 1024, RowGroupBytes: 8 << 20, RowGroupRows: 1_000_000},
		Extensions: ext, AllowUnsynced: true,
	})
}

// fixtureGeometry is what a dataset's footers say about its layout.
type fixtureGeometry struct {
	Rows, Parts, RowGroups, Bytes int64
	MinGroupRows, MaxGroupRows    int64
	Codecs                        map[string][]string // column: compression codecs
	Encodings                     map[string][]string // column: encodings, any chunk
	KeyBytes                      float64             // mean key length, from the key column's uncompressed size
}

func (g fixtureGeometry) String() string {
	return fmt.Sprintf("%d rows, %d parts, %d row groups (%d..%d rows), %.1f B/row on disk, key column %.1f B/row uncompressed",
		g.Rows, g.Parts, g.RowGroups, g.MinGroupRows, g.MaxGroupRows, float64(g.Bytes)/float64(max(g.Rows, 1)), g.KeyBytes)
}

// inspectFixture reads every part's footer under dir/data.
func inspectFixture(dir string) (fixtureGeometry, error) {
	g := fixtureGeometry{Codecs: map[string][]string{}, Encodings: map[string][]string{}, MinGroupRows: math.MaxInt64}
	parts, err := filepath.Glob(filepath.Join(dir, "data", "*.parquet"))
	if err != nil {
		return g, err
	}
	var keyBytes int64
	for _, p := range parts {
		f, err := os.Open(p)
		if err != nil {
			return g, err
		}
		st, err := f.Stat()
		if err != nil {
			_ = f.Close()
			return g, err
		}
		pf, err := parquet.OpenFile(f, st.Size(), parquet.SkipPageIndex(true), parquet.SkipBloomFilters(true))
		if err != nil {
			_ = f.Close()
			return g, fmt.Errorf("%s: %w", p, err)
		}
		g.Parts++
		g.Bytes += st.Size()
		for _, rg := range pf.Metadata().RowGroups {
			g.RowGroups++
			g.Rows += rg.NumRows
			g.MinGroupRows, g.MaxGroupRows = min(g.MinGroupRows, rg.NumRows), max(g.MaxGroupRows, rg.NumRows)
			for _, cc := range rg.Columns {
				name := cc.MetaData.PathInSchema[0]
				g.Codecs[name] = addName(g.Codecs[name], cc.MetaData.Codec.String())
				for _, e := range cc.MetaData.Encoding {
					if e != format.RLE { // levels
						g.Encodings[name] = addName(g.Encodings[name], e.String())
					}
				}
				if name == "key" {
					keyBytes += cc.MetaData.TotalUncompressedSize
				}
			}
		}
		_ = f.Close()
	}
	// The key column's uncompressed bytes are delta-coded: a measure of
	// what the pages hold, not the keys' length.
	g.KeyBytes = float64(keyBytes) / float64(max(g.Rows, 1))
	return g, nil
}

func addName(names []string, n string) []string {
	if !slices.Contains(names, n) {
		names = append(names, n)
		slices.Sort(names)
	}
	return names
}

// TestFixtureFooter: the generated fixture is written as the lister
// writes s3-objects/v1, by its footer: ZSTD throughout, the key and etag
// DELTA_BYTE_ARRAY, storage_class dictionary-encoded, the integers
// DELTA_BINARY_PACKED; and it reads back in key order with nulls where the
// generator puts them.
func TestFixtureFooter(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "ds")
	const rows = 5000
	if err := writeFixture(context.Background(), dir, rows, 1); err != nil {
		t.Fatal(err)
	}
	g, err := inspectFixture(dir)
	if err != nil {
		t.Fatal(err)
	}
	if g.Rows != rows {
		t.Fatalf("%d rows, want %d", g.Rows, rows)
	}
	for col, want := range map[string][]string{
		"key":           {"DELTA_BYTE_ARRAY"},
		"etag":          {"DELTA_BYTE_ARRAY"},
		"storage_class": {"PLAIN", "RLE_DICTIONARY"},
		"size":          {"DELTA_BINARY_PACKED"},
		"last_modified": {"DELTA_BINARY_PACKED"},
	} {
		if got := g.Encodings[col]; !slices.Equal(got, want) {
			t.Errorf("%s encodings %v, want %v", col, got, want)
		}
		if got := g.Codecs[col]; !slices.Equal(got, []string{"ZSTD"}) {
			t.Errorf("%s codecs %v, want ZSTD", col, got)
		}
	}
	d := openDir(t, dir)
	if err := d.CheckSchema(context.Background()); err != nil {
		t.Fatal(err)
	}
	var n, nullClass, folders int
	var prev []byte
	if err := d.Stream(context.Background(), func(o Object) error {
		if string(o.Key) <= string(prev) {
			return fmt.Errorf("key %q after %q", o.Key, prev)
		}
		prev = o.Key
		n++
		if o.StorageClass == nil {
			nullClass++
		}
		if o.Key[len(o.Key)-1] == '/' {
			folders++
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if n != rows || nullClass == 0 || folders == 0 {
		t.Fatalf("%d objects, %d without a class, %d folders", n, nullClass, folders)
	}
}

// TestWriteBenchFixture writes a generated fixture of
// SWATH_READER_BENCH_ROWS rows (default 4,000,000) to the directory
// SWATH_READER_FIXTURE_OUT names, which must not exist, and logs its
// geometry; benchmarks then read it with SWATH_READER_BENCH_DIR. Skipped
// without the variable.
func TestWriteBenchFixture(t *testing.T) {
	out := os.Getenv("SWATH_READER_FIXTURE_OUT")
	if out == "" {
		t.Skip("SWATH_READER_FIXTURE_OUT not set")
	}
	rows := benchRows(t)
	start := time.Now()
	if err := writeFixture(context.Background(), out, rows, 1); err != nil {
		t.Fatal(err)
	}
	g, err := inspectFixture(out)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("wrote %s in %v: %s; encodings %v; codecs %v", out, time.Since(start).Round(time.Millisecond), g, g.Encodings, g.Codecs)
}
