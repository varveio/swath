// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/encoding/thrift"
	"github.com/parquet-go/parquet-go/format"
)

func probeRows(count int) []Row {
	rows := make([]Row, count)
	for i := range rows {
		row := Row{Key: fmt.Sprintf("%s%04d", strings.Repeat("界", 340), i), RowType: "OBJECT"}
		if i%3 != 0 {
			size, modified, latest, text := int64(i)<<33, int64(-123456+i), false, ""
			row.Size, row.LastModified, row.IsLatest = &size, &modified, &latest
			row.ETag, row.StorageClass, row.VersionID = &text, &text, &text
			row.OwnerID, row.OwnerDisplayName = &text, &text
			row.ChecksumAlgorithm, row.ChecksumType = &text, &text
		}
		rows[i] = row
	}
	return rows
}

func TestCanonicalSchemaNullsAndGeometry(t *testing.T) {
	rows := probeRows(113)
	for _, layout := range []string{"served", "direct"} {
		for _, encoding := range []string{"dict", "delta"} {
			t.Run(layout+"/"+encoding, func(t *testing.T) {
				config := writerConfig{Layout: layout, KeyEncoding: encoding, IntegerEncoding: "dict", PageStatistics: true, PageRows: 16, GroupRows: 40}
				path := filepath.Join(t.TempDir(), "part.parquet")
				part, err := writePart(path, rows, config, diskOps())
				if err != nil {
					t.Fatal(err)
				}
				if part.Rows != 113 || *part.MinKey != rows[0].Key || *part.MaxKey != rows[112].Key || len(part.MD5) != 32 {
					t.Fatalf("unexpected successful metadata: %+v", part)
				}
				report, err := verifyPart(path, rows)
				if err != nil {
					t.Fatal(err)
				}
				if !report.VerifiedRows || report.MaxPageRows != 16 || len(report.RowGroups) != 3 {
					t.Fatalf("unexpected geometry: %+v", report)
				}
				if err := checkGeometry(report, config); err != nil {
					t.Fatal(err)
				}
				if err := checkFlushReports(report, part.Encoding, config); err != nil {
					t.Fatal(err)
				}
				for group, expectedRows := range []int64{40, 40, 33} {
					if report.RowGroups[group].Rows != expectedRows || len(report.RowGroups[group].Columns) != 13 {
						t.Fatalf("group %d: %+v", group, report.RowGroups[group])
					}
					key := report.RowGroups[group].Columns[0]
					if *key.ExactKeyBounds != (layout == "served") {
						t.Fatalf("%s exact bounds = %v", layout, *key.ExactKeyBounds)
					}
					for _, bound := range key.KeyBounds {
						if !bound.CoversRows {
							t.Fatalf("unsafe bounds: %+v", bound)
						}
					}
					if key.Codec != "ZSTD" {
						t.Fatalf("codec = %s", key.Codec)
					}
					wantEncoding := "RLE_DICTIONARY"
					if encoding == "delta" {
						wantEncoding = "DELTA_BYTE_ARRAY"
					}
					found := false
					for _, physical := range key.Encodings {
						found = found || physical == wantEncoding
					}
					if !found {
						t.Fatalf("encoding %s missing from %v", wantEncoding, key.Encodings)
					}
				}
			})
		}
	}
}

func TestInspectChecksActualCountsAndCaps(t *testing.T) {
	rows := probeRows(113)
	config := writerConfig{Layout: "served", KeyEncoding: "dict", IntegerEncoding: "dict", PageStatistics: true, PageRows: 16, GroupRows: 40}
	path := filepath.Join(t.TempDir(), "part.parquet")
	if _, err := writePart(path, rows, config, diskOps()); err != nil {
		t.Fatal(err)
	}
	if _, err := inspectPart(path, nil); err == nil {
		t.Fatal("nonempty file passed against an empty corpus")
	}
	report, err := inspectPart(path, rows)
	if err != nil {
		t.Fatal(err)
	}
	tooSmall := config
	tooSmall.PageRows = 15
	if err := checkGeometry(report, tooSmall); err == nil {
		t.Fatal("actual 16-row pages passed a 15-row cap")
	}
	wrongGroups := config
	wrongGroups.GroupRows = 41
	if err := checkGeometry(report, wrongGroups); err == nil {
		t.Fatal("actual 40-row full groups passed a 41-row target")
	}
	var output bytes.Buffer
	if _, err := encodeRows(&output, rows, config); err != nil {
		t.Fatal(err)
	}
	file, err := parquet.OpenFile(bytes.NewReader(output.Bytes()), int64(output.Len()))
	if err != nil {
		t.Fatal(err)
	}
	file.OffsetIndexes()[0].PageLocations[0].FirstRowIndex = 1
	if _, err := inspectFile(file, rows); err == nil {
		t.Fatal("corrupt first-page row offset passed inspection")
	}
}

func TestSortedStampPreconditions(t *testing.T) {
	empty, earlier, later := "", "\ue000", "\U00010000"
	valid := []Row{
		{Key: "k", RowType: "OBJECT"},
		{Key: "k", RowType: "COMMON_PREFIX"},
		{Key: "k", RowType: "DELETE_MARKER"},
		{Key: "k", VersionID: &empty, RowType: "OBJECT"},
		{Key: "k", VersionID: &earlier, RowType: "OBJECT"},
		{Key: "k", VersionID: &later, RowType: "OBJECT"},
	}
	if err := validateRows(valid); err != nil {
		t.Fatal(err)
	}
	for _, rows := range [][]Row{
		{valid[3], valid[0]},
		{valid[5], valid[4]},
		{valid[2], valid[1]},
	} {
		if err := validateRows(rows); err == nil {
			t.Fatalf("unordered versions or row types accepted: %+v", rows)
		}
	}
}

func TestIntegerEncodingAndPageStatistics(t *testing.T) {
	rows := probeRows(113)
	for _, layout := range []string{"served", "direct"} {
		for _, integerEncoding := range []string{"dict", "delta"} {
			for _, pageStatistics := range []bool{true, false} {
				t.Run(fmt.Sprintf("%s/%s/stats=%t", layout, integerEncoding, pageStatistics), func(t *testing.T) {
					config := writerConfig{
						Layout: layout, KeyEncoding: "delta", IntegerEncoding: integerEncoding,
						PageStatistics: pageStatistics, PageRows: 16, GroupRows: 40,
					}
					path := filepath.Join(t.TempDir(), "part.parquet")
					if _, err := writePart(path, rows, config, diskOps()); err != nil {
						t.Fatal(err)
					}
					report, err := verifyPart(path, rows)
					if err != nil {
						t.Fatal(err)
					}
					if err := checkGeometry(report, config); err != nil {
						t.Fatal(err)
					}
					tooSmall := config
					tooSmall.PageRows = 15
					if err := checkGeometry(report, tooSmall); err == nil {
						t.Fatal("actual pages passed a smaller row cap")
					}
					data, err := os.ReadFile(path)
					if err != nil {
						t.Fatal(err)
					}
					file, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
					if err != nil {
						t.Fatal(err)
					}
					for _, column := range []int{0, 1, 2} {
						metadata := file.Metadata().RowGroups[0].Columns[column].MetaData
						if len(metadata.Statistics.MinValue) == 0 || len(metadata.Statistics.MaxValue) == 0 {
							t.Fatalf("column %d footer bounds disappeared with stats=%t", column, pageStatistics)
						}
						reader := bytes.NewReader(data[metadata.DataPageOffset:])
						var header format.PageHeader
						if err := thrift.NewDecoder((&thrift.CompactProtocol{}).NewReader(reader)).Decode(&header); err != nil {
							t.Fatal(err)
						}
						if !header.DataPageHeaderV2.Valid {
							t.Fatal("expected data page v2")
						}
						page := header.DataPageHeaderV2.V
						stats := page.Statistics
						boundBytes := len(stats.Min) + len(stats.Max) + len(stats.MinValue) + len(stats.MaxValue)
						if (boundBytes > 0) != pageStatistics {
							t.Fatalf("column %d header bound bytes=%d, stats=%t", column, boundBytes, pageStatistics)
						}
						if column != 0 {
							want := format.RLEDictionary
							if integerEncoding == "delta" {
								want = format.DeltaBinaryPacked
							}
							if page.Encoding != want {
								t.Fatalf("column %d integer encoding=%s, expected %s", column, page.Encoding, want)
							}
						}
					}
				})
			}
		}
	}
}

func byteTargetRows(count int) []Row {
	random := rand.New(rand.NewSource(9137))
	rows := probeRows(count)
	for i := range rows {
		payload := make([]byte, 500)
		_, _ = random.Read(payload)
		rows[i].Key = fmt.Sprintf("%08d/%s", i, hex.EncodeToString(payload))
		if rows[i].ETag != nil {
			text := hex.EncodeToString(payload[:80])
			rows[i].ETag, rows[i].StorageClass, rows[i].VersionID = &text, &text, &text
			rows[i].OwnerID, rows[i].OwnerDisplayName = &text, &text
			rows[i].ChecksumAlgorithm, rows[i].ChecksumType = &text, &text
		}
	}
	return rows
}

func TestByteTargetGroupsAreDeterministicAndReopen(t *testing.T) {
	for _, corpus := range []struct {
		name string
		rows []Row
	}{
		{"wide compressible nullable", probeRows(513)},
		{"wide random nullable", byteTargetRows(513)},
	} {
		for _, keyEncoding := range []string{"dict", "delta"} {
			t.Run(corpus.name+"/"+keyEncoding, func(t *testing.T) {
				config := writerConfig{
					Layout: "served", KeyEncoding: keyEncoding, IntegerEncoding: "delta",
					PageStatistics: false, PageRows: 16, GroupRows: 256, GroupBytes: 96 << 10,
				}
				var previous []groupFlushReport
				for pass := range 2 {
					path := filepath.Join(t.TempDir(), "part.parquet")
					part, err := writePart(path, corpus.rows, config, diskOps())
					if err != nil {
						t.Fatal(err)
					}
					report, err := verifyPart(path, corpus.rows)
					if err != nil {
						t.Fatal(err)
					}
					if err := checkGeometry(report, config); err != nil {
						t.Fatal(err)
					}
					if err := checkFlushReports(report, part.Encoding, config); err != nil {
						t.Fatal(err)
					}
					engagedAfterEarlierPages := false
					for _, gate := range part.Encoding.Groups {
						engagedAfterEarlierPages = engagedAfterEarlierPages || gate.Reason == flushByteTarget && gate.PreviousPageEstimatedBytes > 0
					}
					if !engagedAfterEarlierPages && corpus.name == "wide random nullable" {
						t.Fatal("wide random fixture did not cross the target after earlier pages")
					}
					if pass == 1 && !reflect.DeepEqual(previous, part.Encoding.Groups) {
						t.Fatalf("same input produced different byte groups: %v vs %v", previous, part.Encoding.Groups)
					}
					previous = part.Encoding.Groups
				}
			})
		}
	}
}

func valueParityRows() []Row {
	zero, maximum, negative := int64(0), int64(math.MaxInt64), int64(-1234567)
	empty, version, text := "", "\U00010000", "\x00\n%+é\ue000\U00010000"
	latest := false
	rows := byteTargetRows(129)
	rows = append(rows,
		Row{Key: "", Size: &zero, LastModified: &zero, ETag: &empty, IsLatest: &latest, RowType: "OBJECT"},
		Row{Key: text, Size: &maximum, LastModified: &negative, StorageClass: &text, OwnerID: &text, OwnerDisplayName: &text, ChecksumAlgorithm: &text, ChecksumType: &text, RowType: "OBJECT"},
		Row{Key: "same", RowType: "OBJECT"},
		Row{Key: "same", RowType: "COMMON_PREFIX"},
		Row{Key: "same", IsDeleteMarker: true, RowType: "DELETE_MARKER"},
		Row{Key: "same", Size: &zero, VersionID: &empty, IsLatest: &latest, RowType: "OBJECT"},
		Row{Key: "same", LastModified: &negative, VersionID: &version, IsLatest: &latest, IsDeleteMarker: true, RowType: "DELETE_MARKER"},
	)
	sort.SliceStable(rows, func(i, j int) bool { return compareRows(rows[i], rows[j]) < 0 })
	return rows
}

func TestValuesAPIPreservesExactParquetBytes(t *testing.T) {
	for _, corpus := range []struct {
		name string
		rows []Row
	}{
		{"all fields and edge values", valueParityRows()},
		{"long Unicode", probeRows(113)},
		{"single page", valueParityRows()[:1]},
		{"empty", nil},
	} {
		for _, policy := range []writerConfig{
			{Layout: "served", KeyEncoding: "dict", IntegerEncoding: "dict", PageStatistics: true, PageRows: 16, GroupRows: 40},
			{Layout: "served", KeyEncoding: "delta", IntegerEncoding: "delta", PageStatistics: false, PageRows: 16, GroupRows: 64, GroupBytes: 32 << 10, SortMode: "versions"},
			{Layout: "direct", KeyEncoding: "dict", IntegerEncoding: "delta", PageStatistics: true, PageRows: 32, GroupRows: 64, GroupBytes: 1, SortMode: "none"},
			{Layout: "direct", KeyEncoding: "delta", IntegerEncoding: "delta", PageStatistics: true, PageRows: 16, GroupRows: 64, SortMode: "none", SinglePageOrder: "ascending"},
			{Layout: "served", KeyEncoding: "delta", IntegerEncoding: "delta", PageStatistics: false, PageRows: 16, GroupRows: 64, ETagEncoding: "delta"},
		} {
			t.Run(fmt.Sprintf("%s/%s/%s/%s/stats=%t/bytes=%d", corpus.name, policy.Layout, policy.KeyEncoding, policy.IntegerEncoding, policy.PageStatistics, policy.GroupBytes), func(t *testing.T) {
				var structure bytes.Buffer
				policy.RowAPI = "struct"
				structResult, err := encodeRows(&structure, corpus.rows, policy)
				if err != nil {
					t.Fatal(err)
				}
				for _, api := range []string{"values", "columns"} {
					t.Run(api, func(t *testing.T) {
						policy.RowAPI = api
						checkPublicAPIParity(t, corpus.rows, policy, structure.Bytes(), structResult)
					})
				}
			})
		}
	}
}

func TestExplicitDeltaETagPreservesNullableSchemaAndOtherEncodings(t *testing.T) {
	rows := valueParityRows()
	config := writerConfig{
		Layout: "served", KeyEncoding: "delta", IntegerEncoding: "delta", PageStatistics: false,
		PageRows: 16, GroupRows: 64, RowAPI: "columns", ETagEncoding: "delta",
	}
	path := filepath.Join(t.TempDir(), "delta-etag.parquet")
	if _, err := writePart(path, rows, config, diskOps()); err != nil {
		t.Fatal(err)
	}
	if _, err := verifyPart(path, rows); err != nil {
		t.Fatal(err)
	}
	data := mustReadFile(t, path)
	file, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	for _, group := range file.Metadata().RowGroups {
		for _, column := range []int{etagColumn, storageClassColumn, versionColumn, ownerColumn, ownerNameColumn, checksumAlgorithmColumn, checksumTypeColumn} {
			metadata := group.Columns[column].MetaData
			var header format.PageHeader
			reader := bytes.NewReader(data[metadata.DataPageOffset:])
			if err := thrift.NewDecoder((&thrift.CompactProtocol{}).NewReader(reader)).Decode(&header); err != nil {
				t.Fatal(err)
			}
			want := format.RLEDictionary
			if column == etagColumn {
				want = format.DeltaByteArray
				if metadata.DictionaryPageOffset != 0 {
					t.Fatal("delta ETag unexpectedly acquired a dictionary")
				}
			}
			if !header.DataPageHeaderV2.Valid || header.DataPageHeaderV2.V.Encoding != want {
				t.Fatalf("column %s first page encoding=%s expected=%s", metadata.PathInSchema, header.DataPageHeaderV2.V.Encoding, want)
			}
		}
	}
}

func TestSinglePageIndexerPreservesBoundsAndMultiPageOrders(t *testing.T) {
	for _, sequence := range [][]string{
		nil,
		{"a"},
		{"a", "b"},
		{"b", "a"},
		{"a", "c", "b"},
	} {
		base := parquet.String().Type().NewColumnIndexer(1024)
		normalized := singlePageAscendingType{parquet.String().Type()}.NewColumnIndexer(1024)
		for _, key := range sequence {
			value := parquet.ValueOf(key)
			base.IndexPage(1, 0, value, value)
			normalized.IndexPage(1, 0, value, value)
		}
		original, actual := base.ColumnIndex(), normalized.ColumnIndex()
		if len(sequence) == 1 {
			if actual.BoundaryOrder != format.Ascending || original.BoundaryOrder != format.Unordered {
				t.Fatalf("single page order: original=%s actual=%s", original.BoundaryOrder, actual.BoundaryOrder)
			}
			actual.BoundaryOrder = original.BoundaryOrder
		}
		if !reflect.DeepEqual(original, actual) {
			t.Fatalf("index arrays, bounds or multipage order changed for %v", sequence)
		}
		normalized.Reset()
		if len(normalized.ColumnIndex().NullPages) != 0 {
			t.Fatal("index reset did not preserve empty state")
		}
	}
}

func TestSinglePageOrderChangesOnlyKeyIndex(t *testing.T) {
	rows := probeRows(1)
	config := writerConfig{
		Layout: "direct", KeyEncoding: "delta", IntegerEncoding: "dict", PageStatistics: true,
		PageRows: 16, GroupRows: 40, SortMode: "none", SinglePageOrder: "ascending",
	}
	path := filepath.Join(t.TempDir(), "normalized.parquet")
	if _, err := writePart(path, rows, config, diskOps()); err != nil {
		t.Fatal(err)
	}
	report, err := verifyPart(path, rows)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Stamps) != 0 {
		t.Fatal("direct normalization introduced a sorted stamp")
	}
	for column, geometry := range report.RowGroups[0].Columns {
		want := "UNORDERED"
		if column == 0 {
			want = "ASCENDING"
		}
		if geometry.BoundsOrder != want {
			t.Fatalf("column %s order=%s expected=%s", geometry.Name, geometry.BoundsOrder, want)
		}
	}
	config.KeyEncoding = "dict"
	created := false
	ops := fileOps{create: func(string) (durableFile, error) { created = true; return nil, io.ErrClosedPipe }}
	if result, err := writePart("unused", rows, config, ops); err == nil || created || !reflect.DeepEqual(result, partResult{}) {
		t.Fatalf("unsupported dictionary normalization reached file creation: result=%+v created=%t error=%v", result, created, err)
	}
}

func checkPublicAPIParity(t *testing.T, rows []Row, config writerConfig, reference []byte, referenceResult encodeResult) {
	t.Helper()
	var candidate bytes.Buffer
	encoded, err := encodeRows(&candidate, rows, config)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(reference, candidate.Bytes()) {
		t.Fatalf("public %s API changed Parquet bytes: struct=%d candidate=%d", config.RowAPI, len(reference), candidate.Len())
	}
	if !reflect.DeepEqual(referenceResult.Groups, encoded.Groups) {
		t.Fatalf("public %s API changed flush gates: %v vs %v", config.RowAPI, referenceResult.Groups, encoded.Groups)
	}
	path := filepath.Join(t.TempDir(), config.RowAPI+".parquet")
	part, err := writePart(path, rows, config, diskOps())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(candidate.Bytes(), mustReadFile(t, path)) {
		t.Fatal("durable output differs from byte-identical reference")
	}
	geometry, err := verifyPart(path, rows)
	if err != nil {
		t.Fatal(err)
	}
	if err := checkGeometry(geometry, config); err != nil {
		t.Fatal(err)
	}
	if err := checkFlushReports(geometry, part.Encoding, config); err != nil {
		t.Fatal(err)
	}
	file, err := parquet.OpenFile(bytes.NewReader(candidate.Bytes()), int64(candidate.Len()))
	if err != nil {
		t.Fatal(err)
	}
	if file.NumRows() != int64(len(rows)) {
		t.Fatal("public API reported the wrong file row count")
	}
	for _, group := range file.Metadata().RowGroups {
		for _, column := range group.Columns {
			if column.MetaData.NumValues != group.NumRows {
				t.Fatalf("column %s values=%d, group rows=%d", column.MetaData.PathInSchema, column.MetaData.NumValues, group.NumRows)
			}
		}
	}
}

func TestColumnAPIPreservesAutomaticBytePageGeometry(t *testing.T) {
	// This wide direct-layout input crosses the 1 MiB page buffer before its
	// 20,000-row cap, making a lost 64-row write quantum observable on disk.
	rows := byteTargetRows(2177)
	config := writerConfig{
		Layout: "direct", KeyEncoding: "delta", IntegerEncoding: "delta", PageStatistics: false,
		PageRows: 20000, GroupRows: 4096, RowAPI: "struct",
	}
	var reference bytes.Buffer
	encoded, err := encodeRows(&reference, rows, config)
	if err != nil {
		t.Fatal(err)
	}
	file, err := parquet.OpenFile(bytes.NewReader(reference.Bytes()), int64(reference.Len()))
	if err != nil {
		t.Fatal(err)
	}
	index, err := file.RowGroups()[0].ColumnChunks()[0].OffsetIndex()
	if err != nil || index.NumPages() < 2 {
		t.Fatalf("fixture did not engage automatic byte pages: index=%v error=%v", index, err)
	}
	for _, api := range []string{"values", "columns"} {
		t.Run(api, func(t *testing.T) {
			config.RowAPI = api
			checkPublicAPIParity(t, rows, config, reference.Bytes(), encoded)
		})
	}
}

func mustReadFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestValuesAPIOutputFailuresReturnNoMeasurements(t *testing.T) {
	rows := valueParityRows()
	for _, api := range []string{"values", "columns"} {
		t.Run(api, func(t *testing.T) {
			config := writerConfig{
				Layout: "served", KeyEncoding: "dict", IntegerEncoding: "dict", PageStatistics: true,
				PageRows: 16, GroupRows: 40, RowAPI: api,
			}
			_, cuts := referenceCuts(t, rows, config)
			for _, cut := range cuts {
				t.Run(cut.name, func(t *testing.T) {
					var partial bytes.Buffer
					failure := &prefixFailure{output: &partial, cut: cut.offset, err: io.ErrClosedPipe}
					result, err := encodeRows(failure, rows, config)
					if err == nil || !reflect.DeepEqual(result, encodeResult{}) {
						t.Fatalf("failed %s output returned measurements: %+v, %v", api, result, err)
					}
				})
			}
		})
	}
}

var benchmarkValueRows []parquet.Row

func BenchmarkFillCanonicalValues(b *testing.B) {
	input := valueParityRows()
	batch := newRowValues(len(input))
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		benchmarkValueRows = batch.fill(input)
	}
}

func TestByteTargetLowLimitOvershootAndRowCeiling(t *testing.T) {
	rows := byteTargetRows(129)
	config := writerConfig{
		Layout: "served", KeyEncoding: "dict", IntegerEncoding: "dict",
		PageStatistics: true, PageRows: 16, GroupRows: 64, GroupBytes: 1,
	}
	path := filepath.Join(t.TempDir(), "part.parquet")
	part, err := writePart(path, rows, config, diskOps())
	if err != nil {
		t.Fatal(err)
	}
	report, err := verifyPart(path, rows)
	if err != nil {
		t.Fatal(err)
	}
	if err := checkFlushReports(report, part.Encoding, config); err != nil {
		t.Fatal(err)
	}
	if len(part.Encoding.Groups) != 9 {
		t.Fatalf("low target produced %d groups, expected one per batch", len(part.Encoding.Groups))
	}
	for group, gate := range part.Encoding.Groups {
		if gate.Reason != flushByteTarget || gate.PreviousPageEstimatedBytes != 0 || gate.TargetOvershootBytes != gate.EstimatedBufferBytes-1 || gate.Rows > 16 {
			t.Fatalf("group %d did not capture single-batch overshoot: %+v", group, gate)
		}
		if group > 0 && gate.SizeBaselineBytes <= part.Encoding.Groups[group-1].SizeBaselineBytes {
			t.Fatal("post-Flush baseline did not advance across preceding groups")
		}
	}
	config.GroupBytes = 1 << 60
	part, err = writePart(filepath.Join(t.TempDir(), "high.parquet"), rows, config, diskOps())
	if err != nil {
		t.Fatal(err)
	}
	if got := part.Encoding.Groups; len(got) != 3 || got[0].Rows != 64 || got[0].Reason != flushRowLimit || got[1].Rows != 64 || got[1].Reason != flushRowLimit || got[2].Rows != 1 || got[2].Reason != flushEnd {
		t.Fatalf("high target changed row ceiling behavior: %+v", got)
	}
}

func TestByteTargetFlushFailuresReturnNoMeasurements(t *testing.T) {
	config := writerConfig{
		Layout: "served", KeyEncoding: "dict", IntegerEncoding: "dict",
		PageStatistics: true, PageRows: 16, GroupRows: 64, GroupBytes: 1,
	}
	if result, err := encodeRows(io.Discard, nil, config); err != nil || len(result.Groups) != 0 {
		t.Fatalf("empty input emitted a group: %+v, %v", result, err)
	}
	config.GroupBytes = -1
	var output bytes.Buffer
	if result, err := encodeRows(&output, byteTargetRows(17), config); err == nil || !reflect.DeepEqual(result, encodeResult{}) || output.Len() != 0 {
		t.Fatal("negative target accepted or emitted output")
	}
	config.GroupBytes = 1
	rows := byteTargetRows(129)
	var reference bytes.Buffer
	if _, err := encodeRows(&reference, rows, config); err != nil {
		t.Fatal(err)
	}
	file, err := parquet.OpenFile(bytes.NewReader(reference.Bytes()), int64(reference.Len()))
	if err != nil {
		t.Fatal(err)
	}
	if len(file.RowGroups()) < 2 {
		t.Fatal("fixture did not create multiple byte-target groups")
	}
	// Fail inside a later physically emitted group, after earlier byte gates
	// succeeded, and independently in the final trailer.
	cuts := []int64{file.Metadata().RowGroups[1].Columns[0].MetaData.DataPageOffset + 1, int64(reference.Len() - 2)}
	for _, cut := range cuts {
		var partial bytes.Buffer
		failure := &prefixFailure{output: &partial, cut: cut, err: io.ErrClosedPipe}
		result, err := encodeRows(failure, rows, config)
		if err == nil || !reflect.DeepEqual(result, encodeResult{}) {
			t.Fatalf("failed byte target output returned measurements: %+v, %v", result, err)
		}
	}
}

func TestFlushReportMustMatchActualGroups(t *testing.T) {
	rows := byteTargetRows(129)
	config := writerConfig{
		Layout: "served", KeyEncoding: "dict", IntegerEncoding: "dict",
		PageStatistics: true, PageRows: 16, GroupRows: 64, GroupBytes: 1,
	}
	part, err := writePart(filepath.Join(t.TempDir(), "part.parquet"), rows, config, diskOps())
	if err != nil {
		t.Fatal(err)
	}
	report, err := inspectPart(part.Path, rows)
	if err != nil {
		t.Fatal(err)
	}
	wrong := part.Encoding
	wrong.Groups = append([]groupFlushReport(nil), part.Encoding.Groups...)
	wrong.Groups[0].Rows++
	if err := checkFlushReports(report, wrong, config); err == nil {
		t.Fatal("flush report accepted incorrect physical group rows")
	}
	wrong = part.Encoding
	wrong.Groups = wrong.Groups[:len(wrong.Groups)-1]
	if err := checkFlushReports(report, wrong, config); err == nil {
		t.Fatal("flush report accepted missing physical group")
	}
}

func TestDirectOutputOmitsSortedStampAndExplicitVersionMode(t *testing.T) {
	rows := probeRows(17)
	for i := range rows {
		rows[i].VersionID, rows[i].IsLatest = nil, nil
	}
	config := writerConfig{
		Layout: "direct", KeyEncoding: "dict", IntegerEncoding: "dict",
		PageStatistics: true, PageRows: 16, GroupRows: 64, SortMode: "none",
	}
	path := filepath.Join(t.TempDir(), "direct.parquet")
	part, err := writePart(path, rows, config, diskOps())
	if err != nil {
		t.Fatal(err)
	}
	report, err := verifyPart(path, rows)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Stamps) != 0 {
		t.Fatalf("direct part has sorted footer keys: %v", report.Stamps)
	}
	if err := checkFlushReports(report, part.Encoding, config); err != nil {
		t.Fatal(err)
	}
	config.SortMode = "versions"
	path = filepath.Join(t.TempDir(), "explicit-versions.parquet")
	if _, err := writePart(path, rows, config, diskOps()); err != nil {
		t.Fatal(err)
	}
	report, err = verifyPart(path, rows)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Stamps) != 5 || report.Stamps["swath.sort.mode"] != "versions" {
		t.Fatalf("explicit mode was inferred from part contents instead: %v", report.Stamps)
	}
}

func TestRawCanonicalSchema(t *testing.T) {
	var output bytes.Buffer
	config := writerConfig{Layout: "served", KeyEncoding: "dict", IntegerEncoding: "dict", PageStatistics: true, PageRows: 16, GroupRows: 40}
	if _, err := encodeRows(&output, probeRows(1), config); err != nil {
		t.Fatal(err)
	}
	file, err := parquet.OpenFile(bytes.NewReader(output.Bytes()), int64(output.Len()))
	if err != nil {
		t.Fatal(err)
	}
	schema := file.Metadata().Schema
	if len(schema) != 14 || schema[0].Name != "swath_listing" {
		t.Fatalf("schema = %+v", schema)
	}
	names := []string{"key", "size", "last_modified", "etag", "storage_class", "version_id", "is_latest", "is_delete_marker", "owner_id", "owner_display_name", "checksum_algorithm", "checksum_type", "row_type"}
	for index, name := range names {
		field := schema[index+1]
		if field.Name != name {
			t.Fatalf("column %d is %s, expected %s", index, field.Name, name)
		}
		repetition := format.Optional
		if index == 0 || index == 7 || index == 12 {
			repetition = format.Required
		}
		if !field.RepetitionType.Valid || field.RepetitionType.V != repetition {
			t.Fatalf("column %s repetition = %+v", name, field.RepetitionType)
		}
		physical := format.ByteArray
		switch index {
		case 1, 2:
			physical = format.Int64
		case 6, 7:
			physical = format.Boolean
		}
		if !field.Type.Valid || field.Type.V != physical {
			t.Fatalf("column %s physical type = %+v", name, field.Type)
		}
		if physical == format.ByteArray {
			if _, ok := field.LogicalType.Value.(*format.StringType); !ok {
				t.Fatalf("column %s lacks STRING annotation", name)
			}
		}
	}
	stamp, ok := schema[3].LogicalType.Value.(*format.TimestampType)
	if !ok || !stamp.IsAdjustedToUTC {
		t.Fatalf("timestamp annotation = %+v", schema[3].LogicalType)
	}
	if _, ok := stamp.Unit.Value.(*format.MicroSeconds); !ok {
		t.Fatalf("timestamp unit = %+v", stamp.Unit)
	}
	if schema[2].LogicalType.Value != nil || schema[2].ConvertedType.Valid {
		t.Fatalf("size must be unannotated INT64, got logical=%+v converted=%+v", schema[2].LogicalType, schema[2].ConvertedType)
	}
}

func TestSinglePageAndEmptyOutput(t *testing.T) {
	config := writerConfig{Layout: "served", KeyEncoding: "dict", IntegerEncoding: "dict", PageStatistics: true, PageRows: 16, GroupRows: 40}
	for _, rows := range [][]Row{nil, probeRows(1)} {
		path := filepath.Join(t.TempDir(), "part.parquet")
		part, err := writePart(path, rows, config, diskOps())
		if err != nil {
			t.Fatal(err)
		}
		report, err := verifyPart(path, rows)
		if err != nil {
			t.Fatal(err)
		}
		if report.Rows != int64(len(rows)) {
			t.Fatalf("rows = %d", report.Rows)
		}
		if len(rows) == 0 {
			if part.MinKey != nil || part.MaxKey != nil || len(report.RowGroups) != 0 {
				t.Fatalf("empty file metadata = %+v", part)
			}
		} else if report.RowGroups[0].Columns[0].BoundsOrder != "UNORDERED" {
			t.Fatal("stock single-page boundary order changed; recheck Java compatibility")
		}
	}
}

func TestInvalidKeysFailBeforeOutput(t *testing.T) {
	config := writerConfig{Layout: "served", KeyEncoding: "dict", IntegerEncoding: "dict", PageStatistics: true, PageRows: 16, GroupRows: 40}
	for _, rows := range [][]Row{
		{{Key: strings.Repeat("x", 1025), RowType: "OBJECT"}},
		{{Key: "\xff", RowType: "OBJECT"}},
		{{Key: "b", RowType: "OBJECT"}, {Key: "a", RowType: "OBJECT"}},
	} {
		var output bytes.Buffer
		result, err := encodeRows(&output, rows, config)
		if err == nil || !reflect.DeepEqual(result, encodeResult{}) || output.Len() != 0 {
			t.Fatalf("invalid rows accepted: %+v, %v, %d bytes", result, err, output.Len())
		}
	}
}
