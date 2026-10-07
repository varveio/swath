// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/encoding"
)

const sortOrder = "key_bytes_unsigned,version_id_null_first,row_type_rank"

type writerConfig struct {
	Layout          string `json:"layout"`
	KeyEncoding     string `json:"key_encoding"`
	IntegerEncoding string `json:"integer_encoding"`
	PageStatistics  bool   `json:"page_statistics"`
	PageRows        int    `json:"page_rows"`
	GroupRows       int    `json:"group_rows"`
	GroupBytes      int64  `json:"group_bytes"`
	SortMode        string `json:"sort_mode"`
	RowAPI          string `json:"row_api"`
	Codec           string `json:"codec"`
	SinglePageOrder string `json:"single_page_order"`
	ETagEncoding    string `json:"etag_encoding"`
}

func (config writerConfig) validate() error {
	if config.Layout != "served" && config.Layout != "direct" {
		return errors.New("layout must be served or direct")
	}
	if config.KeyEncoding != "dict" && config.KeyEncoding != "delta" {
		return errors.New("key encoding must be dict or delta")
	}
	if config.IntegerEncoding != "dict" && config.IntegerEncoding != "delta" {
		return errors.New("integer encoding must be dict or delta")
	}
	if config.PageRows <= 0 || config.GroupRows <= 0 {
		return errors.New("page and group row limits must be positive")
	}
	if config.GroupBytes < 0 {
		return errors.New("group byte target must not be negative")
	}
	switch config.SortMode {
	case "", "auto", "objects", "versions", "none":
	default:
		return errors.New("sort mode must be auto, objects, versions or none")
	}
	switch config.RowAPI {
	case "", "struct", "values", "columns":
	default:
		return errors.New("row API must be struct, values or columns")
	}
	switch config.Codec {
	case "", "go", "go-entropy", "native":
	default:
		return errors.New("codec must be go, go-entropy or native")
	}
	switch config.SinglePageOrder {
	case "", "library":
	case "ascending":
		if config.KeyEncoding != "delta" {
			return errors.New("ascending single-page order requires delta key encoding; dictionary construction replaces the public indexer type")
		}
	default:
		return errors.New("single-page order must be library or ascending")
	}
	switch config.ETagEncoding {
	case "", "dict", "delta":
	default:
		return errors.New("ETag encoding must be dict or delta")
	}
	return nil
}

func (config writerConfig) dictionaryBytes() int64 {
	if config.Layout == "served" {
		return 8 << 10
	}
	return 1 << 20
}

func (config writerConfig) boundBytes() int {
	if config.Layout == "served" {
		return 1024
	}
	return 64
}

func (config writerConfig) schema() *parquet.Schema {
	// Encoding tags are writer-only; the physical/logical schema stays canonical.
	key := "key,dict"
	if config.KeyEncoding == "delta" {
		key = "key,delta"
	}
	tag := reflect.StructTag(fmt.Sprintf("parquet:%q", key))
	root := parquet.SchemaOf(new(Row), parquet.StructTag(tag, "Key"))
	return parquet.NewSchema("swath_listing", canonicalRoot{
		Node: root, singlePageAscending: config.SinglePageOrder == "ascending", deltaETag: config.ETagEncoding == "delta",
	})
}

type memoryReport struct {
	HeapBefore uint64 `json:"heap_before_bytes"`
	HeapAfter  uint64 `json:"heap_after_bytes"`
	Allocated  uint64 `json:"allocated_bytes"`
	GCs        uint32 `json:"gc_cycles"`
}

type encodeResult struct {
	Encode time.Duration      `json:"encode_ns"`
	Footer time.Duration      `json:"footer_ns"`
	Memory memoryReport       `json:"memory"`
	Groups []groupFlushReport `json:"group_flushes"`
}

type groupFlushReason string

const (
	flushByteTarget groupFlushReason = "byte_target"
	flushRowLimit   groupFlushReason = "row_limit"
	flushEnd        groupFlushReason = "end"
)

// EstimatedBufferBytes is parquet-go's hybrid payload estimate after column
// pages flush: compressed pages/headers, current column values, raw dictionary
// values and filters. It excludes earlier groups via SizeBaselineBytes, and is
// neither Java's exact accounting nor an uncompressed-byte or heap bound.
type groupFlushReport struct {
	Rows                       int64            `json:"rows"`
	Reason                     groupFlushReason `json:"reason"`
	EstimatedBufferBytes       int64            `json:"estimated_buffer_bytes"`
	SizeBaselineBytes          int64            `json:"size_baseline_bytes"`
	PreviousPageEstimatedBytes int64            `json:"previous_page_estimated_bytes"`
	LastPageGrowthBytes        int64            `json:"last_page_growth_bytes"`
	LastPageRows               int64            `json:"last_page_rows"`
	TargetOvershootBytes       int64            `json:"target_overshoot_bytes"`
}

// encodeRows includes writer construction, page encoding and explicit group
// flushes in Encode. Footer includes Close and codec release after rows flush.
// It never closes or syncs output. A write failure is terminal; Close is not
// retried because parquet-go does not support continuing after a write error.
func encodeRows(output io.Writer, rows []Row, config writerConfig) (result encodeResult, err error) {
	if err := config.validate(); err != nil {
		return encodeResult{}, err
	}
	if err := validateRows(rows); err != nil {
		return encodeResult{}, err
	}
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	mode := config.SortMode
	if mode == "" || mode == "auto" {
		mode = "objects"
		for _, row := range rows {
			if row.VersionID != nil || row.RowType == "DELETE_MARKER" {
				mode = "versions"
				break
			}
		}
	}
	start := time.Now()
	codec, cleanup, err := newWriterCodec(config.Codec)
	if err != nil {
		return encodeResult{}, err
	}
	defer finishCodec(&result, &err, cleanup)
	var integerEncoding encoding.Encoding = &parquet.RLEDictionary
	if config.IntegerEncoding == "delta" {
		integerEncoding = &parquet.DeltaBinaryPacked
	}
	options := []parquet.WriterOption{
		config.schema(),
		parquet.Compression(codec),
		parquet.DataPageVersion(2),
		parquet.DataPageStatistics(config.PageStatistics),
		parquet.PageBufferSize(1 << 20),
		parquet.WriteBufferSize(4096),
		parquet.MaxRowsPerRowGroup(int64(config.GroupRows)),
		parquet.DictionaryMaxBytes(config.dictionaryBytes()),
		parquet.ColumnIndexSizeLimit(func([]string) int { return config.boundBytes() }),
		parquet.DefaultEncodingFor(parquet.ByteArray, &parquet.RLEDictionary),
		parquet.DefaultEncodingFor(parquet.Int64, integerEncoding),
		parquet.DefaultEncodingFor(parquet.Boolean, &parquet.Plain),
	}
	if mode != "none" {
		options = append(options,
			parquet.KeyValueMetadata("swath.sort.order", sortOrder),
			parquet.KeyValueMetadata("swath.sort.mode", mode),
			parquet.KeyValueMetadata("swath.sort.format_version", "1"),
			parquet.KeyValueMetadata("swath.sort.file_index", "1"),
			parquet.KeyValueMetadata("swath.sort.file_final", "true"),
		)
	}
	writer := parquet.NewGenericWriter[Row](output, options...)
	var batch *rowValues
	var columns *columnValues
	if config.RowAPI == "values" {
		batch = newRowValues(min(config.PageRows, config.GroupRows, len(rows)))
	} else if config.RowAPI == "columns" {
		// Direct columns bypass the generic writer's automatic row counter.
		// This loop enforces the row ceiling; the footer counts column zero.
		columns = newColumnValues(min(config.PageRows, config.GroupRows, len(rows)))
	}
	var groups []groupFlushReport
	baseline := writer.Size()
	for groupStart := 0; groupStart < len(rows); {
		groupEnd := groupStart + min(config.GroupRows, len(rows)-groupStart)
		var previousEstimate int64
		var gate groupFlushReport
		for pageStart := groupStart; pageStart < groupEnd; {
			pageEnd := pageStart + min(config.PageRows, groupEnd-pageStart)
			var n int
			var err error
			if columns != nil {
				columns.fill(rows[pageStart:pageEnd])
				n, err = columns.write(writer.ColumnWriters(), pageEnd-pageStart)
			} else if batch != nil {
				n, err = writer.WriteRows(batch.fill(rows[pageStart:pageEnd]))
			} else {
				n, err = writer.Write(rows[pageStart:pageEnd])
			}
			if err != nil {
				return encodeResult{}, err
			}
			if n != pageEnd-pageStart {
				return encodeResult{}, io.ErrShortWrite
			}
			for _, column := range writer.ColumnWriters() {
				if err := column.Flush(); err != nil {
					return encodeResult{}, err
				}
			}
			estimate := writer.Size() - baseline
			var reason groupFlushReason
			switch {
			case config.GroupBytes > 0 && estimate >= config.GroupBytes:
				reason = flushByteTarget
			case pageEnd-groupStart == config.GroupRows:
				reason = flushRowLimit
			case pageEnd == len(rows):
				reason = flushEnd
			}
			if reason != "" {
				gate = groupFlushReport{
					Rows: int64(pageEnd - groupStart), Reason: reason, EstimatedBufferBytes: estimate,
					SizeBaselineBytes: baseline, PreviousPageEstimatedBytes: previousEstimate,
					LastPageGrowthBytes: estimate - previousEstimate, LastPageRows: int64(pageEnd - pageStart),
				}
				if config.GroupBytes > 0 {
					gate.TargetOvershootBytes = max(0, estimate-config.GroupBytes)
				}
				break
			}
			previousEstimate = estimate
			pageStart = pageEnd
		}
		if err := writer.Flush(); err != nil {
			return encodeResult{}, err
		}
		groups = append(groups, gate)
		// Size includes preceding groups and bytes accepted by the transport
		// buffer. Capture its own post-Flush baseline, rather than output bytes.
		baseline = writer.Size()
		groupStart += int(gate.Rows)
	}
	encoded := time.Since(start)
	start = time.Now()
	if err := writer.Close(); err != nil {
		return encodeResult{}, err
	}
	footer := time.Since(start)
	runtime.ReadMemStats(&after)
	return encodeResult{Encode: encoded, Footer: footer, Groups: groups, Memory: memoryReport{
		HeapBefore: before.HeapAlloc, HeapAfter: after.HeapAlloc,
		Allocated: after.TotalAlloc - before.TotalAlloc, GCs: after.NumGC - before.NumGC,
	}}, nil
}

type durableFile interface {
	io.Writer
	Sync() error
	Close() error
}

type fileOps struct {
	create     func(string) (durableFile, error)
	syncParent func(string) error
}

func diskOps() fileOps {
	return fileOps{
		create: func(path string) (durableFile, error) {
			return os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
		},
		syncParent: func(path string) error {
			parent, err := os.Open(filepath.Dir(path))
			if err != nil {
				return err
			}
			return errors.Join(parent.Sync(), parent.Close())
		},
	}
}

type partResult struct {
	Path         string        `json:"path"`
	Bytes        int64         `json:"bytes"`
	MD5          string        `json:"md5"`
	Rows         int64         `json:"rows"`
	MinKey       *string       `json:"min_key"`
	MaxKey       *string       `json:"max_key"`
	LogicalBytes int64         `json:"logical_bytes"`
	Encoding     encodeResult  `json:"encoding"`
	FileOpen     time.Duration `json:"file_open_ns"`
	FileSync     time.Duration `json:"file_sync_ns"`
	FileClose    time.Duration `json:"file_close_ns"`
	ParentSync   time.Duration `json:"parent_sync_ns"`
}

// writePart returns metadata only after footer, file sync, file close and
// parent sync succeed. It leaves failed output for inspection, never publishes
// a dataset, and calls each acquired file's Close exactly once.
func writePart(path string, rows []Row, config writerConfig, ops fileOps) (result partResult, err error) {
	if err := config.validate(); err != nil {
		return partResult{}, err
	}
	if err := validateRows(rows); err != nil {
		return partResult{}, err
	}
	payloadBytes := logicalBytes(rows)
	start := time.Now()
	file, err := ops.create(path)
	if err != nil {
		return partResult{}, err
	}
	fileOpen := time.Since(start)
	closed := false
	defer func() {
		if !closed {
			err = errors.Join(err, file.Close())
		}
		if err != nil {
			result = partResult{}
		}
	}()
	tracked := &trackingWriter{output: file, digest: md5.New()}
	encoded, err := encodeRows(tracked, rows, config)
	if err != nil {
		return partResult{}, fmt.Errorf("encode part: %w", err)
	}
	start = time.Now()
	if err := file.Sync(); err != nil {
		return partResult{}, fmt.Errorf("sync part: %w", err)
	}
	fileSync := time.Since(start)
	start = time.Now()
	closed = true
	if err := file.Close(); err != nil {
		return partResult{}, fmt.Errorf("close part: %w", err)
	}
	fileClose := time.Since(start)
	start = time.Now()
	if err := ops.syncParent(path); err != nil {
		return partResult{}, fmt.Errorf("sync parent: %w", err)
	}
	result = partResult{
		Path: path, Bytes: tracked.bytes, MD5: hex.EncodeToString(tracked.digest.Sum(nil)),
		Rows: int64(len(rows)), LogicalBytes: payloadBytes, Encoding: encoded,
		FileOpen: fileOpen, FileSync: fileSync, FileClose: fileClose, ParentSync: time.Since(start),
	}
	if len(rows) > 0 {
		result.MinKey = &rows[0].Key
		result.MaxKey = &rows[len(rows)-1].Key
	}
	return result, nil
}

type trackingWriter struct {
	output io.Writer
	digest hash.Hash
	bytes  int64
}

func (writer *trackingWriter) Write(data []byte) (int, error) {
	n, err := writer.output.Write(data)
	if n > 0 {
		_, _ = writer.digest.Write(data[:n])
		writer.bytes += int64(n)
	}
	if n != len(data) && err == nil {
		err = io.ErrShortWrite
	}
	return n, err
}
