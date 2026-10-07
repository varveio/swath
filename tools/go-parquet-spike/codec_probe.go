// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/zstd"
	"github.com/parquet-go/parquet-go/encoding/thrift"
	"github.com/parquet-go/parquet-go/format"
)

type codecBlock struct {
	Column        string `json:"column"`
	Kind          string `json:"kind"`
	Encoding      string `json:"encoding"`
	Data          []byte `json:"data"`
	ExpectedBytes int    `json:"expected_bytes"`
	LevelBytes    int    `json:"excluded_level_bytes"`
	SourceBytes   int    `json:"source_value_bytes"`
	Compressed    bool   `json:"source_compressed"`
	PageOffset    int64  `json:"page_offset"`
	HeaderBytes   int    `json:"page_header_bytes"`
}

type codecColumn struct {
	Blocks          int      `json:"blocks"`
	DataPages       int      `json:"data_pages"`
	DictionaryPages int      `json:"dictionary_pages"`
	RawBytes        int64    `json:"raw_bytes"`
	LevelBytes      int64    `json:"excluded_level_bytes"`
	SourceBytes     int64    `json:"source_value_bytes"`
	Encodings       []string `json:"encodings"`
	FooterEncodings []string `json:"footer_encodings"`
	SourceRawBlocks int      `json:"source_uncompressed_blocks"`
}

func runCodecProbe(args []string, output io.Writer) error {
	flags := flag.NewFlagSet("codec-probe", flag.ContinueOnError)
	mode := flags.String("mode", "bench", "extract, bench, verify or self-check")
	input := flags.String("parquet", "", "source Parquet for extraction")
	blocks := flags.String("blocks", "", "JSONL uncompressed encoded payloads")
	compressed := flags.String("compressed", "", "JSONL frames to cross-decode")
	encodedOutput := flags.String("compressed-output", "", "JSONL resulting Zstd frames")
	warmup := flags.Int("warmup", 10, "warmup iterations outside clocks")
	iterations := flags.Int("iterations", 20, "measured iterations")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if (*mode != "self-check" && *blocks == "") || flags.NArg() != 0 || *warmup < 0 || *iterations <= 0 {
		return errors.New("codec-probe requires blocks, nonnegative warmup and positive iterations")
	}
	switch *mode {
	case "self-check":
		return checkCodecExtraction(output)
	case "extract":
		if *input == "" {
			return errors.New("extraction requires parquet")
		}
		return extractCodecBlocks(*input, *blocks, output)
	case "bench":
		return benchmarkCodec(*blocks, *encodedOutput, *warmup, *iterations, output)
	case "verify":
		return verifyCodecFrames(*blocks, *compressed, output)
	default:
		return fmt.Errorf("unknown codec-probe mode %q", *mode)
	}
}

// checkCodecExtraction constructs standard Parquet-v2 page frames with public
// Thrift and Zstd APIs. It independently expects the value bytes, and requires
// incorrect declared lengths and level boundaries to fail before benchmarking.
func checkCodecExtraction(output io.Writer) error {
	var raw bytes.Buffer
	for _, value := range []string{"alpha", "beta"} {
		if err := binary.Write(&raw, binary.LittleEndian, uint32(len(value))); err != nil {
			return err
		}
		raw.WriteString(value)
	}
	codec := &zstd.Codec{Level: zstd.SpeedDefault, Concurrency: 1}
	encoded, err := codec.Encode(nil, raw.Bytes())
	if err != nil {
		return err
	}
	levels := []byte{3, 5} // RLE bit-packed group: definition levels 1,0,1.
	body := append(append([]byte(nil), levels...), encoded...)
	cases := []struct {
		name   string
		mutate func(*format.PageHeader)
	}{
		{name: "valid"},
		{name: "wrong decoded length", mutate: func(h *format.PageHeader) { h.UncompressedPageSize++ }},
		{name: "wrong stored length", mutate: func(h *format.PageHeader) { h.CompressedPageSize++ }},
		{name: "wrong level boundary", mutate: func(h *format.PageHeader) { h.DataPageHeaderV2.V.DefinitionLevelsByteLength++ }},
	}
	for _, test := range cases {
		header := format.PageHeader{Type: format.DataPageV2,
			UncompressedPageSize: int32(len(levels) + raw.Len()), CompressedPageSize: int32(len(body)),
			CRC: int32(crc32.ChecksumIEEE(body)),
			DataPageHeaderV2: thrift.New(format.DataPageHeaderV2{NumValues: 3, NumNulls: 1, NumRows: 3,
				Encoding: format.Plain, DefinitionLevelsByteLength: int32(len(levels)), IsCompressed: thrift.New(true)})}
		if test.mutate != nil {
			test.mutate(&header)
		}
		prefix, err := thrift.Marshal(&thrift.CompactProtocol{}, &header)
		if err != nil {
			return err
		}
		page := append(prefix, body...)
		file := append([]byte("PAR1"), page...)
		block, err := extractCodecBlock(bytes.NewReader(file), int64(len(file)), 4, len(page), "test", "data_v2", codec)
		if test.mutate == nil {
			if err != nil || !bytes.Equal(block.Data, raw.Bytes()) || block.ExpectedBytes != raw.Len() {
				return fmt.Errorf("valid extracted body differs from independently encoded values: %v", err)
			}
		} else if err == nil {
			return fmt.Errorf("extraction accepted %s", test.name)
		}
	}
	return json.NewEncoder(output).Encode(map[string]any{"verified": true, "cases": len(cases),
		"checks": "standard v2 value round-trip; wrong decoded size, stored size and repetition/definition boundary rejected"})
}

func extractCodecBlocks(path, destination string, report io.Writer) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	parquetFile, err := parquet.OpenFile(file, info.Size())
	if err != nil {
		return err
	}
	dump, err := os.Create(destination)
	if err != nil {
		return err
	}
	defer dump.Close()
	writer := bufio.NewWriter(dump)
	encoder := json.NewEncoder(writer)
	codec := &zstd.Codec{Level: zstd.SpeedDefault, Concurrency: 1}
	columns := make(map[string]*codecColumn)
	var expectedPages, observedPages int
	for groupIndex, group := range parquetFile.RowGroups() {
		for columnIndex, chunk := range group.ColumnChunks() {
			metadata := parquetFile.Metadata().RowGroups[groupIndex].Columns[columnIndex].MetaData
			if metadata.Codec != format.Zstd {
				return fmt.Errorf("column %v has unsupported source codec %v", metadata.PathInSchema, metadata.Codec)
			}
			name := strings.Join(metadata.PathInSchema, ".")
			stats := columns[name]
			if stats == nil {
				stats = &codecColumn{}
				columns[name] = stats
			}
			for _, encoding := range metadata.Encoding {
				if !containsEncoding(stats.FooterEncodings, encoding.String()) {
					stats.FooterEncodings = append(stats.FooterEncodings, encoding.String())
				}
			}
			appendBlock := func(offset int64, length int, kind string) error {
				block, err := extractCodecBlock(file, info.Size(), offset, length, name, kind, codec)
				if err != nil {
					return err
				}
				if err := encoder.Encode(block); err != nil {
					return err
				}
				stats.Blocks++
				stats.RawBytes += int64(len(block.Data))
				stats.LevelBytes += int64(block.LevelBytes)
				stats.SourceBytes += int64(block.SourceBytes)
				if !block.Compressed {
					stats.SourceRawBlocks++
				}
				if kind == "dictionary" {
					stats.DictionaryPages++
				} else {
					stats.DataPages++
					observedPages++
				}
				if !containsEncoding(stats.Encodings, block.Encoding) {
					stats.Encodings = append(stats.Encodings, block.Encoding)
				}
				return nil
			}
			if metadata.DictionaryPageOffset > 0 {
				length := metadata.DataPageOffset - metadata.DictionaryPageOffset
				if length <= 0 || length > info.Size() {
					return errors.New("invalid dictionary page extent")
				}
				if err := appendBlock(metadata.DictionaryPageOffset, int(length), "dictionary"); err != nil {
					return err
				}
			}
			offsets, err := chunk.OffsetIndex()
			if err != nil || offsets == nil {
				return fmt.Errorf("column %s offset index: %w", name, err)
			}
			expectedPages += offsets.NumPages()
			for page := 0; page < offsets.NumPages(); page++ {
				if err := appendBlock(offsets.Offset(page), int(offsets.CompressedPageSize(page)), "data_v2"); err != nil {
					return fmt.Errorf("column %s page %d: %w", name, page, err)
				}
			}
		}
	}
	if err := writer.Flush(); err != nil {
		return err
	}
	if observedPages != expectedPages || expectedPages == 0 {
		return errors.New("extraction did not cover every declared data page")
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	digest := sha256.New()
	if _, err := io.Copy(digest, file); err != nil {
		return err
	}
	return json.NewEncoder(report).Encode(map[string]any{
		"source": path, "source_sha256": hex.EncodeToString(digest.Sum(nil)), "rows": parquetFile.NumRows(),
		"row_groups": len(parquetFile.RowGroups()), "data_pages": observedPages, "columns": columns,
		"coverage": "all OffsetIndex data-v2 value bodies and all dictionary bodies; repetition/definition level prefixes excluded; source and round-trip decoded lengths checked",
	})
}

func containsEncoding(encodings []string, encoding string) bool {
	for _, present := range encodings {
		if present == encoding {
			return true
		}
	}
	return false
}

func extractCodecBlock(file io.ReaderAt, size, offset int64, length int, column, kind string, codec *zstd.Codec) (codecBlock, error) {
	if offset < 4 || length <= 0 || offset > size || int64(length) > size-offset {
		return codecBlock{}, errors.New("page extent exceeds file")
	}
	page := make([]byte, length)
	if _, err := file.ReadAt(page, offset); err != nil {
		return codecBlock{}, err
	}
	reader := (&thrift.CompactProtocol{}).NewReaderFromBytes(page)
	var header format.PageHeader
	if err := thrift.NewDecoder(reader).Decode(&header); err != nil {
		return codecBlock{}, err
	}
	headerBytes := reader.BytesRead()
	if header.CompressedPageSize < 0 || header.UncompressedPageSize < 0 || headerBytes+int(header.CompressedPageSize) != length {
		return codecBlock{}, errors.New("page header sizes disagree with OffsetIndex/column extent")
	}
	body := page[headerBytes:]
	if header.CRC != 0 && uint32(header.CRC) != crc32.ChecksumIEEE(body) {
		return codecBlock{}, errors.New("source page CRC mismatch")
	}
	levels := 0
	compressed := true
	encoding := ""
	if kind == "data_v2" {
		if header.Type != format.DataPageV2 || !header.DataPageHeaderV2.Valid {
			return codecBlock{}, errors.New("expected data-v2 header")
		}
		data := header.DataPageHeaderV2.V
		if data.RepetitionLevelsByteLength < 0 || data.DefinitionLevelsByteLength < 0 {
			return codecBlock{}, errors.New("negative level prefix length")
		}
		levels = int(data.RepetitionLevelsByteLength) + int(data.DefinitionLevelsByteLength)
		if data.IsCompressed.Valid {
			compressed = data.IsCompressed.V
		}
		encoding = data.Encoding.String()
	} else {
		if header.Type != format.DictionaryPage || !header.DictionaryPageHeader.Valid {
			return codecBlock{}, errors.New("expected dictionary header")
		}
		encoding = header.DictionaryPageHeader.V.Encoding.String()
	}
	if levels > len(body) || levels > int(header.UncompressedPageSize) {
		return codecBlock{}, errors.New("level prefix exceeds declared body")
	}
	values := body[levels:]
	var decoded []byte
	var err error
	if compressed {
		decoded, err = codec.Decode(nil, values)
		if err != nil {
			return codecBlock{}, err
		}
	} else {
		decoded = append([]byte(nil), values...)
	}
	expected := int(header.UncompressedPageSize) - levels
	if len(decoded) != expected {
		return codecBlock{}, fmt.Errorf("decoded value length %d != declared %d", len(decoded), expected)
	}
	// Round-trip extraction through the public codec independently of the
	// original file's compressed bytes, outside every benchmark clock.
	encoded, err := codec.Encode(nil, decoded)
	if err != nil {
		return codecBlock{}, err
	}
	reopened, err := codec.Decode(nil, encoded)
	if err != nil || !bytes.Equal(decoded, reopened) {
		return codecBlock{}, errors.New("extracted payload failed codec round trip")
	}
	return codecBlock{Column: column, Kind: kind, Encoding: encoding, Data: decoded, ExpectedBytes: expected,
		LevelBytes: levels, SourceBytes: len(values), Compressed: compressed, PageOffset: offset, HeaderBytes: headerBytes}, nil
}

func readCodecBlocks(path string) ([]codecBlock, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	var blocks []codecBlock
	lines := bufio.NewScanner(file)
	lines.Buffer(make([]byte, 64<<10), 32<<20)
	for lines.Scan() {
		var block codecBlock
		if err := json.Unmarshal(lines.Bytes(), &block); err != nil {
			return nil, err
		}
		blocks = append(blocks, block)
	}
	return blocks, lines.Err()
}

type codecWork struct {
	block codecBlock
	data  []byte
}

type codecTiming struct {
	WallNS          int64 `json:"wall_ns"`
	CPUNS           int64 `json:"cpu_ns"`
	CompressedBytes int64 `json:"compressed_bytes"`
}

func codecPass(codec *zstd.Codec, work []*codecWork) (int64, error) {
	var total int64
	for _, item := range work {
		encoded, err := codec.Encode(item.data, item.block.Data)
		if err != nil {
			return 0, err
		}
		item.data = encoded
		total += int64(len(encoded))
	}
	return total, nil
}

func benchmarkCodec(path, frames string, warmup, iterations int, output io.Writer) error {
	blocks, err := readCodecBlocks(path)
	if err != nil || len(blocks) == 0 {
		return fmt.Errorf("load codec blocks: %w", err)
	}
	codec := &zstd.Codec{Level: zstd.SpeedDefault, Concurrency: 1}
	var work []*codecWork
	byColumn := make(map[string][]*codecWork)
	var rawBytes int64
	for _, block := range blocks {
		if len(block.Data) != block.ExpectedBytes {
			return errors.New("block length differs from extracted uncompressed length")
		}
		item := &codecWork{block: block, data: make([]byte, 0, len(block.Data)*2+1024)}
		work = append(work, item)
		byColumn[block.Column] = append(byColumn[block.Column], item)
		rawBytes += int64(len(block.Data))
	}
	for range warmup {
		if _, err := codecPass(codec, work); err != nil {
			return err
		}
	}
	var runs []codecTiming
	for range iterations {
		cpu := processCPUTime()
		started := time.Now()
		compressedBytes, err := codecPass(codec, work)
		wall := time.Since(started)
		cpuElapsed := processCPUTime() - cpu
		if err != nil {
			return err
		}
		runs = append(runs, codecTiming{int64(wall), int64(cpuElapsed), compressedBytes})
	}
	columns := make(map[string]codecTiming)
	names := make([]string, 0, len(byColumn))
	for name := range byColumn {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		items := byColumn[name]
		for range warmup {
			if _, err := codecPass(codec, items); err != nil {
				return err
			}
		}
		cpu := processCPUTime()
		started := time.Now()
		var compressedBytes int64
		for range iterations {
			compressedBytes, err = codecPass(codec, items)
			if err != nil {
				return err
			}
		}
		wall := time.Since(started)
		cpuElapsed := processCPUTime() - cpu
		columns[name] = codecTiming{int64(wall) / int64(iterations), int64(cpuElapsed) / int64(iterations), compressedBytes}
	}
	for _, item := range work {
		decoded, err := codec.Decode(nil, item.data)
		if err != nil || !bytes.Equal(decoded, item.block.Data) {
			return fmt.Errorf("column %s codec output failed round-trip", item.block.Column)
		}
	}
	if frames != "" {
		file, err := os.Create(frames)
		if err != nil {
			return err
		}
		writer := bufio.NewWriter(file)
		encoder := json.NewEncoder(writer)
		for _, item := range work {
			block := item.block
			block.Data = item.data
			if err := encoder.Encode(block); err != nil {
				file.Close()
				return err
			}
		}
		if err := errors.Join(writer.Flush(), file.Close()); err != nil {
			return err
		}
	}
	return json.NewEncoder(output).Encode(map[string]any{"codec": "parquet-go public zstd.Codec SpeedDefault, concurrency 1, CRC false",
		"warmup": warmup, "iterations": iterations, "blocks": len(blocks), "raw_bytes": rawBytes,
		"runs": runs, "columns": columns, "validation": "every compressed block round-trips byte-exact outside clocks",
		"timing": "total per-iteration runs; column wall/CPU means over batched iterations; payload loading, buffer allocation, decoding and JSON excluded"})
}

func verifyCodecFrames(rawPath, framePath string, output io.Writer) error {
	raw, err := readCodecBlocks(rawPath)
	if err != nil {
		return err
	}
	frames, err := readCodecBlocks(framePath)
	if err != nil {
		return err
	}
	if len(raw) == 0 || len(raw) != len(frames) {
		return errors.New("frame/block cardinality mismatch")
	}
	codec := &zstd.Codec{Level: zstd.SpeedDefault, Concurrency: 1}
	for index, block := range raw {
		if block.Column != frames[index].Column || block.Kind != frames[index].Kind {
			return errors.New("frame/block identity mismatch")
		}
		decoded, err := codec.Decode(nil, frames[index].Data)
		if err != nil || len(decoded) != block.ExpectedBytes || !bytes.Equal(decoded, block.Data) {
			return fmt.Errorf("cross-codec round-trip mismatch at block %d", index)
		}
	}
	return json.NewEncoder(output).Encode(map[string]any{"verified": true, "blocks": len(raw), "decoder": "pure-Go Zstd", "frames": framePath})
}
