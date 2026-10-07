// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"
	"time"
)

type runReport struct {
	Iteration  int           `json:"iteration"`
	WriteCall  time.Duration `json:"write_call_ns"`
	ProcessCPU time.Duration `json:"process_cpu_ns"`
	Part       partResult    `json:"part"`
	Geometry   fileReport    `json:"geometry"`
}

type writeReport struct {
	Command        string       `json:"command"`
	Config         writerConfig `json:"config"`
	GOMAXPROCS     int          `json:"gomaxprocs"`
	EncodingPolicy string       `json:"encoding_policy"`
	Runs           []runReport  `json:"runs"`
}

func run(args []string, output io.Writer) error {
	if len(args) == 0 {
		return errors.New("usage: go-parquet-spike write|verify [flags]")
	}
	switch args[0] {
	case "codec-probe":
		return runCodecProbe(args[1:], output)
	case "resume-probe":
		return runResumeProbe(args[1:])
	case "write":
		flags := flag.NewFlagSet("write", flag.ContinueOnError)
		input := flags.String("input", "", "canonical JSONL corpus")
		path := flags.String("output", "", "output Parquet file; multiple iterations use stem.N.parquet")
		cpuProfile := flags.String("cpu-profile", "", "optional writer-loop CPU profile; excludes input loading and file inspection")
		config := writerConfig{}
		flags.StringVar(&config.Layout, "layout", "served", "served or direct")
		flags.StringVar(&config.SortMode, "sort-mode", "auto", "auto, objects, versions, or none to omit sort stamps on direct parts")
		flags.StringVar(&config.KeyEncoding, "key-encoding", "dict", "dict or delta")
		flags.StringVar(&config.ETagEncoding, "etag-encoding", "dict", "dict or delta for nullable ETags")
		flags.StringVar(&config.SinglePageOrder, "single-page-order", "library", "library or ascending for old-reader compatibility; ascending requires delta keys")
		flags.StringVar(&config.RowAPI, "row-api", "struct", "struct mapping, WriteRows values, or column-major WriteRowValues")
		flags.StringVar(&config.Codec, "codec", "go", "go, go-entropy, or optional native zstd (requires CGO_ENABLED=1 and -tags nativezstd)")
		flags.StringVar(&config.IntegerEncoding, "integer-encoding", "dict", "dict or delta for INT64 columns")
		flags.BoolVar(&config.PageStatistics, "page-statistics", true, "include data-page header statistics; column indexes and footer statistics remain")
		flags.IntVar(&config.PageRows, "page-rows", 0, "row cap per page; defaults to served 1024 or direct 20000")
		flags.IntVar(&config.GroupRows, "group-rows", 65536, "rows per row group; not Java's byte target")
		flags.Int64Var(&config.GroupBytes, "group-bytes", 0, "current-group hybrid buffer target at page boundaries; zero disables; not a heap limit")
		iterations := flags.Int("iterations", 1, "encodes in this process; load input once before timings")
		if err := flags.Parse(args[1:]); err != nil {
			return err
		}
		if *input == "" || *path == "" || *iterations <= 0 || flags.NArg() != 0 {
			return errors.New("write requires input, output and positive iterations")
		}
		if config.PageRows == 0 {
			config.PageRows = 1024
			if config.Layout == "direct" {
				config.PageRows = 20000
			}
		}
		if err := config.validate(); err != nil {
			return err
		}
		rows, err := loadRows(*input)
		if err != nil {
			return err
		}
		stopProfile, err := startCPUProfile(*cpuProfile)
		if err != nil {
			return err
		}
		defer stopProfile()
		report := writeReport{
			Command: "write", Config: config, GOMAXPROCS: runtime.GOMAXPROCS(0),
			EncodingPolicy: "key and INT64 encoding selected; other BYTE_ARRAY dictionary with soft fallback; BOOLEAN plain; page header statistics selected independently of column indexes/footer statistics; ZSTD SpeedDefault (~level 3), codec concurrency 1; page v2; write buffer 4096 bytes; group limit is rows; Go heap sampled only before/after encoding, outside clocks, not peak heap or RSS; writer construction included in encode; inspection after all iterations",
		}
		if config.Codec == "native" {
			report.EncodingPolicy = strings.Replace(report.EncodingPolicy,
				"ZSTD SpeedDefault (~level 3), codec concurrency 1",
				"gozstd v1.26.0 / bundled ZSTD 1.5.7, streaming level 3, checksum disabled, mutex-serialized reusable frames; Reset and frame Close then owned Release per writer; footer clock includes Release", 1)
		} else if config.Codec == "go-entropy" {
			report.EncodingPolicy = strings.Replace(report.EncodingPolicy,
				"ZSTD SpeedDefault (~level 3), codec concurrency 1",
				"klauspost v1.19.1 SpeedDefault, concurrency 1, checksum disabled, all-literal entropy compression enabled, owned contexts; footer clock includes Close", 1)
		}
		report.EncodingPolicy = strings.Replace(report.EncodingPolicy, "group limit is rows",
			"group row ceiling plus optional hybrid byte target checked after page batches, not a heap limit", 1)
		report.EncodingPolicy = strings.Replace(report.EncodingPolicy, "key and INT64 encoding selected; other BYTE_ARRAY",
			"key, ETag and INT64 encoding selected; remaining BYTE_ARRAY", 1)
		for iteration := 0; iteration < *iterations; iteration++ {
			partPath := *path
			if *iterations > 1 {
				partPath = fmt.Sprintf("%s.%d.parquet", strings.TrimSuffix(*path, ".parquet"), iteration)
			}
			cpuBefore := processCPUTime()
			started := time.Now()
			part, err := writePart(partPath, rows, config, diskOps())
			elapsed := time.Since(started)
			cpuElapsed := processCPUTime() - cpuBefore
			if err != nil {
				return err
			}
			report.Runs = append(report.Runs, runReport{Iteration: iteration, WriteCall: elapsed, ProcessCPU: cpuElapsed, Part: part})
		}
		if err := stopProfile(); err != nil {
			return err
		}
		// Keep metadata inspection allocations and I/O out of the repeated
		// encode loop, matching the Java diagnostic's timing boundary.
		for i := range report.Runs {
			geometry, err := inspectPart(report.Runs[i].Part.Path, rows)
			if err != nil {
				return err
			}
			if err := checkGeometry(geometry, config); err != nil {
				return err
			}
			if err := checkFlushReports(geometry, report.Runs[i].Part.Encoding, config); err != nil {
				return err
			}
			report.Runs[i].Geometry = geometry
		}
		return json.NewEncoder(output).Encode(report)
	case "verify":
		flags := flag.NewFlagSet("verify", flag.ContinueOnError)
		input := flags.String("input", "", "canonical JSONL corpus")
		path := flags.String("parquet", "", "Parquet file to verify")
		if err := flags.Parse(args[1:]); err != nil {
			return err
		}
		if *input == "" || *path == "" || flags.NArg() != 0 {
			return errors.New("verify requires input and parquet")
		}
		rows, err := loadRows(*input)
		if err != nil {
			return err
		}
		geometry, err := verifyPart(*path, rows)
		if err != nil {
			return err
		}
		return json.NewEncoder(output).Encode(geometry)
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
