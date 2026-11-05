// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Command swath-local lists a local directory tree in parallel and writes a
// globally key-sorted Parquet dataset in Swath's sorted dataset layout.
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"runtime/pprof"
	"time"

	"github.com/varveio/swath/tools/swath-local-spike/internal/proc"
	"github.com/varveio/swath/tools/swath-local-spike/pqout"
	"github.com/varveio/swath/tools/swath-local-spike/record"
	"github.com/varveio/swath/tools/swath-local-spike/sortout"
	"github.com/varveio/swath/tools/swath-local-spike/walk"
)

const usage = `usage: swath-local list <root> [--out DIR] [flags]

  --out DIR               output dataset directory (required unless --count-only)
  --overwrite             replace a non-empty --out
  --workers N             walker goroutines (default min(64, max(4, 3*GOMAXPROCS)))
  --engine E              fstatat|getdents|parallel-lstat|portable|walkdir (default: platform fast path)
  --fields T              names|stat|statx (default stat)
  --statx-mask M          basic|all (statx tier; default all = basic + btime)
  --no-dirs               omit kind=DIR rows
  --no-link-target        skip readlink for symlinks
  --ino-sort              stat entries in d_ino order (getdents engine)
  --split-entries N       stat directories larger than N entries in parallel chunks (default 4096, 0 = off)
  --sort-buffer-rows N    rows per in-memory sort run (default 2000000)
  --sort-buffer-bytes B   also spill at this estimated byte size (default 0 = off)
  --part-size BYTES       final part byte target (default 67108864)
  --row-group-bytes B     row-group byte target (default 8388608)
  --codec C               go|go-entropy|go-entropy-fastest (default go-entropy)
  --encoders N            encoder goroutines (default GOMAXPROCS)
  --encode-scheme S       column (default: one part at a time, page columns encoded in parallel) |
                          part (N goroutines each write whole parts; boundaries fixed by a router with a
                          compression ratio frozen after part 0; faster with --order none and >= N parts)
  --order O               none (parallel walk + external sort) | dfs-sequential | dfs-parallel
                          (sorted depth-first emission streamed straight into the writer)
  --reorder-rows N        dfs-parallel: max rows read but not yet emitted (default 2000000)
  --count-only            walk without writing
  --json-stats            print the summary as JSON instead of one line
  --gc-percent N          set GOGC for the run (default: Go's 100)
  --cpuprofile FILE       write a Go CPU profile
  --memprofile FILE       write a Go allocation profile (allocs)
`

type summary struct {
	Tool        string         `json:"tool"`
	SchemaID    string         `json:"schema_id"`
	Root        string         `json:"root"`
	Out         string         `json:"out,omitempty"`
	Engine      string         `json:"engine"`
	Order       string         `json:"order"`
	Fields      string         `json:"fields"`
	StatxMask   string         `json:"statx_mask,omitempty"`
	Workers     int            `json:"workers"`
	Encoders    int            `json:"encoders,omitempty"`
	Scheme      string         `json:"encode_scheme,omitempty"`
	GOMAXPROCS  int            `json:"gomaxprocs"`
	CountOnly   bool           `json:"count_only"`
	Walk        walk.Stats     `json:"walk"`
	Errors      int64          `json:"errors"`
	Rows        int64          `json:"rows"`
	WalkSec     float64        `json:"walk_seconds"`
	SortSec     float64        `json:"sort_write_seconds"` // after the walk: final sort + merge + encode + publish
	TotalSec    float64        `json:"total_seconds"`
	UserSec     float64        `json:"user_cpu_seconds"`
	SysSec      float64        `json:"sys_cpu_seconds"`
	PeakRSS     int64          `json:"peak_rss_bytes"`
	WalkRate    float64        `json:"walk_entries_per_second"`
	WriteRate   float64        `json:"write_rows_per_second"` // rows / sort_write_seconds
	Sort        *sortout.Stats `json:"sort,omitempty"`
	Parts       int            `json:"parts"`
	OutputBytes int64          `json:"output_bytes"`
}

func main() {
	if len(os.Args) < 2 || os.Args[1] != "list" {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	if err := list(os.Args[2:]); err != nil {
		fmt.Fprintln(os.Stderr, "swath-local:", err)
		os.Exit(1)
	}
}

func list(args []string) error {
	fs := flag.NewFlagSet("list", flag.ContinueOnError)
	fs.Usage = func() { fmt.Fprint(os.Stderr, usage) }
	out := fs.String("out", "", "")
	overwrite := fs.Bool("overwrite", false, "")
	workers := fs.Int("workers", 0, "")
	engine := fs.String("engine", walk.DefaultEngine(), "")
	fields := fs.String("fields", "stat", "")
	statxMask := fs.String("statx-mask", "all", "")
	noDirs := fs.Bool("no-dirs", false, "")
	noLink := fs.Bool("no-link-target", false, "")
	inoSort := fs.Bool("ino-sort", false, "")
	split := fs.Int("split-entries", 4096, "")
	bufRows := fs.Int("sort-buffer-rows", 2_000_000, "")
	bufBytes := fs.Int64("sort-buffer-bytes", 0, "")
	partSize := fs.Int64("part-size", 64<<20, "")
	groupBytes := fs.Int64("row-group-bytes", 8<<20, "")
	codec := fs.String("codec", "go-entropy", "")
	encoders := fs.Int("encoders", runtime.GOMAXPROCS(0), "")
	scheme := fs.String("encode-scheme", sortout.SchemeColumn, "")
	countOnly := fs.Bool("count-only", false, "")
	order := fs.String("order", walk.OrderNone, "")
	reorderRows := fs.Int("reorder-rows", 2_000_000, "")
	jsonStats := fs.Bool("json-stats", false, "")
	cpuProfile := fs.String("cpuprofile", "", "")
	memProfile := fs.String("memprofile", "", "")
	gcPercent := fs.Int("gc-percent", 0, "")
	// Accept <root> before or after flags.
	var root string
	rest := args
	for {
		if err := fs.Parse(rest); err != nil {
			return err
		}
		if fs.NArg() == 0 {
			break
		}
		if root != "" {
			return fmt.Errorf("unexpected argument %q", fs.Arg(0))
		}
		root = fs.Arg(0)
		rest = fs.Args()[1:]
	}
	if root == "" {
		return errors.New("missing <root>")
	}
	if !*countOnly && *out == "" {
		return errors.New("--out is required unless --count-only")
	}
	tier, err := walk.ParseFields(*fields)
	if err != nil {
		return err
	}
	if *split != 0 && *split < 2 {
		return errors.New("--split-entries must be 0 (off) or >= 2")
	}
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return err
	}
	// Validate the root before touching --out (Prepare may create or clear it).
	if fi, err := os.Stat(absRoot); err != nil {
		return err
	} else if !fi.IsDir() {
		return fmt.Errorf("%s is not a directory", root)
	}
	opt := walk.Options{
		Engine: *engine, Workers: *workers, Fields: tier, StatxMask: *statxMask, NoDirs: *noDirs,
		LinkTarget: !*noLink, InoSort: *inoSort, SplitEntries: *split,
	}
	if opt.Workers <= 0 {
		opt.Workers = walk.DefaultWorkers()
	}
	sum := summary{
		Tool: "swath-local", SchemaID: record.SchemaID, Root: absRoot, Out: *out, Engine: *engine, Order: *order, Fields: tier.String(),
		Workers: opt.Workers, GOMAXPROCS: runtime.GOMAXPROCS(0), CountOnly: *countOnly,
	}
	if tier == walk.FieldsStatx {
		sum.StatxMask = *statxMask
	}
	if *gcPercent != 0 {
		debug.SetGCPercent(*gcPercent)
	}
	if *cpuProfile != "" {
		f, err := os.Create(*cpuProfile)
		if err != nil {
			return err
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			return err
		}
		defer pprof.StopCPUProfile()
	}
	if *memProfile != "" {
		runtime.MemProfileRate = 64 << 10
		defer func() {
			if f, err := os.Create(*memProfile); err == nil {
				_ = pprof.Lookup("allocs").WriteTo(f, 0)
				f.Close()
			}
		}()
	}
	start := time.Now()
	var sink walk.Sink = discard{}
	var sorter interface {
		Finish() ([]pqout.PartInfo, sortout.Stats, error)
	}
	ordered := *order != walk.OrderNone
	if !*countOnly {
		if err := sortout.Prepare(*out, absRoot, *overwrite); err != nil {
			return err
		}
		// An --out inside the root must not list itself (its spill runs and
		// half-written parts): skip it by directory identity.
		if rel, inside, err := sortout.LocateInside(absRoot, *out); err != nil {
			return err
		} else if inside {
			opt.SkipDirKeys = map[string]bool{rel + "/": true}
		}
		cfg := pqout.DefaultConfig()
		cfg.GroupBytes, cfg.Codec, cfg.Encoders = *groupBytes, *codec, *encoders
		if *scheme != sortout.SchemePart && *scheme != sortout.SchemeColumn {
			return fmt.Errorf("--encode-scheme must be part or column")
		}
		sopt := sortout.Options{Out: *out, SortBufferRows: *bufRows, SortBufferBytes: *bufBytes, PartSize: *partSize, Writer: cfg, EncodeScheme: *scheme}
		if ordered {
			st := sortout.NewStreamer(sopt)
			sorter, sink = st, st
		} else {
			st, err := sortout.NewSorter(sopt)
			if err != nil {
				return err
			}
			sorter, sink = st, st
		}
		sum.Encoders, sum.Scheme = *encoders, *scheme
	}
	var ws walk.Stats
	if ordered {
		ws, err = walk.WalkOrdered(root, *order, opt, *reorderRows, sink)
	} else {
		ws, err = walk.Walk(root, opt, sink)
	}
	if err != nil {
		return err
	}
	walkDone := time.Now()
	sum.Walk = ws
	sum.Errors = ws.Errors()
	sum.Rows = ws.Files
	if !opt.NoDirs {
		sum.Rows += ws.Dirs
	}
	sum.WalkSec = walkDone.Sub(start).Seconds()
	sum.WalkRate = float64(ws.Files+ws.Dirs) / sum.WalkSec
	var parts []pqout.PartInfo
	if sorter != nil {
		var st sortout.Stats
		parts, st, err = sorter.Finish()
		if err != nil {
			return err
		}
		if st.Rows != sum.Rows {
			return fmt.Errorf("row count mismatch: walked %d, wrote %d", sum.Rows, st.Rows)
		}
		sum.Sort = &st
		sum.Parts = st.Parts
		sum.OutputBytes = st.DatasetBytes
	}
	finish := func() {
		end := time.Now()
		sum.TotalSec = end.Sub(start).Seconds()
		if sorter != nil {
			sum.SortSec = end.Sub(walkDone).Seconds()
			sum.WriteRate = float64(sum.Rows) / sum.SortSec
		}
		u, s := proc.CPU()
		sum.UserSec, sum.SysSec = u.Seconds(), s.Seconds()
		sum.PeakRSS = proc.PeakRSS()
	}
	if sorter != nil {
		finish() // summary inside the dataset excludes its own publication I/O
		if _, err := sortout.Publish(*out, absRoot, os.Args[1:], parts, sum); err != nil {
			return err
		}
	}
	finish()
	if *jsonStats {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(sum)
	}
	spills := 0
	if sum.Sort != nil {
		spills = sum.Sort.Runs
	}
	fmt.Printf("files=%d dirs=%d bytes=%d errors=%d rows=%d parts=%d out_bytes=%d walk=%.3fs sort+write=%.3fs total=%.3fs user=%.2fs sys=%.2fs peak_rss=%.1fMiB engine=%s fields=%s workers=%d order=%s spill_runs=%d reorder_max=%d\n",
		ws.Files, ws.Dirs, ws.Bytes, sum.Errors, sum.Rows, sum.Parts, sum.OutputBytes, sum.WalkSec, sum.SortSec, sum.TotalSec,
		sum.UserSec, sum.SysSec, float64(sum.PeakRSS)/(1<<20), *engine, tier, opt.Workers, *order, spills, ws.ReorderMaxBuffered)
	return nil
}

type discard struct{}

func (discard) Emit(rows []record.Row) []record.Row { return rows[:0] }

func (discard) EmitSegments([][]record.Row) {}
