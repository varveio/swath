// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	seekparquet "github.com/varveio/seekparquet/go"

	"github.com/varveio/swath/internal/dataset"
	"github.com/varveio/swath/internal/faults"
	"github.com/varveio/swath/internal/order"
	"github.com/varveio/swath/internal/parquetout"
	"github.com/varveio/swath/internal/source/localfs"
	"github.com/varveio/swath/internal/source/s3"
	"github.com/varveio/swath/objfile/gcs"
)

func mk(t *testing.T, root string, files ...string) {
	t.Helper()
	for _, f := range files {
		p := filepath.Join(root, filepath.FromSlash(f))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func noScratch(t *testing.T, out string) {
	t.Helper()
	if tmps, _ := filepath.Glob(filepath.Join(out, dataset.Workspace, dataset.PartsDir, "*")); len(tmps) > 0 {
		t.Fatalf("unpublished parts left: %v", tmps)
	}
	if dataset.Committed(out) {
		t.Fatal("manifest present")
	}
}

func TestEndToEnd(t *testing.T) {
	src := t.TempDir()
	mk(t, src, "a/1", "a/2", "b", "z/y/x")
	out := filepath.Join(t.TempDir(), "ds")
	sum, err := Run(context.Background(), Config{Source: SourceConfig{URI: src}, Output: OutputConfig{Dir: out}})
	if err != nil {
		t.Fatal(err)
	}
	if sum.Output.RowsEmitted != 7 || sum.Output.PartsWritten != 1 {
		t.Fatalf("rows %d parts %d", sum.Output.RowsEmitted, sum.Output.PartsWritten)
	}
	for _, f := range []string{dataset.ManifestName, dataset.ReportName, "data/part-00000.parquet"} {
		if _, err := os.Stat(filepath.Join(out, f)); err != nil {
			t.Fatal(err)
		}
	}
	for _, f := range []string{"_SUCCESS", "symlink.txt"} {
		if _, err := os.Stat(filepath.Join(out, f)); !os.IsNotExist(err) {
			t.Fatalf("%s written: %v", f, err)
		}
	}
	var m dataset.Manifest
	readJSON(t, filepath.Join(out, dataset.ManifestName), &m)
	if m.Format != seekparquet.Format || m.FormatVersion != int(seekparquet.FormatVersion) || m.Source.Kind != "local-fs" || m.Scope.Kind != "whole" ||
		!m.Coverage.Complete || m.Coverage.SkippedEntries != 0 || len(m.Parts) != 1 || m.Parts[0].Rows != 7 || m.Parts[0].Path != "data/part-00000.parquet" {
		t.Fatalf("manifest %+v", m)
	}
	// The source's filesystem space, measured when the listing ended, on
	// the platforms that measure it.
	switch fs := m.Filesystem; runtime.GOOS {
	case "linux", "darwin", "windows":
		if fs == nil || fs.TotalBytes <= 0 || fs.AvailableBytes > fs.TotalBytes || fs.MeasuredAt.Before(m.Coverage.ListingStartedAt) {
			t.Fatalf("filesystem %+v", fs)
		}
	default:
		if fs != nil {
			t.Fatalf("filesystem %+v on %s", fs, runtime.GOOS)
		}
	}
}

// Keys are bytes: a name that is not UTF-8 is listed, and the manifest
// carries its exact bytes (base64), not a replacement character.
func TestKeysAreBytes(t *testing.T) {
	if runtime.GOOS == "windows" || runtime.GOOS == "darwin" {
		t.Skip("NTFS and APFS refuse names that are not UTF-8")
	}
	src := t.TempDir()
	mk(t, src, "a", "z\xff")
	out := filepath.Join(t.TempDir(), "ds")
	if _, err := Run(context.Background(), Config{Source: SourceConfig{URI: src}, Output: OutputConfig{Dir: out}}); err != nil {
		t.Fatal(err)
	}
	var m dataset.Manifest
	readJSON(t, filepath.Join(out, dataset.ManifestName), &m)
	if len(m.Parts) != 1 || m.Parts[0].MinKey != "a" || m.Parts[0].MaxKey != "z\xff" {
		t.Fatalf("parts %+v", m.Parts)
	}
	raw, _ := os.ReadFile(filepath.Join(out, dataset.ManifestName))
	if !strings.Contains(string(raw), `"max_key_base64": "ev8="`) {
		t.Fatalf("manifest bound not base64: %s", raw)
	}
}

// An output inside the root is skipped by identity, for both engines, even
// while its parts are being written during the walk.
func TestOutputInsideRootIsSkipped(t *testing.T) {
	for _, portable := range []bool{false, true} {
		src := t.TempDir()
		mk(t, src, "a/1", "a/2", "b", "z/y/x")
		out := filepath.Join(src, "a", "out")
		sum, err := Run(context.Background(), Config{Source: SourceConfig{URI: src, Options: fsOptions(portable)}, Output: OutputConfig{Dir: out}, Limits: Limits{PartSize: 1}})
		if err != nil {
			t.Fatal(err)
		}
		if sum.Output.RowsEmitted != 7 || fsStats(sum).Skipped != 1 {
			t.Fatalf("portable=%v: rows %d skipped %d", portable, sum.Output.RowsEmitted, fsStats(sum).Skipped)
		}
	}
}

// TestCancellationLeavesNothing: a run cancelled mid-walk, while its first
// part is being written, fails with the cancellation and leaves no
// scratch. The cancel point is established by construction: the encoder
// cancels at its first batch (crash point "encoding", the part's temporary
// file created), and a part size of one row makes the router wait for
// that encoder before it moves past the first cut, so the ordered walk
// cannot finish before the cancellation is requested. The row budget only
// bounds memory; spill stays on, so the source traversal itself is not
// held back. (A goroutine polling for the temporary file every 50 µs raced
// the walk: on one core the walk could finish before it looked.)
func TestCancellationLeavesNothing(t *testing.T) {
	src := t.TempDir()
	for i := range 20000 {
		mk(t, src, "d"+strconv.Itoa(i%200)+"/f"+strconv.Itoa(i))
	}
	out := filepath.Join(t.TempDir(), "ds")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var partOpen atomic.Bool
	restore := faults.Set(func(p string) error {
		if p == "encoding" && !partOpen.Swap(true) {
			if _, err := os.Stat(dataset.TempPath(out, 0)); err != nil {
				t.Errorf("the first batch's part is not open: %v", err)
			}
			cancel()
		}
		return nil
	})
	defer restore()
	sum, err := Run(ctx, Config{Source: SourceConfig{URI: src}, Output: OutputConfig{Dir: out},
		Limits: Limits{PartSize: 1, Workers: 1, RowBudget: minRowBudget}})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if !partOpen.Load() {
		t.Fatal("cancelled before the first part was written")
	}
	if sum.Output.RowsEmitted >= 20200 {
		t.Fatalf("walk finished (%d rows) before cancellation took effect", sum.Output.RowsEmitted)
	}
	noScratch(t, out)
}

// A sink failure during the walk stops it and leaves nothing published.
func TestEncoderFailureLeavesNothing(t *testing.T) {
	src := t.TempDir()
	mk(t, src, "a", "b", "c")
	out := filepath.Join(t.TempDir(), "ds")
	if err := dataset.Prepare(out, src, localfs.Schema.ID, false); err != nil {
		t.Fatal(err)
	}
	// Make the part path unwritable: a directory where part 0's file goes.
	if err := os.MkdirAll(filepath.Join(dataset.TempPath(out, 0), "x"), 0o755); err != nil {
		t.Fatal(err)
	}
	_, err := Run(context.Background(), Config{Source: SourceConfig{URI: src}, Output: OutputConfig{Dir: out, Replace: true}})
	if err == nil {
		t.Fatal("run succeeded")
	}
	if dataset.Committed(out) {
		t.Fatal("manifest present")
	}
}

func TestCountOnly(t *testing.T) {
	src := t.TempDir()
	mk(t, src, "a/1", "b")
	sum, err := Run(context.Background(), Config{Source: SourceConfig{URI: src}})
	if err != nil {
		t.Fatal(err)
	}
	if sum.Output.RowsEmitted != 3 || fsStats(sum).Files != 2 || fsStats(sum).Dirs != 1 {
		t.Fatalf("%+v", sum)
	}
}

// TestConfigRejectsInvalid: invalid values and combinations are a
// *ConfigError before anything is opened.
func TestConfigRejectsInvalid(t *testing.T) {
	src := t.TempDir()
	out := filepath.Join(t.TempDir(), "ds")
	for name, cfg := range map[string]Config{
		"no source":         {},
		"negative workers":  {Source: SourceConfig{URI: src}, Limits: Limits{Workers: -1}},
		"tiny row budget":   {Source: SourceConfig{URI: src}, Limits: Limits{RowBudget: 1}},
		"resume no output":  {Source: SourceConfig{URI: src}, Output: OutputConfig{Resume: true}},
		"resume replace":    {Source: SourceConfig{URI: src}, Output: OutputConfig{Dir: out, Resume: true, Replace: true}},
		"resume no journal": {Source: SourceConfig{URI: src}, Output: OutputConfig{Dir: out, Resume: true, NoJournal: true}},
		"replace no output": {Source: SourceConfig{URI: src}, Output: OutputConfig{Replace: true}},
	} {
		var ce *ConfigError
		if _, err := Run(context.Background(), cfg); !errors.As(err, &ce) {
			t.Errorf("%s: %v, want a *ConfigError", name, err)
		}
	}
	if _, err := os.Stat(out); !os.IsNotExist(err) {
		t.Fatal("an invalid configuration touched the output")
	}
}

// TestResourceAdmission: encoders are admitted against an encoder
// allowance and the descriptor limit, never below one; the
// default spill reserve is derived from the encoders' parts, and the
// default spill cap is half the free space after it; a negative one is none.
func TestResourceAdmission(t *testing.T) {
	s3Encoder := encoderPrice("s3://bkt")
	if s3Encoder != parquetout.EncoderBytes(&s3.Schema) || encoderPrice("scripted://x") != parquetout.DefaultEncoderBytes {
		t.Fatalf("encoder prices %d, scripted %d", s3Encoder, encoderPrice("scripted://x"))
	}
	for _, tc := range []struct {
		budget  int64
		workers int
		fds     int64
		want    int
	}{
		{128 << 20, 4, 0, 4},                      // 128 MiB / 31.5 MiB (the S3 schema's encoder)
		{512 << 20, 4, 0, 16},                     // 512 MiB / 31.5 MiB
		{512 << 20, 64, 180, 180 - 16 - 128 - 32}, // 4: the descriptors (16 spill: 8 drainer readers, 8 read-ahead reads)
		{512 << 20, 64, 100, 1},                   // the floor
		{minRowBudget, 4, 0, 1},                   // the floor
	} {
		if got := admitEncoders(tc.budget, s3Encoder, tc.workers, tc.fds); got != tc.want {
			t.Errorf("admitEncoders(%d, %d, %d) = %d, want %d", tc.budget, tc.workers, tc.fds, got, tc.want)
		}
	}
	// Once listing has ended, each worker's page (1 MiB) joins the
	// allowance: 128 MiB at 64 workers carries 6, at 256 workers 12.
	for _, tc := range []struct {
		workers int
		want    int
	}{{64, 6}, {256, 12}, {4, 4}} {
		if got := admitAfterListing(128<<20, s3Encoder, tc.workers, 0); got != tc.want {
			t.Errorf("admitAfterListing(128 MiB, %d) = %d, want %d", tc.workers, got, tc.want)
		}
	}
	// The tail's segment workers: the encoders admitted after listing, and
	// the 16 spill descriptors plus what the limit leaves.
	for _, tc := range []struct {
		encoders, workers int
		fds               int64
		want              int
	}{{32, 1024, 0, 32}, {32, 64, 1 << 20, 32}, {32, 64, 16 + 128 + 32 + 32 + 4, 20}, {32, 64, 100, 16}, {1, 64, 0, 1}} {
		if got := segmentWorkers(tc.encoders, tc.workers, tc.fds); got != tc.want {
			t.Errorf("segmentWorkers(%d, %d, %d) = %d, want %d", tc.encoders, tc.workers, tc.fds, got, tc.want)
		}
	}
	// The reserve: one part per encoder, at least 1 GiB (the defaults' 5
	// encoders of 64 MiB parts take the floor); negative is none.
	if a, b := spillReserve(5, DefaultPartSize), spillReserve(16, 256<<20); a != 1<<30 || b != 4<<30 {
		t.Errorf("derived reserves %d, %d", a, b)
	}
	// A product past int64 saturates rather than wrapping.
	if got := spillReserve(1<<20, math.MaxInt64/(1<<19)); got != math.MaxInt64 {
		t.Errorf("an overflowing reserve derived as %d", got)
	}
	for in, want := range map[int64]int64{0: 1 << 30, -1: 0, 7: 7} {
		c := Config{Source: SourceConfig{URI: "s3://b"}, Limits: Limits{Encoders: 2, Spill: SpillLimits{Reserve: in}}}
		if err := c.resolve(); err != nil || c.Limits.Spill.Reserve != want {
			t.Errorf("reserve %d resolved to %d (%v), want %d", in, c.Limits.Spill.Reserve, err, want)
		}
	}
	dir := t.TempDir()
	if got := spillMax(SpillLimits{Max: 5}, dir); got != 5 {
		t.Errorf("explicit cap: %d", got)
	}
	if got := spillMax(SpillLimits{Max: -1}, dir); got != 0 {
		t.Errorf("no cap: %d", got)
	}
	free := order.FreeBytes(dir)
	if got := spillMax(SpillLimits{Reserve: 1 << 20}, filepath.Join(dir, "not", "yet")); free != order.Unlimited && got != (free-(1<<20))/2 && got > free/2 {
		t.Errorf("derived cap %d, free %d", got, free)
	}
}

// withMemory makes the memory limits found m for t.
func withMemory(t *testing.T, m memoryLimits) {
	t.Helper()
	old := readMemoryLimits
	t.Cleanup(func() { readMemoryLimits = old })
	readMemoryLimits = func() memoryLimits { return m }
}

// TestEncoderAllowance: encoders are admitted against an allowance of
// their own, the default row budget's share of the memory found (1/16,
// within the minimum and 1 GiB, 128 MiB unknown), whatever --sort-buffer
// says, each at its writer's price plus, for a dataset in a bucket, its
// upload chunk's; the count is then capped by the processors and the
// descriptors, raised to one, and raised after listing by the workers'
// pages; an explicit count stands. The arithmetic for the S3 schema
// (31.5 MiB a writer, 39.5 MiB with an 8 MiB upload chunk) at 128 MiB of
// rows:
//
//	memory  allowance  output  start  after listing (64 / 256 workers)
//	2 GiB   128 MiB    local   4      6 / 12
//	2 GiB   128 MiB    GCS     3      4 / 9
//	8 GiB   512 MiB    local   16     18 / 24
//	8 GiB   512 MiB    GCS     12     14 / 19
//
// before the processors cap them (8 on an 8-vCPU host).
func TestEncoderAllowance(t *testing.T) {
	// The table's bucket rows hold for each provider: their default
	// upload buffers are the same 8 MiB (below).
	writer := encoderPrice("s3://b")
	upload := int64(gcs.DefaultChunkSize)
	if writer != 31*mib+mib/2 || upload != 8*mib {
		t.Fatalf("prices: writer %d, upload %d", writer, upload)
	}
	// Each provider's upload is priced at the buffer its writer holds:
	// GCS rounds a chunk up to the SDK's 256 KiB; S3 sends a part, and
	// Azure stages a block, of exactly the size given.
	for _, c := range []struct {
		out  RemoteOutput
		want int64
	}{
		{RemoteOutput{Location: "gs://d/p"}, 8 * mib},
		{RemoteOutput{Location: "gs://d/p", ChunkSize: 300 << 10}, 512 << 10},
		{RemoteOutput{Location: "s3://d/p"}, 8 * mib},
		{RemoteOutput{Location: "s3://d/p", ChunkSize: 5<<20 + 300<<10}, 5<<20 + 300<<10},
		{RemoteOutput{Location: "az://a/c/p"}, 8 * mib},
		{RemoteOutput{Location: "az://a/c/p", ChunkSize: 300 << 10}, 300 << 10},
	} {
		if got := uploadBytes(&c.out); got != c.want {
			t.Errorf("%s, chunk %d: upload priced %d, want %d", c.out.Location, c.out.ChunkSize, got, c.want)
		}
	}
	for _, c := range []struct {
		memory                   int64
		remote                   bool
		start, after64, after256 int
	}{
		{2 * gib, false, 4, 6, 12},
		{2 * gib, true, 3, 4, 9},
		{8 * gib, false, 16, 18, 24},
		{8 * gib, true, 12, 14, 19},
	} {
		allowance, _ := rowBudgetFor(0, memoryLimits{physical: c.memory})
		per := writer
		if c.remote {
			per += upload
		}
		start, by := admission(allowance, per, 64, 0, 1<<20)
		after64, _ := admission(allowance+64*listingPageBytes, per, 64, 0, 1<<20)
		after256, _ := admission(allowance+256*listingPageBytes, per, 256, 0, 1<<20)
		if start != c.start || after64 != c.after64 || after256 != c.after256 || by != BoundMemory {
			t.Errorf("%d GiB, remote %v: %d (%s), %d, %d; want %d, %d, %d", c.memory/gib, c.remote, start, by, after64, after256, c.start, c.after64, c.after256)
		}
		// The processors and the descriptors cap it; one is the floor.
		if n, by := admission(allowance, per, 64, 0, 8); n != min(8, c.start) || (c.start > 8) != (by == BoundCPU) {
			t.Errorf("8 processors: %d (%s)", n, by)
		}
		if n, by := admission(allowance, per, 64, 16+128+32+2, 1<<20); n != 2 || by != BoundDescriptors {
			t.Errorf("descriptors for 2: %d (%s)", n, by)
		}
		if n, by := admission(allowance, per, 64, 100, 1<<20); n != 1 || by != BoundMinimum {
			t.Errorf("no descriptors: %d (%s)", n, by)
		}
	}
	// Resolved: the sort buffer is 128 MiB as given, the allowance is the
	// memory's share; a dataset in a bucket prices its upload, rounded up
	// to the SDK's 256 KiB; an explicit count stands.
	withMemory(t, memoryLimits{physical: 8 * gib})
	resolve := func(c Config) Limits {
		t.Helper()
		if err := c.resolve(); err != nil {
			t.Fatal(err)
		}
		return c.Limits
	}
	procs := runtime.GOMAXPROCS(0)
	l := resolve(Config{Source: SourceConfig{URI: "s3://b"}, Output: OutputConfig{Dir: t.TempDir()}, Limits: Limits{RowBudget: 128 * mib, Workers: 64}})
	if want, by := admission(512*mib, writer, 64, fdLimit(), procs); l.RowBudget != 128*mib || l.encoderAllowance != 512*mib || l.Encoders != want || l.startBy != by || l.uploadBytes != 0 {
		t.Errorf("local: %+v, want %d encoders (%s)", l, want, by)
	}
	l = resolve(Config{Source: SourceConfig{URI: "s3://b"}, Output: OutputConfig{Dir: t.TempDir(), Remote: &RemoteOutput{Location: "gs://d/p", ChunkSize: 300 << 10}},
		Limits: Limits{RowBudget: 128 * mib, Workers: 256}})
	if l.uploadBytes != 512<<10 || l.encoderBytes != writer+512<<10 {
		t.Errorf("a 300 KiB chunk is priced %d (encoder %d), want 512 KiB as the SDK sends it", l.uploadBytes, l.encoderBytes)
	}
	if want, by := admission(512*mib+256*listingPageBytes, l.encoderBytes, 256, fdLimit(), procs); l.maxEncoders != max(want, l.Encoders) || l.afterBy == "" || want == l.maxEncoders && l.afterBy != by {
		t.Errorf("GCS after listing: %d (%s), want %d (%s)", l.maxEncoders, l.afterBy, want, by)
	}
	l = resolve(Config{Source: SourceConfig{URI: "s3://b"}, Output: OutputConfig{Dir: t.TempDir()}, Limits: Limits{RowBudget: 128 * mib, Encoders: 3}})
	if l.Encoders != 3 || l.maxEncoders != 3 || l.startBy != BoundExplicit || l.afterBy != BoundExplicit {
		t.Errorf("explicit: %+v", l)
	}
	// The constraint reported after listing is the planned one only once
	// listing raised the count to it: 2 GiB, local, 5 processors start 4
	// encoders (memory) and plan 5 (cpu); a run stopped before listing
	// ended still has 4, bound by memory.
	withMemory(t, memoryLimits{physical: 2 * gib})
	prev := runtime.GOMAXPROCS(5)
	t.Cleanup(func() { runtime.GOMAXPROCS(prev) })
	l = resolve(Config{Source: SourceConfig{URI: "s3://b"}, Output: OutputConfig{Dir: t.TempDir()}, Limits: Limits{RowBudget: 128 << 10, Workers: 64}})
	if l.Encoders != 4 || l.startBy != BoundMemory || l.maxEncoders != 5 || l.afterBy != BoundCPU {
		t.Fatalf("2 GiB on 5 processors: %d (%s), after listing %d (%s)", l.Encoders, l.startBy, l.maxEncoders, l.afterBy)
	}
	if by := l.boundAfterListing(4, false); by != BoundMemory {
		t.Errorf("listing not done, 4 encoders: bound by %s, want memory", by)
	}
	if by := l.boundAfterListing(5, true); by != BoundCPU {
		t.Errorf("listing done, 5 encoders: bound by %s, want cpu", by)
	}
	// On 4 processors listing raises nothing: 4 by memory at start, and
	// still after listing, though the plan is bound by cpu.
	runtime.GOMAXPROCS(4)
	l = resolve(Config{Source: SourceConfig{URI: "s3://b"}, Output: OutputConfig{Dir: t.TempDir()}, Limits: Limits{RowBudget: 128 << 10, Workers: 64}})
	if l.Encoders != 4 || l.maxEncoders != 4 || l.afterBy != BoundCPU {
		t.Fatalf("2 GiB on 4 processors: %d (%s), after listing %d (%s)", l.Encoders, l.startBy, l.maxEncoders, l.afterBy)
	}
	if by := l.boundAfterListing(4, true); by != BoundMemory {
		t.Errorf("listing done, count not raised: bound by %s, want memory", by)
	}
	// Unknown memory: the allowance is the fallback, 128 MiB.
	withMemory(t, memoryLimits{})
	if l := resolve(Config{Source: SourceConfig{URI: "s3://b"}, Output: OutputConfig{Dir: t.TempDir()}, Limits: Limits{RowBudget: 1 << 30}}); l.encoderAllowance != DefaultRowBudget {
		t.Errorf("unknown memory: allowance %d", l.encoderAllowance)
	}
}
