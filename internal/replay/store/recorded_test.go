// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"

	"github.com/varveio/swath/internal/replay/legacy/legacytest"
	"github.com/varveio/swath/internal/replay/sortfixture"
)

// recordedRoot returns the fixture root named by SWATH_REPLAY_FIXTURES, or
// skips: these fixtures are large or real recordings and are never
// committed. The root holds fixtures/ (the synthetic and evidence fixtures
// bench/replay/fixtures writes) and, optionally,
// recordings/BUCKET/dataset/data (the Java tool's sorted listings of public
// buckets).
func recordedRoot(t *testing.T) string {
	root := os.Getenv("SWATH_REPLAY_FIXTURES")
	if root == "" {
		t.Skip("SWATH_REPLAY_FIXTURES is not set: the replay fixtures are not available")
	}
	return root
}

// walk pages a store from the start at limit 1,000 and returns the row
// count and the SHA-256 over each key followed by a newline.
func walk(t *testing.T, s Store) (int, string) {
	t.Helper()
	digest := sha256.New()
	n := 0
	var from []byte
	for {
		page, err := s.Rows(context.Background(), from, false, nil, 1000, Projection{})
		if err != nil {
			t.Fatal(err)
		}
		if len(page) == 0 {
			return n, hex.EncodeToString(digest.Sum(nil))
		}
		for _, o := range page {
			digest.Write(o.Key)
			digest.Write([]byte{'\n'})
		}
		n += len(page)
		from = page[len(page)-1].Key
	}
}

// TestGeneratedFixtures opens the sorted fixtures bench/replay/fixtures
// writes, requires each to be a SeekParquet dataset of sortfixture.SchemaID
// and sorted-rolled to have more than one part, and requires a full walk
// with the row count and key digest the Java server's walk of the Java
// tool's fixtures from the same inputs recorded.
func TestGeneratedFixtures(t *testing.T) {
	root := recordedRoot(t)
	cases := []struct {
		path, keys string
		rows       int
		rolled     bool
	}{
		{"fixtures/synthetic-10k/sorted", "c1120808838fb37f025e09a7052f00013bd7f4d7298e266338ebb5d5f1da60ef", 10_000, false},
		{"fixtures/synthetic-1m/sorted", "dc0d1a4c3e4d4d15edfded5ef51f705b4d6a320fb4db179de122eab2ca1c140c", 1_000_000, false},
		{"fixtures/synthetic-1m/sorted-rolled", "dc0d1a4c3e4d4d15edfded5ef51f705b4d6a320fb4db179de122eab2ca1c140c", 1_000_000, true},
		{"fixtures/evidence-gcs/sorted", "580359890e444fa01ceb86fe51216fff0ff36529cdc4e3ff88ccc21c6441c518", 1_084, false},
	}
	for _, c := range cases {
		t.Run(c.path, func(t *testing.T) {
			s, meta, err := Open(context.Background(), filepath.Join(root, c.path), Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			if meta.Kind != KindSeekParquet || meta.SchemaID != sortfixture.SchemaID {
				t.Errorf("a %s dataset of schema %s", meta.Kind, meta.SchemaID)
			}
			if c.rolled && meta.Parts < 2 {
				t.Errorf("%d parts, want more than one", meta.Parts)
			}
			if n, keys := walk(t, s); n != c.rows || keys != c.keys {
				t.Errorf("walk: %d rows, keys %s; want %d, %s", n, keys, c.rows, c.keys)
			}
		})
	}
}

// TestRecordings walks every recorded bucket and compares the root and
// first-level "/" rollups with the range walk.
func TestRecordings(t *testing.T) {
	root := recordedRoot(t)
	dirs, _ := filepath.Glob(filepath.Join(root, "recordings", "*", "dataset", "data"))
	if len(dirs) == 0 {
		t.Skip("no recordings")
	}
	for _, dir := range dirs {
		bucket := filepath.Base(filepath.Dir(filepath.Dir(dir)))
		t.Run(bucket, func(t *testing.T) {
			s, meta, err := Open(context.Background(), dir, Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			if n, _ := walk(t, s); int64(n) != meta.Rows {
				t.Fatalf("walk %d rows, metadata %d", n, meta.Rows)
			}
			sorted := s.(*Sorted)
			top, ok, err := s.DelimitedRollup(context.Background(), nil, true, nil, nil, []byte("/"), 1000, Projection{}, true)
			if err != nil || !ok {
				t.Fatal(ok, err)
			}
			if d := diffEntries(top, rangeWalk(storeWalk(t, sorted, nil), nil, true, nil, 1000, true)); d != "" {
				t.Fatalf("root rollup: %s", d)
			}
			for _, e := range top[:min(len(top), 3)] {
				if e.CommonPrefix == nil {
					continue
				}
				prefix := e.CommonPrefix
				upper := append([]byte(nil), prefix...)
				upper[len(upper)-1]++
				got, _, err := s.DelimitedRollup(context.Background(), prefix, true, upper, prefix, []byte("/"), 1000, Projection{}, true)
				if err != nil {
					t.Fatal(err)
				}
				if d := diffEntries(got, rangeWalk(storeWalk(t, sorted, upper), prefix, true, prefix, 1000, true)); d != "" {
					t.Fatalf("rollup under %q: %s", prefix, d)
				}
			}
		})
	}
}

// TestSortsTheAzureEvidenceCorpus sorts the Azure evidence capture, whose
// names reach 2,010 bytes, with a key limit, and walks the result.
func TestSortsTheAzureEvidenceCorpus(t *testing.T) {
	root := recordedRoot(t)
	out := filepath.Join(t.TempDir(), "azure")
	stats, err := sortfixture.Run(context.Background(), sortfixture.Options{
		Capture: filepath.Join(root, "fixtures", "evidence-azure", "capture"), Output: out, KeyLimit: 4096,
	}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	s, meta, err := Open(context.Background(), out, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if n, _ := walk(t, s); n != 5_083 || stats.Objects != 5_083 || meta.SchemaID != sortfixture.SchemaID {
		t.Fatalf("walk %d rows, sorted %d, schema %s", n, stats.Objects, meta.SchemaID)
	}
}

// TestStartupCost logs the time Open takes to build the routing index and
// the process's resident set after it, for the 1M-row fixtures.
func TestStartupCost(t *testing.T) {
	root := recordedRoot(t)
	for _, path := range []string{"fixtures/synthetic-1m/sorted", "fixtures/synthetic-1m/sorted-rolled"} {
		runtime.GC()
		before := residentKiB()
		start := time.Now()
		s, meta, err := Open(context.Background(), filepath.Join(root, path), Options{})
		if err != nil {
			t.Fatal(err)
		}
		elapsed := time.Since(start)
		after := residentKiB()
		t.Logf("%s: open %v, %d parts, %d row groups, %d rows; RSS %d KiB before, %d KiB after (GOMAXPROCS %d)",
			path, elapsed.Round(time.Microsecond), meta.Parts, meta.RowGroups, meta.Rows, before, after, runtime.GOMAXPROCS(0))
		_ = s.Close()
	}
}

// residentKiB reads VmRSS from /proc/self/status, 0 where there is none.
func residentKiB() int64 {
	f, err := os.Open("/proc/self/status")
	if err != nil {
		return 0
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		if rest, ok := strings.CutPrefix(scanner.Text(), "VmRSS:"); ok {
			var kib int64
			for _, field := range strings.Fields(rest) {
				for _, r := range field {
					if r < '0' || r > '9' {
						return kib
					}
					kib = kib*10 + int64(r-'0')
				}
				return kib
			}
		}
	}
	return 0
}

// memoryChild names, in a child process, the capture TestSortFixtureMemory
// sorts there, so the heap measured is the sort's alone.
const memoryChild = "SWATH_SORT_MEMORY_CHILD"

// TestSortFixtureMemory sorts two 1M-row captures, each in a child process,
// and bounds the heap the child obtained from the system:
//
//   - the synthetic capture (all OBJECT rows): one object array, about 300
//     bytes a row (sortfixture's package doc), fits 450 MiB; a second copy
//     of the rows, which the sort once made, does not;
//   - a capture of 1M COMMON_PREFIX rows, written here: the other rows'
//     keys fit 100 MiB; an object array sized from every capture row, as
//     the sort once allocated, does not.
func TestSortFixtureMemory(t *testing.T) {
	if capture := os.Getenv(memoryChild); capture != "" {
		sortForMemory(t, capture)
		return
	}
	root := recordedRoot(t)
	prefixes := filepath.Join(t.TempDir(), "prefixes")
	writePrefixCapture(t, prefixes, 1_000_000)
	for _, c := range []struct {
		name, capture string
		limitMiB      uint64
	}{
		{"synthetic-1m", filepath.Join(root, "fixtures", "synthetic-1m", "capture"), 450},
		{"common-prefix-1m", prefixes, 100},
	} {
		t.Run(c.name, func(t *testing.T) {
			cmd := exec.Command(os.Args[0], "-test.run=^TestSortFixtureMemory$", "-test.v", "-test.count=1")
			cmd.Env = append(os.Environ(), memoryChild+"="+c.capture)
			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("child: %v\n%s", err, out)
			}
			var heap uint64
			for _, line := range strings.Split(string(out), "\n") {
				if _, rest, ok := strings.Cut(line, "heap_sys_mib="); ok {
					_, _ = fmt.Sscan(rest, &heap)
					t.Log(strings.TrimSpace(line))
				}
			}
			if heap == 0 || heap > c.limitMiB {
				t.Fatalf("heap obtained %d MiB, limit %d MiB\n%s", heap, c.limitMiB, out)
			}
		})
	}
}

// sortForMemory is the child: it sorts capture and logs its heap.
func sortForMemory(t *testing.T, capture string) {
	runtime.GC()
	start := time.Now()
	stats, err := sortfixture.Run(context.Background(), sortfixture.Options{Capture: capture, Output: filepath.Join(t.TempDir(), "sorted")}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	t.Logf("rows=%d objects=%d wall=%v resident_kib=%d gomaxprocs=%d heap_sys_mib=%d",
		stats.CaptureRows, stats.Objects, time.Since(start).Round(time.Millisecond), residentKiB(), runtime.GOMAXPROCS(0), ms.HeapSys>>20)
}

// writePrefixCapture writes n distinct COMMON_PREFIX rows as one unsorted
// capture part, in batches so the writer holds few at once.
func writePrefixCapture(t *testing.T, dir string, n int) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(filepath.Join(dir, "part-00000.parquet"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	w := parquet.NewGenericWriter[legacytest.Row](f)
	batch := make([]legacytest.Row, 0, 10_000)
	for i := range n {
		// Descending, so the part is not already sorted.
		batch = append(batch, legacytest.Row{Key: fmt.Sprintf("logs/p%07d/", n-i), RowType: "COMMON_PREFIX"})
		if len(batch) == cap(batch) || i == n-1 {
			if _, err := w.Write(batch); err != nil {
				t.Fatal(err)
			}
			batch = batch[:0]
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}
