// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/replay/sortfixture"
)

// recordedRoot returns the fixture root named by SWATH_REPLAY_FIXTURES, or
// skips: these fixtures are large or real recordings and are never
// committed. The root holds fixtures/ (the synthetic and evidence fixtures
// go/bench/replay/make-fixtures.sh writes with the Java tool) and,
// optionally, recordings/BUCKET/dataset/data (the Java tool's sorted
// listings of public buckets).
func recordedRoot(t *testing.T) string {
	root := os.Getenv("SWATH_REPLAY_FIXTURES")
	if root == "" {
		t.Skip("SWATH_REPLAY_FIXTURES is not set: the Java-written fixtures are not available")
	}
	return root
}

// walk pages a store from the start at limit 1,000 and returns the row
// count and the SHA-256 over each key followed by a newline, the digest the
// fixture inventory records for the Java server's walks.
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

// fixturesTime is the modification time make-fixtures.sh gives every file
// it writes, which makes the fixture identity reproducible.
var fixturesTime = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// madeByScript reports whether every part under dir carries fixturesTime.
func madeByScript(t *testing.T, dir string) bool {
	t.Helper()
	parts, err := filepath.Glob(filepath.Join(dir, "*.parquet"))
	if err != nil || len(parts) == 0 {
		t.Fatalf("no parts in %s: %v", dir, err)
	}
	for _, part := range parts {
		info, err := os.Stat(part)
		if err != nil {
			t.Fatal(err)
		}
		if !info.ModTime().Equal(fixturesTime) {
			return false
		}
	}
	return true
}

// TestJavaWrittenFixtures opens the Java sort-fixture outputs that
// go/bench/replay/make-fixtures.sh writes and requires a full walk with the
// row count and key digest of the Java server's walk of the same fixture,
// and, for a tree the script wrote (its fixed modification time), the
// fixture identity the Java server reports for it.
func TestJavaWrittenFixtures(t *testing.T) {
	root := recordedRoot(t)
	cases := []struct {
		path, identity, keys string
		rows                 int
	}{
		{"fixtures/synthetic-10k/sorted", "5605661b90219d9a03e4d09e7554e8ed3472c13d5f468b4739308bae4e7f4dfd", "c1120808838fb37f025e09a7052f00013bd7f4d7298e266338ebb5d5f1da60ef", 10_000},
		{"fixtures/synthetic-1m/sorted", "dfb9d41d22a0c73e05e8ee7792557ffab3ac31f479ed0bb95d588fe8366759f3", "dc0d1a4c3e4d4d15edfded5ef51f705b4d6a320fb4db179de122eab2ca1c140c", 1_000_000},
		{"fixtures/synthetic-1m/sorted-rolled", "55cfec6d03c61348560fec1668f8ba987c133857e3ea792f19439d18d73b9c96", "dc0d1a4c3e4d4d15edfded5ef51f705b4d6a320fb4db179de122eab2ca1c140c", 1_000_000},
		{"fixtures/evidence-gcs/sorted", "2b1ce480c0e87a09b1bd0fd9ead5da6731110141c5124b318b2c93aeea13c83a", "580359890e444fa01ceb86fe51216fff0ff36529cdc4e3ff88ccc21c6441c518", 1_084},
	}
	for _, c := range cases {
		t.Run(c.path, func(t *testing.T) {
			dir := filepath.Join(root, c.path)
			s, meta, err := Open(context.Background(), dir, Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			if !madeByScript(t, dir) {
				t.Logf("identity not compared: the parts' modification times are not the script's")
			} else if meta.Identity != c.identity {
				t.Errorf("identity %s, the Java server's %s", meta.Identity, c.identity)
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
