// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Command fixtures writes the replay fixtures the gated replay tests read
// (SWATH_REPLAY_FIXTURES=ROOT reads ROOT/fixtures, so pass ROOT/fixtures as
// OUT). Run it at the repository root; OUT must not exist or be empty:
//
//	go run ./bench/replay/fixtures OUT
//
// It writes:
//
//	synthetic-10k/{capture,sorted}       10,000 synthetic rows, one sorted part
//	synthetic-1m/{capture,sorted}        1,000,000 rows, the default part size
//	synthetic-1m/sorted-rolled           the same capture in 1 MiB parts
//	evidence-gcs/{capture,sorted}        the GCS evidence corpus (1,084 names)
//	evidence-azure/capture               the Azure evidence corpus (5,083 names
//	                                     up to 2,010 bytes; the tests sort it
//	                                     with a key limit)
//
// A capture is one zstd Parquet part of the 13-column canonical schema
// (legacytest.Row); a sorted fixture is the SeekParquet dataset
// `swath-replay sort-fixture` (sortfixture.Run) writes from it. The evidence
// corpora are read from the manifests in docs/replay-provider-evidence.tar.gz:
// each object's name, size and updated time, every other column null. About 5 s and 85 MB on a 4-vCPU host.
package main

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/parquet-go/parquet-go"

	"github.com/varveio/swath/internal/replay/legacy/legacytest"
	"github.com/varveio/swath/internal/replay/sortfixture"
)

// The corpus manifests in the evidence archive.
const (
	gcsManifest   = "gcs/manifest.json-v1.json"
	azureManifest = "azure/manifest.2026-06-06.json"
)

// rolledPartSize cuts synthetic-1m/sorted-rolled into many parts.
const rolledPartSize = 1 << 20

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: fixtures OUT")
		os.Exit(2)
	}
	archive := filepath.Join("docs", "replay-provider-evidence.tar.gz")
	if err := writeAll(context.Background(), os.Args[1], archive, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "fixtures:", err)
		os.Exit(1)
	}
}

// writeAll writes every fixture under out.
func writeAll(ctx context.Context, out, archive string, log io.Writer) error {
	if entries, err := os.ReadDir(out); err == nil && len(entries) > 0 {
		return fmt.Errorf("%s is not empty", out)
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	manifests, err := readManifests(archive)
	if err != nil {
		return err
	}
	if err := writeSynthetic(ctx, filepath.Join(out, "synthetic-10k"), 10_000, log); err != nil {
		return err
	}
	big := filepath.Join(out, "synthetic-1m")
	if err := writeSynthetic(ctx, big, 1_000_000, log); err != nil {
		return err
	}
	if err := sortCapture(ctx, big, "sorted-rolled", rolledPartSize, log); err != nil {
		return err
	}
	if err := writeEvidence(ctx, filepath.Join(out, "evidence-gcs"), manifests[gcsManifest], time.RFC3339Nano, true, log); err != nil {
		return err
	}
	if err := writeEvidence(ctx, filepath.Join(out, "evidence-azure"), manifests[azureManifest], time.RFC1123, false, log); err != nil {
		return err
	}
	fmt.Fprintf(log, "fixtures written to %s\n", out)
	return nil
}

// writeSynthetic writes dir/capture, n current objects under four
// top-level prefixes and dated directories, and dir/sorted.
func writeSynthetic(ctx context.Context, dir string, n int, log io.Writer) error {
	if err := writeCapture(filepath.Join(dir, "capture"), n, synthetic, log); err != nil {
		return err
	}
	return sortCapture(ctx, dir, "sorted", 0, log)
}

var (
	prefixes = [...]string{"alpha", "beta", "gamma", "delta"}
	epoch    = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixMicro()
)

// synthetic is row i of a synthetic capture.
func synthetic(i int) legacytest.Row {
	sum := md5.Sum([]byte(strconv.Itoa(i)))
	size := int64(1000 + (i*7919)%1_000_000)
	modified := epoch + int64(i)*1_000_003
	etag, class, latest := hex.EncodeToString(sum[:]), "STANDARD", true
	return legacytest.Row{
		Key:  fmt.Sprintf("logs/%s/dt=2026-%02d-%02d/part-%05d.json.gz", prefixes[i%4], 1+(i/400)%12, 1+(i/13)%28, i),
		Size: &size, LastModified: &modified, ETag: &etag, StorageClass: &class, IsLatest: &latest,
		RowType: "OBJECT",
	}
}

// manifest is the part of an evidence corpus manifest a capture holds.
type manifest struct {
	Objects []struct {
		Name    string `json:"name"`
		Size    int64  `json:"size"`
		Updated string `json:"updated"`
	} `json:"objects"`
}

// writeEvidence writes dir/capture from a corpus manifest, whose updated
// times are spelled in layout, and, when sorted, dir/sorted.
func writeEvidence(ctx context.Context, dir string, raw []byte, layout string, sorted bool, log io.Writer) error {
	var m manifest
	if err := json.Unmarshal(raw, &m); err != nil {
		return fmt.Errorf("%s: manifest: %w", dir, err)
	}
	rows := make([]legacytest.Row, len(m.Objects))
	latest := true
	for i, o := range m.Objects {
		updated, err := time.Parse(layout, o.Updated)
		if err != nil {
			return fmt.Errorf("%s: %q: %w", dir, o.Name, err)
		}
		size, modified := o.Size, updated.UnixMicro()
		rows[i] = legacytest.Row{Key: o.Name, Size: &size, LastModified: &modified, IsLatest: &latest, RowType: "OBJECT"}
	}
	if err := writeCapture(filepath.Join(dir, "capture"), len(rows), func(i int) legacytest.Row { return rows[i] }, log); err != nil {
		return err
	}
	if !sorted {
		return nil
	}
	return sortCapture(ctx, dir, "sorted", 0, log)
}

// writeCapture writes rows 0..n-1 of row to dir/part-00000.parquet in
// batches.
func writeCapture(dir string, n int, row func(int) legacytest.Row, log io.Writer) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	path := filepath.Join(dir, "part-00000.parquet")
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	w := parquet.NewGenericWriter[legacytest.Row](f, parquet.Compression(&parquet.Zstd), parquet.MaxRowsPerRowGroup(1<<17))
	batch := make([]legacytest.Row, 0, 8192)
	for i := range n {
		batch = append(batch, row(i))
		if len(batch) == cap(batch) || i == n-1 {
			if _, err := w.Write(batch); err != nil {
				_ = f.Close()
				return fmt.Errorf("%s: %w", path, err)
			}
			batch = batch[:0]
		}
	}
	if err := w.Close(); err != nil {
		_ = f.Close()
		return fmt.Errorf("%s: %w", path, err)
	}
	if err := f.Close(); err != nil {
		return err
	}
	fmt.Fprintf(log, "capture rows=%d output=%s\n", n, path)
	return nil
}

// sortCapture sorts dir/capture into dir/name; partSize 0 is the default.
func sortCapture(ctx context.Context, dir, name string, partSize int64, log io.Writer) error {
	_, err := sortfixture.Run(ctx, sortfixture.Options{
		Capture: filepath.Join(dir, "capture"), Output: filepath.Join(dir, name), PartSize: partSize,
	}, log)
	return err
}

// readManifests returns the corpus manifests from the evidence archive,
// by archive path.
func readManifests(archive string) (map[string][]byte, error) {
	f, err := os.Open(archive)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	zr, err := gzip.NewReader(f)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", archive, err)
	}
	out := map[string][]byte{}
	tr := tar.NewReader(zr)
	for {
		h, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("%s: %w", archive, err)
		}
		if h.Typeflag != tar.TypeReg || (h.Name != gcsManifest && h.Name != azureManifest) {
			continue
		}
		if out[h.Name], err = io.ReadAll(io.LimitReader(tr, 64<<20)); err != nil {
			return nil, fmt.Errorf("%s: %s: %w", archive, h.Name, err)
		}
	}
	for _, name := range []string{gcsManifest, azureManifest} {
		if out[name] == nil {
			return nil, fmt.Errorf("%s holds no %s", archive, name)
		}
	}
	return out, nil
}
