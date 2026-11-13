// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package azblob

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/varveio/swath/internal/dataset/datasettest"
	"github.com/varveio/swath/internal/order"
	"github.com/varveio/swath/internal/source/azblob/azblobtest"
	"github.com/varveio/swath/internal/source/keyrange"
)

// TestDurableLegacySpillUpgrade uses files written with the original Azure
// payload and durable row codec revision 1, not this build's encoder. The
// unchanged schema ID and journal identity must permit recovery, but none of
// those incompatible certificates may remove intervals from live listing.
func TestDurableLegacySpillUpgrade(t *testing.T) {
	datasettest.RequireJournal(t)
	dir := t.TempDir()
	file, err := os.Open("testdata/legacy-spill-v1.tar.gz")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	gz, err := gzip.NewReader(file)
	if err != nil {
		t.Fatal(err)
	}
	defer gz.Close()
	tr := tar.NewReader(gz)
	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(dir, h.Name)
		if h.Typeflag == tar.TypeDir {
			err = os.MkdirAll(path, 0o755)
		} else {
			var b []byte
			b, err = io.ReadAll(tr)
			if err == nil {
				err = os.WriteFile(path, b, 0o644)
			}
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	// Model the journal's committed frontier: only the suffix may be emitted.
	const prefix = "r1/000-000"
	k := prefix
	cache, err := order.RecoverDurable(root, nil, "00112233445566778899aabbccddeeff", "ident", Schema.ID, &k)
	if err != nil {
		t.Fatal(err)
	}
	cache.Start(1000, 0, nil)
	defer cache.Close()
	rep := cache.Report().Resume
	if rep.Accepted != 0 || rep.Rejected["codec"] != 4 || rep.Rejected["corrupt"] != 0 || len(cache.Spans()) != 0 {
		t.Fatalf("legacy spill must become live gaps before replay: %+v", rep)
	}
	var keys []string
	var want []Row
	for i := range 6 {
		for page := range 3 {
			for j := range 200 {
				key := fmt.Sprintf("r%d/%03d-%03d", i, page, j)
				keys = append(keys, key)
				if key > prefix {
					want = append(want, Row{Key: key, Data: Object{Size: int64(len(key)), BlobType: "BlockBlob"}})
				}
			}
		}
	}
	// The provider's real resume tiling gets no reusable spans, so the
	// previously spilled intervals must be fetched again from the store.
	st := azblobtest.NewStore(keys)
	p := azureEngine(t, st, keyrange.Options{Resume: &keyrange.Resume{After: prefix}, Workers: 2})
	p.Reuse(nil)
	got := &collect{}
	_, err = order.Run[Range, Object](context.Background(), p, got, &Schema, order.Options{
		Workers: 2, Budget: 1 << 20, SpillDir: t.TempDir(), Durable: cache, After: &k,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got.rows, want) || len(want) != 2999 {
		t.Fatalf("relisted suffix differs: got %d rows, want %d", len(got.rows), len(want))
	}
	if cache.Report().Resume.ReplayedRows != 0 {
		t.Fatal("legacy payload reached replay")
	}
}
