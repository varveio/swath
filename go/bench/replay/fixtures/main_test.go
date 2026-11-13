// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"path/filepath"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/replay/sortfixture"
	"github.com/varveio/swath/go/internal/replay/store"
)

// TestWritesTheFixtures writes the synthetic-10k and GCS evidence fixtures
// and walks each sorted dataset: its row count and the SHA-256 over each
// key followed by a newline are the pins of the store's TestGeneratedFixtures,
// first taken from the Java tool's fixtures of the same inputs.
func TestWritesTheFixtures(t *testing.T) {
	ctx := context.Background()
	out := t.TempDir()
	manifests, err := readManifests(filepath.Join("..", "..", "..", "..", "docs", "replay-provider-evidence.tar.gz"))
	if err != nil {
		t.Fatal(err)
	}
	if err := writeSynthetic(ctx, filepath.Join(out, "synthetic-10k"), 10_000, io.Discard); err != nil {
		t.Fatal(err)
	}
	if err := writeEvidence(ctx, filepath.Join(out, "evidence-gcs"), manifests[gcsManifest], time.RFC3339Nano, true, io.Discard); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		path, keys string
		rows       int
	}{
		{"synthetic-10k/sorted", "c1120808838fb37f025e09a7052f00013bd7f4d7298e266338ebb5d5f1da60ef", 10_000},
		{"evidence-gcs/sorted", "580359890e444fa01ceb86fe51216fff0ff36529cdc4e3ff88ccc21c6441c518", 1_084},
	} {
		s, meta, err := store.Open(ctx, filepath.Join(out, c.path), store.Options{})
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		if meta.Kind != store.KindSeekParquet || meta.SchemaID != sortfixture.SchemaID {
			t.Errorf("%s: %s dataset of schema %s", c.path, meta.Kind, meta.SchemaID)
		}
		digest := sha256.New()
		n := 0
		var from []byte
		for {
			page, err := s.Rows(ctx, from, false, nil, 1000, store.Projection{})
			if err != nil {
				t.Fatal(err)
			}
			if len(page) == 0 {
				break
			}
			for _, o := range page {
				digest.Write(o.Key)
				digest.Write([]byte{'\n'})
			}
			n += len(page)
			from = page[len(page)-1].Key
		}
		if keys := hex.EncodeToString(digest.Sum(nil)); n != c.rows || keys != c.keys {
			t.Errorf("%s: %d rows, keys %s; want %d, %s", c.path, n, keys, c.rows, c.keys)
		}
	}
}
