// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	pq "github.com/parquet-go/parquet-go"

	"github.com/varveio/swath/internal/dataset"
)

// datasetKeys is a committed dataset's keys, part by part in manifest
// order.
func datasetKeys(t *testing.T, dir string) []string {
	t.Helper()
	var m dataset.Manifest
	b, err := os.ReadFile(filepath.Join(dir, dataset.ManifestName))
	if err != nil || json.Unmarshal(b, &m) != nil {
		t.Fatalf("manifest of %s: %v", dir, err)
	}
	var ks []string
	for _, p := range m.Parts {
		f, err := os.Open(filepath.Join(dir, p.Path))
		if err != nil {
			t.Fatal(err)
		}
		pf, err := pq.OpenFile(f, p.Bytes)
		if err != nil {
			t.Fatal(err)
		}
		rows := make([]pq.Row, pf.NumRows())
		r := pq.NewReader(pf)
		n, _ := r.ReadRows(rows)
		r.Close()
		f.Close()
		for _, row := range rows[:n] {
			ks = append(ks, string(row[0].ByteArray()))
		}
	}
	return ks
}

// readManifest decodes the manifest of the dataset at dir.
func readManifest(t *testing.T, dir string) dataset.Manifest {
	t.Helper()
	var m dataset.Manifest
	b, err := os.ReadFile(filepath.Join(dir, dataset.ManifestName))
	if err != nil || json.Unmarshal(b, &m) != nil {
		t.Fatalf("manifest of %s: %v", dir, err)
	}
	return m
}
