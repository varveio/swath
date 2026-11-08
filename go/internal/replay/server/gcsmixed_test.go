// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"

	"github.com/varveio/swath/go/internal/dataset/datasettest"
	"github.com/varveio/swath/go/internal/faults"
	"github.com/varveio/swath/go/internal/listing"
	"github.com/varveio/swath/go/internal/replay/store"
	sourcegcs "github.com/varveio/swath/go/internal/source/gcs"
	"github.com/varveio/swath/go/internal/source/gcs/gcstest"
)

// TestResumedGCSRecordingMixesParts: a GCS listing started by a build
// that wrote no time_created column (the schema without its last column)
// and resumed by one that does leaves parts with and without the column
// under one schema id. The resume is accepted (the journal freezes the
// schema id, not the column set), the store opens the dataset, and both
// routes serve every object: the GCS route lists timeCreated exactly for
// the objects of parts that hold it, and the S3 route's LastModified is
// their timeCreated, the updated time for the others, on the window cache
// and off it.
func TestResumedGCSRecordingMixesParts(t *testing.T) {
	datasettest.RequireJournal(t)
	keys := gcstest.Keys(3000)
	slices.Sort(keys)
	ep := gcstest.NewStrict(keys).Serve(t)
	out := filepath.Join(t.TempDir(), "ds")
	config := func(resume bool) listing.Config {
		return listing.Config{Source: listing.SourceConfig{URI: "gs://bkt", Options: &sourcegcs.Options{Endpoint: ep, Anonymous: true}},
			Output: listing.OutputConfig{Dir: out, Resume: resume}, Limits: listing.Limits{PartSize: 6 << 10, Workers: 4, PartInterval: -1}}
	}
	columns := sourcegcs.Schema.Columns
	if columns[len(columns)-1].Name != "time_created" {
		t.Fatalf("last column %s", columns[len(columns)-1].Name)
	}
	sourcegcs.Schema.Columns = columns[:len(columns)-1]
	var renamed atomic.Int64
	restore := faults.Set(func(p string) error {
		if p == "renamed" && renamed.Add(1) == 2 {
			return faults.ErrCrash
		}
		return nil
	})
	_, err := listing.Run(context.Background(), config(false))
	restore()
	sourcegcs.Schema.Columns = columns
	if !errors.Is(err, faults.ErrCrash) {
		t.Fatalf("crash not injected: %v", err)
	}
	if _, err := listing.Run(context.Background(), config(true)); err != nil {
		t.Fatalf("resume: %v", err)
	}

	// The objects of the parts written without the column.
	raw, err := os.ReadFile(filepath.Join(out, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var manifest struct {
		Parts []struct {
			Path string `json:"path"`
			Min  string `json:"min_key_base64"`
			Max  string `json:"max_key_base64"`
		} `json:"parts"`
	}
	if err := json.Unmarshal(raw, &manifest); err != nil {
		t.Fatal(err)
	}
	untimed := map[string]bool{}
	var with, without int
	for _, p := range manifest.Parts {
		f, err := os.Open(filepath.Join(out, filepath.FromSlash(p.Path)))
		if err != nil {
			t.Fatal(err)
		}
		info, _ := f.Stat()
		pf, err := parquet.OpenFile(f, info.Size())
		if err != nil {
			t.Fatal(err)
		}
		_, has := pf.Schema().Lookup("time_created")
		_ = f.Close()
		if has {
			with++
			continue
		}
		without++
		lo, _ := base64.StdEncoding.DecodeString(p.Min)
		hi, _ := base64.StdEncoding.DecodeString(p.Max)
		for _, k := range keys {
			if bytes.Compare([]byte(k), lo) >= 0 && bytes.Compare([]byte(k), hi) <= 0 {
				untimed[k] = true
			}
		}
	}
	if without != 2 || with < 2 {
		t.Fatalf("%d parts without time_created, %d with: want the 2 retained without and the resumed ones with", without, with)
	}
	const updated = "2026-11-06T01:02:03.456"
	createdOf := func(i int) time.Time {
		c, err := time.Parse(time.RFC3339Nano, gcstest.Object(i, keys[i])["timeCreated"].(string))
		if err != nil {
			t.Fatal(err)
		}
		return c.UTC()
	}

	itemRe := regexp.MustCompile(`\{"kind":"storage#object".*?\}`)
	nameRe := regexp.MustCompile(`"name":"([^"]*)"`)
	lastModifiedRe := regexp.MustCompile(`<Key>([^<]*)</Key><LastModified>([^<]*)</LastModified>`)
	tokenRe := regexp.MustCompile(`"nextPageToken":"([^"]*)"`)
	s3TokenRe := regexp.MustCompile(`<NextContinuationToken>([^<]*)<`)
	for _, opts := range []store.Options{{}, {Prefetch: store.Prefetch{Off: true}}} {
		s := serveGCSRecording(t, out, opts)
		var items []string
		for token := ""; ; {
			q := "/storage/v1/b/bucket/o?maxResults=700"
			if token != "" {
				q += "&pageToken=" + token
			}
			r := get(t, s, q)
			if r.status != 200 {
				t.Fatalf("GCS %s: %d %s", q, r.status, r.body)
			}
			items = append(items, itemRe.FindAllString(r.body, -1)...)
			m := tokenRe.FindStringSubmatch(r.body)
			if m == nil {
				break
			}
			token = m[1]
		}
		if len(items) != len(keys) {
			t.Fatalf("GCS: %d items, want %d", len(items), len(keys))
		}
		for i, it := range items {
			k := keys[i]
			if m := nameRe.FindStringSubmatch(it); m == nil || m[1] != k {
				t.Fatalf("GCS item %d: %s, want %s", i, it, k)
			}
			want := `"timeCreated":"` + createdOf(i).Format("2006-01-02T15:04:05.000000Z") + `","updated"`
			if got := strings.Contains(it, `"timeCreated"`); got == untimed[k] || !untimed[k] && !strings.Contains(it, want) {
				t.Fatalf("GCS %s (untimed %v): %s", k, untimed[k], it)
			}
		}

		var got [][]string
		for token := ""; ; {
			q := "/bucket?list-type=2&max-keys=900"
			if token != "" {
				q += "&continuation-token=" + token
			}
			r := get(t, s, q)
			if r.status != 200 {
				t.Fatalf("S3 %s: %d %s", q, r.status, r.body)
			}
			got = append(got, lastModifiedRe.FindAllStringSubmatch(r.body, -1)...)
			m := s3TokenRe.FindStringSubmatch(r.body)
			if m == nil {
				break
			}
			token = m[1]
		}
		if len(got) != len(keys) {
			t.Fatalf("S3: %d objects, want %d", len(got), len(keys))
		}
		for i, m := range got {
			want := updated + "Z"
			if !untimed[keys[i]] {
				want = createdOf(i).Format("2006-01-02T15:04:05.000Z")
			}
			if m[1] != keys[i] || m[2] != want {
				t.Fatalf("S3 %d: %s %s, want %s %s", i, m[1], m[2], keys[i], want)
			}
		}
	}
}
