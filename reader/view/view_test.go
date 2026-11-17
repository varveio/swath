// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package view

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	seekparquet "github.com/varveio/seekparquet/go"

	"github.com/varveio/swath/reader"
	"github.com/varveio/swath/reader/listing"
)

// objectSchemas are the schemas of current objects, which the view reads
// alike.
var objectSchemas = []reader.SchemaID{reader.S3ObjectsV1, reader.GCSObjectsV1, reader.AzureObjectsV1}

// TestEvidenceFromTheManifest: every answer carries the manifest's
// evidence and never widens it: its digest, interval, publication, scope
// and, only for a complete, unfiltered listing with nothing excluded, its
// count of unreadable entries.
func TestEvidenceFromTheManifest(t *testing.T) {
	dir := write(t, fixture{id: reader.S3ObjectsV1, rows: objects()})
	inv := open(t, dir)
	raw, err := os.ReadFile(filepath.Join(dir, seekparquet.ManifestName))
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	e := inv.evidence(listing.StopCompleted)
	at := func(s string) time.Time {
		v, err := time.Parse(time.RFC3339, s)
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	if e.Origin != listing.OriginInventory || e.Manifest != "sha256:"+hex.EncodeToString(sum[:]) || e.Scope != nil ||
		!e.Started.Equal(at("2026-11-12T09:00:00Z")) || !e.Finished.Equal(at("2026-11-12T09:00:01Z")) ||
		!e.Published.Equal(at("2026-11-12T09:00:02Z")) || e.Unlocated == nil || *e.Unlocated != 0 || e.Stop != listing.StopCompleted {
		t.Fatalf("evidence %+v", e)
	}
	*e.Unlocated, e.Started = 9, nil
	if again := inv.evidence(listing.StopCompleted); *again.Unlocated != 0 || again.Started == nil {
		t.Fatal("an answer's evidence shares the inventory's")
	}

	for _, c := range []struct {
		name string
		set  map[string]any
		want *int64
	}{
		{"unreadable entries", map[string]any{"complete": false, "unreadable": 3}, ptr[int64](3)},
		{"stopped early", map[string]any{"complete": false, "stop_reason": "max_keys"}, nil},
		{"a filter", map[string]any{"filters": []any{map[string]any{"filter": "include", "value": "*.txt"}}}, nil},
		{"an exclusion", map[string]any{"exclusions": []string{"out/"}}, nil},
		{"no count", map[string]any{"unreadable": nil}, nil},
		{"no filters member", map[string]any{"filters": nil}, nil},
		{"complete with unreadable entries", map[string]any{"unreadable": 2}, nil},
	} {
		t.Run(c.name, func(t *testing.T) {
			ext := coverage(extension(reader.S3ObjectsV1), c.set)
			inv := open(t, write(t, fixture{id: reader.S3ObjectsV1, ext: ext, rows: objects()[:3]}))
			got := inv.evidence(listing.StopCompleted).Unlocated
			if (got == nil) != (c.want == nil) || got != nil && *got != *c.want {
				t.Fatalf("Unlocated = %v, want %v", got, c.want)
			}
		})
	}

	// A manifest that records when its listing finished, not when it
	// started: the start is unknown, the finish stands.
	ext := coverage(extension(reader.S3ObjectsV1), map[string]any{"listing_started_at": nil})
	e = open(t, write(t, fixture{id: reader.S3ObjectsV1, ext: ext, rows: objects()[:3]})).evidence(listing.StopCompleted)
	if e.Started != nil || e.Finished == nil || !e.Finished.Equal(at("2026-11-12T09:00:01Z")) {
		t.Errorf("only a finish recorded: Started %v, Finished %v", e.Started, e.Finished)
	}
}

// TestOpenRefuses: a dataset of versions, whose keys repeat, and one whose
// manifest records no scope are refused as unsupported.
func TestOpenRefuses(t *testing.T) {
	version := map[string]any{"is_latest": true, "is_delete_marker": false}
	versions := write(t, fixture{id: reader.S3VersionsV1, rows: []rec{{key: "a", extra: version}, {key: "b", extra: version}}})
	ext := extension(reader.S3ObjectsV1)
	delete(ext, "scope")
	noScope := write(t, fixture{id: reader.S3ObjectsV1, ext: ext, rows: objects()[:2]})
	for name, dir := range map[string]string{"versions": versions, "no scope": noScope} {
		d, err := reader.OpenDir(context.Background(), dir, seekparquet.OpenOptions{})
		if err != nil {
			t.Fatal(err)
		}
		defer d.Close()
		if _, err := Open(d); !errors.Is(err, listing.ErrUnsupported) {
			t.Errorf("%s: Open = %v, want ErrUnsupported", name, err)
		}
	}
}

// TestEmptyInventory: a complete listing of nothing proves every key in its
// scope absent, and answers every question with nothing.
func TestEmptyInventory(t *testing.T) {
	ctx := context.Background()
	inv := open(t, write(t, fixture{id: reader.S3ObjectsV1}))
	if r, err := inv.Lookup(ctx, []byte("a")); err != nil || r.State != listing.LookupAbsent {
		t.Fatalf("Lookup = %+v, %v", r, err)
	}
	if p, err := inv.Page(ctx, listing.PageRequest{Limit: 10}); err != nil || len(p.Entries) != 0 || p.Next != "" || p.Self != nil {
		t.Fatalf("Page = %+v, %v", p, err)
	}
	s, err := inv.Walk(ctx, listing.WalkRequest{}, func(b listing.WalkBatch) error {
		t.Fatalf("emit %+v", b)
		return nil
	})
	if err != nil || s.Entries != 0 || s.Evidence.Stop != listing.StopCompleted {
		t.Fatalf("Walk = %+v, %v", s, err)
	}
	sums, _, err := inv.Summarize(ctx, []listing.Target{{Subtree: true}, {Key: []byte("a")}}, listing.SummaryOptions{})
	if err != nil || sums[0].Entries != 0 || *sums[0].Bytes != 0 || sums[1].State != listing.LookupAbsent {
		t.Fatalf("Summarize = %+v, %v", sums, err)
	}
}

// TestRowFaults: a value swath never writes is refused when the view reads
// it, never handed on or counted: by Walk and Page, and by Summarize where
// it sums the value (a key's form is not one), as listing.ErrInvalidPage
// with the reader's error beside it. A common prefix holding a NUL is
// refused as its rows' keys are.
func TestRowFaults(t *testing.T) {
	ctx := context.Background()
	for _, c := range []struct {
		name string
		f    fixture
		want error
	}{
		{"negative size", fixture{id: reader.S3ObjectsV1, rows: []rec{{key: "a", size: ptr[int64](-1)}}}, reader.ErrRow},
		{"negative space on disk", fixture{id: reader.LocalV1, rows: []rec{{key: "a", kind: "file", alloc: ptr[int64](-8)}}}, reader.ErrRow},
		{"a kind swath does not write", fixture{id: reader.LocalV1, rows: []rec{{key: "a", kind: "door"}}}, reader.ErrRow},
		{"a directory without its slash", fixture{id: reader.LocalV1, rows: []rec{{key: "a", kind: "dir"}}}, reader.ErrRow},
		{"a file with a slash", fixture{id: reader.LocalV1, rows: []rec{{key: "a/", kind: "file"}}}, reader.ErrRow},
		{"a NUL in a key", fixture{id: reader.S3ObjectsV1, rows: []rec{{key: "a\x00b", size: ptr[int64](1)}}}, listing.ErrInvalidPage},
		{"a NUL in a common prefix", fixture{id: reader.S3ObjectsV1, rows: []rec{{key: "a\x00/b", size: ptr[int64](1)}}}, listing.ErrInvalidPage},
	} {
		t.Run(c.name, func(t *testing.T) {
			inv := open(t, write(t, c.f))
			_, err := inv.Walk(ctx, listing.WalkRequest{Workers: 1}, func(listing.WalkBatch) error { return nil })
			if !errors.Is(err, c.want) || !errors.Is(err, listing.ErrInvalidPage) {
				t.Errorf("Walk = %v, want %v", err, c.want)
			}
			if _, err := inv.Page(ctx, listing.PageRequest{Limit: 5}); !errors.Is(err, c.want) || !errors.Is(err, listing.ErrInvalidPage) {
				t.Errorf("Page = %v, want %v", err, c.want)
			}
			if c.want == listing.ErrInvalidPage || strings.Contains(c.name, "slash") {
				return
			}
			if _, _, err := inv.Summarize(ctx, []listing.Target{{Subtree: true}}, listing.SummaryOptions{}); !errors.Is(err, c.want) || !errors.Is(err, listing.ErrInvalidPage) {
				t.Errorf("Summarize = %v, want %v", err, c.want)
			}
		})
	}
}

// TestFailureClasses: a failure to read the dataset is returned in a class
// of package listing, the cause kept; a context's error and an error the
// view classed are returned as they are.
func TestFailureClasses(t *testing.T) {
	ended, cancel := context.WithCancel(context.Background())
	cancel()
	io := errors.New("read: input/output error")
	for _, c := range []struct {
		ctx       context.Context
		err, want error
	}{
		{context.Background(), &os.PathError{Op: "open", Err: os.ErrPermission}, listing.ErrAuth},
		{context.Background(), statusError(403), listing.ErrAuth},
		{context.Background(), &os.PathError{Op: "open", Err: os.ErrNotExist}, listing.ErrNotFound},
		{context.Background(), &seekparquet.RefusalError{Code: seekparquet.CodeInvalidParquet}, listing.ErrInvalidPage},
		{context.Background(), &seekparquet.UnsupportedError{Code: seekparquet.UnsupportedEncoding}, listing.ErrUnsupported},
		{context.Background(), io, listing.ErrUnavailable},
		{context.Background(), errNotDirectory, listing.ErrNotDirectory},
		{ended, io, context.Canceled},
	} {
		err := failed(c.ctx, c.err)
		if !errors.Is(err, c.want) || c.ctx.Err() == nil && !errors.Is(err, c.err) {
			t.Errorf("failed(%v) = %v, want %v", c.err, err, c.want)
		}
		if c.want == listing.ErrUnavailable != listing.Retryable(err) {
			t.Errorf("failed(%v) = %v, retryable %t", c.err, err, listing.Retryable(err))
		}
	}
}

// statusError is a store's answer with an HTTP status, as objfile's carry.
type statusError int

func (e statusError) Error() string       { return "the store answered " + strconv.Itoa(int(e)) }
func (e statusError) HTTPStatusCode() int { return int(e) }

// TestRequestChecks: a request of the wrong form is ErrInvalidRequest,
// before anything is read.
func TestRequestChecks(t *testing.T) {
	ctx := context.Background()
	objects := open(t, write(t, fixture{id: reader.S3ObjectsV1, rows: objects()[:3]}))
	files := open(t, write(t, fixture{id: reader.LocalV1, rows: tree()}))
	emit := func(listing.WalkBatch) error { return nil }
	for name, err := range map[string]error{
		"empty key":               second(objects.Lookup(ctx, nil)),
		"NUL key":                 second(objects.Lookup(ctx, []byte("a\x00"))),
		"absolute local key":      second(files.Lookup(ctx, []byte("/a.txt"))),
		"zero limit":              second(objects.Page(ctx, listing.PageRequest{})),
		"local prefix no slash":   second(files.Page(ctx, listing.PageRequest{Prefix: []byte("d"), Limit: 1})),
		"local walk no slash":     second(files.Walk(ctx, listing.WalkRequest{Prefix: []byte("d")}, emit)),
		"negative workers":        second(objects.Walk(ctx, listing.WalkRequest{Workers: -1}, emit)),
		"no emit":                 second(objects.Walk(ctx, listing.WalkRequest{}, nil)),
		"negative budget":         second(objects.Walk(ctx, listing.WalkRequest{Budget: listing.Budget{MaxEntries: -1}}, emit)),
		"no kind":                 third(objects.Summarize(ctx, nil, listing.SummaryOptions{Kinds: []listing.Kind{0}})),
		"empty exact target":      third(objects.Summarize(ctx, []listing.Target{{}}, listing.SummaryOptions{})),
		"local subtree no slash":  third(files.Summarize(ctx, []listing.Target{{Key: []byte("d"), Subtree: true}}, listing.SummaryOptions{})),
		"negative child budget":   second(objects.Page(ctx, listing.PageRequest{Limit: 1, ChildTotals: &listing.SummaryOptions{Budget: listing.Budget{MaxEntries: -1}}})),
		"negative page duration":  second(objects.Page(ctx, listing.PageRequest{Limit: 1, MaxDuration: -1})),
		"NUL in a subtree target": third(objects.Summarize(ctx, []listing.Target{{Key: []byte{0}, Subtree: true}}, listing.SummaryOptions{})),
	} {
		if !errors.Is(err, listing.ErrInvalidRequest) {
			t.Errorf("%s: %v, want ErrInvalidRequest", name, err)
		}
	}
}

func second[T any](_ T, err error) error { return err }

func third[T, U any](_ T, _ U, err error) error { return err }
