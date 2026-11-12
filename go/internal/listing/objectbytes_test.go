// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"net/http"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/varveio/swath/go/internal/dataset"
	"github.com/varveio/swath/go/internal/dataset/datasettest"
	"github.com/varveio/swath/go/internal/faults"
	"github.com/varveio/swath/go/internal/journal"
	"github.com/varveio/swath/go/internal/source/gcs/gcstest"
	"github.com/varveio/swath/go/internal/source/s3/s3test"
)

// keyBytes is the object bytes of s3test's objects named keys in [lo, hi]
// (each object's body is its key: its size is the key's length).
func keyBytes(keys []string, lo, hi string) int64 {
	var n int64
	for _, k := range keys {
		if k >= lo && k <= hi {
			n += int64(len(k))
		}
	}
	return n
}

// allKeyBytes is the object bytes of every one of keys.
func allKeyBytes(keys []string) int64 {
	var n int64
	for _, k := range keys {
		n += int64(len(k))
	}
	return n
}

// committedObjectBytes checks the three places a committed dataset's
// object bytes are stated, the report sum, the manifest in out and the run
// report beside it, against want (nil: none states them).
func committedObjectBytes(t *testing.T, name string, sum Report, out string, want *int64) {
	t.Helper()
	same := func(got *int64) bool { return (got == nil) == (want == nil) && (got == nil || *got == *want) }
	str := func(v *int64) any {
		if v == nil {
			return nil
		}
		return *v
	}
	if !same(sum.Output.CommittedObjectBytes) {
		t.Fatalf("%s: report committed_object_bytes %v, want %v", name, str(sum.Output.CommittedObjectBytes), str(want))
	}
	b, err := os.ReadFile(filepath.Join(out, dataset.ManifestName))
	if err != nil {
		t.Fatal(err)
	}
	var m dataset.Manifest
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	if !same(m.CommittedObjectBytes) {
		t.Fatalf("%s: manifest committed_object_bytes %v, want %v", name, str(m.CommittedObjectBytes), str(want))
	}
	// Unknown is absent, never 0: the member is not written at all.
	if has := bytes.Contains(b, []byte(`"committed_object_bytes"`)); has != (want != nil) {
		t.Fatalf("%s: the manifest's committed_object_bytes member present %v, want %v", name, has, want != nil)
	}
	var run map[string]any
	readJSON(t, filepath.Join(out, dataset.ReportName), &run)
	got, has := run["output"].(map[string]any)["committed_object_bytes"]
	if has != (want != nil) || has && got.(float64) != float64(*want) {
		t.Fatalf("%s: run.json committed_object_bytes %v (present %v), want %v", name, got, has, str(want))
	}
}

// TestCommittedObjectBytes: an uninterrupted run's dataset states its
// object bytes exactly: every part's count (in its journal record) is the
// sizes of the keys between its bounds, and their sum, the dataset's, is
// in the report, the manifest and run.json, equal to this invocation's
// object_bytes (it listed every row). A resume whose final part is
// already recorded (the commit only) states the same from the records.
func TestCommittedObjectBytes(t *testing.T) {
	datasettest.RequireJournal(t)
	keys := gcstest.Keys(8000)
	want := allKeyBytes(keys)
	url := crashBucket(t)

	ref := filepath.Join(t.TempDir(), "ref")
	sum, err := Run(context.Background(), crashConfig(url, ref, false))
	if err != nil {
		t.Fatal(err)
	}
	if sum.Output.ObjectBytes == nil || *sum.Output.ObjectBytes != want {
		t.Fatalf("object_bytes %v, want %d", sum.Output.ObjectBytes, want)
	}
	committedObjectBytes(t, "uninterrupted", sum, ref, &want)

	// Killed after every part is recorded, before the manifest: the
	// journal holds each part's count.
	out := filepath.Join(t.TempDir(), "ds")
	restore := faults.Set(func(p string) error {
		if p == "published" {
			return faults.ErrCrash
		}
		return nil
	})
	_, err = Run(context.Background(), crashConfig(url, out, false))
	restore()
	if !errors.Is(err, faults.ErrCrash) {
		t.Fatalf("crash not injected: %v", err)
	}
	st, err := journal.Read(out)
	if err != nil || len(st.Records) < 2 || !st.Last().Final {
		t.Fatalf("journal: %v, %d records", err, len(st.Records))
	}
	var total int64
	for i, rec := range st.Records {
		w := keyBytes(keys, string(rec.Part.MinKey), string(rec.Part.MaxKey))
		if rec.ObjectBytes == nil || *rec.ObjectBytes != w {
			t.Fatalf("record %d: object_bytes %v, want %d", i, rec.ObjectBytes, w)
		}
		total += w
	}
	if total != want {
		t.Fatalf("records sum to %d object bytes, want %d", total, want)
	}
	sum, err = Run(context.Background(), crashConfig(url, out, true))
	if err != nil {
		t.Fatal(err)
	}
	if sum.Output.RowsEmitted != 0 || sum.Output.RowsCommitted != int64(len(keys)) {
		t.Fatalf("a commit-only resume: emitted %d, committed %d", sum.Output.RowsEmitted, sum.Output.RowsCommitted)
	}
	committedObjectBytes(t, "commit-only resume", sum, out, &want)
	sameDataset(t, "commit-only resume", ref, out)
}

// TestCommittedObjectBytesResumed: a run killed at a crash point and
// resumed, with spill off and on, states the uninterrupted run's object
// bytes: the retained parts' counts from their records, the rest from the
// resume's writers. This invocation's object_bytes is what it listed only.
func TestCommittedObjectBytesResumed(t *testing.T) {
	datasettest.RequireJournal(t)
	want := allKeyBytes(gcstest.Keys(8000))
	url := crashBucket(t)
	for _, spill := range []bool{false, true} {
		for _, c := range []struct {
			point string
			nth   int64
		}{{"journal-append", 3}, {"recorded", 2}, {"renamed", 4}, {"encoding", 5}} {
			t.Run(fmt.Sprintf("spill=%v/%s#%d", spill, c.point, c.nth), func(t *testing.T) {
				out := filepath.Join(t.TempDir(), "ds")
				cfg := crashConfig(url, out, false)
				cfg.Limits.Spill.Off = !spill
				var seen atomic.Int64
				restore := faults.Set(func(p string) error {
					if p == c.point && seen.Add(1) == c.nth {
						return faults.ErrCrash
					}
					return nil
				})
				_, err := Run(context.Background(), cfg)
				restore()
				if !errors.Is(err, faults.ErrCrash) {
					t.Fatalf("crash not injected: %v", err)
				}
				cfg.Output.Resume = true
				sum, err := Run(context.Background(), cfg)
				if err != nil {
					t.Fatal(err)
				}
				if sum.Output.RowsRetained == 0 || sum.Output.ObjectBytes == nil || *sum.Output.ObjectBytes >= want {
					t.Fatalf("retained %d rows; this invocation's object_bytes %v of %d", sum.Output.RowsRetained, sum.Output.ObjectBytes, want)
				}
				committedObjectBytes(t, c.point, sum, out, &want)
			})
		}
	}
}

// TestCommittedObjectBytesReplayed: a resume that replays certified spill
// files (rows it never listed, which its object_bytes leaves out) states
// the dataset's object bytes exactly, each replayed row counted once.
func TestCommittedObjectBytesReplayed(t *testing.T) {
	datasettest.RequireJournal(t)
	want := allKeyBytes(gcstest.Keys(30000))
	url, _, ref, out := durableFixture(t)
	var m dataset.Manifest
	readJSON(t, filepath.Join(ref, dataset.ManifestName), &m)
	if m.CommittedObjectBytes == nil || *m.CommittedObjectBytes != want {
		t.Fatalf("the uninterrupted run's committed_object_bytes %v, want %d", m.CommittedObjectBytes, want)
	}
	sum, err := Run(context.Background(), durableConfig(url, out, true))
	if err != nil {
		t.Fatal(err)
	}
	d := sum.Resources.Spill.Durable
	if d == nil || d.Resume == nil || d.Resume.ReplayedRows == 0 {
		t.Fatalf("nothing replayed: %+v", d)
	}
	committedObjectBytes(t, "replayed", sum, out, &want)
	sameDataset(t, "replayed", ref, out)
}

// TestCommittedObjectBytesRetried: list requests that fail and are retried
// (the client's retries and the engine's requeues) count no row twice.
func TestCommittedObjectBytesRetried(t *testing.T) {
	keys := gcstest.Keys(8000)
	want := allKeyBytes(keys)
	s3test.NoCredentials(t)
	t.Setenv("AWS_REGION", "")
	t.Setenv("AWS_DEFAULT_REGION", "")
	h := s3test.Handler(s3test.Backend(t, keys))
	var n atomic.Int64
	url := s3test.Serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Has("list-type") && n.Add(1)%4 == 0 {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`<Error><Code>InternalError</Code><Message>m</Message></Error>`))
			return
		}
		h.ServeHTTP(w, r)
	}))
	out := filepath.Join(t.TempDir(), "ds")
	sum, err := Run(context.Background(), crashConfig(url, out, false))
	if err != nil {
		t.Fatal(err)
	}
	if sum.Requests == nil || sum.Requests.Retries == 0 {
		t.Fatalf("nothing retried: %+v", sum.Requests)
	}
	committedObjectBytes(t, "retried", sum, out, &want)
}

// TestCommittedObjectBytesLegacy: a resume of a run whose journal records
// carry no object bytes (an earlier build's) commits a dataset whose
// object bytes are unknown: absent from the report, the manifest and
// run.json, never 0; this invocation's own object_bytes stays.
func TestCommittedObjectBytesLegacy(t *testing.T) {
	datasettest.RequireJournal(t)
	url := crashBucket(t)
	out := filepath.Join(t.TempDir(), "ds")
	var seen atomic.Int64
	restore := faults.Set(func(p string) error {
		if p == "journal-append" && seen.Add(1) == 3 {
			return faults.ErrCrash
		}
		return nil
	})
	_, err := Run(context.Background(), crashConfig(url, out, false))
	restore()
	if !errors.Is(err, faults.ErrCrash) {
		t.Fatalf("crash not injected: %v", err)
	}
	if n := stripObjectBytes(t, out); n == 0 {
		t.Fatal("no record carried object bytes")
	}
	sum, err := Run(context.Background(), crashConfig(url, out, true))
	if err != nil {
		t.Fatal(err)
	}
	if sum.Output.RowsRetained == 0 || sum.Output.ObjectBytes == nil || *sum.Output.ObjectBytes == 0 {
		t.Fatalf("retained %d; object_bytes %v", sum.Output.RowsRetained, sum.Output.ObjectBytes)
	}
	committedObjectBytes(t, "legacy", sum, out, nil)
}

// stripObjectBytes rewrites out's journal as an earlier build of the same
// journal version wrote it: its records without object_bytes. It returns
// how many records had them.
func stripObjectBytes(t *testing.T, out string) int {
	t.Helper()
	path := journal.Path(out)
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	table := crc32.MakeTable(crc32.Castagnoli)
	var w bytes.Buffer
	n := 0
	for len(b) >= 8 {
		size := binary.LittleEndian.Uint32(b[:4])
		payload := b[8 : 8+size]
		b = b[8+size:]
		var rec map[string]any
		if err := json.Unmarshal(payload, &rec); err != nil {
			t.Fatal(err)
		}
		if _, ok := rec["object_bytes"]; ok {
			delete(rec, "object_bytes")
			n++
			if payload, err = json.Marshal(rec); err != nil {
				t.Fatal(err)
			}
		}
		var hdr [8]byte
		binary.LittleEndian.PutUint32(hdr[:4], uint32(len(payload)))
		binary.LittleEndian.PutUint32(hdr[4:], crc32.Update(crc32.Checksum(hdr[:4], table), table, payload))
		w.Write(hdr[:])
		w.Write(payload)
	}
	if err := os.WriteFile(path, w.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
	st, err := journal.Read(out)
	if err != nil {
		t.Fatal(err)
	}
	for i, rec := range st.Records {
		if rec.ObjectBytes != nil {
			t.Fatalf("record %d kept its object bytes", i)
		}
	}
	return n
}

// TestCommittedObjectBytesEmpty: a dataset of no part holds 0 object
// bytes, stated (a real zero), as is a dataset of empty objects.
func TestCommittedObjectBytesEmpty(t *testing.T) {
	s3test.NoCredentials(t)
	t.Setenv("AWS_REGION", "")
	t.Setenv("AWS_DEFAULT_REGION", "")
	zero := int64(0)
	out := filepath.Join(t.TempDir(), "empty")
	sum, err := Run(context.Background(), crashConfig(s3test.Bucket(t, nil), out, false))
	if err != nil {
		t.Fatal(err)
	}
	if sum.Output.PartsWritten != 0 || sum.Output.RowsCommitted != 0 {
		t.Fatalf("an empty bucket: %d parts, %d rows", sum.Output.PartsWritten, sum.Output.RowsCommitted)
	}
	committedObjectBytes(t, "empty", sum, out, &zero)

	// Objects of no bytes: one part, 0 object bytes.
	b := s3test.Backend(t, nil)
	for _, k := range []string{"a", "b", "c"} {
		if _, err := b.PutObject(s3test.Name, k, nil, bytes.NewReader(nil), 0, nil); err != nil {
			t.Fatal(err)
		}
	}
	out = filepath.Join(t.TempDir(), "zero")
	sum, err = Run(context.Background(), crashConfig(s3test.Serve(t, s3test.Handler(b)), out, false))
	if err != nil {
		t.Fatal(err)
	}
	if sum.Output.RowsCommitted != 3 {
		t.Fatalf("%d rows", sum.Output.RowsCommitted)
	}
	committedObjectBytes(t, "empty objects", sum, out, &zero)
}

// TestRemoteResumedCommitAccount: a remote run killed once its manifest is
// created (the creation's answer lost: no report, no finalization) is
// resumed; the resume finds the commit made, and the run.json it writes
// beside the manifest accounts for the committed dataset from the
// manifest: its rows, part bytes and object bytes, the coverage, and
// nothing listed or emitted by the invocation.
func TestRemoteResumedCommitAccount(t *testing.T) {
	eachProvider(t, func(t *testing.T, p provider) {
		datasettest.RequireJournal(t)
		want := allKeyBytes(gcstest.Keys(8000))
		url := crashBucket(t)
		d := newDestination(t, p, nil)
		ws := filepath.Join(t.TempDir(), "work")
		crashRemote(t, url, ws, d.output("ds"), false, "manifest-created", 1)
		if _, ok := d.Object("ds/" + dataset.ReportName); ok {
			t.Fatal("run.json before the resume")
		}
		_, err := Run(context.Background(), remoteConfig(url, ws, true, d.output("ds")))
		if !errors.Is(err, ErrComplete) {
			t.Fatalf("resume: %v", err)
		}
		m := d.manifest(t, "ds")
		if m.CommittedObjectBytes == nil || *m.CommittedObjectBytes != want {
			t.Fatalf("manifest committed_object_bytes %v, want %d", m.CommittedObjectBytes, want)
		}
		var partBytes int64
		for _, part := range m.Parts {
			partBytes += part.Bytes
		}
		o, ok := d.Object("ds/" + dataset.ReportName)
		if !ok {
			t.Fatal("no run.json after the resume")
		}
		var run Report
		if err := json.Unmarshal(o, &run); err != nil {
			t.Fatal(err)
		}
		out := run.Output
		if out.RowsCommitted != sumRows(m) || out.RowsCommitted != 8000 || out.RowsDurable != out.RowsCommitted || out.OutputBytes != partBytes ||
			out.CommittedObjectBytes == nil || *out.CommittedObjectBytes != want || out.RowsEmitted != 0 || out.RowsRetained != 0 ||
			out.Remote == nil || out.Remote.Commit != "resumed" {
			t.Fatalf("run.json output %+v (manifest: %d rows, %d bytes)", out, sumRows(m), partBytes)
		}
		if run.Outcome.Status != StatusCommitted || !run.Outcome.Complete || run.Outcome.StopReason != m.Coverage.StopReason {
			t.Fatalf("run.json outcome %+v", run.Outcome)
		}
	})
}
