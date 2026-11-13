// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/varveio/swath/internal/dataset"
	"github.com/varveio/swath/internal/dataset/datasettest"
	"github.com/varveio/swath/internal/faults"
	"github.com/varveio/swath/internal/listing"
	"github.com/varveio/swath/internal/source/s3/s3test"
	"github.com/varveio/swath/objfile/gcs/gcsfake"
)

// committedReport checks a report of an invocation that found the dataset
// m committed: the dataset's rows, part bytes and object bytes (want; nil:
// absent) from the manifest, nothing of its own listed or emitted, and the
// outcome completed, or incomplete with exit 1 when the manifest's coverage
// is partial.
func committedReport(t *testing.T, name string, raw []byte, m dataset.Manifest, want *int64) {
	t.Helper()
	var rep listing.Report
	if err := json.Unmarshal(raw, &rep); err != nil {
		t.Fatal(err)
	}
	var rows, bytes int64
	for _, p := range m.Parts {
		rows, bytes = rows+p.Rows, bytes+p.Bytes
	}
	o := rep.Output
	if rows == 0 || o.RowsCommitted != rows || o.RowsDurable != rows || o.OutputBytes != bytes || o.RowsEmitted != 0 || o.RowsRetained != 0 || o.PartsWritten != 0 {
		t.Fatalf("%s: output %+v; the manifest has %d rows, %d bytes", name, o, rows, bytes)
	}
	if got := o.CommittedObjectBytes; (got == nil) != (want == nil) || got != nil && *got != *want {
		t.Fatalf("%s: committed_object_bytes %v, want %v", name, got, want)
	}
	if has := strings.Contains(string(raw), `"committed_object_bytes"`); has != (want != nil) {
		t.Fatalf("%s: committed_object_bytes present %v", name, has)
	}
	class, code := listing.ClassCompleted, exitCompleted
	if !m.Coverage.Complete {
		class, code = listing.ClassIncomplete, exitFatal
	}
	if oc := rep.Outcome; oc.Status != listing.StatusCommitted || oc.Class != class || oc.ExitCode != code || oc.Complete != m.Coverage.Complete {
		t.Fatalf("%s: outcome %+v", name, oc)
	}
}

// TestResumeCompleteAccount: swath resume of a finished local dataset
// exits 0 and its report accounts for the committed dataset from the
// manifest (rows_committed, output_bytes, committed_object_bytes), with
// nothing of its own. A manifest without committed_object_bytes (written before the
// member) leaves committed_object_bytes absent, never 0.
func TestResumeCompleteAccount(t *testing.T) {
	datasettest.RequireJournal(t)
	captureStderr(t)
	ctx := context.Background()
	src := t.TempDir()
	var want int64
	for i, size := range []int{0, 1, 70, 4096, 12345} {
		if err := os.WriteFile(filepath.Join(src, "f"+string(rune('a'+i))), make([]byte, size), 0o644); err != nil {
			t.Fatal(err)
		}
		want += int64(size)
	}
	if err := os.Mkdir(filepath.Join(src, "dir"), 0o755); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(t.TempDir(), "ds")
	if err := run(ctx, []string{src, "-o", dir}, io.Discard, false); err != nil {
		t.Fatal(err)
	}
	var m dataset.Manifest
	readJSON(t, filepath.Join(dir, dataset.ManifestName), &m)
	if m.CommittedObjectBytes == nil || *m.CommittedObjectBytes != want {
		t.Fatalf("manifest committed_object_bytes %v, want %d", m.CommittedObjectBytes, want)
	}
	report := filepath.Join(t.TempDir(), "report.json")
	if err := resume(ctx, []string{"--report", report, dir}, io.Discard); err != nil {
		t.Fatalf("resume of a complete dataset: %v", err)
	}
	b, err := os.ReadFile(report)
	if err != nil {
		t.Fatal(err)
	}
	committedReport(t, "complete", b, m, &want)

	// The same dataset as an earlier build wrote its manifest.
	m.CommittedObjectBytes = nil
	if err := dataset.ReplaceJSON(filepath.Join(dir, dataset.ManifestName), m, dataset.Durable); err != nil {
		t.Fatal(err)
	}
	if err := resume(ctx, []string{"--report", report, dir}, io.Discard); err != nil {
		t.Fatalf("resume of a complete dataset: %v", err)
	}
	if b, err = os.ReadFile(report); err != nil {
		t.Fatal(err)
	}
	committedReport(t, "legacy manifest", b, m, nil)
}

// TestRemoteResumeFindsCommit: a run writing to a bucket is killed once its
// manifest is created, its answer lost (no marker of the outcome, no
// run.json); swath resume WORK_DIR --report FILE finds the commit made and
// finishes its aftermath, and both its report and the run.json it puts
// beside the manifest account for the committed dataset from the manifest:
// completed with exit 0 when it covers its scope, incomplete with exit 1
// when a key limit left it partial, as --report says.
func TestRemoteResumeFindsCommit(t *testing.T) {
	datasettest.RequireJournal(t)
	captureStderr(t)
	t.Setenv("STORAGE_EMULATOR_HOST", "")
	s3test.NoCredentials(t)
	keys := s3test.Keys(5252, 7)
	var all int64
	for _, k := range keys {
		all += int64(len(k)) // each object's body is its key
	}
	src := s3test.Bucket(t, keys)
	for _, c := range []struct {
		name  string
		extra []string
		diag  bool
	}{
		{"complete", nil, false},
		// A partial commit (a key limit) is a diag run's, never resumable;
		// listing.TestCommittedOutcome covers its run.json outcome.
	} {
		t.Run(c.name, func(t *testing.T) {
			f := gcsfake.New("dst")
			dst := httptest.NewServer(f)
			t.Cleanup(dst.Close)
			ctx := context.Background()
			work := filepath.Join(t.TempDir(), "work")
			args := append([]string{"s3://bkt", "--endpoint-url", src, "--s3-path-style", "--anonymous", "--workers", "2", "--part-size", "4KiB",
				"-o", "gs://dst/ds", "--work-dir", work, "--output-endpoint-url", dst.URL + "/storage/v1/", "--output-gcs-credentials-file", serviceAccount(t, "tok")}, c.extra...)
			restore := faults.Set(func(p string) error {
				if p == "manifest-created" {
					return faults.ErrCrash
				}
				return nil
			})
			err := run(ctx, args, io.Discard, c.diag)
			restore()
			if !errors.Is(err, faults.ErrCrash) {
				t.Fatalf("first attempt: %v", err)
			}
			o, ok := f.Get("ds/" + dataset.ManifestName)
			if !ok {
				t.Fatal("no manifest")
			}
			if _, ok := f.Get("ds/" + dataset.ReportName); ok {
				t.Fatal("run.json before the resume")
			}
			var m dataset.Manifest
			if err := json.Unmarshal(o.Data, &m); err != nil || len(m.Parts) < 1 {
				t.Fatalf("manifest: %v, %d parts", err, len(m.Parts))
			}
			if m.Coverage.Complete != (c.name == "complete") {
				t.Fatalf("manifest coverage %+v", m.Coverage)
			}
			want := m.CommittedObjectBytes
			if c.name == "complete" && (want == nil || *want != all) {
				t.Fatalf("manifest committed_object_bytes %v, want %d", want, all)
			}
			report := filepath.Join(t.TempDir(), "report.json")
			err = resume(ctx, []string{work, "--report", report, "--output-gcs-credentials-file", serviceAccount(t, "tok2")}, io.Discard)
			code := exitCompleted
			if err != nil {
				code = exitCode(t, err)
			}
			if wantCode := map[bool]int{true: exitCompleted, false: exitFatal}[m.Coverage.Complete]; code != wantCode {
				t.Fatalf("resume: exit %d, want %d (%v)", code, wantCode, err)
			}
			b, err := os.ReadFile(report)
			if err != nil {
				t.Fatal(err)
			}
			committedReport(t, "--report", b, m, want)
			var rep listing.Report
			_ = json.Unmarshal(b, &rep)
			if r := rep.Output.Remote; r == nil || r.Commit != "resumed" {
				t.Fatalf("--report remote %+v", r)
			}
			rj, ok := f.Get("ds/" + dataset.ReportName)
			if !ok {
				t.Fatal("no run.json after the resume")
			}
			committedReport(t, "run.json", rj.Data, m, want)
		})
	}
}
