// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"

	"github.com/varveio/swath/go/internal/dataset"
	"github.com/varveio/swath/go/internal/faults"
	"github.com/varveio/swath/go/internal/journal"
)

// crashBucket serves 8,000 keys with 1 ms per request, enough for many
// parts and parallel splits.
func crashBucket(t *testing.T) string {
	t.Helper()
	for _, k := range []string{"AWS_PROFILE", "AWS_SESSION_TOKEN", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"} {
		t.Setenv(k, "")
	}
	t.Setenv("AWS_CONFIG_FILE", "/nonexistent")
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", "/nonexistent")
	b := s3mem.New(s3mem.WithTimeSource(&step{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}))
	if err := b.CreateBucket("bkt"); err != nil {
		t.Fatal(err)
	}
	for i := range 8000 {
		k := fmt.Sprintf("d%02d/sub%d/obj+%06d", i%9, i%4, i)
		if _, err := b.PutObject("bkt", k, nil, bytes.NewReader([]byte(k)), int64(len(k)), nil); err != nil {
			t.Fatal(err)
		}
	}
	h := gofakes3.New(b).Server()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(time.Millisecond)
		h.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

func crashConfig(url, out string, resume bool) Config {
	// A small reorder budget (512 rows) keeps listing and publication
	// interleaved, so crashes during listing find published parts.
	return Config{Source: SourceConfig{URI: "s3://bkt", Options: &S3Config{Endpoint: url, PathStyle: true, Anonymous: true}}, Output: OutputConfig{Dir: out, Resume: resume}, Limits: Limits{PartSize: 6 << 10, Workers: 8, Memory: 256 << 10, Spill: SpillLimits{Off: true}}}
}

// TestCrashMatrix kills the run (an injected crash that skips all cleanup,
// as a kill would) at every crash point, at its first and a later
// occurrence, then resumes; the final dataset must be byte-identical to an
// uninterrupted run. It is the step-2 gate (contracts I1, I5, I6).
func TestCrashMatrix(t *testing.T) {
	url := crashBucket(t)
	ref := filepath.Join(t.TempDir(), "ref")
	if _, err := Run(context.Background(), crashConfig(url, ref, false)); err != nil {
		t.Fatal(err)
	}
	for _, spill := range []bool{false, true} {
		for _, point := range faults.Points {
			for _, nth := range []int64{1, 4, 12} {
				t.Run(fmt.Sprintf("spill=%v/%s#%d", spill, point, nth), func(t *testing.T) {
					out := filepath.Join(t.TempDir(), "ds")
					var seen atomic.Int64
					restore := faults.Set(func(p string) error {
						if p == point && seen.Add(1) == nth {
							return faults.ErrCrash
						}
						return nil
					})
					cfg := crashConfig(url, out, false)
					cfg.Limits.Spill.Off = !spill
					_, err := Run(context.Background(), cfg)
					restore()
					if seen.Load() < nth {
						if err != nil {
							t.Fatal(err)
						}
						t.Skipf("%s reached %d times only", point, seen.Load())
					}
					if !errors.Is(err, faults.ErrCrash) {
						t.Fatalf("crash not injected: %v", err)
					}
					sum, err := Run(context.Background(), crashConfig(url, out, true))
					if point == "manifest" {
						if !errors.Is(err, ErrComplete) {
							t.Fatalf("resume after the manifest: %v", err)
						}
					} else if err != nil {
						t.Fatalf("resume: %v", err)
					}
					sameDataset(t, point, ref, out)
					t.Logf("resumed %d parts (%d rows), listed %d more rows", sum.Output.PartsRetained, sum.Output.RowsRetained, sum.Output.RowsListed)
				})
			}
		}
	}
}

// TestResumeChecksIdentity: a resume with settings that change the dataset
// is refused with a typed error; one with new workers is accepted.
func TestResumeChecksIdentity(t *testing.T) {
	url := crashBucket(t)
	out := filepath.Join(t.TempDir(), "ds")
	var n atomic.Int64
	restore := faults.Set(func(p string) error {
		if p == "renamed" && n.Add(1) == 2 {
			return faults.ErrCrash
		}
		return nil
	})
	_, err := Run(context.Background(), crashConfig(url, out, false))
	restore()
	if !errors.Is(err, faults.ErrCrash) {
		t.Fatal(err)
	}
	cfg := crashConfig(url, out, true)
	cfg.Limits.PartSize = 7 << 10
	var mm *journal.MismatchError
	if _, err := Run(context.Background(), cfg); !errors.As(err, &mm) || mm.Field != "part size" {
		t.Fatalf("changed part size accepted: %v", err)
	}
	cfg = crashConfig(url, out, true)
	cfg.Source.Options.(*S3Config).Endpoint = strings.Replace(url, "127.0.0.1", "localhost", 1) // another route, maybe another bucket
	if _, err := Run(context.Background(), cfg); !errors.As(err, &mm) || mm.Field != "endpoint" {
		t.Fatalf("changed endpoint accepted: %v", err)
	}
	cfg = crashConfig(url, out, false)
	if _, err := Run(context.Background(), cfg); err == nil {
		t.Fatal("a fresh run over an interrupted one was accepted without -overwrite")
	}
	cfg = crashConfig(url, out, true)
	cfg.Limits.Workers = 3
	cfg.Source.Options.(*S3Config).Endpoint = strings.ToUpper(url[:4]) + url[4:] + "/" // an equivalent spelling of the same endpoint
	if _, err := Run(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	if _, err := Run(context.Background(), cfg); !errors.Is(err, ErrComplete) {
		t.Fatalf("second resume: %v", err)
	}
	if _, err := Run(context.Background(), crashConfig(url, filepath.Join(t.TempDir(), "none"), true)); !errors.Is(err, ErrNothingToResume) {
		t.Fatalf("resume of nothing: %v", err)
	}
}

// TestAlteredPartIsRefused: a retained part that no longer matches its
// journal record (size or digest) stops the resume.
func TestAlteredPartIsRefused(t *testing.T) {
	url := crashBucket(t)
	out := filepath.Join(t.TempDir(), "ds")
	var n atomic.Int64
	restore := faults.Set(func(p string) error {
		if p == "renamed" && n.Add(1) == 3 {
			return faults.ErrCrash
		}
		return nil
	})
	_, err := Run(context.Background(), crashConfig(url, out, false))
	restore()
	if !errors.Is(err, faults.ErrCrash) {
		t.Fatal(err)
	}
	part := filepath.Join(out, "data", "part-00001.parquet")
	b, err := os.ReadFile(part)
	if err != nil {
		t.Fatal(err)
	}
	b[len(b)/2] ^= 1
	os.WriteFile(part, b, 0o644)
	if _, err := Run(context.Background(), crashConfig(url, out, true)); !errors.Is(err, ErrPartAltered) {
		t.Fatalf("altered part accepted: %v", err)
	}
}

// TestNoJournalAndCountOnly: without a journal the dataset is the same, and
// an S3 count-only run works.
func TestNoJournalAndCountOnly(t *testing.T) {
	url := crashBucket(t)
	ref := filepath.Join(t.TempDir(), "ref")
	if _, err := Run(context.Background(), crashConfig(url, ref, false)); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(t.TempDir(), "ds")
	cfg := crashConfig(url, out, false)
	cfg.Output.NoJournal = true
	if _, err := Run(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	sameDataset(t, "no journal", ref, out)
	if _, err := os.Stat(filepath.Join(out, ".swath", "journal")); !os.IsNotExist(err) {
		t.Fatalf("journal written with NoJournal: %v", err)
	}
	cfg = crashConfig(url, "", false)
	if sum, err := Run(context.Background(), cfg); err != nil || sum.Output.RowsListed != 8000 {
		t.Fatalf("count-only: %v %d", err, sum.Output.RowsListed)
	}
}

// TestEncoderFailureStopsListing: the first asynchronous failure (here an
// encoder error) cancels the whole pipeline through one cause: listing
// stops early and Run returns that error, not a cancellation.
func TestEncoderFailureStopsListing(t *testing.T) {
	url := crashBucket(t)
	diskFull := errors.New("disk full")
	var n atomic.Int64
	restore := faults.Set(func(p string) error {
		if p == "encoding" && n.Add(1) == 2 {
			return diskFull
		}
		return nil
	})
	defer restore()
	cfg := crashConfig(url, filepath.Join(t.TempDir(), "ds"), false)
	cfg.Limits.Spill.Off = true
	sum, err := Run(context.Background(), cfg)
	if !errors.Is(err, diskFull) {
		t.Fatalf("got %v, want the encoder's error", err)
	}
	if sum.Output.RowsListed >= 8000 {
		t.Fatalf("the pipeline ran to the end (%d rows ordered) after the encoder failed", sum.Output.RowsListed)
	}
}

// TestCompetingRunsAreRefused: while a run owns the output (paused inside
// an encoder), a resume and an overwrite by another run are refused before
// they touch anything, and the owner then finishes byte-identically (Sol
// P1-2).
func TestCompetingRunsAreRefused(t *testing.T) {
	url := crashBucket(t)
	ref := filepath.Join(t.TempDir(), "ref")
	if _, err := Run(context.Background(), crashConfig(url, ref, false)); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(t.TempDir(), "ds")
	paused, release := make(chan struct{}), make(chan struct{})
	var n atomic.Int64
	restore := faults.Set(func(p string) error {
		if p == "encoding" && n.Add(1) == 3 {
			close(paused)
			<-release
		}
		return nil
	})
	defer restore()
	done := make(chan error, 1)
	go func() { _, err := Run(context.Background(), crashConfig(url, out, false)); done <- err }()
	<-paused
	snapshot := func() []string {
		var names []string
		filepath.Walk(out, func(p string, info os.FileInfo, err error) error {
			if err == nil && !info.IsDir() {
				names = append(names, p)
			}
			return nil
		})
		return names
	}
	before := snapshot()
	if _, err := Run(context.Background(), crashConfig(url, out, true)); !errors.Is(err, journal.ErrLocked) {
		t.Fatalf("competing resume: %v", err)
	}
	over := crashConfig(url, out, false)
	over.Output.Replace = true
	if _, err := Run(context.Background(), over); !errors.Is(err, journal.ErrLocked) {
		t.Fatalf("competing overwrite: %v", err)
	}
	// The owner's other encoders keep working while one is paused, so new
	// temporary parts may appear and earlier ones be published; nothing
	// the owner had may be gone (a published part counts as its temporary).
	norm := func(p string) string {
		return strings.Replace(p, string(filepath.Separator)+filepath.Join(dataset.Workspace, dataset.PartsDir)+string(filepath.Separator),
			string(filepath.Separator)+dataset.DataDir+string(filepath.Separator), 1)
	}
	after := snapshot()
	for i := range after {
		after[i] = norm(after[i])
	}
	for _, name := range before {
		if !slices.Contains(after, norm(name)) {
			t.Fatalf("a refused run removed the owner's %s:\n%v\n%v", name, before, after)
		}
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	sameDataset(t, "owner", ref, out)
}

// TestRenameFailureAfterRecordResumes: an ordinary error between a part's
// durable record and its rename leaves the recorded temporary part in
// place, and -resume publishes it (Sol P1-3).
func TestRenameFailureAfterRecordResumes(t *testing.T) {
	url := crashBucket(t)
	ref := filepath.Join(t.TempDir(), "ref")
	if _, err := Run(context.Background(), crashConfig(url, ref, false)); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(t.TempDir(), "ds")
	eio := errors.New("rename: input/output error")
	var n atomic.Int64
	restore := faults.Set(func(p string) error {
		if p == "rename" && n.Add(1) == 3 {
			return eio
		}
		return nil
	})
	_, err := Run(context.Background(), crashConfig(url, out, false))
	restore()
	if !errors.Is(err, eio) {
		t.Fatalf("got %v", err)
	}
	if _, err := Run(context.Background(), crashConfig(url, out, true)); err != nil {
		t.Fatalf("resume: %v", err)
	}
	sameDataset(t, "after rename failure", ref, out)
}

// TestDamagedJournalRefusesBeforeDeleting: a damaged interior frame length
// (overrunning the file) and a corrupt last frame of an acknowledged,
// published part are refused, and nothing is deleted (Sol P1-6).
func TestDamagedJournalRefusesBeforeDeleting(t *testing.T) {
	url := crashBucket(t)
	for _, damage := range []string{"interior length", "last frame"} {
		out := filepath.Join(t.TempDir(), "ds")
		var n atomic.Int64
		restore := faults.Set(func(p string) error {
			if p == "renamed" && n.Add(1) == 4 {
				return faults.ErrCrash
			}
			return nil
		})
		_, err := Run(context.Background(), crashConfig(url, out, false))
		restore()
		if !errors.Is(err, faults.ErrCrash) {
			t.Fatal(err)
		}
		path := journal.Path(out)
		b, _ := os.ReadFile(path)
		// Walk the frames: header, then records 0..3.
		var offs []int
		for off := 0; off+8 <= len(b); {
			offs = append(offs, off)
			off += 8 + int(binary.LittleEndian.Uint32(b[off:off+4]))
		}
		switch damage {
		case "interior length": // record 1's length overruns the file
			binary.LittleEndian.PutUint32(b[offs[2]:offs[2]+4], uint32(len(b)))
		case "last frame": // bit rot in the last acknowledged record
			b[len(b)-3] ^= 0xff
		}
		os.WriteFile(path, b, 0o644)
		before, _ := os.ReadDir(filepath.Join(out, "data"))
		if _, err := Run(context.Background(), crashConfig(url, out, true)); !errors.Is(err, journal.ErrCorrupt) {
			t.Fatalf("%s: resume did not refuse: %v", damage, err)
		}
		after, _ := os.ReadDir(filepath.Join(out, "data"))
		if len(after) != len(before) {
			t.Fatalf("%s: recovery deleted parts (%d -> %d) before refusing", damage, len(before), len(after))
		}
	}
}

// TestResumeAfterFinalPartDoesNotList: once the final part is durable a
// resume publishes without listing again, so a bucket that changed in
// between cannot add a second final part (Sol P2, finality).
func TestResumeAfterFinalPartDoesNotList(t *testing.T) {
	url := crashBucket(t)
	ref := filepath.Join(t.TempDir(), "ref")
	if _, err := Run(context.Background(), crashConfig(url, ref, false)); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(t.TempDir(), "ds")
	restore := faults.Set(func(p string) error {
		if p == "published" { // every part durable and published, no manifest yet
			return faults.ErrCrash
		}
		return nil
	})
	_, err := Run(context.Background(), crashConfig(url, out, false))
	restore()
	if !errors.Is(err, faults.ErrCrash) {
		t.Fatal(err)
	}
	sum, err := Run(context.Background(), crashConfig(url, out, true))
	if err != nil {
		t.Fatal(err)
	}
	if s3Stats(sum) != nil && s3Stats(sum).Counters.Value("requests") != 0 {
		t.Fatalf("resume after the final part listed again (%d requests)", s3Stats(sum).Counters.Value("requests"))
	}
	sameDataset(t, "final", ref, out)
}

// TestENOSPCOnJournalResumes: a disk-full error on a journal append fails
// the run without losing anything durable; -resume completes it.
func TestENOSPCOnJournalResumes(t *testing.T) {
	url := crashBucket(t)
	ref := filepath.Join(t.TempDir(), "ref")
	if _, err := Run(context.Background(), crashConfig(url, ref, false)); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(t.TempDir(), "ds")
	var n atomic.Int64
	restore := faults.Set(func(p string) error {
		if p == "journal-append" && n.Add(1) == 4 {
			return syscall.ENOSPC
		}
		return nil
	})
	_, err := Run(context.Background(), crashConfig(url, out, false))
	restore()
	if !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("got %v", err)
	}
	if _, err := Run(context.Background(), crashConfig(url, out, true)); err != nil {
		t.Fatalf("resume: %v", err)
	}
	sameDataset(t, "after ENOSPC", ref, out)
}
