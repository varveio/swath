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
	url, _ := crashBackend(t)
	return url
}

// crashBackend is crashBucket with its backend, to change the source.
func crashBackend(t *testing.T) (string, *s3mem.Backend) {
	t.Helper()
	for _, k := range []string{"AWS_PROFILE", "AWS_SESSION_TOKEN", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_REGION", "AWS_DEFAULT_REGION"} {
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
	return srv.URL, b
}

func crashConfig(url, out string, resume bool) Config {
	// A small reorder budget (512 rows) keeps listing and publication
	// interleaved, so crashes during listing find published parts.
	return Config{Source: SourceConfig{URI: "s3://bkt", Options: &S3Config{Endpoint: url, PathStyle: true, Anonymous: true}}, Output: OutputConfig{Dir: out, Resume: resume}, Limits: Limits{PartSize: 6 << 10, Workers: 8, Memory: 256 << 10, Spill: SpillLimits{Off: true}}}
}

// TestCrashMatrix kills the run (an injected crash that skips all cleanup,
// as a kill would) at every crash point, at its first and a later
// occurrence, then resumes; the final dataset must be byte-identical to an
// uninterrupted run. It is the step-2 gate (contracts I1, I5, I6), for each
// journaled provider (spill is provider-neutral: S3 runs it both ways).
func TestCrashMatrix(t *testing.T) {
	s3url, gcsep := crashBucket(t), gcsBucket(t, 8000)
	for _, prov := range []struct {
		name   string
		config func(out string, resume bool) Config
		spill  []bool
		nth    []int64 // occurrences to crash at, within each run (the GCS run has fewer parts and requests)
	}{
		{"s3", func(out string, resume bool) Config { return crashConfig(s3url, out, resume) }, []bool{false, true}, []int64{1, 4, 12}},
		{"gcs", func(out string, resume bool) Config { return gcsConfig(gcsep, out, resume) }, []bool{false}, []int64{1, 3, 7}},
	} {
		ref := filepath.Join(t.TempDir(), "ref")
		if _, err := Run(context.Background(), prov.config(ref, false)); err != nil {
			t.Fatal(err)
		}
		for _, spill := range prov.spill {
			for _, point := range faults.Points {
				for _, nth := range prov.nth {
					t.Run(fmt.Sprintf("%s/spill=%v/%s#%d", prov.name, spill, point, nth), func(t *testing.T) {
						out := filepath.Join(t.TempDir(), "ds")
						var seen atomic.Int64
						restore := faults.Set(func(p string) error {
							if p == point && seen.Add(1) == nth {
								return faults.ErrCrash
							}
							return nil
						})
						cfg := prov.config(out, false)
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
						sum, err := Run(context.Background(), prov.config(out, true))
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
	if _, err := Run(context.Background(), cfg); !errors.As(err, &mm) || mm.Field != "route" {
		t.Fatalf("changed endpoint accepted: %v", err)
	}
	cfg = crashConfig(url, out, true)
	cfg.Source.Options.(*S3Config).Region = "eu-west-1" // the same endpoint, another signing region
	if _, err := Run(context.Background(), cfg); !errors.As(err, &mm) || mm.Field != "region" {
		t.Fatalf("changed region accepted: %v", err)
	}
	t.Setenv("AWS_REGION", "us-west-2") // nothing asked, the environment resolves another region
	if _, err := Run(context.Background(), crashConfig(url, out, true)); !errors.As(err, &mm) || mm.Field != "region" {
		t.Fatalf("a resume in an environment with another region was accepted: %v", err)
	}
	t.Setenv("AWS_REGION", "")
	cfg = crashConfig(url, out, true)
	cfg.Source.Options.(*S3Config).Sequential, cfg.Source.Options.(*S3Config).MaxKeys = true, 100
	if _, err := Run(context.Background(), cfg); err == nil || !strings.Contains(err.Error(), "cannot resume") {
		t.Fatalf("a capped resume was accepted: %v", err)
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
	// The refusal happens under the lock and is settled there (the outcome
	// of the interrupted run must not stand as current).
	cfg := crashConfig(url, out, true)
	var settled []error
	cfg.Settle = func(_ Report, err error) {
		if l, lerr := journal.TakeLock(out); !errors.Is(lerr, journal.ErrLocked) {
			l.Release()
			t.Error("Settle ran without the lock")
		}
		settled = append(settled, err)
	}
	if _, err := Run(context.Background(), cfg); !errors.Is(err, ErrPartAltered) {
		t.Fatalf("altered part accepted: %v", err)
	}
	if len(settled) != 1 || !errors.Is(settled[0], ErrPartAltered) {
		t.Fatalf("the refused recovery was settled %v", settled)
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
	// A crashed run's spill files are reclaimed by the resume, spill on or off.
	orphan := filepath.Join(out, dataset.JournalDir, "spill", "orphan")
	if err := os.MkdirAll(orphan, 0o755); err != nil {
		t.Fatal(err)
	}
	cfg := crashConfig(url, out, true)
	cfg.Limits.Spill.Off = true
	if _, err := Run(context.Background(), cfg); err != nil {
		t.Fatalf("resume: %v", err)
	}
	if _, err := os.Stat(filepath.Dir(orphan)); err == nil {
		t.Fatal("the resume left a crashed run's spill files")
	}
	sameDataset(t, "after ENOSPC", ref, out)
}

// crashAtRecorded leaves a run whose third part's record is durable and
// whose rename never happened: the part exists only in .swath/parts.
func crashAtRecorded(t *testing.T, url, out string) {
	t.Helper()
	var n atomic.Int64
	restore := faults.Set(func(p string) error {
		if p == "recorded" && n.Add(1) == 3 {
			return faults.ErrCrash
		}
		return nil
	})
	defer restore()
	if _, err := Run(context.Background(), crashConfig(url, out, false)); !errors.Is(err, faults.ErrCrash) {
		t.Fatal(err)
	}
}

// TestAcknowledgedRecordDamageRefuses: when the last record is durable but
// its part not yet renamed, damage to that record (bit rot, an overrunning
// length, a damaged closing byte) is refused and the acknowledged temporary
// part kept; a record that reads as torn (bytes missing, or a tail of zeros
// that never reached the disk) is dropped and the run resumes identically.
// A damaged header length is refused too, never taken for "no journal".
func TestAcknowledgedRecordDamageRefuses(t *testing.T) {
	url := crashBucket(t)
	ref := filepath.Join(t.TempDir(), "ref")
	if _, err := Run(context.Background(), crashConfig(url, ref, false)); err != nil {
		t.Fatal(err)
	}
	for _, damage := range []string{"bit rot", "overrunning length", "header length", "closing byte", "closing byte lost", "torn", "torn, zero tail"} {
		out := filepath.Join(t.TempDir(), "ds")
		crashAtRecorded(t, url, out)
		path := journal.Path(out)
		b, _ := os.ReadFile(path)
		var offs []int
		for off := 0; off+8 <= len(b); off += 8 + int(binary.LittleEndian.Uint32(b[off:off+4])) {
			offs = append(offs, off)
		}
		last := offs[len(offs)-1]
		switch damage {
		case "bit rot":
			b[last+20] ^= 0xff
		case "overrunning length":
			binary.LittleEndian.PutUint32(b[last:last+4], binary.LittleEndian.Uint32(b[last:last+4])+7)
		case "header length":
			binary.LittleEndian.PutUint32(b[:4], uint32(len(b)))
		case "closing byte":
			b[len(b)-1] = ']'
		case "closing byte lost":
			b[len(b)-1] = ' '
		case "torn":
			b = b[:len(b)-5]
		case "torn, zero tail":
			clear(b[len(b)-5:])
		}
		os.WriteFile(path, b, 0o644)
		_, err := Run(context.Background(), crashConfig(url, out, true))
		if strings.HasPrefix(damage, "torn") {
			if err != nil {
				t.Fatalf("torn: %v", err)
			}
			sameDataset(t, damage, ref, out)
			if ents, _ := os.ReadDir(filepath.Join(out, dataset.Workspace, dataset.SuspectDir)); len(ents) != 0 {
				t.Fatalf("%s: the reproduced suspect part was kept", damage)
			}
			continue
		}
		if !errors.Is(err, journal.ErrCorrupt) {
			t.Fatalf("%s: resume did not refuse: %v", damage, err)
		}
		if _, err := os.Stat(dataset.TempPath(out, 2)); err != nil {
			t.Fatalf("%s: the acknowledged temporary part is gone: %v", damage, err)
		}
	}
}

// TestAmbiguousTailRefusesOnChangedSource: a record that reads as torn
// while its part was not yet renamed may have been acknowledged and
// truncated or zeroed since: nothing on disk tells. With the source
// changed inside that part's range, relisting would silently replace what
// the run committed; the resume refuses instead, with the part kept, and
// keeps refusing (Sol, fourth review, minimum change 5; the static-source
// cases above cannot see the loss).
func TestAmbiguousTailRefusesOnChangedSource(t *testing.T) {
	for _, damage := range []string{"truncated", "zero tail", "truncated, rest deleted"} {
		url, b := crashBackend(t)
		out := filepath.Join(t.TempDir(), "ds")
		crashAtRecorded(t, url, out)
		st, err := journal.Read(out)
		if err != nil || len(st.Records) != 3 {
			t.Fatalf("%s: %v (%d records)", damage, err, len(st.Records))
		}
		acked := st.Records[2].Part
		// The first key of part 2 is deleted from the source.
		if _, err := b.DeleteObject("bkt", string(acked.MinKey)); err != nil {
			t.Fatal(err)
		}
		if strings.HasSuffix(damage, "rest deleted") {
			// Nothing is left after the last intact record: the resume
			// lists no part 2 at all.
			for i := range 8000 {
				if k := fmt.Sprintf("d%02d/sub%d/obj+%06d", i%9, i%4, i); k > string(st.Records[1].Part.MaxKey) {
					b.DeleteObject("bkt", k)
				}
			}
		}
		path := journal.Path(out)
		jb, _ := os.ReadFile(path)
		if strings.HasPrefix(damage, "truncated") {
			jb = jb[:len(jb)-5]
		} else {
			clear(jb[len(jb)-5:])
		}
		os.WriteFile(path, jb, 0o644)
		kept := filepath.Join(out, dataset.Workspace, dataset.SuspectDir, dataset.PartName(2))
		for attempt := range 2 {
			_, err := Run(context.Background(), crashConfig(url, out, true))
			if !errors.Is(err, ErrAmbiguousTail) {
				t.Fatalf("%s, attempt %d: resume did not refuse: %v", damage, attempt, err)
			}
			got, err := measure(kept)
			if err != nil || got.MD5 != acked.MD5 || got.Bytes != acked.Bytes {
				t.Fatalf("%s, attempt %d: the acknowledged part is not kept: %v %+v", damage, attempt, err, got)
			}
			if _, err := os.Stat(filepath.Join(out, dataset.DataDir, dataset.PartName(2))); err == nil {
				t.Fatalf("%s: a replacement part 2 was published", damage)
			}
			if st, err := journal.Read(out); err != nil || len(st.Records) != 2 {
				t.Fatalf("%s: a replacement record 2 was written: %v", damage, err)
			}
		}
	}
}

// TestAcknowledgedTailTruncationRefuses: a last record cut short after its
// part was renamed into data/ was acknowledged (the rename follows the
// record's sync), so the cut is not a torn append: recovery refuses before
// deleting anything, for a truncated frame and a damaged closing byte alike,
// and the published part stays.
func TestAcknowledgedTailTruncationRefuses(t *testing.T) {
	url := crashBucket(t)
	for _, damage := range []string{"truncated", "zero tail", "closing byte"} {
		out := filepath.Join(t.TempDir(), "ds")
		var n atomic.Int64
		restore := faults.Set(func(p string) error {
			if p == "renamed" && n.Add(1) == 3 {
				return faults.ErrCrash
			}
			return nil
		})
		if _, err := Run(context.Background(), crashConfig(url, out, false)); !errors.Is(err, faults.ErrCrash) {
			t.Fatal(err)
		}
		restore()
		path := journal.Path(out)
		b, _ := os.ReadFile(path)
		switch damage {
		case "truncated":
			b = b[:len(b)-5]
		case "zero tail":
			clear(b[len(b)-5:])
		case "closing byte":
			b[len(b)-1] = ']'
		}
		os.WriteFile(path, b, 0o644)
		if _, err := Run(context.Background(), crashConfig(url, out, true)); !errors.Is(err, journal.ErrCorrupt) {
			t.Fatalf("%s: resume did not refuse: %v", damage, err)
		}
		if _, err := os.Stat(filepath.Join(out, dataset.DataDir, dataset.PartName(2))); err != nil {
			t.Fatalf("%s: the published part is gone: %v", damage, err)
		}
	}
}

// TestRepeatedRecovery: a resume that crashes inside its own recovery,
// after renaming the recorded part and before retiring the temporary
// parts, is recovered by the next resume, identically.
func TestRepeatedRecovery(t *testing.T) {
	url := crashBucket(t)
	ref := filepath.Join(t.TempDir(), "ref")
	if _, err := Run(context.Background(), crashConfig(url, ref, false)); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(t.TempDir(), "ds")
	crashAtRecorded(t, url, out)
	restore := faults.Set(func(p string) error {
		if p == "recovered" {
			return faults.ErrCrash
		}
		return nil
	})
	_, err := Run(context.Background(), crashConfig(url, out, true))
	restore()
	if !errors.Is(err, faults.ErrCrash) {
		t.Fatalf("recovery crash: %v", err)
	}
	if _, err := os.Stat(filepath.Join(out, "data", dataset.PartName(2))); err != nil {
		t.Fatalf("the recorded part was not renamed before the crash: %v", err)
	}
	if _, err := Run(context.Background(), crashConfig(url, out, true)); err != nil {
		t.Fatal(err)
	}
	sameDataset(t, "repeated recovery", ref, out)
}
