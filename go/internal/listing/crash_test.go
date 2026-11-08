// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/johannesboyne/gofakes3/backend/s3mem"

	"github.com/varveio/swath/go/internal/dataset"
	"github.com/varveio/swath/go/internal/faults"
	"github.com/varveio/swath/go/internal/journal"
	"github.com/varveio/swath/go/internal/source/gcs/gcstest"
	"github.com/varveio/swath/go/internal/source/s3/s3test"
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
	s3test.NoCredentials(t)
	t.Setenv("AWS_REGION", "")
	t.Setenv("AWS_DEFAULT_REGION", "")
	b := s3test.Backend(t, gcstest.Keys(8000), s3mem.WithTimeSource(&step{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}))
	return s3test.Serve(t, s3test.Delayed(s3test.Handler(b), time.Millisecond)), b
}

func crashConfig(url, out string, resume bool) Config {
	// A small reorder budget (512 rows) keeps listing and publication
	// interleaved, so crashes during listing find published parts.
	return Config{Source: SourceConfig{URI: "s3://bkt", Options: &S3Config{Endpoint: url, PathStyle: true, Anonymous: true}}, Output: OutputConfig{Dir: out, Resume: resume}, Limits: Limits{PartSize: 6 << 10, Workers: 8, RowBudget: 128 << 10, Spill: SpillLimits{Off: true}, PartInterval: -1}}
}

// TestCrashMatrix kills the run (an injected crash that skips all cleanup,
// as a kill would) at every crash point, at its first and a later
// occurrence, then resumes; the final dataset must be byte-identical to an
// uninterrupted run. It is the step-2 gate (contracts I1, I5, I6), for each
// journaled provider (spill is provider-neutral: S3 runs it both ways; with
// spill on its durable spill is sealed, ledgered and retired, and the kill
// points include each of those steps: the resume replays what it accepts).
// With the part interval on, part boundaries depend on timing (the crashed
// run's and the resume's differ): the resume must then complete without a
// refusal and hold exactly the clean run's rows, in order. The object
// stores answer in bursts, so their interval arm cuts by time now and
// then; the trickling scripted source (one key per range, 1 ms apart, a
// 4 ms interval) cuts nearly every part by time, so every crash point
// meets a time-cut part.
func TestCrashMatrix(t *testing.T) {
	s3url, gcsep := crashBucket(t), gcsBucket(t, 8000)
	var trickle []string
	for i := range 400 {
		trickle = append(trickle, fmt.Sprintf("k%04d", i))
	}
	const off = -1
	for _, prov := range []struct {
		name      string
		config    func(out string, resume bool) Config
		spill     []bool
		intervals []time.Duration
		nth       []int64 // occurrences to crash at, within each run (the GCS run has fewer parts and requests)
		// absent: the points not on this provider's path (the scripted
		// source sends no requests and has no range engine). Every other
		// point's first occurrence must be reached; later ones are
		// optional, since a run may hold fewer parts or splits.
		absent []string
		// also: further occurrences of one point. journal-append#2 is the
		// first part's record (#1 is the header): the calibrating part
		// claimed but unrecorded, reproduced at its pinned boundary.
		also map[string][]int64
	}{
		{"s3", func(out string, resume bool) Config { return crashConfig(s3url, out, resume) }, []bool{false, true}, []time.Duration{off, time.Millisecond}, []int64{1, 4, 12}, nil, calibrating},
		{"gcs", func(out string, resume bool) Config { return gcsConfig(gcsep, out, resume) }, []bool{false}, []time.Duration{off, time.Millisecond}, []int64{1, 3, 7}, nil, calibrating},
		{"trickle", func(out string, resume bool) Config {
			scriptMu.Lock()
			scriptNext = &scripted{keys: trickle, chunk: 1, slow: time.Millisecond}
			scriptMu.Unlock()
			return Config{Source: SourceConfig{URI: "scripted://x"}, Output: OutputConfig{Dir: out, Resume: resume},
				Limits: Limits{PartSize: 2 << 10, Workers: 2, RowBudget: 128 << 10, Spill: SpillLimits{Off: true}, PartInterval: off}}
		}, []bool{false}, []time.Duration{4 * time.Millisecond}, []int64{1, 4, 12}, []string{"response-read", "split", "range-done"}, nil},
	} {
		ref := filepath.Join(t.TempDir(), "ref")
		if _, err := Run(context.Background(), prov.config(ref, false)); err != nil {
			t.Fatal(err)
		}
		refKeys := checkContract(t, ref)
		for _, mode := range []struct {
			spill    bool
			interval time.Duration
		}{{false, off}, {true, off}, {false, time.Millisecond}, {true, time.Millisecond}, {false, 4 * time.Millisecond}} {
			spill := mode.spill
			if !slices.Contains(prov.spill, spill) || !slices.Contains(prov.intervals, mode.interval) {
				continue
			}
			for _, point := range faults.Points {
				for _, nth := range append(slices.Clone(prov.nth), prov.also[point]...) {
					t.Run(fmt.Sprintf("%s/spill=%v/interval=%v/%s#%d", prov.name, spill, mode.interval > 0, point, nth), func(t *testing.T) {
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
						cfg.Limits.PartInterval = mode.interval
						_, err := Run(context.Background(), cfg)
						restore()
						if seen.Load() < nth {
							if err != nil {
								t.Fatal(err)
							}
							switch {
							case slices.Contains(prov.absent, point):
								t.Skipf("%s is not on this provider's path", point)
							case nth == 1:
								t.Fatalf("%s never reached: the matrix no longer crosses this boundary", point)
							}
							t.Skipf("%s reached %d times only", point, seen.Load())
						}
						if !errors.Is(err, faults.ErrCrash) {
							t.Fatalf("crash not injected: %v", err)
						}
						rcfg := prov.config(out, true)
						rcfg.Limits.PartInterval = mode.interval
						sum, err := Run(context.Background(), rcfg)
						if point == "manifest" {
							if !errors.Is(err, ErrComplete) {
								t.Fatalf("resume after the manifest: %v", err)
							}
						} else if err != nil {
							t.Fatalf("resume: %v", err)
						}
						if mode.interval > 0 {
							if got := checkContract(t, out); !slices.Equal(got, refKeys) {
								t.Fatalf("%d rows after the resume, want %d", len(got), len(refKeys))
							}
						} else {
							sameDataset(t, point, ref, out)
						}
						t.Logf("resumed %d parts (%d rows), listed %d more rows, %d parts cut by time", sum.Output.PartsRetained, sum.Output.RowsRetained, sum.Output.RowsEmitted, sum.Resources.PartTimeCuts)
					})
				}
			}
		}
	}
}

// calibrating is the crash matrices' extra occurrence: the first part's
// journal append.
var calibrating = map[string][]int64{"journal-append": {2}}

// TestCalibratingClaimRecoversRepeatedly: the first size-cut part (the
// one that calibrates the part size) is claimed and the run killed before
// its record; the resume reproduces it at its pinned boundary and is
// itself killed (before the reproduction is claimed, at its claim again,
// or after the next part's record), and a second resume finishes. Every
// part is the clean run's, byte for byte: the reproduction calibrates as
// the original did.
func TestCalibratingClaimRecoversRepeatedly(t *testing.T) {
	url := crashBucket(t)
	ref := filepath.Join(t.TempDir(), "ref")
	if _, err := Run(context.Background(), crashConfig(url, ref, false)); err != nil {
		t.Fatal(err)
	}
	crashAt := func(out string, resume bool, point string, nth int64) {
		t.Helper()
		var seen int64
		restore := faults.Set(func(p string) error {
			if p == point {
				if seen++; seen == nth {
					return faults.ErrCrash
				}
			}
			return nil
		})
		_, err := Run(context.Background(), crashConfig(url, out, resume))
		restore()
		if !errors.Is(err, faults.ErrCrash) {
			t.Fatalf("%s#%d: crash not injected: %v", point, nth, err)
		}
	}
	for _, again := range []struct {
		point string
		nth   int64
	}{{"part-closed", 1}, {"journal-append", 1}, {"recorded", 2}} {
		t.Run(fmt.Sprintf("%s#%d", again.point, again.nth), func(t *testing.T) {
			out := filepath.Join(t.TempDir(), "ds")
			crashAt(out, false, "journal-append", 2)
			crashAt(out, true, again.point, again.nth)
			if _, err := Run(context.Background(), crashConfig(url, out, true)); err != nil {
				t.Fatal(err)
			}
			sameDataset(t, "calibrating claim, recovered twice", ref, out)
		})
	}
	// Inside recovery itself: the claimed part has been moved into the
	// suspect directory and the resume dies syncing that directory; the
	// next resume finds the suspect (or the claim, if the move was lost)
	// and reproduces it.
	t.Run("suspect-dir-sync", func(t *testing.T) {
		out := filepath.Join(t.TempDir(), "ds")
		crashAt(out, false, "journal-append", 2)
		var synced atomic.Int64
		restore := faults.SetIO(func(op, name string, _ []byte) (int, error) {
			if op == "sync" && filepath.Base(name) == dataset.SuspectDir {
				synced.Add(1)
				return 0, faults.ErrCrash
			}
			return 0, nil
		})
		_, err := Run(context.Background(), crashConfig(url, out, true))
		restore()
		if !errors.Is(err, faults.ErrCrash) || synced.Load() == 0 {
			t.Fatalf("recovery not killed at the suspect directory's sync: %v", err)
		}
		if m, _ := filepath.Glob(filepath.Join(out, dataset.Workspace, dataset.SuspectDir, "*")); len(m) == 0 {
			t.Fatal("no suspect set aside before the kill: the test proves nothing")
		}
		if _, err := Run(context.Background(), crashConfig(url, out, true)); err != nil {
			t.Fatal(err)
		}
		sameDataset(t, "calibrating suspect, recovery killed", ref, out)
	})
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
	if sum, err := Run(context.Background(), cfg); err != nil || sum.Output.RowsEmitted != 8000 {
		t.Fatalf("count-only: %v %d", err, sum.Output.RowsEmitted)
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
	if sum.Output.RowsEmitted >= 8000 {
		t.Fatalf("the pipeline ran to the end (%d rows ordered) after the encoder failed", sum.Output.RowsEmitted)
	}
}

// TestCompetingRunsAreRefused: while a run owns the output (paused inside
// an encoder), a resume and an overwrite by another run are refused before
// they touch anything, and the owner then finishes byte-identically
// (durability P1-2).
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
// place, and -resume publishes it (durability P1-3).
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
	rep, err := Run(context.Background(), crashConfig(url, out, false))
	restore()
	if !errors.Is(err, eio) {
		t.Fatalf("got %v", err)
	}
	// The report's durable totals are the journal's: the third part was
	// acknowledged (its append returned) although its rename failed.
	st, jerr := journal.Read(out)
	if jerr != nil {
		t.Fatal(jerr)
	}
	var bytes int64
	for _, rec := range st.Records {
		bytes += rec.Part.Bytes
	}
	last := st.Records[len(st.Records)-1]
	if len(st.Records) < 3 || rep.Output.PartsWritten != len(st.Records) || rep.Output.OutputBytes != bytes || rep.Output.RowsDurable != last.RowsTotal {
		t.Fatalf("report: %d parts, %d bytes, %d durable rows; the journal: %d records, %d bytes, %d rows",
			rep.Output.PartsWritten, rep.Output.OutputBytes, rep.Output.RowsDurable, len(st.Records), bytes, last.RowsTotal)
	}
	if _, err := Run(context.Background(), crashConfig(url, out, true)); err != nil {
		t.Fatalf("resume: %v", err)
	}
	sameDataset(t, "after rename failure", ref, out)
}

// TestDamagedJournalRefusesBeforeDeleting: a damaged interior frame length
// (overrunning the file) and a corrupt last frame of an acknowledged,
// published part are refused, and nothing is deleted (durability P1-6).
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
// between cannot add a second final part (durability P2, finality).
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
		if _, err := os.Stat(dataset.ClaimPath(out, 2)); err != nil {
			t.Fatalf("%s: the acknowledged temporary part is gone: %v", damage, err)
		}
	}
}

// TestAmbiguousTailRefusesOnChangedSource: a record that reads as torn
// while its part was not yet renamed may have been acknowledged and
// truncated or zeroed since: nothing on disk tells. With the source
// changed inside that part's range, relisting would silently replace what
// the run committed; the resume refuses instead, with the part kept, and
// keeps refusing (durability review; the static-source cases above cannot
// see the loss).
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

// TestAcknowledgedWholeLastFrameLossPreservesPart: the last record is
// durable but its part not yet renamed, and the whole frame is then lost,
// the journal ending at the preceding frame boundary. That reads as a clean
// end (no torn bytes), the same as an append that never began, so the
// part in .swath/parts/ is set aside as the torn tail's is: an unchanged
// source reproduces it and resumes identically; a deleted row inside it or
// a deleted suffix is refused with the part kept, at every attempt
// (durability review).
func TestAcknowledgedWholeLastFrameLossPreservesPart(t *testing.T) {
	url0 := crashBucket(t)
	ref := filepath.Join(t.TempDir(), "ref")
	if _, err := Run(context.Background(), crashConfig(url0, ref, false)); err != nil {
		t.Fatal(err)
	}
	for _, change := range []string{"unchanged", "deleted row", "deleted suffix"} {
		url, b := crashBackend(t)
		out := filepath.Join(t.TempDir(), "ds")
		crashAtRecorded(t, url, out)
		st, err := journal.Read(out)
		if err != nil || len(st.Records) != 3 {
			t.Fatalf("%s: %v (%d records)", change, err, len(st.Records))
		}
		acked := st.Records[2].Part
		path := journal.Path(out)
		jb, _ := os.ReadFile(path)
		var offs []int
		for off := 0; off+8 <= len(jb); off += 8 + int(binary.LittleEndian.Uint32(jb[off:off+4])) {
			offs = append(offs, off)
		}
		os.WriteFile(path, jb[:offs[len(offs)-1]], 0o644)
		if st, err := journal.Read(out); err != nil || len(st.Records) != 2 || st.TornBytes != 0 {
			t.Fatalf("%s: the cut journal does not end cleanly after record 1: %v", change, err)
		}
		switch change {
		case "deleted row":
			if _, err := b.DeleteObject("bkt", string(acked.MinKey)); err != nil {
				t.Fatal(err)
			}
		case "deleted suffix":
			for i := range 8000 {
				if k := fmt.Sprintf("d%02d/sub%d/obj+%06d", i%9, i%4, i); k > string(st.Records[1].Part.MaxKey) {
					b.DeleteObject("bkt", k)
				}
			}
		}
		if change == "unchanged" {
			if _, err := Run(context.Background(), crashConfig(url, out, true)); err != nil {
				t.Fatalf("unchanged: %v", err)
			}
			sameDataset(t, change, ref, out)
			if ents, _ := os.ReadDir(filepath.Join(out, dataset.Workspace, dataset.SuspectDir)); len(ents) != 0 {
				t.Fatal("unchanged: the reproduced suspect part was kept")
			}
			continue
		}
		kept := filepath.Join(out, dataset.Workspace, dataset.SuspectDir, dataset.PartName(2))
		for attempt := range 2 {
			_, err := Run(context.Background(), crashConfig(url, out, true))
			if !errors.Is(err, ErrAmbiguousTail) {
				t.Fatalf("%s, attempt %d: resume did not refuse: %v", change, attempt, err)
			}
			got, err := measure(kept)
			if err != nil || got.MD5 != acked.MD5 || got.Bytes != acked.Bytes {
				t.Fatalf("%s, attempt %d: the acknowledged part is not kept: %v %+v", change, attempt, err, got)
			}
			if _, err := os.Stat(filepath.Join(out, dataset.DataDir, dataset.PartName(2))); err == nil {
				t.Fatalf("%s: a replacement part 2 was published", change)
			}
			if st, err := journal.Read(out); err != nil || len(st.Records) != 2 {
				t.Fatalf("%s: a replacement record 2 was written: %v", change, err)
			}
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
