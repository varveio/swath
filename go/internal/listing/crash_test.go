// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"

	"github.com/varveio/swath/go/internal/durable"
	"github.com/varveio/swath/go/internal/faults"
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
	return Config{Root: "s3://bkt", Out: out, PartSize: 6 << 10, Workers: 8, Memory: 1 << 20, Resume: resume,
		S3: S3Config{Endpoint: url, PathStyle: true, Anonymous: true}}
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
	for _, point := range faults.Points {
		for _, nth := range []int64{1, 4, 12} {
			t.Run(fmt.Sprintf("%s#%d", point, nth), func(t *testing.T) {
				out := filepath.Join(t.TempDir(), "ds")
				var seen atomic.Int64
				restore := faults.Set(func(p string) error {
					if p == point && seen.Add(1) == nth {
						return faults.ErrCrash
					}
					return nil
				})
				_, err := Run(context.Background(), crashConfig(url, out, false))
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
				if point == "success" {
					if !errors.Is(err, ErrComplete) {
						t.Fatalf("resume after _SUCCESS: %v", err)
					}
				} else if err != nil {
					t.Fatalf("resume: %v", err)
				}
				sameDataset(t, point, ref, out)
				t.Logf("resumed %d parts (%d rows), listed %d more rows", sum.ResumedParts, sum.ResumedRows, sum.Rows)
			})
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
	cfg.PartSize = 7 << 10
	var mm *durable.MismatchError
	if _, err := Run(context.Background(), cfg); !errors.As(err, &mm) || mm.Field != "part size" {
		t.Fatalf("changed part size accepted: %v", err)
	}
	cfg = crashConfig(url, out, false)
	if _, err := Run(context.Background(), cfg); err == nil {
		t.Fatal("a fresh run over an interrupted one was accepted without -overwrite")
	}
	cfg = crashConfig(url, out, true)
	cfg.Workers = 3
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
	cfg.S3.NoJournal = true
	if _, err := Run(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	sameDataset(t, "no journal", ref, out)
	if _, err := os.Stat(filepath.Join(out, ".swath", "journal")); !os.IsNotExist(err) {
		t.Fatalf("journal written with NoJournal: %v", err)
	}
	cfg = crashConfig(url, "", false)
	if sum, err := Run(context.Background(), cfg); err != nil || sum.Rows != 8000 {
		t.Fatalf("count-only: %v %d", err, sum.Rows)
	}
}
