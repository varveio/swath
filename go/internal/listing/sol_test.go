// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/faults"
	"github.com/varveio/swath/go/internal/journal"
)

// TestCredentialRotationResumes: credentials are not identity. A run
// signed with one access key, interrupted, resumes signed with another
// (the same route and region) to the uninterrupted dataset; rotated keys
// together with another region are refused for the region.
func TestCredentialRotationResumes(t *testing.T) {
	url := crashBucket(t)
	signed := func(out string, resume bool, key, region string) Config {
		t.Setenv("AWS_ACCESS_KEY_ID", key)
		t.Setenv("AWS_SECRET_ACCESS_KEY", "secret-"+key)
		cfg := crashConfig(url, out, resume)
		o := cfg.Source.Options.(*S3Config)
		o.Anonymous, o.Region = false, region
		return cfg
	}
	ref := filepath.Join(t.TempDir(), "ref")
	if _, err := Run(context.Background(), signed(ref, false, "AKIDFIRST", "us-east-1")); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(t.TempDir(), "ds")
	var n atomic.Int64
	restore := faults.Set(func(p string) error {
		if p == "renamed" && n.Add(1) == 2 {
			return faults.ErrCrash
		}
		return nil
	})
	_, err := Run(context.Background(), signed(out, false, "AKIDFIRST", "us-east-1"))
	restore()
	if !errors.Is(err, faults.ErrCrash) {
		t.Fatalf("crash not injected: %v", err)
	}
	var mm *journal.MismatchError
	if _, err := Run(context.Background(), signed(out, true, "AKIDROTATED", "eu-west-1")); !errors.As(err, &mm) || mm.Field != "region" {
		t.Fatalf("rotated keys and another region: %v, want a region mismatch", err)
	}
	sum, err := Run(context.Background(), signed(out, true, "AKIDROTATED", "us-east-1"))
	if err != nil {
		t.Fatalf("resume with rotated keys: %v", err)
	}
	if sum.Output.PartsRetained == 0 {
		t.Fatal("the resume retained no part")
	}
	if st := s3Stats(sum); st == nil || st.Anonymous {
		t.Fatalf("the resume did not sign: %+v", st)
	}
	sameDataset(t, "rotation", ref, out)
}

// TestWatchdogWriteInFlightAtRelease: a watchdog write that is under way
// when the run ends holds the output's ownership: the run does not release
// its lock (nor return) until the write finishes, the lock is still held
// during it, and a write after the run is refused.
func TestWatchdogWriteInFlightAtRelease(t *testing.T) {
	url := crashBucket(t)
	out := filepath.Join(t.TempDir(), "ds")
	pr := &Progress{}
	entered, finish := make(chan struct{}), make(chan struct{})
	heldDuring := make(chan bool, 1)
	var once atomic.Bool
	restore := faults.Set(func(p string) error {
		if p == "manifest" && once.CompareAndSwap(false, true) {
			go pr.Owned(func() { // the watchdog's tick, as cmd/swath's supervisor writes
				close(entered)
				<-finish
				l, err := journal.TakeLock(out)
				if err == nil {
					l.Release()
				}
				heldDuring <- errors.Is(err, journal.ErrLocked)
			})
			<-entered
		}
		return nil
	})
	defer restore()
	cfg := crashConfig(url, out, false)
	cfg.Progress = pr
	done := make(chan error, 1)
	go func() {
		_, err := Run(context.Background(), cfg)
		done <- err
	}()
	<-entered
	select {
	case err := <-done:
		t.Fatalf("the run ended (%v) while a watchdog write was in flight", err)
	case <-time.After(300 * time.Millisecond):
	}
	close(finish)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if !<-heldDuring {
		t.Fatal("the output lock was not held during the in-flight write")
	}
	if pr.Owned(func() { t.Error("a write after the run ran") }) {
		t.Fatal("ownership outlived the run")
	}
	l, err := journal.TakeLock(out)
	if err != nil {
		t.Fatalf("the lock was not released: %v", err)
	}
	l.Release()
}
