// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build !windows

package main

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
)

// TestSIGKILLResume kills a real swath process with SIGKILL once parts are
// published, resumes it with swath resume, and requires the dataset to be
// byte-identical to an uninterrupted run's.
func TestSIGKILLResume(t *testing.T) {
	if testing.Short() {
		t.Skip("builds the binary")
	}
	bin := filepath.Join(t.TempDir(), "swath")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	b := s3mem.New()
	if err := b.CreateBucket("bkt"); err != nil {
		t.Fatal(err)
	}
	for i := range 6000 {
		k := fmt.Sprintf("p%02d/obj-%06d", i%11, i)
		if _, err := b.PutObject("bkt", k, nil, bytes.NewReader([]byte(k)), int64(len(k)), nil); err != nil {
			t.Fatal(err)
		}
	}
	h := gofakes3.New(b).Server()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(20 * time.Millisecond) // slow enough to kill mid-run
		h.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	args := func(out string, extra ...string) []string {
		return append([]string{"list", "s3://bkt", "-endpoint", srv.URL, "-path-style", "-anonymous", "-workers", "8",
			"-part-size", "4KiB", "-memory", "1MiB", "-o", out}, extra...)
	}
	ref := filepath.Join(t.TempDir(), "ref")
	if out, err := exec.Command(bin, args(ref)...).CombinedOutput(); err != nil {
		t.Fatalf("reference run: %v\n%s", err, out)
	}
	out := filepath.Join(t.TempDir(), "ds")
	cmd := exec.Command(bin, args(out)...)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		if _, err := os.Stat(filepath.Join(out, "data", "part-00003.parquet")); err == nil {
			break
		}
		if time.Now().After(deadline) {
			cmd.Process.Kill()
			t.Fatal("no part published in time")
		}
		time.Sleep(2 * time.Millisecond)
	}
	if err := cmd.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	cmd.Wait()
	if _, err := os.Stat(filepath.Join(out, "_SUCCESS")); err == nil {
		t.Skip("the run finished before the kill")
	}
	// Only runtime flags may change on resume: a frozen one is a usage error.
	if o, err := exec.Command(bin, "resume", out, "--part-size", "8KiB").CombinedOutput(); err == nil || !bytes.Contains(o, []byte("only workers")) {
		t.Fatalf("resume with a frozen flag: %v\n%s", err, o)
	} else if ee, ok := err.(*exec.ExitError); !ok || ee.ExitCode() != 2 {
		t.Fatalf("resume with a frozen flag: exit %v, want 2", err)
	}
	if o, err := exec.Command(bin, "resume", "--workers", "3", out).CombinedOutput(); err != nil {
		t.Fatalf("resume: %v\n%s", err, o)
	}
	parts, _ := filepath.Glob(filepath.Join(ref, "data", "*.parquet"))
	got, _ := filepath.Glob(filepath.Join(out, "data", "*.parquet"))
	if len(parts) < 5 || len(got) != len(parts) {
		t.Fatalf("%d parts, reference %d", len(got), len(parts))
	}
	for _, p := range parts {
		a, _ := os.ReadFile(p)
		b, err := os.ReadFile(filepath.Join(out, "data", filepath.Base(p)))
		if err != nil || !bytes.Equal(a, b) {
			t.Fatalf("%s differs after resume (%v)", filepath.Base(p), err)
		}
	}
	insp, err := exec.Command(bin, "show", "--journal", out).Output()
	if err != nil || !bytes.Contains(insp, []byte(`"type":"header"`)) {
		t.Fatalf("show --journal: %v %s", err, insp)
	}
}

// TestSIGKILLAtEveryPoint makes the real process SIGKILL itself at each
// crash point (response read, encoding, journal append and sync, rename,
// publication, ...), resumes, and compares with an uninterrupted run.
func TestSIGKILLAtEveryPoint(t *testing.T) {
	if testing.Short() {
		t.Skip("builds the binary")
	}
	bin := filepath.Join(t.TempDir(), "swath")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	b := s3mem.New()
	if err := b.CreateBucket("bkt"); err != nil {
		t.Fatal(err)
	}
	for i := range 4000 {
		k := fmt.Sprintf("p%02d/obj-%06d", i%7, i)
		b.PutObject("bkt", k, nil, bytes.NewReader([]byte(k)), int64(len(k)), nil)
	}
	srv := httptest.NewServer(gofakes3.New(b).Server())
	t.Cleanup(srv.Close)
	args := func(out string, extra ...string) []string {
		return append([]string{"list", "s3://bkt", "-endpoint", srv.URL, "-path-style", "-anonymous", "-workers", "8",
			"-part-size", "4KiB", "-memory", "1MiB", "-o", out}, extra...)
	}
	ref := filepath.Join(t.TempDir(), "ref")
	if out, err := exec.Command(bin, args(ref)...).CombinedOutput(); err != nil {
		t.Fatalf("reference: %v\n%s", err, out)
	}
	for _, point := range []string{"response-read", "split", "range-done", "encoding", "part-closed", "journal-append", "journal-sync",
		"recorded", "rename", "renamed-unsynced", "renamed", "published", "manifest"} {
		for _, nth := range []string{"1", "5"} {
			out := filepath.Join(t.TempDir(), "ds")
			cmd := exec.Command(bin, args(out)...)
			cmd.Env = append(os.Environ(), "SWATH_TEST_KILL_AT="+point+":"+nth)
			err := cmd.Run()
			if err == nil {
				t.Logf("%s:%s not reached; run completed", point, nth)
				continue
			}
			if ws, ok := cmd.ProcessState.Sys().(syscall.WaitStatus); !ok || !ws.Signaled() || ws.Signal() != syscall.SIGKILL {
				t.Fatalf("%s:%s: not killed: %v", point, nth, err)
			}
			if o, err := exec.Command(bin, "resume", out).CombinedOutput(); err != nil {
				switch {
				case point == "manifest" && bytes.Contains(o, []byte("already complete")):
					// Killed after the commit: the dataset is complete,
					// which is what swath resume reports.
				case bytes.Contains(o, []byte("before recording its command")):
					// Killed before the journal header was durable:
					// nothing was published; the run starts over.
					if o, err := exec.Command(bin, args(out, "--overwrite")...).CombinedOutput(); err != nil {
						t.Fatalf("%s:%s rerun: %v\n%s", point, nth, err, o)
					}
				default:
					t.Fatalf("%s:%s resume: %v\n%s", point, nth, err, o)
				}
			}
			parts, _ := filepath.Glob(filepath.Join(ref, "data", "*.parquet"))
			got, _ := filepath.Glob(filepath.Join(out, "data", "*.parquet"))
			if len(got) != len(parts) {
				t.Fatalf("%s:%s: %d parts, want %d", point, nth, len(got), len(parts))
			}
			for _, p := range parts {
				a, _ := os.ReadFile(p)
				g, _ := os.ReadFile(filepath.Join(out, "data", filepath.Base(p)))
				if !bytes.Equal(a, g) {
					t.Fatalf("%s:%s: %s differs", point, nth, filepath.Base(p))
				}
			}
		}
	}
}
