// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build !windows

package main

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"syscall"
	"testing"
	"time"

	pq "github.com/parquet-go/parquet-go"

	"github.com/varveio/swath/go/internal/dataset"
	"github.com/varveio/swath/go/internal/faults"
	"github.com/varveio/swath/go/internal/source/gcs/gcstest"
	"github.com/varveio/swath/go/internal/source/s3/s3test"
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
	// Slow enough to kill mid-run.
	url := s3test.Serve(t, s3test.Delayed(s3test.Handler(s3test.Backend(t, s3test.Keys(6000, 11))), 20*time.Millisecond))
	args := func(out string, extra ...string) []string {
		return append([]string{"list", "s3://bkt", "-endpoint", url, "-path-style", "-anonymous", "-workers", "8",
			"-part-size", "4KiB", "-row-budget", "512KiB", "-o", out}, extra...)
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
// publication, ...), resumes, and compares with an uninterrupted run, for
// each journaled provider. The byte-comparison arms run (and resume) with
// --part-interval=0: the default 30 s interval would close a part by time
// when a run under load is slow, and the part boundaries would then differ
// between runs whose rows agree. The "s3-interval" arm runs with a 1 ms
// part interval against a slower bucket, so parts are closed by time and
// boundaries depend on timing: it requires the clean run's rows, in order,
// and no refusal.
func TestSIGKILLAtEveryPoint(t *testing.T) {
	if testing.Short() {
		t.Skip("builds the binary")
	}
	bin := filepath.Join(t.TempDir(), "swath")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	b := s3test.Backend(t, s3test.Keys(4000, 7))
	url := s3test.Serve(t, s3test.Handler(b))
	slowURL := s3test.Serve(t, s3test.Delayed(s3test.Handler(b), 3*time.Millisecond))
	gcs := gcstest.Bucket(t, gcstest.Keys(4000), 0)
	common := []string{"-workers", "8", "-part-size", "4KiB", "-row-budget", "512KiB"}
	for _, prov := range []struct {
		name    string
		src     []string
		runtime []string // also given to swath resume
		byTime  bool     // parts are closed by time: compare rows, not part bytes
	}{
		{"s3", []string{"s3://bkt", "-endpoint", url, "-path-style", "-anonymous"}, []string{"--part-interval=0"}, false},
		{"gcs", []string{"gs://bkt", "--gcs-endpoint", gcs, "--gcs-anonymous"}, []string{"--part-interval=0"}, false},
		{"s3-interval", []string{"s3://bkt", "-endpoint", slowURL, "-path-style", "-anonymous"}, []string{"--part-interval", "1ms"}, true},
	} {
		args := func(out string, extra ...string) []string {
			a := append(append(append(append([]string{"list"}, prov.src...), common...), prov.runtime...), "-o", out)
			return append(a, extra...)
		}
		ref := filepath.Join(t.TempDir(), "ref")
		if out, err := exec.Command(bin, args(ref)...).CombinedOutput(); err != nil {
			t.Fatalf("%s reference: %v\n%s", prov.name, err, out)
		}
		for _, point := range faults.Points {
			for _, nth := range []string{"1", "5"} {
				at := prov.name + " " + point + ":" + nth
				out := filepath.Join(t.TempDir(), "ds")
				cmd := exec.Command(bin, args(out)...)
				cmd.Env = append(os.Environ(), "SWATH_TEST_KILL_AT="+point+":"+nth)
				err := cmd.Run()
				if err == nil {
					t.Logf("%s not reached; run completed", at)
					continue
				}
				if ws, ok := cmd.ProcessState.Sys().(syscall.WaitStatus); !ok || !ws.Signaled() || ws.Signal() != syscall.SIGKILL {
					t.Fatalf("%s: not killed: %v", at, err)
				}
				if o, err := exec.Command(bin, append(append([]string{"resume"}, prov.runtime...), out)...).CombinedOutput(); err != nil {
					switch {
					case point == "manifest" && bytes.Contains(o, []byte("already complete")):
						// Killed after the commit: the dataset is complete,
						// which is what swath resume reports.
					case bytes.Contains(o, []byte("before recording its command")):
						// Killed before the journal header was durable:
						// nothing was published; the run starts over.
						if o, err := exec.Command(bin, args(out, "--overwrite")...).CombinedOutput(); err != nil {
							t.Fatalf("%s rerun: %v\n%s", at, err, o)
						}
					default:
						t.Fatalf("%s resume: %v\n%s", at, err, o)
					}
				}
				if prov.byTime {
					sameRows(t, at, ref, out)
				} else {
					sameParts(t, at, ref, out)
				}
			}
		}
	}
}

// TestSIGKILLDurableSpill makes the real process SIGKILL itself at each of
// the durable spill's steps (faults.SpillPoints: a durable file's header (with the
// capture start), later frames and trailer writes, its seal's fsync, the flusher's
// directory sync, ledger append, ledger sync and unlink), at the first and
// fourth occurrence, then inside a resume's recovery (faults.
// RecoveryPoints: the ledger's truncation, the retirements' append, their
// sync, an unlink, the directory sync, the first recovery replay) on a
// damaged crashed output that reaches each one; a resume follows, and the
// parts must equal an uninterrupted run's. The fixture is the durable
// tests' slow frontier: the first tiles' requests are slow, so the
// ranges ahead of them spill and seal certificates. Every point must be
// reached: a run that completes instead fails the test.
func TestSIGKILLDurableSpill(t *testing.T) {
	if testing.Short() {
		t.Skip("builds the binary")
	}
	bin := filepath.Join(t.TempDir(), "swath")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	h := s3test.Handler(s3test.Backend(t, gcstest.Keys(30000)))
	url := s3test.Serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if _, ok := q["list-type"]; ok && q.Get("delimiter") == "" && q.Get("continuation-token") == "" && q.Get("start-after") < "d06/" {
			time.Sleep(50 * time.Millisecond) // the slow frontier
		}
		time.Sleep(time.Millisecond)
		h.ServeHTTP(w, r)
	}))
	runtime := []string{"--part-interval=0"}
	args := func(out string) []string {
		return append([]string{"list", "s3://bkt", "-endpoint", url, "-path-style", "-anonymous", "-workers", "32",
			"-part-size", "6KiB", "-row-budget", "128KiB", "--spill-max", "64MiB", "-o", out}, runtime...)
	}
	ref := filepath.Join(t.TempDir(), "ref")
	if out, err := exec.Command(bin, args(ref)...).CombinedOutput(); err != nil {
		t.Fatalf("reference: %v\n%s", err, out)
	}
	// killed runs cmd with SWATH_TEST_KILL_AT=at and requires the SIGKILL.
	killed := func(at string, cmd *exec.Cmd) {
		t.Helper()
		cmd.Env = append(os.Environ(), "SWATH_TEST_KILL_AT="+at)
		out, err := cmd.CombinedOutput()
		if ws, ok := cmd.ProcessState.Sys().(syscall.WaitStatus); err == nil || !ok || !ws.Signaled() || ws.Signal() != syscall.SIGKILL {
			t.Fatalf("%s: not killed there (not reached): %v\n%s", at, err, out)
		}
	}
	resume := func(at, out string) {
		t.Helper()
		if o, err := exec.Command(bin, append(append([]string{"resume"}, runtime...), out)...).CombinedOutput(); err != nil {
			t.Fatalf("%s resume: %v\n%s", at, err, o)
		}
		sameParts(t, at, ref, out)
	}
	for _, point := range faults.SpillPoints {
		for _, nth := range []string{"1", "4"} {
			at := point + ":" + nth
			out := filepath.Join(t.TempDir(), "ds")
			killed(at, exec.Command(bin, args(out)...))
			resume(at, out)
		}
	}
	for _, point := range faults.RecoveryPoints {
		at := "recovery " + point
		out := filepath.Join(t.TempDir(), "ds")
		killed("spill-unlink:1", exec.Command(bin, args(out)...))
		damageLedger(t, out)
		killed(point+":1", exec.Command(bin, append(append([]string{"resume"}, runtime...), out)...))
		resume(at, out)
	}
}

// damageLedger damages a killed run's durable spill so a resume's
// recovery reaches each of its steps: a recorded file removed (retired:
// the append, sync, unlink and directory sync) and an interior ledger
// record zeroed with the last one intact (the truncation); the records
// before it stand (accepted certificates: the recovery replay).
func damageLedger(t *testing.T, out string) {
	t.Helper()
	dir := filepath.Join(out, ".swath", "spill", "durable")
	path := filepath.Join(dir, "ledger")
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var offs []int
	for off := 0; off+9 <= len(b); off += 8 + int(binary.LittleEndian.Uint32(b[off:])) {
		offs = append(offs, off)
	}
	// The victim must be live in the surviving prefix (a seal with no
	// retirement after it): killed at the first unlink, the ledger already
	// holds that file's retirement, and removing a retired file retires
	// nothing (listing's damagedSnapshot has the flake this avoids).
	retired := map[string]bool{}
	for _, off := range offs {
		if b[off+8] == 'X' {
			retired[string(b[off+9:off+25])] = true
		}
	}
	victim := -1
	for i := 1; i+2 < len(offs); i++ {
		if b[offs[i]+8] != 'S' || retired[string(b[offs[i]+9:offs[i]+25])] {
			continue
		}
		name := hex.EncodeToString(b[offs[i]+9:offs[i]+25]) + ".spill"
		if os.Remove(filepath.Join(dir, name)) == nil {
			victim = i
			break
		}
	}
	if victim < 0 {
		t.Fatalf("no recorded file to remove among %d ledger frames", len(offs))
	}
	lost := len(offs) - 2 // interior (the last frame stays intact), after the victim: the records before it stand
	if lost <= victim {
		t.Fatalf("ledger of %d frames: no interior record after the victim's", len(offs))
	}
	clear(b[offs[lost]:offs[lost+1]])
	os.WriteFile(path, b, 0o644)
}

// sameRows compares the keys of two datasets, in manifest order: parts
// closed by time differ between runs, their rows may not.
func sameRows(t *testing.T, at, ref, out string) {
	t.Helper()
	keys := func(dir string) []string {
		var m dataset.Manifest
		b, err := os.ReadFile(filepath.Join(dir, dataset.ManifestName))
		if err != nil || json.Unmarshal(b, &m) != nil {
			t.Fatalf("%s: manifest of %s: %v", at, dir, err)
		}
		var ks []string
		for _, p := range m.Parts {
			f, err := os.Open(filepath.Join(dir, p.Path))
			if err != nil {
				t.Fatal(err)
			}
			pf, err := pq.OpenFile(f, p.Bytes)
			if err != nil {
				t.Fatal(err)
			}
			rows := make([]pq.Row, pf.NumRows())
			r := pq.NewReader(pf)
			n, _ := r.ReadRows(rows)
			r.Close()
			f.Close()
			for _, row := range rows[:n] {
				ks = append(ks, string(row[0].ByteArray()))
			}
		}
		return ks
	}
	want, got := keys(ref), keys(out)
	if !slices.Equal(want, got) {
		t.Fatalf("%s: %d rows, want %d (or they differ)", at, len(got), len(want))
	}
}
