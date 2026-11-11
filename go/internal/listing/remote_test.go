// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/dataset"
	"github.com/varveio/swath/go/internal/dataset/datasettest"
	"github.com/varveio/swath/go/internal/faults"
	"github.com/varveio/swath/go/internal/journal"
	"github.com/varveio/swath/go/internal/source/gcs/gcstest"
	"github.com/varveio/swath/go/internal/source/s3/s3test"
)

// manifest is the committed manifest under prefix, and its parts' bytes
// checked against it.
func (d *destination) manifest(t *testing.T, prefix string) dataset.Manifest {
	t.Helper()
	o, ok := d.Object(prefix + "/" + dataset.ManifestName)
	if !ok {
		t.Fatalf("no manifest under %s: %v", prefix, d.Names())
	}
	var m dataset.Manifest
	if err := json.Unmarshal(o, &m); err != nil {
		t.Fatal(err)
	}
	for _, p := range m.Parts {
		obj, ok := d.Object(prefix + "/" + p.Path)
		if !ok {
			t.Fatalf("manifest part %s is not in the bucket", p.Path)
		}
		sum := sha256.Sum256(obj)
		if int64(len(obj)) != p.Bytes || hex.EncodeToString(sum[:]) != p.SHA256 {
			t.Fatalf("part %s does not match the manifest", p.Path)
		}
	}
	return m
}

// sameAsLocal: the remote dataset holds the local reference's parts, byte
// for byte (by size and SHA-256), with the same manifest apart from the
// part paths and the times; and nothing else of this run is left under
// prefix but the manifest and the run report.
func (d *destination) sameAsLocal(t *testing.T, name, ref, prefix string, orphans bool) {
	t.Helper()
	m := d.manifest(t, prefix)
	var lm dataset.Manifest
	readJSON(t, filepath.Join(ref, dataset.ManifestName), &lm)
	if len(m.Parts) != len(lm.Parts) || len(m.Parts) < 2 {
		t.Fatalf("%s: %d parts, want %d (several)", name, len(m.Parts), len(lm.Parts))
	}
	for i := range m.Parts {
		if m.Parts[i].PartInfo != lm.Parts[i].PartInfo {
			t.Fatalf("%s: part %d differs: %+v, want %+v", name, i, m.Parts[i].PartInfo, lm.Parts[i].PartInfo)
		}
		if b := filepath.Base(m.Parts[i].Path); b != dataset.PartName(i) || !strings.HasPrefix(m.Parts[i].Path, dataset.DataDir+"/") {
			t.Fatalf("%s: part %d is %s", name, i, m.Parts[i].Path)
		}
	}
	strip := func(m dataset.Manifest) string {
		m.Parts, m.PublishedAt, m.Coverage.ListingStartedAt, m.Coverage.ListingFinishedAt = nil, time.Time{}, time.Time{}, time.Time{}
		return fmt.Sprintf("%+v", m)
	}
	if strip(m) != strip(lm) {
		t.Fatalf("%s: manifests differ:\n%s\n%s", name, strip(m), strip(lm))
	}
	if !orphans {
		return
	}
	want := []string{prefix + "/" + dataset.ManifestName, prefix + "/" + dataset.ReportName}
	for _, p := range m.Parts {
		want = append(want, prefix+"/"+p.Path)
	}
	slices.Sort(want)
	var got []string
	for _, n := range d.Names() {
		if strings.HasPrefix(n, prefix+"/") {
			got = append(got, n)
		}
	}
	if !slices.Equal(got, want) {
		t.Fatalf("%s: objects under %s:\n%s\nwant:\n%s", name, prefix, strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

// remoteConfig is crashConfig writing to the destination, the work
// directory at ws.
func remoteConfig(url, ws string, resume bool, out *RemoteOutput) Config {
	cfg := crashConfig(url, ws, resume)
	cfg.Output.Remote = out
	return cfg
}

// TestRemoteDataset: a dataset written straight to a bucket holds the
// local run's parts byte for byte under unique names, its manifest is the
// commit, the run report sits beside it, and the work directory holds no
// part; nothing else is left in the bucket.
func TestRemoteDataset(t *testing.T) {
	eachProvider(t, func(t *testing.T, p provider) {
		url := crashBucket(t)
		ref := filepath.Join(t.TempDir(), "ref")
		if _, err := Run(context.Background(), crashConfig(url, ref, false)); err != nil {
			t.Fatal(err)
		}
		d := newDestination(t, p, nil)
		ws := filepath.Join(t.TempDir(), "work")
		sum, err := Run(context.Background(), remoteConfig(url, ws, false, d.output("ds")))
		if err != nil {
			t.Fatal(err)
		}
		d.sameAsLocal(t, "remote", ref, "ds", true)
		r := sum.Output.Remote
		if r == nil || r.Commit != dataset.CommitCreated || r.Uploads != int64(sum.Output.PartsWritten) || r.UploadBytes != sum.Output.OutputBytes ||
			r.OrphansDeleted != 0 || r.Manifest != d.uri("ds/manifest.json") || r.ManifestIdentity == "" || r.WorkDir != ws {
			t.Fatalf("report %+v (parts %d, bytes %d)", r, sum.Output.PartsWritten, sum.Output.OutputBytes)
		}
		if !sum.Outcome.Complete || sum.Outcome.Status != StatusCommitted {
			t.Fatalf("outcome %+v", sum.Outcome)
		}
		if !dataset.Committed(ws) || dataset.Resumable(ws) {
			t.Fatal("the work directory does not record the commit")
		}
		for _, n := range []string{dataset.DataDir, dataset.ManifestName} {
			if _, err := os.Stat(filepath.Join(ws, n)); err == nil {
				t.Fatalf("the work directory holds %s", n)
			}
		}
		var rep Report
		o, _ := d.Object("ds/" + dataset.ReportName)
		if err := json.Unmarshal(o, &rep); err != nil || rep.Output.Remote == nil || rep.Output.Remote.Commit != dataset.CommitCreated {
			t.Fatalf("remote run report: %v %+v", err, rep.Output.Remote)
		}
		// A second run to the same destination is refused before it owns
		// anything: replacement is not supported.
		ws2 := filepath.Join(t.TempDir(), "work2")
		if _, err := Run(context.Background(), remoteConfig(url, ws2, false, d.output("ds"))); !errors.Is(err, ErrDestinationExists) {
			t.Fatalf("a run over a committed destination: %v", err)
		}
		if _, err := os.Stat(ws2); err == nil {
			t.Fatal("the refused run created its work directory")
		}
		// Resuming the committed work directory: complete (where a journaled
		// run, and so resume, is offered).
		if !datasettest.NoJournal() {
			if _, err := Run(context.Background(), remoteConfig(url, ws, true, d.output("ds"))); !errors.Is(err, ErrComplete) {
				t.Fatalf("resume of a committed run: %v", err)
			}
		}
	})
}

// remotePoints are the crash points a remote run crosses: the listing's,
// the upload's (a name allocated, a chunk sent, an upload completed), the
// claim's and the record's, the commit's (the candidate saved, the
// manifest about to be created, created, and the commit done) and its
// aftermath's (the run report, the marker finalized).
var remotePoints = []string{"response-read", "split", "range-done", "encoding", "allocated", "upload-chunk", "upload-completed",
	"part-closed", "claim-receipt", "claimed", "journal-append", "journal-sync", "recorded",
	"candidate-saved", "manifest-put", "manifest-created", "manifest", "run-report", "finalized"}

// TestRemoteCrashMatrix kills a remote run at each of its crash points, at
// a first and a later occurrence, with spill off and on (its durable
// spill's certificates retired as K moves), and resumes it: the resumed dataset
// holds the uninterrupted run's parts byte for byte, and every object the
// crashed attempts left that the manifest does not name is deleted. A kill
// once the manifest exists leaves a run whose resume finishes the
// aftermath (orphans deleted, the run report) and reports it complete.
func TestRemoteCrashMatrix(t *testing.T) {
	eachProvider(t, func(t *testing.T, p provider) {
		datasettest.RequireJournal(t)
		url := crashBucket(t)
		ref := filepath.Join(t.TempDir(), "ref")
		if _, err := Run(context.Background(), crashConfig(url, ref, false)); err != nil {
			t.Fatal(err)
		}
		d := newDestination(t, p, nil)
		var n atomic.Int64
		for _, spill := range []bool{false, true} {
			for _, point := range remotePoints {
				nths := []int64{1, 3}
				if point == "journal-append" {
					nths = append(nths, 2) // the first part's record: the calibrating part claimed, unrecorded
				}
				for _, nth := range nths {
					t.Run(fmt.Sprintf("spill=%v/%s#%d", spill, point, nth), func(t *testing.T) {
						prefix := fmt.Sprintf("m%d", n.Add(1))
						ws := filepath.Join(t.TempDir(), "work")
						var seen atomic.Int64
						restore := faults.Set(func(p string) error {
							if p == point && seen.Add(1) == nth {
								return faults.ErrCrash
							}
							return nil
						})
						cfg := remoteConfig(url, ws, false, d.output(prefix))
						cfg.Limits.Spill.Off = !spill
						_, err := Run(context.Background(), cfg)
						restore()
						if seen.Load() < nth {
							if err != nil {
								t.Fatal(err)
							}
							if nth == 1 {
								t.Fatalf("%s never reached", point)
							}
							t.Skipf("%s reached %d times only", point, seen.Load())
						}
						if !errors.Is(err, faults.ErrCrash) {
							t.Fatalf("crash not injected: %v", err)
						}
						_, err = Run(context.Background(), remoteConfig(url, ws, true, d.output(prefix)))
						switch point {
						case "manifest-created", "manifest", "run-report", "finalized":
							// Committed: the resume finishes the aftermath (orphans,
							// run report) and reports the run complete.
							if !errors.Is(err, ErrComplete) {
								t.Fatalf("resume after the manifest's creation: %v", err)
							}
							d.sameAsLocal(t, point, ref, prefix, true)
							return
						}
						if err != nil {
							t.Fatalf("resume: %v", err)
						}
						d.sameAsLocal(t, point, ref, prefix, true)
					})
				}
			}
		}
	})
}

// TestRemoteRecoveryKilled: a resume killed while it verifies the retained
// objects, or once recovery is done, is resumed again.
func TestRemoteRecoveryKilled(t *testing.T) {
	eachProvider(t, func(t *testing.T, p provider) {
		datasettest.RequireJournal(t)
		url := crashBucket(t)
		ref := filepath.Join(t.TempDir(), "ref")
		if _, err := Run(context.Background(), crashConfig(url, ref, false)); err != nil {
			t.Fatal(err)
		}
		d := newDestination(t, p, nil)
		for i, p := range []struct {
			point string
			nth   int64
		}{{"remote-verify", 2}, {"recovered", 1}} {
			t.Run(p.point, func(t *testing.T) {
				prefix := fmt.Sprintf("r%d", i)
				ws := filepath.Join(t.TempDir(), "work")
				crashRemote(t, url, ws, d.output(prefix), false, "journal-append", 4)
				crashRemote(t, url, ws, d.output(prefix), true, p.point, p.nth)
				if _, err := Run(context.Background(), remoteConfig(url, ws, true, d.output(prefix))); err != nil {
					t.Fatal(err)
				}
				d.sameAsLocal(t, p.point, ref, prefix, true)
			})
		}
	})
}

// crashRemote runs (or resumes) a remote run killed at point's nth
// occurrence.
func crashRemote(t *testing.T, url, ws string, out *RemoteOutput, resume bool, point string, nth int64) {
	t.Helper()
	var seen atomic.Int64
	restore := faults.Set(func(p string) error {
		if p == point && seen.Add(1) == nth {
			return faults.ErrCrash
		}
		return nil
	})
	_, err := Run(context.Background(), remoteConfig(url, ws, resume, out))
	restore()
	if !errors.Is(err, faults.ErrCrash) {
		t.Fatalf("%s#%d: crash not injected: %v", point, nth, err)
	}
}

// TestRemoteRetainedChecked: a resume reads every retained object whole
// through its recorded generation; one replaced (even by the same bytes:
// another generation), deleted, or altered is refused before anything is
// listed or written.
func TestRemoteRetainedChecked(t *testing.T) {
	eachProvider(t, func(t *testing.T, p provider) {
		datasettest.RequireJournal(t)
		url := crashBucket(t)
		d := newDestination(t, p, nil)
		for i, change := range []struct {
			name string
			do   func(name string, data []byte)
		}{
			{"rewritten", func(name string, data []byte) { d.Put(name, data) }},
			{"deleted", func(name string, _ []byte) { d.Remove(name) }},
			{"altered", func(name string, data []byte) { data = slices.Clone(data); data[len(data)/2] ^= 1; d.Put(name, data) }},
		} {
			t.Run(change.name, func(t *testing.T) {
				prefix := fmt.Sprintf("v%d", i)
				ws := filepath.Join(t.TempDir(), "work")
				crashRemote(t, url, ws, d.output(prefix), false, "recorded", 3)
				var first string
				for _, n := range d.Names() {
					if strings.HasPrefix(n, prefix+"/data/") && strings.HasSuffix(n, dataset.PartName(1)) {
						first = n
					}
				}
				o, ok := d.Object(first)
				if !ok {
					t.Fatalf("no part 1 in %v", d.Names())
				}
				change.do(first, o)
				_, err := Run(context.Background(), remoteConfig(url, ws, true, d.output(prefix)))
				if !errors.Is(err, dataset.ErrRemoteAltered) {
					t.Fatalf("resume over a %s part: %v", change.name, err)
				}
			})
		}
	})
}

// TestRemoteLostResponse: the manifest's creation succeeds but its answer
// is lost; the SDK's retry is refused (the object exists) and the read
// back finds the candidate: the run committed.
func TestRemoteLostResponse(t *testing.T) {
	eachProvider(t, func(t *testing.T, p provider) {
		url := crashBucket(t)
		ref := filepath.Join(t.TempDir(), "ref")
		if _, err := Run(context.Background(), crashConfig(url, ref, false)); err != nil {
			t.Fatal(err)
		}
		d := newDestination(t, p, nil)
		var lost atomic.Int64
		d.SetFault(func(op, name string, after bool) int {
			if op == opCreate && after && name == "lr/"+dataset.ManifestName && lost.Add(1) == 1 {
				return http.StatusServiceUnavailable
			}
			return 0
		})
		sum, err := Run(context.Background(), remoteConfig(url, filepath.Join(t.TempDir(), "work"), false, d.output("lr")))
		if err != nil {
			t.Fatal(err)
		}
		if lost.Load() == 0 || sum.Output.Remote.Commit != dataset.CommitConfirmed || sum.Output.Remote.Retries == 0 {
			t.Fatalf("lost %d, report %+v", lost.Load(), sum.Output.Remote)
		}
		d.sameAsLocal(t, "lost response", ref, "lr", true)
	})
}

// TestRemoteRace: two work directories list different sources to one
// destination and reach the commit together; exactly one manifest is
// created, the loser's report says so, its parts are deleted and the
// winner's are untouched, and resuming the loser is refused.
func TestRemoteRace(t *testing.T) {
	eachProvider(t, func(t *testing.T, p provider) {
		datasettest.RequireJournal(t)
		urlA, urlB := crashBucket(t), crashBucket(t)
		d := newDestination(t, p, nil)
		// A second source, with other rows: the same keys under another
		// prefix scope (the first 4000 keys only, by a filter).
		var arrived sync.WaitGroup
		arrived.Add(2)
		restore := faults.Set(func(p string) error {
			if p == "manifest-put" {
				arrived.Done()
				arrived.Wait() // both committers are past their fresh checks
			}
			return nil
		})
		defer restore()
		type result struct {
			sum Report
			err error
			ws  string
		}
		res := make([]result, 2)
		var wg sync.WaitGroup
		for i, u := range []string{urlA, urlB} {
			wg.Add(1)
			go func() {
				defer wg.Done()
				ws := filepath.Join(t.TempDir(), fmt.Sprintf("w%d", i))
				cfg := remoteConfig(u, ws, false, d.output("race"))
				if i == 1 {
					cfg.Limits.PartSize = 4 << 10 // other parts, another manifest
				}
				res[i].sum, res[i].err = Run(context.Background(), cfg)
				res[i].ws = ws
			}()
		}
		wg.Wait()
		restore()
		win, lose := 0, 1
		if res[0].err != nil {
			win, lose = 1, 0
		}
		if res[win].err != nil || !errors.Is(res[lose].err, dataset.ErrLostRace) {
			t.Fatalf("results: %v, %v", res[0].err, res[1].err)
		}
		if r := res[lose].sum.Output.Remote; r == nil || r.Commit != "lost_race" || r.OrphansDeleted == 0 || r.OrphansLeft != 0 {
			t.Fatalf("loser's report %+v", r)
		}
		m := d.manifest(t, "race")
		var names []string
		for _, n := range d.Names() {
			if strings.HasPrefix(n, "race/data/") {
				names = append(names, strings.TrimPrefix(n, "race/"))
			}
		}
		if len(names) != len(m.Parts) {
			t.Fatalf("%d part objects for %d manifest parts: the loser's were not removed", len(names), len(m.Parts))
		}
		for _, p := range m.Parts {
			if !slices.Contains(names, p.Path) {
				t.Fatalf("the winner's part %s is gone", p.Path)
			}
		}
		if _, err := Run(context.Background(), remoteConfig(urlA, res[lose].ws, true, d.output("race"))); !errors.Is(err, dataset.ErrLostRace) && !errors.Is(err, ErrNothingToResume) && !errors.Is(err, ErrComplete) {
			t.Fatalf("resume of the losing run: %v", err)
		}
	})
}

// TestRemoteEmpty: a listing of nothing commits a manifest of no parts.
func TestRemoteEmpty(t *testing.T) {
	eachProvider(t, func(t *testing.T, p provider) {
		d := newDestination(t, p, nil)
		scriptMu.Lock()
		scriptNext = &scripted{chunk: 1}
		scriptMu.Unlock()
		ws := filepath.Join(t.TempDir(), "work")
		_, err := Run(context.Background(), Config{Source: SourceConfig{URI: "scripted://x"}, Output: OutputConfig{Dir: ws, Remote: d.output("empty"), NoJournal: datasettest.NoJournal()},
			Limits: Limits{PartSize: 2 << 10, Workers: 2, RowBudget: 128 << 10, PartInterval: -1}})
		if err != nil {
			t.Fatal(err)
		}
		if m := d.manifest(t, "empty"); len(m.Parts) != 0 {
			t.Fatalf("%d parts", len(m.Parts))
		}
	})
}

// TestRemoteUploadsBounded: with every chunk slowed down, a run holds at
// most one open upload per encoder admitted, waits on them (the report's
// upload_wait_ns), and still writes the reference dataset; a part whose
// uploads keep failing fails the run, which a resume then completes.
func TestRemoteUploadsBounded(t *testing.T) {
	eachProvider(t, func(t *testing.T, p provider) {
		datasettest.RequireJournal(t)
		url := crashBucket(t)
		ref := filepath.Join(t.TempDir(), "ref")
		if _, err := Run(context.Background(), crashConfig(url, ref, false)); err != nil {
			t.Fatal(err)
		}
		var d *destination
		var open, peak atomic.Int64
		d = newDestination(t, p, func(h http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if p.uploading(r) {
					n := open.Add(1)
					for p := peak.Load(); n > p && !peak.CompareAndSwap(p, n); p = peak.Load() {
					}
					time.Sleep(3 * time.Millisecond)
					defer open.Add(-1)
				}
				h.ServeHTTP(w, r)
			})
		})
		cfg := remoteConfig(url, filepath.Join(t.TempDir(), "work"), false, d.output("slow"))
		cfg.Limits.Encoders = 2
		sum, err := Run(context.Background(), cfg)
		if err != nil {
			t.Fatal(err)
		}
		if p := peak.Load(); p > int64(sum.Resources.Drain.EncodersAfterListing) {
			t.Fatalf("%d upload requests at once, %d encoders", p, sum.Resources.Drain.EncodersAfterListing)
		}
		if sum.Output.Remote.UploadWaitNs == 0 {
			t.Fatal("no upload wait accounted")
		}
		d.sameAsLocal(t, "slow", ref, "slow", true)

		// Failing uploads: the third part's upload is refused until the run
		// has failed; the resume completes it.
		var failing atomic.Bool
		failing.Store(true)
		d.SetFault(func(op, name string, after bool) int {
			if !after && failing.Load() && strings.HasPrefix(name, "fail/") && strings.HasSuffix(name, dataset.PartName(2)) {
				return http.StatusServiceUnavailable
			}
			return 0
		})
		ws := filepath.Join(t.TempDir(), "work2")
		sum, err = Run(context.Background(), remoteConfig(url, ws, false, d.output("fail")))
		if err == nil || sum.Output.Remote.Retries == 0 || sum.Output.Remote.RetryClasses["http_503"] == 0 {
			t.Fatalf("run with a failing upload: %v, report %+v", err, sum.Output.Remote)
		}
		failing.Store(false)
		if _, err := Run(context.Background(), remoteConfig(url, ws, true, d.output("fail"))); err != nil {
			t.Fatal(err)
		}
		d.sameAsLocal(t, "failing upload resumed", ref, "fail", true)
	})
}

// TestRemoteCancelled: a run cancelled while it uploads stops; no object
// of an unfinished upload exists, and the resume completes the dataset.
func TestRemoteCancelled(t *testing.T) {
	eachProvider(t, func(t *testing.T, p provider) {
		datasettest.RequireJournal(t)
		url := crashBucket(t)
		ref := filepath.Join(t.TempDir(), "ref")
		if _, err := Run(context.Background(), crashConfig(url, ref, false)); err != nil {
			t.Fatal(err)
		}
		d := newDestination(t, p, nil)
		ctx, cancel := context.WithCancel(context.Background())
		var n atomic.Int64
		restore := faults.Set(func(p string) error {
			if p == "upload-chunk" && n.Add(1) == 3 {
				cancel()
			}
			return nil
		})
		ws := filepath.Join(t.TempDir(), "work")
		_, err := Run(ctx, remoteConfig(url, ws, false, d.output("c")))
		restore()
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled run: %v", err)
		}
		if _, ok := d.Object("c/" + dataset.ManifestName); ok {
			t.Fatal("a cancelled run committed")
		}
		if _, err := Run(context.Background(), remoteConfig(url, ws, true, d.output("c"))); err != nil {
			t.Fatal(err)
		}
		d.sameAsLocal(t, "cancelled", ref, "c", true)
	})
}

// TestRemoteRefusals: what a remote dataset refuses before it owns
// anything.
func TestRemoteRefusals(t *testing.T) {
	url := crashBucket(t)
	d := newDestination(t, gcsProvider, nil)
	ws := filepath.Join(t.TempDir(), "work")
	for name, c := range map[string]func(*Config){
		"replace":  func(c *Config) { c.Output.Replace = true },
		"no dir":   func(c *Config) { c.Output.Dir = "" },
		"az dest":  func(c *Config) { c.Output.Remote = &RemoteOutput{Location: "az://acct/c/p"} },
		"bad dest": func(c *Config) { c.Output.Remote = &RemoteOutput{Location: "gs://B/p"} },
	} {
		cfg := remoteConfig(url, ws, false, d.output("x"))
		c(&cfg)
		if _, err := Run(context.Background(), cfg); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if _, err := os.Stat(ws); err == nil {
		t.Fatal("a refused run created its work directory")
	}
	// The listed bucket and prefix overlap the destination's.
	ep := gcsBucket(t, 10)
	for _, dst := range []string{"gs://bkt", "gs://bkt/out", "gs://bkt/a"} {
		cfg := gcsConfig(ep, ws, false)
		cfg.Source.URI = "gs://bkt/a"
		cfg.Output.Remote = &RemoteOutput{Location: dst}
		if _, err := Run(context.Background(), cfg); !errors.Is(err, ErrOverlap) && dst != "gs://bkt/out" {
			t.Errorf("source gs://bkt/a, destination %s: %v", dst, err)
		}
	}
	// S3 alike: the listed s3://bkt/a holds, or is held by, these (a
	// destination elsewhere is opened, so none is tried here).
	for _, dst := range []string{"s3://bkt", "s3://bkt/a", "s3://bkt/ab/c"} {
		cfg := remoteConfig(url, ws, false, &RemoteOutput{Location: dst})
		cfg.Source.URI = "s3://bkt/a"
		if _, err := Run(context.Background(), cfg); !errors.Is(err, ErrOverlap) {
			t.Errorf("source s3://bkt/a, destination %s: %v", dst, err)
		}
	}
	if _, err := os.Stat(ws); err == nil {
		t.Fatal("a refused run created its work directory")
	}
}

// TestMeasureRemoteOverhead compares a local and a remote run of one
// in-process listing (SWATH_MEASURE_REMOTE=1 runs it): wall time, process
// CPU (the fakes' included), peak heap and the remote run's upload wait.
// It records numbers; it asserts nothing about them.
func TestMeasureRemoteOverhead(t *testing.T) {
	eachProvider(t, func(t *testing.T, p provider) {
		if os.Getenv("SWATH_MEASURE_REMOTE") == "" {
			t.Skip("SWATH_MEASURE_REMOTE=1 runs it")
		}
		s3test.NoCredentials(t)
		t.Setenv("AWS_REGION", "")
		t.Setenv("AWS_DEFAULT_REGION", "")
		url := s3test.Serve(t, s3test.Handler(s3test.Backend(t, gcstest.Keys(400_000))))
		d := newDestination(t, p, nil)
		cfg := func(ws string, remote bool) Config {
			c := crashConfig(url, ws, false)
			c.Limits.PartSize, c.Limits.RowBudget, c.Limits.Encoders = 256<<10, 64<<20, 4
			if remote {
				c.Output.Remote = d.output("measure" + filepath.Base(ws))
				c.Output.Remote.ChunkSize = 8 << 20
			}
			return c
		}
		var prev int64 // getrusage is cumulative: each run's CPU is the difference
		for i := range 3 {
			for _, remote := range []bool{false, true} {
				ws := filepath.Join(t.TempDir(), fmt.Sprintf("w%d", i))
				start := time.Now()
				sum, err := Run(context.Background(), cfg(ws, remote))
				if err != nil {
					t.Fatal(err)
				}
				wall := time.Since(start)
				u, s := int64(0), int64(0)
				if sum.Timings.UserCPUMs != nil {
					u, s = *sum.Timings.UserCPUMs, *sum.Timings.SysCPUMs
				}
				line := fmt.Sprintf("remote=%v wall=%v cpu=%dms parts=%d bytes=%d heap=%d", remote, wall.Round(time.Millisecond), u+s-prev, sum.Output.PartsWritten, sum.Output.OutputBytes, sum.Resources.PeakHeapLiveBytes)
				prev = u + s
				if r := sum.Output.Remote; r != nil {
					line += fmt.Sprintf(" upload_wait=%v uploads=%d", time.Duration(r.UploadWaitNs).Round(time.Millisecond), r.Uploads)
				}
				t.Log(line)
			}
		}
	})
}

// TestRemoteResumeIdentity: the destination is frozen: a resume to another
// prefix, or of the work directory as a local dataset, is refused before
// anything is verified or written.
func TestRemoteResumeIdentity(t *testing.T) {
	eachProvider(t, func(t *testing.T, p provider) {
		datasettest.RequireJournal(t)
		url := crashBucket(t)
		d := newDestination(t, p, nil)
		ws := filepath.Join(t.TempDir(), "work")
		crashRemote(t, url, ws, d.output("id"), false, "recorded", 2)
		before := d.Count(opRead)
		for name, out := range map[string]*RemoteOutput{"another prefix": d.output("id2"), "a local dataset": nil} {
			_, err := Run(context.Background(), remoteConfig(url, ws, true, out))
			if err == nil || !strings.Contains(err.Error(), "cannot resume: destination") {
				t.Errorf("resume to %s: %v", name, err)
			}
		}
		if d.Count(opRead) != before {
			t.Fatal("a refused resume read the retained parts")
		}
		if _, err := Run(context.Background(), remoteConfig(url, ws, true, d.output("id"))); err != nil {
			t.Fatal(err)
		}
	})
}

// TestRemoteLostJournalTail: the last record, acknowledged, is then torn
// (its last five bytes) or lost whole: its claim receipt is the suspect,
// and the resume reproduces that part byte for byte before recording it
// again; when the source no longer holds the part's rows the resume
// refuses (ErrAmbiguousTail) and commits nothing.
func TestRemoteLostJournalTail(t *testing.T) {
	eachProvider(t, func(t *testing.T, p provider) {
		datasettest.RequireJournal(t)
		url := crashBucket(t)
		ref := filepath.Join(t.TempDir(), "ref")
		if _, err := Run(context.Background(), crashConfig(url, ref, false)); err != nil {
			t.Fatal(err)
		}
		d := newDestination(t, p, nil)
		cut := func(t *testing.T, ws string, whole bool) {
			st, err := journal.Read(ws)
			if err != nil || len(st.Records) != 3 {
				t.Fatalf("%v, %d records", err, len(st.Records))
			}
			b, _ := os.ReadFile(journal.Path(ws))
			n := 5
			if whole {
				last, _ := json.Marshal(st.Records[2])
				n = 8 + len(last)
			}
			if err := os.WriteFile(journal.Path(ws), b[:len(b)-n], 0o644); err != nil {
				t.Fatal(err)
			}
		}
		for i, whole := range []bool{false, true} {
			t.Run(fmt.Sprintf("whole=%v", whole), func(t *testing.T) {
				prefix := fmt.Sprintf("tail%d", i)
				ws := filepath.Join(t.TempDir(), "work")
				crashRemote(t, url, ws, d.output(prefix), false, "recorded", 3)
				cut(t, ws, whole)
				if st, err := journal.Read(ws); err != nil || len(st.Records) != 2 {
					t.Fatalf("after the cut: %v", err)
				}
				if _, err := Run(context.Background(), remoteConfig(url, ws, true, d.output(prefix))); err != nil {
					t.Fatal(err)
				}
				d.sameAsLocal(t, "lost tail", ref, prefix, true)
			})
		}
		t.Run("changed source", func(t *testing.T) {
			url, b := crashBackend(t)
			ws := filepath.Join(t.TempDir(), "work")
			crashRemote(t, url, ws, d.output("tailx"), false, "recorded", 3)
			st, _ := journal.Read(ws)
			suspect := st.Records[2].Part
			cut(t, ws, true)
			if _, err := b.DeleteObject("bkt", string(suspect.MinKey)); err != nil {
				t.Fatal(err)
			}
			_, err := Run(context.Background(), remoteConfig(url, ws, true, d.output("tailx")))
			if !errors.Is(err, ErrAmbiguousTail) {
				t.Fatalf("resume over a changed source: %v", err)
			}
			if _, ok := d.Object("tailx/" + dataset.ManifestName); ok {
				t.Fatal("committed")
			}
		})
	})
}

// TestRemoteVersionGroups: a versions listing (keys repeat, a part never
// ends inside a key's group) killed inside a hot group resumes to the
// local run's parts.
func TestRemoteVersionGroups(t *testing.T) {
	eachProvider(t, func(t *testing.T, p provider) {
		datasettest.RequireJournal(t)
		ref := filepath.Join(t.TempDir(), "ref")
		if _, err := Run(context.Background(), versionListingConfig(ref, true, false, versionFixture(0))); err != nil {
			t.Fatal(err)
		}
		d := newDestination(t, p, nil)
		ws := filepath.Join(t.TempDir(), "work")
		cfg := versionListingConfig(ws, true, false, versionFixture(4200))
		cfg.Output.Remote = d.output("versions")
		if _, err := Run(context.Background(), cfg); !errors.Is(err, faults.ErrCrash) {
			t.Fatalf("did not stop mid-group: %v", err)
		}
		cfg = versionListingConfig(ws, true, true, versionFixture(0))
		cfg.Output.Remote = d.output("versions")
		if _, err := Run(context.Background(), cfg); err != nil {
			t.Fatal(err)
		}
		d.sameAsLocal(t, "versions", ref, "versions", true)
		if m := d.manifest(t, "versions"); m.Key.Unique {
			t.Fatal("a versions manifest declares unique keys")
		}
	})
}

// TestRemoteLocalTree: a local tree keeps no journal: its parts are
// uploaded as they close and committed at the end; the work directory
// inside the listed tree is excluded from it. A run that fails before its
// commit (a manifest already created by another writer while it ran)
// deletes its own uploads.
func TestRemoteLocalTree(t *testing.T) {
	eachProvider(t, func(t *testing.T, p provider) {
		root := t.TempDir()
		for i := range 3000 {
			dir := filepath.Join(root, fmt.Sprintf("d%02d", i%17))
			_ = os.MkdirAll(dir, 0o755)
			if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("f%05d", i)), []byte("x"), 0o644); err != nil {
				t.Fatal(err)
			}
		}
		d := newDestination(t, p, nil)
		cfg := func(ws, prefix string) Config {
			return Config{Source: SourceConfig{URI: root}, Output: OutputConfig{Dir: ws, Remote: d.output(prefix)},
				Limits: Limits{PartSize: 8 << 10, Workers: 4, RowBudget: 128 << 10, PartInterval: -1}}
		}
		ref := filepath.Join(t.TempDir(), "ref")
		if _, err := Run(context.Background(), Config{Source: SourceConfig{URI: root}, Output: OutputConfig{Dir: ref},
			Limits: Limits{PartSize: 8 << 10, Workers: 4, RowBudget: 128 << 10, PartInterval: -1}}); err != nil {
			t.Fatal(err)
		}
		ws := filepath.Join(root, "work") // inside the listed tree: excluded
		sum, err := Run(context.Background(), cfg(ws, "tree"))
		if err != nil {
			t.Fatal(err)
		}
		m := d.manifest(t, "tree")
		if len(m.Parts) < 2 || sum.Output.RowsCommitted != sumRows(manifestOf(t, ref)) || sumRows(m) != sum.Output.RowsCommitted {
			t.Fatalf("%d parts, %d rows committed", len(m.Parts), sum.Output.RowsCommitted)
		}
		if len(m.Coverage.Exclusions) == 0 {
			t.Fatal("the work directory inside the tree is not excluded")
		}
		// A manifest appears under the next run's prefix while it lists: its
		// commit loses, and its uploads go.
		var once atomic.Bool
		restore := faults.Set(func(p string) error {
			if p == "candidate-saved" && once.CompareAndSwap(false, true) {
				d.Put("tree2/"+dataset.ManifestName, []byte(`{"other":true}`))
			}
			return nil
		})
		defer restore()
		sum, err = Run(context.Background(), cfg(filepath.Join(t.TempDir(), "w2"), "tree2"))
		if !errors.Is(err, dataset.ErrLostRace) || sum.Output.Remote.OrphansDeleted == 0 {
			t.Fatalf("a run that lost its commit: %v, %+v", err, sum.Output.Remote)
		}
		for _, n := range d.Names() {
			if strings.HasPrefix(n, "tree2/data/") {
				t.Fatalf("%s left behind", n)
			}
		}
	})
}

func sumRows(m dataset.Manifest) int64 {
	var n int64
	for _, p := range m.Parts {
		n += p.Rows
	}
	return n
}
