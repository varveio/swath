// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"context"
	"crypto/md5"
	"encoding/base64"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/faults"
	"github.com/varveio/swath/go/internal/journal"
	gcsclient "github.com/varveio/swath/go/internal/source/gcs/client"
	"github.com/varveio/swath/go/internal/source/gcs/gcstest"
	"github.com/varveio/swath/go/internal/source/objhttp"
)

// gcsBucket serves n names with 1 ms per request (gcstest.Bucket).
func gcsBucket(t *testing.T, n int) string {
	t.Helper()
	return gcstest.Bucket(t, gcstest.Keys(n), time.Millisecond)
}

func gcsConfig(ep, out string, resume bool) Config {
	return Config{Source: SourceConfig{URI: "gs://bkt", Options: &GCSConfig{Endpoint: ep, Anonymous: true}},
		Output: OutputConfig{Dir: out, Resume: resume}, Limits: Limits{PartSize: 6 << 10, Workers: 8, Memory: 256 << 10, Spill: SpillLimits{Off: true}}}
}

// TestGCSResumeRefusesAnotherRoute: a GCS run interrupted with parts
// retained is refused on resume through another route or another billed
// project (the crash points themselves: TestCrashMatrix).
func TestGCSResumeRefusesAnotherRoute(t *testing.T) {
	ep := gcsBucket(t, 8000)
	for name, change := range map[string]func(*GCSConfig){
		"route":   func(o *GCSConfig) { o.Endpoint = strings.Replace(ep, "127.0.0.1", "localhost", 1) },
		"project": func(o *GCSConfig) { o.UserProject = "other" },
	} {
		t.Run("refused/"+name, func(t *testing.T) {
			out := filepath.Join(t.TempDir(), "ds")
			var seen atomic.Int64
			restore := faults.Set(func(p string) error {
				if p == "renamed" && seen.Add(1) == 2 {
					return faults.ErrCrash
				}
				return nil
			})
			_, err := Run(context.Background(), gcsConfig(ep, out, false))
			restore()
			if !errors.Is(err, faults.ErrCrash) {
				t.Fatalf("crash not injected: %v", err)
			}
			cfg := gcsConfig(ep, out, true)
			change(cfg.Source.Options.(*GCSConfig))
			var mm *journal.MismatchError
			if _, err := Run(context.Background(), cfg); !errors.As(err, &mm) {
				t.Fatalf("resume with another %s: %v, want a mismatch", name, err)
			}
		})
	}
}

// TestGCSRedirectCannotChangeResumeSource: endpoint A serves namespace B
// until a run is interrupted with parts retained, then redirects to
// namespace C. The resume through the unchanged endpoint is refused with
// cause redirect, no manifest appears and the retained files are untouched;
// a fresh run through a redirecting endpoint is refused the same way.
func TestGCSRedirectCannotChangeResumeSource(t *testing.T) {
	b, c := gcsBucket(t, 8000), gcsBucket(t, 5000)
	var redirect atomic.Bool
	bu, _ := url.Parse(b)
	cu, _ := url.Parse(c)
	front := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if redirect.Load() {
			http.Redirect(w, r, "http://"+cu.Host+r.URL.RequestURI(), http.StatusTemporaryRedirect)
			return
		}
		httputil.NewSingleHostReverseProxy(&url.URL{Scheme: "http", Host: bu.Host}).ServeHTTP(w, r)
	}))
	t.Cleanup(front.Close)
	ep := front.URL + "/storage/v1/"
	out := filepath.Join(t.TempDir(), "ds")
	var seen atomic.Int64
	restore := faults.Set(func(p string) error {
		if p == "renamed" && seen.Add(1) == 2 {
			return faults.ErrCrash
		}
		return nil
	})
	_, err := Run(context.Background(), gcsConfig(ep, out, false))
	restore()
	if !errors.Is(err, faults.ErrCrash) {
		t.Fatalf("crash not injected: %v", err)
	}
	before := tree(t, out)
	redirect.Store(true)
	_, err = Run(context.Background(), gcsConfig(ep, out, true))
	if gcsclient.Classify(err) != objhttp.ClassRedirect {
		t.Fatalf("resume through a redirect: %v (class %q), want redirect", err, gcsclient.Classify(err))
	}
	if after := tree(t, out); !maps.Equal(before, after) {
		t.Fatalf("retained files changed:\nbefore %v\nafter  %v", before, after)
	}
	if _, err := os.Stat(filepath.Join(out, "manifest.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("manifest after a refused resume: %v", err)
	}
	fresh := filepath.Join(t.TempDir(), "fresh")
	if _, err := Run(context.Background(), gcsConfig(ep, fresh, false)); gcsclient.Classify(err) != objhttp.ClassRedirect {
		t.Fatalf("fresh run through a redirect: %v, want redirect", err)
	}
}

// tree maps the retained files under dir (the journal and the published
// parts; not the lock, the outcome or unrecorded temporary parts, which a
// resume may discard) to their size and content hash.
func tree(t *testing.T, dir string) map[string]string {
	t.Helper()
	m := map[string]string{}
	err := filepath.WalkDir(dir, func(p string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		name := d.Name()
		if strings.Contains(name, "lock") || strings.HasSuffix(name, "error.json") || filepath.Base(filepath.Dir(p)) == "parts" {
			return nil
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(dir, p)
		m[rel] = fmt.Sprintf("%d/%x", len(b), md5.Sum(b))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return m
}

// TestGCSInvalidEnvelopeNeverCommits: after the run has published parts,
// the ranges past d05 get 200 answers whose body is `null` or an error
// envelope. Before the fix those read as empty final pages and the run
// committed a dataset missing those keys; now each is retried and the run
// completes only when valid pages return, with exactly the reference rows.
func TestGCSInvalidEnvelopeNeverCommits(t *testing.T) {
	ep := gcsBucket(t, 8000)
	ref := filepath.Join(t.TempDir(), "ref")
	if _, err := Run(context.Background(), gcsConfig(ep, ref, false)); err != nil {
		t.Fatal(err)
	}
	u, _ := url.Parse(ep)
	bodies := []string{`null`, `{"error":{"code":500,"message":"backend"}}`}
	var served atomic.Int64
	const k = 6
	front := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		tok, _ := base64.RawURLEncoding.DecodeString(q.Get("pageToken"))
		if max(q.Get("startOffset"), string(tok)) >= "d05" {
			if i := served.Add(1); i <= k {
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprint(w, bodies[i%2])
				return
			}
		}
		httputil.NewSingleHostReverseProxy(&url.URL{Scheme: "http", Host: u.Host}).ServeHTTP(w, r)
	}))
	t.Cleanup(front.Close)
	out := filepath.Join(t.TempDir(), "ds")
	if _, err := Run(context.Background(), gcsConfig(front.URL+"/storage/v1/", out, false)); err != nil {
		t.Fatal(err)
	}
	if served.Load() <= k {
		t.Fatalf("only %d requests past d05: the invalid bodies were not all served", served.Load())
	}
	sameDataset(t, "invalid-envelope", ref, out)
}
