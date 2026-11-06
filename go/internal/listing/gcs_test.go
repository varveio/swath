// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/faults"
	"github.com/varveio/swath/go/internal/journal"
	gcsclient "github.com/varveio/swath/go/internal/source/gcs/client"
	"github.com/varveio/swath/go/internal/source/objhttp"
)

// gcsBucket serves objects.list over n names with 1 ms per request: a
// minimal JSON API (prefix, startOffset inclusive, endOffset exclusive,
// maxResults, a token naming the last name; no delimiter: the default
// policy sends none) for the run-level GCS tests.
func gcsBucket(t *testing.T, n int) string {
	t.Helper()
	keys := make([]string, n)
	for i := range keys {
		keys[i] = fmt.Sprintf("d%02d/sub%d/obj+%06d", i%9, i%4, i)
	}
	sort.Strings(keys)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(time.Millisecond)
		q := r.URL.Query()
		if q.Get("delimiter") != "" || r.URL.Path != "/storage/v1/b/bkt/o" {
			http.Error(w, `{"error":{"code":400,"message":"unsupported"}}`, 400)
			return
		}
		after := ""
		if tok := q.Get("pageToken"); tok != "" {
			b, err := base64.RawURLEncoding.DecodeString(tok)
			if err != nil {
				http.Error(w, `{"error":{"code":400,"message":"token"}}`, 400)
				return
			}
			after = string(b)
		}
		limit := 1000
		if n, err := strconv.Atoi(q.Get("maxResults")); err == nil && n > 0 && n < limit {
			limit = n
		}
		var resp struct {
			Items []map[string]string `json:"items,omitempty"`
			Next  string              `json:"nextPageToken,omitempty"`
		}
		start, end, prefix := q.Get("startOffset"), q.Get("endOffset"), q.Get("prefix")
		for i := sort.SearchStrings(keys, max(start, prefix)); i < len(keys); i++ {
			k := keys[i]
			if !strings.HasPrefix(k, prefix) || (end != "" && k >= end) {
				break
			}
			if k <= after {
				continue
			}
			if len(resp.Items) == limit {
				resp.Next = base64.RawURLEncoding.EncodeToString([]byte(resp.Items[limit-1]["name"]))
				break
			}
			resp.Items = append(resp.Items, map[string]string{"name": k, "size": strconv.Itoa(len(k)),
				"updated": "2026-01-01T00:00:00Z", "generation": strconv.Itoa(1000 + i), "etag": "e" + strconv.Itoa(i)})
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	t.Cleanup(srv.Close)
	return srv.URL + "/storage/v1/"
}

func gcsConfig(ep, out string, resume bool) Config {
	return Config{Source: SourceConfig{URI: "gs://bkt", Options: &GCSConfig{Endpoint: ep, Anonymous: true}},
		Output: OutputConfig{Dir: out, Resume: resume}, Limits: Limits{PartSize: 6 << 10, Workers: 8, Memory: 256 << 10, Spill: SpillLimits{Off: true}}}
}

// TestGCSInterruptedResume: a GCS run killed (an injected crash that skips
// all cleanup) at a response, at a split, between a part's record and its
// rename, and after a part's rename, then resumed, yields the dataset of an
// uninterrupted run byte for byte; a resume through another route or
// another billed project is refused.
func TestGCSInterruptedResume(t *testing.T) {
	ep := gcsBucket(t, 8000)
	ref := filepath.Join(t.TempDir(), "ref")
	if _, err := Run(context.Background(), gcsConfig(ep, ref, false)); err != nil {
		t.Fatal(err)
	}
	for _, point := range []string{"response-read", "split", "recorded", "renamed"} {
		for _, nth := range []int64{1, 4} {
			t.Run(fmt.Sprintf("%s#%d", point, nth), func(t *testing.T) {
				out := filepath.Join(t.TempDir(), "ds")
				var seen atomic.Int64
				restore := faults.Set(func(p string) error {
					if p == point && seen.Add(1) == nth {
						return faults.ErrCrash
					}
					return nil
				})
				_, err := Run(context.Background(), gcsConfig(ep, out, false))
				restore()
				if seen.Load() < nth {
					t.Fatalf("%s reached %d times only (err %v)", point, seen.Load(), err)
				}
				if !errors.Is(err, faults.ErrCrash) {
					t.Fatalf("crash not injected: %v", err)
				}
				sum, err := Run(context.Background(), gcsConfig(ep, out, true))
				if err != nil {
					t.Fatalf("resume: %v", err)
				}
				sameDataset(t, point, ref, out)
				t.Logf("resumed %d parts (%d rows), listed %d more rows", sum.Output.PartsRetained, sum.Output.RowsRetained, sum.Output.RowsListed)
			})
		}
	}
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
