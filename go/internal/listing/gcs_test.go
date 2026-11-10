// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"context"
	"crypto/md5"
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
	"github.com/varveio/swath/go/internal/source/objhttp"
)

// gcsBucket serves n names with 1 ms per request (gcstest.Bucket).
func gcsBucket(t *testing.T, n int) string {
	t.Helper()
	return gcstest.Bucket(t, gcstest.Keys(n), time.Millisecond)
}

func gcsConfig(ep, out string, resume bool) Config {
	return Config{Source: SourceConfig{URI: "gs://bkt", Options: &GCSConfig{Endpoint: ep, Anonymous: true}},
		Output: OutputConfig{Dir: out, Resume: resume, NoJournal: datasettest.NoJournal()}, Limits: Limits{PartSize: 6 << 10, Workers: 8, RowBudget: 128 << 10, Spill: SpillLimits{Off: true}, PartInterval: -1}}
}

// TestGCSResumeRefusesAnotherRoute: a GCS run interrupted with parts
// retained is refused on resume through another route or another billed
// project (the crash points themselves: TestCrashMatrix).
func TestGCSResumeRefusesAnotherRoute(t *testing.T) {
	datasettest.RequireJournal(t)
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
// HTTP status 307, no manifest appears and the retained files are untouched;
// a fresh run through a redirecting endpoint is refused the same way.
func TestGCSRedirectCannotChangeResumeSource(t *testing.T) {
	datasettest.RequireJournal(t)
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
	var failure *objhttp.Failure
	if !errors.As(err, &failure) || failure.Status != 307 || failure.Transient {
		t.Fatalf("resume through a redirect: %v, failure %+v", err, failure)
	}
	if after := tree(t, out); !maps.Equal(before, after) {
		t.Fatalf("retained files changed:\nbefore %v\nafter  %v", before, after)
	}
	if _, err := os.Stat(filepath.Join(out, "manifest.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("manifest after a refused resume: %v", err)
	}
	fresh := filepath.Join(t.TempDir(), "fresh")
	if _, err := Run(context.Background(), gcsConfig(ep, fresh, false)); !errors.As(err, &failure) || failure.Status != 307 || failure.Transient {
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
	// The page token is opaque: a request's position is its startOffset or,
	// for a continuation, the last name of the page that issued its token,
	// as the front recorded it.
	var after sync.Map // nextPageToken -> the position it continues from
	proxy := httputil.NewSingleHostReverseProxy(&url.URL{Scheme: "http", Host: u.Host})
	front := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		pos := q.Get("startOffset")
		if tok := q.Get("pageToken"); tok != "" {
			if p, ok := after.Load(tok); ok {
				pos = max(pos, p.(string))
			}
		}
		if pos >= "d05" {
			if i := served.Add(1); i <= k {
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprint(w, bodies[i%2])
				return
			}
		}
		rec := httptest.NewRecorder()
		proxy.ServeHTTP(rec, r)
		var page struct {
			Items         []struct{ Name string }
			Prefixes      []string
			NextPageToken string
		}
		if json.Unmarshal(rec.Body.Bytes(), &page) == nil && page.NextPageToken != "" {
			last := pos
			for _, it := range page.Items {
				last = max(last, it.Name)
			}
			for _, p := range page.Prefixes {
				last = max(last, p)
			}
			after.Store(page.NextPageToken, last)
		}
		maps.Copy(w.Header(), rec.Header())
		w.WriteHeader(rec.Code)
		w.Write(rec.Body.Bytes())
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

// TestGCSCredentialRotationResumes: credentials are not identity on GCS
// either. A run signed with one authorized-user credential (bearer tokens
// from a fake token service), interrupted with parts retained, resumes
// signed with a rotated credential on the same route to the uninterrupted
// dataset; the rotated credential together with another billed project is
// refused for the project. The bucket refuses unsigned requests.
func TestGCSCredentialRotationResumes(t *testing.T) {
	datasettest.RequireJournal(t)
	tok := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.ParseForm()
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"access_token":"tok-%s","token_type":"Bearer","expires_in":3600}`, r.Form.Get("refresh_token"))
	}))
	t.Cleanup(tok.Close)
	dir := t.TempDir()
	cred := func(name string) string {
		p := filepath.Join(dir, name+".json")
		os.WriteFile(p, fmt.Appendf(nil, `{"type":"authorized_user","client_id":"c","client_secret":"s","refresh_token":%q,"token_uri":%q}`, name, tok.URL), 0o600)
		return p
	}
	first, rotated := cred("first"), cred("rotated")
	h := gcstest.Handler(gcstest.Keys(8000), time.Millisecond)
	var signedBy sync.Map // bearer tokens seen
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bearer, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			fmt.Fprint(w, `{"error":{"code":401,"message":"Anonymous caller"}}`)
			return
		}
		signedBy.Store(bearer, true)
		h.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	ep := srv.URL + gcstest.Path
	signed := func(out string, resume bool, file, project string) Config {
		cfg := gcsConfig(ep, out, resume)
		o := cfg.Source.Options.(*GCSConfig)
		o.Anonymous, o.CredentialsFile, o.UserProject = false, file, project
		return cfg
	}
	ref := filepath.Join(t.TempDir(), "ref")
	if _, err := Run(context.Background(), signed(ref, false, first, "")); err != nil {
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
	_, err := Run(context.Background(), signed(out, false, first, ""))
	restore()
	if !errors.Is(err, faults.ErrCrash) {
		t.Fatalf("crash not injected: %v", err)
	}
	var mm *journal.MismatchError
	if _, err := Run(context.Background(), signed(out, true, rotated, "other")); !errors.As(err, &mm) || mm.Field != "user project" {
		t.Fatalf("rotated credential and another project: %v, want a user project mismatch", err)
	}
	signedBy.Delete("tok-rotated")
	sum, err := Run(context.Background(), signed(out, true, rotated, ""))
	if err != nil {
		t.Fatalf("resume with a rotated credential: %v", err)
	}
	if sum.Output.PartsRetained == 0 {
		t.Fatal("the resume retained no part")
	}
	if _, ok := signedBy.Load("tok-rotated"); !ok {
		t.Fatal("the resume did not sign with the rotated credential")
	}
	sameDataset(t, "rotation", ref, out)
}

// Listing also accepts Config from callers other than the CLI. Raw recorded
// arguments must not reach its reports or the journal, even on success.
func TestRecordedEndpointUserinfo(t *testing.T) {
	ep := gcsBucket(t, 2)
	cfg := gcsConfig(ep, filepath.Join(t.TempDir(), "ds"), false)
	cfg.Argv = []string{"list", "gs://bkt",
		"--endpoint", "http://private-user:private-password@s3.example/",
		"--endpoint=http://private-user:private-password@s3.example/",
		"-endpoint", "http://private-user:private-password@gcs.example/",
		"-endpoint=http://private-user:private-password@gcs.example/"}
	original := strings.Join(cfg.Argv, " ")
	rep, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(strings.Join(rep.Invocation.Argv, " "), "private-") {
		t.Fatalf("raw arguments in successful invocation: %q", rep.Invocation.Argv)
	}
	if strings.Join(cfg.Argv, " ") != original {
		t.Fatal("listing changed the caller's arguments")
	}
	paths := []string{filepath.Join(cfg.Output.Dir, dataset.ReportName)}
	if !cfg.Output.NoJournal {
		paths = append(paths, journal.Path(cfg.Output.Dir))
	}
	for _, path := range paths {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(raw), "private-") {
			t.Fatalf("raw arguments in %s", path)
		}
	}
}
