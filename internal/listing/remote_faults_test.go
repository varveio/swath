// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"

	"github.com/varveio/swath/internal/dataset"
	"github.com/varveio/swath/internal/dataset/datasettest"
	"github.com/varveio/swath/internal/faults"
	"github.com/varveio/swath/internal/journal"
	"github.com/varveio/swath/internal/source/gcs/gcstest"
	"github.com/varveio/swath/internal/source/s3/s3test"
	"github.com/varveio/swath/objfile"
	"github.com/varveio/swath/objfile/gcs"
	"github.com/varveio/swath/objfile/gcs/gcsfake"
	"google.golang.org/api/googleapi"
)

// Fault schedules of a dataset in a bucket: commits whose outcome is
// unknown, secrets in errors, links in the work directory, a damaged
// journal header, a drifted endpoint, bounded waits and multi-chunk
// uploads.

// localTree is a small local tree (no journal: its run publishes at the
// end, the path that deleted its parts after an ambiguous commit).
func localTree(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	for i := range 200 {
		if err := os.WriteFile(filepath.Join(root, fmt.Sprintf("f%03d", i)), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

func treeConfig(root, ws string, out *RemoteOutput) Config {
	return Config{Source: SourceConfig{URI: root}, Output: OutputConfig{Dir: ws, Remote: out},
		Limits: Limits{Workers: 1, Encoders: 1, PartSize: 2 << 10, RowBudget: 128 << 10, PartInterval: -1}}
}

// committedPartsExist fails unless every part the manifest under prefix
// names exists.
func committedPartsExist(t *testing.T, d *destination, prefix string) {
	t.Helper()
	o, ok := d.gcs().Get(prefix + "/" + dataset.ManifestName)
	if !ok {
		t.Fatal("no manifest")
	}
	var m dataset.Manifest
	if err := json.Unmarshal(o.Data, &m); err != nil {
		t.Fatal(err)
	}
	for _, p := range m.Parts {
		if _, ok := d.gcs().Get(prefix + "/" + p.Path); !ok {
			t.Fatalf("the committed manifest names %s, which is gone", p.Path)
		}
	}
}

// delayedWriter's manifest creations time out without creating, keeping
// the bytes, as a request accepted behind a failed connection would.
type delayedWriter struct {
	objfile.Writer
	pending []byte
}

func (w *delayedWriter) PutIfAbsent(ctx context.Context, n string, b []byte) (objfile.Receipt, error) {
	if n == dataset.ManifestName {
		w.pending = bytes.Clone(b)
		return objfile.Receipt{}, context.DeadlineExceeded
	}
	return w.Writer.PutIfAbsent(ctx, n, b)
}

// TestRemoteDelayedCommit: a manifest creation that fails ambiguously
// (a timeout, a transport failure, a proxy's 503 while the creation is
// still in flight downstream) and then reads back as absent settles
// nothing: the run is unresolved and deletes no part, so a creation that
// lands later commits a dataset whose parts all exist. A resume of a
// journaled run creates the very same candidate.
func TestRemoteDelayedCommit(t *testing.T) {
	t.Run("timeout", func(t *testing.T) {
		d := newDestination(t, gcsProvider, nil)
		out := d.output("late")
		open := out.Open
		var w *delayedWriter
		out.Open = func(ctx context.Context, s *dataset.UploadStats) (*dataset.Remote, func() error, error) {
			r, closeFn, err := open(ctx, s)
			if err == nil {
				w = &delayedWriter{Writer: r.Writer}
				r.Writer = w
			}
			return r, closeFn, err
		}
		rep, err := Run(context.Background(), treeConfig(localTree(t), filepath.Join(t.TempDir(), "work"), out))
		if !errors.Is(err, dataset.ErrCommitUnresolved) || rep.Output.Remote.Commit != "unresolved" || rep.Output.Remote.OrphansDeleted != 0 {
			t.Fatalf("%v, %+v", err, rep.Output.Remote)
		}
		d.gcs().Put("late/"+dataset.ManifestName, w.pending) // the accepted request lands
		committedPartsExist(t, d, "late")
	})
	t.Run("transport", func(t *testing.T) {
		// The connection fails while the request goes on downstream, and
		// is held there until after the run settles.
		f := gcsfake.New(dstBucket)
		entered, release := make(chan struct{}), make(chan struct{})
		var once sync.Once
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, q *http.Request) {
			if q.Method == http.MethodPost && q.URL.Query().Get("name") == "http/manifest.json" {
				b, _ := io.ReadAll(q.Body)
				q.Body = io.NopCloser(bytes.NewReader(b))
				once.Do(func() { close(entered) })
				<-release
			}
			f.ServeHTTP(w, q)
		}))
		t.Cleanup(srv.Close)
		var done sync.WaitGroup
		tr := roundTripper(func(q *http.Request) (*http.Response, error) {
			if q.Method != http.MethodPost || q.URL.Query().Get("name") != "http/manifest.json" {
				return http.DefaultTransport.RoundTrip(q)
			}
			b, _ := io.ReadAll(q.Body)
			detached := q.Clone(context.WithoutCancel(q.Context()))
			detached.Body = io.NopCloser(bytes.NewReader(b))
			done.Add(1)
			go func() {
				defer done.Done()
				if r, err := http.DefaultTransport.RoundTrip(detached); err == nil {
					io.Copy(io.Discard, r.Body)
					r.Body.Close()
				}
			}()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
			}
			return nil, io.ErrUnexpectedEOF
		})
		out := &RemoteOutput{Location: "gs://" + dstBucket + "/http", Open: func(ctx context.Context, s *dataset.UploadStats) (*dataset.Remote, func() error, error) {
			c, err := storage.NewClient(ctx, option.WithEndpoint(srv.URL+"/storage/v1/"), option.WithoutAuthentication(), option.WithHTTPClient(&http.Client{Transport: tr}))
			if err != nil {
				return nil, nil, err
			}
			l, _ := objfile.ParseLocation("gs://" + dstBucket + "/http")
			w := gcs.NewWriter(c, dstBucket, gcs.WriteOptions{MaxAttempts: 1})
			return &dataset.Remote{Location: l, Opener: objfile.Root(gcs.New(c, dstBucket), "http"), Writer: objfile.RootWriter(w, "http")}, c.Close, nil
		}}
		_, err := Run(context.Background(), treeConfig(localTree(t), filepath.Join(t.TempDir(), "work"), out))
		close(release)
		done.Wait()
		if !errors.Is(err, dataset.ErrCommitUnresolved) {
			t.Fatalf("%v", err)
		}
		if f.Count(gcsfake.OpDelete) != 0 {
			t.Fatal("parts were deleted after an unresolved commit")
		}
		committedPartsExist(t, &destination{bucketFake: gcsFake{f}, p: gcsProvider}, "http")
	})
	t.Run("proxy 503", func(t *testing.T) {
		url := crashBucket(t)
		delayed := make(chan func(), 1)
		var once sync.Once
		d := newDestination(t, gcsProvider, func(h http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPost && r.URL.Query().Get("uploadType") == "multipart" {
					body, _ := io.ReadAll(r.Body)
					r.Body = io.NopCloser(bytes.NewReader(body))
					_, params, _ := mime.ParseMediaType(r.Header.Get("Content-Type"))
					var meta struct{ Name string }
					if p, err := multipart.NewReader(bytes.NewReader(body), params["boundary"]).NextPart(); err == nil {
						_ = json.NewDecoder(p).Decode(&meta)
					}
					if meta.Name == "late503/manifest.json" {
						sent := false
						once.Do(func() {
							req := r.Clone(context.Background())
							req.Body = io.NopCloser(bytes.NewReader(body))
							delayed <- func() { h.ServeHTTP(httptest.NewRecorder(), req) }
							sent = true
						})
						if sent {
							http.Error(w, "the proxy lost the downstream answer", http.StatusServiceUnavailable)
							return
						}
					}
				}
				h.ServeHTTP(w, r)
			})
		})
		cfg := remoteConfig(url, filepath.Join(t.TempDir(), "work"), false, d.output("late503"))
		cfg.Output.NoJournal = true
		_, err := Run(context.Background(), cfg)
		select {
		case finish := <-delayed:
			finish()
		default:
			t.Fatalf("the manifest's creation was not intercepted: %v", err)
		}
		if err != nil && !errors.Is(err, dataset.ErrCommitUnresolved) {
			t.Fatal(err)
		}
		committedPartsExist(t, d, "late503")
	})
	t.Run("resume retries the candidate", func(t *testing.T) {
		datasettest.RequireJournal(t)
		url := crashBucket(t)
		d := newDestination(t, gcsProvider, nil)
		ws := filepath.Join(t.TempDir(), "work")
		out := d.output("same")
		open := out.Open
		var w *delayedWriter
		out.Open = func(ctx context.Context, s *dataset.UploadStats) (*dataset.Remote, func() error, error) {
			r, closeFn, err := open(ctx, s)
			if err == nil {
				w = &delayedWriter{Writer: r.Writer}
				r.Writer = w
			}
			return r, closeFn, err
		}
		if _, err := Run(context.Background(), remoteConfig(url, ws, false, out)); !errors.Is(err, dataset.ErrCommitUnresolved) {
			t.Fatal(err)
		}
		cand := w.pending
		if _, err := Run(context.Background(), remoteConfig(url, ws, true, d.output("same"))); err != nil {
			t.Fatal(err)
		}
		if o, _ := d.gcs().Get("same/" + dataset.ManifestName); !bytes.Equal(o.Data, cand) {
			t.Fatal("the resume committed another manifest than the unresolved candidate")
		}
	})
}

type roundTripper func(*http.Request) (*http.Response, error)

func (f roundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// TestRemoteSessionSecrecy: a resumable session's URI never reaches the
// run's report, its warnings or its settled outcome, when a middle chunk
// or a completion fails in transport.
func TestRemoteSessionSecrecy(t *testing.T) {
	url := bigBucket(t)
	for _, last := range []bool{false, true} {
		t.Run(fmt.Sprintf("last=%v", last), func(t *testing.T) {
			f := gcsfake.New(dstBucket)
			srv := httptest.NewServer(f)
			t.Cleanup(srv.Close)
			tr := roundTripper(func(q *http.Request) (*http.Response, error) {
				final := !strings.HasSuffix(q.Header.Get("Content-Range"), "/*")
				if q.URL.Query().Get("upload_id") != "" && final == last {
					return nil, errors.New("connection reset")
				}
				return http.DefaultTransport.RoundTrip(q)
			})
			out := &RemoteOutput{Location: "gs://" + dstBucket + "/s", Open: func(ctx context.Context, s *dataset.UploadStats) (*dataset.Remote, func() error, error) {
				c, err := storage.NewClient(ctx, option.WithEndpoint(srv.URL+"/storage/v1/"), option.WithoutAuthentication(), option.WithHTTPClient(&http.Client{Transport: tr}))
				if err != nil {
					return nil, nil, err
				}
				l, _ := objfile.ParseLocation("gs://" + dstBucket + "/s")
				w := gcs.NewWriter(c, dstBucket, gcs.WriteOptions{ChunkSize: 256 << 10, MaxAttempts: 1, ChunkRetryDeadline: 100 * time.Millisecond})
				return &dataset.Remote{Location: l, Opener: objfile.Root(gcs.New(c, dstBucket), "s"), Writer: objfile.RootWriter(w, "s")}, c.Close, nil
			}}
			cfg := remoteConfig(url, filepath.Join(t.TempDir(), "work"), false, out)
			cfg.Limits.PartSize = 1 << 20 // parts of several chunks: a session
			var said []string
			var mu sync.Mutex
			cfg.Warn = func(m string) { mu.Lock(); said = append(said, m); mu.Unlock() }
			cfg.Settle = func(r Report, err error) {
				mu.Lock()
				said = append(said, r.Outcome.Error, fmt.Sprint(err))
				mu.Unlock()
			}
			cfg.Limits.RowBudget, cfg.Limits.Encoders = 32<<20, 2
			rep, err := Run(context.Background(), cfg)
			if err == nil {
				t.Fatal("a run whose sessions fail succeeded")
			}
			b, _ := json.Marshal(rep)
			for _, text := range append(said, err.Error(), string(b)) {
				if strings.Contains(text, "upload_id") || strings.Contains(text, "uploadType") {
					t.Fatalf("a session URI escaped: %s", text)
				}
			}
		})
	}
}

// TestRemoteWorkDirLinks: remote state reached through a link is refused,
// before anything is written outside the work directory: the remote
// directory or the claims directory replaced by a link to another
// directory before a resume, and the claims directory swapped for one
// while the run goes.
func TestRemoteWorkDirLinks(t *testing.T) {
	datasettest.RequireJournal(t)
	url := crashBucket(t)
	d := newDestination(t, gcsProvider, nil)
	for i, c := range []struct {
		name, point, rel string
	}{{"remote", "upload-completed", dataset.RemoteRel}, {"claims", "allocated", dataset.ClaimsRel}} {
		t.Run(c.name, func(t *testing.T) {
			prefix := fmt.Sprintf("links%d", i)
			ws := filepath.Join(t.TempDir(), "work")
			crashRemote(t, url, ws, d.output(prefix), false, c.point, 1)
			external := filepath.Join(t.TempDir(), "external")
			if err := os.Rename(filepath.Join(ws, c.rel), external); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(external, filepath.Join(ws, c.rel)); err != nil {
				t.Fatal(err)
			}
			before, _ := os.ReadDir(external)
			_, err := Run(context.Background(), remoteConfig(url, ws, true, d.output(prefix)))
			if !errors.Is(err, dataset.ErrUnsafeLayout) {
				t.Fatalf("resume through a link: %v", err)
			}
			if after, _ := os.ReadDir(external); len(after) != len(before) {
				t.Fatalf("written outside the work directory: %d entries, %d before", len(after), len(before))
			}
		})
	}
	t.Run("swapped while running", func(t *testing.T) {
		ws := filepath.Join(t.TempDir(), "work")
		external := t.TempDir()
		var n atomic.Int64
		restore := faults.Set(func(p string) error {
			if p == "recorded" && n.Add(1) == 2 {
				claims := filepath.Join(ws, dataset.ClaimsRel)
				if err := os.Rename(claims, filepath.Join(t.TempDir(), "moved")); err != nil {
					t.Error(err)
				}
				if err := os.Symlink(external, claims); err != nil {
					t.Error(err)
				}
			}
			return nil
		})
		_, err := Run(context.Background(), remoteConfig(url, ws, false, d.output("swap")))
		restore()
		if err == nil {
			t.Fatal("a run whose claims directory became a link went on")
		}
		if ents, _ := os.ReadDir(external); len(ents) > 0 {
			t.Fatalf("%d receipts written outside the work directory", len(ents))
		}
	})
}

// TestRemoteDamagedHeader: a remote run's journal header destroyed after
// parts were acknowledged is corruption, never a fresh start: the work
// directory's remote state proves the header was durable.
func TestRemoteDamagedHeader(t *testing.T) {
	datasettest.RequireJournal(t)
	url := crashBucket(t)
	d := newDestination(t, gcsProvider, nil)
	ws := filepath.Join(t.TempDir(), "work")
	crashRemote(t, url, ws, d.output("header"), false, "recorded", 3)
	if err := os.WriteFile(journal.Path(ws), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := Run(context.Background(), remoteConfig(url, ws, true, d.output("header")))
	if !errors.Is(err, journal.ErrCorrupt) {
		t.Fatalf("damaged header with acknowledged remote parts: %v", err)
	}
	if l, err := readLedger(ws); err != nil || len(l.Names) == 0 {
		t.Fatalf("the remote state was discarded: %v", err)
	}
}

func readLedger(ws string) (dataset.Ledger, error) {
	r, err := os.OpenRoot(ws)
	if err != nil {
		return dataset.Ledger{}, err
	}
	defer r.Close()
	return dataset.ReadAllocations(r)
}

// TestRemoteEndpointFrozen: the destination's effective endpoint, the
// emulator STORAGE_EMULATOR_HOST names included, is frozen: a resume under
// another value of the variable is refused, and nothing reaches the other
// server.
func TestRemoteEndpointFrozen(t *testing.T) {
	datasettest.RequireJournal(t)
	url := crashBucket(t)
	a, b := newDestination(t, gcsProvider, nil), newDestination(t, gcsProvider, nil)
	ws := filepath.Join(t.TempDir(), "work")
	out := &RemoteOutput{Location: "gs://" + dstBucket + "/route", ChunkSize: 256 << 10}
	t.Setenv("STORAGE_EMULATOR_HOST", a.url)
	crashRemote(t, url, ws, out, false, "recorded", 2)
	t.Setenv("STORAGE_EMULATOR_HOST", b.url)
	_, err := Run(context.Background(), remoteConfig(url, ws, true, out))
	var mm *journal.MismatchError
	if !errors.As(err, &mm) || mm.Field != "endpoint" {
		t.Fatalf("resume under another emulator: %v", err)
	}
	if len(b.Names()) != 0 {
		t.Fatalf("the other endpoint received %v", b.Names())
	}
	t.Setenv("STORAGE_EMULATOR_HOST", a.url)
	if _, err := Run(context.Background(), remoteConfig(url, ws, true, out)); err != nil {
		t.Fatal(err)
	}
	if _, ok := a.Object("route/" + dataset.ManifestName); !ok {
		t.Fatal("not committed at the recorded endpoint")
	}
}

// TestRemoteCommitBounded: a commit whose creation fails ambiguously and
// whose read-back hangs ends at its time bound, unresolved, with every
// part kept.
func TestRemoteCommitBounded(t *testing.T) {
	url := crashBucket(t)
	hang := make(chan struct{})
	t.Cleanup(func() { close(hang) })
	var committing atomic.Bool // the fresh run's check reads the manifest too: only the commit's read-back hangs
	d := newDestination(t, gcsProvider, func(h http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if strings.HasSuffix(r.URL.Path, "bounded%2Fmanifest.json") || strings.HasSuffix(r.URL.Path, "bounded/manifest.json") {
				if r.Method == http.MethodGet && committing.Load() {
					select {
					case <-hang:
					case <-r.Context().Done():
					}
					return
				}
			}
			if r.Method == http.MethodPost && r.URL.Query().Get("name") == "bounded/manifest.json" {
				committing.Store(true)
				http.Error(w, "unavailable", http.StatusServiceUnavailable)
				return
			}
			h.ServeHTTP(w, r)
		})
	})
	out := d.output("bounded")
	out.CommitTimeout = 2 * time.Second
	cfg := remoteConfig(url, filepath.Join(t.TempDir(), "work"), false, out)
	cfg.Output.NoJournal = true
	start := time.Now()
	_, err := Run(context.Background(), cfg)
	if !errors.Is(err, dataset.ErrCommitUnresolved) {
		t.Fatalf("%v", err)
	}
	if took := time.Since(start); took > time.Minute {
		t.Fatalf("the commit took %v", took)
	}
	if d.Count(gcsfake.OpDelete) != 0 {
		t.Fatal("parts deleted after an unresolved commit")
	}
}

// bigBucket serves 150,000 keys: enough for parts of several 256 KiB
// chunks each.
func bigBucket(t *testing.T) string {
	t.Helper()
	s3test.NoCredentials(t)
	t.Setenv("AWS_REGION", "")
	t.Setenv("AWS_DEFAULT_REGION", "")
	return s3test.Serve(t, s3test.Handler(s3test.Backend(t, gcstest.Keys(150_000))))
}

// TestRemoteMultiChunk: parts of several 256 KiB chunks (resumable
// sessions): a connection dropped in a middle chunk is retried and the
// dataset is the local run's; a kill inside a later chunk of an upload is
// resumed to the same dataset, with nothing of the killed session left.
func TestRemoteMultiChunk(t *testing.T) {
	datasettest.RequireJournal(t)
	url := bigBucket(t)
	config := func(ws string, resume bool, out *RemoteOutput) Config {
		c := crashConfig(url, ws, resume)
		c.Limits.PartSize, c.Limits.RowBudget, c.Limits.Encoders = 512<<10, 32<<20, 2
		c.Output.Remote = out
		return c
	}
	ref := filepath.Join(t.TempDir(), "ref")
	if _, err := Run(context.Background(), config(ref, false, nil)); err != nil {
		t.Fatal(err)
	}
	var dropped atomic.Int64
	d := newDestination(t, gcsProvider, func(h http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			cr := r.Header.Get("Content-Range")
			if r.URL.Query().Get("upload_id") != "" && strings.HasSuffix(cr, "/*") && !strings.HasPrefix(cr, "bytes 0-") && dropped.Add(1) == 1 {
				// A middle chunk: the connection drops half way through.
				io.CopyN(io.Discard, r.Body, 1000)
				if hj, ok := w.(http.Hijacker); ok {
					if c, _, err := hj.Hijack(); err == nil {
						c.Close()
						return
					}
				}
			}
			h.ServeHTTP(w, r)
		})
	})
	sum, err := Run(context.Background(), config(filepath.Join(t.TempDir(), "w1"), false, d.output("chunks")))
	if err != nil {
		t.Fatal(err)
	}
	if dropped.Load() == 0 || d.Count(gcsfake.OpChunk) < 4 {
		t.Fatalf("no multi-chunk session: %d dropped, %d chunks", dropped.Load(), d.Count(gcsfake.OpChunk))
	}
	if sum.Output.Remote.Retries == 0 {
		t.Fatal("the dropped chunk was not retried")
	}
	d.sameAsLocal(t, "dropped chunk", ref, "chunks", true)

	// Killed after an upload's first chunk went out, inside a later one.
	ws := filepath.Join(t.TempDir(), "w2")
	var n atomic.Int64
	restore := faults.Set(func(p string) error {
		if p == "upload-chunk" && n.Add(1) == 6 { // 64 KiB a step: past the first 256 KiB chunk
			return faults.ErrCrash
		}
		return nil
	})
	_, err = Run(context.Background(), config(ws, false, d.output("killed")))
	restore()
	if !errors.Is(err, faults.ErrCrash) {
		t.Fatalf("crash not injected: %v", err)
	}
	if _, err := Run(context.Background(), config(ws, true, d.output("killed"))); err != nil {
		t.Fatal(err)
	}
	d.sameAsLocal(t, "killed in a chunk", ref, "killed", true)
}

// mixedWriter's first manifest creation fails ambiguously (its request
// kept, as one still in flight), every later one is refused (403).
type mixedWriter struct {
	objfile.Writer
	pending []byte
	calls   int
}

func (w *mixedWriter) PutIfAbsent(ctx context.Context, n string, b []byte) (objfile.Receipt, error) {
	if n != dataset.ManifestName {
		return w.Writer.PutIfAbsent(ctx, n, b)
	}
	w.calls++
	if w.calls == 1 {
		w.pending = bytes.Clone(b)
		return objfile.Receipt{}, objfile.ErrUnresolved
	}
	return objfile.Receipt{}, &googleapi.Error{Code: http.StatusForbidden}
}

// TestRemoteAmbiguityKept: once any creation of the manifest failed
// ambiguously, a later refusal settles nothing: the commit is unresolved
// and no part is deleted, whether the refusal comes from the commit's own
// retry or from the SDK's retries of one request, so the first creation
// landing later commits a dataset whose parts all exist.
func TestRemoteAmbiguityKept(t *testing.T) {
	t.Run("commit retry", func(t *testing.T) {
		d := newDestination(t, gcsProvider, nil)
		out := d.output("mixed")
		open := out.Open
		var w *mixedWriter
		out.Open = func(ctx context.Context, s *dataset.UploadStats) (*dataset.Remote, func() error, error) {
			r, closeFn, err := open(ctx, s)
			if err == nil {
				w = &mixedWriter{Writer: r.Writer}
				r.Writer = w
			}
			return r, closeFn, err
		}
		rep, err := Run(context.Background(), treeConfig(localTree(t), filepath.Join(t.TempDir(), "work"), out))
		if !errors.Is(err, dataset.ErrCommitUnresolved) || w.calls < 2 || rep.Output.Remote.OrphansDeleted != 0 {
			t.Fatalf("%v; %d creations; %+v", err, w.calls, rep.Output.Remote)
		}
		if _, err := w.Writer.PutIfAbsent(context.Background(), dataset.ManifestName, w.pending); err != nil {
			t.Fatal(err)
		}
		committedPartsExist(t, d, "mixed")
	})
	t.Run("sdk retries", func(t *testing.T) {
		// The first request goes on downstream behind two proxy 503s (the
		// SDK's two attempts); every later creation is refused.
		var pending func()
		var attempts int
		d := newDestination(t, gcsProvider, func(h http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, q *http.Request) {
				if q.Method == http.MethodPost && q.URL.Query().Get("name") == "httpmixed/manifest.json" {
					attempts++
					if attempts == 1 {
						b, _ := io.ReadAll(q.Body)
						detached := q.Clone(context.Background())
						detached.Body = io.NopCloser(bytes.NewReader(b))
						pending = func() { h.ServeHTTP(httptest.NewRecorder(), detached) }
					}
					code := http.StatusServiceUnavailable
					if attempts > 2 {
						code = http.StatusForbidden
					}
					http.Error(w, "proxy failure", code)
					return
				}
				h.ServeHTTP(w, q)
			})
		})
		_, err := Run(context.Background(), treeConfig(localTree(t), filepath.Join(t.TempDir(), "work"), d.output("httpmixed")))
		if !errors.Is(err, dataset.ErrCommitUnresolved) || pending == nil {
			t.Fatalf("%v (%d attempts)", err, attempts)
		}
		if d.Count(gcsfake.OpDelete) != 0 {
			t.Fatal("parts deleted after an ambiguous creation")
		}
		pending()
		committedPartsExist(t, d, "httpmixed")
	})
}

// TestRemoteCandidateKept: a resume that would commit other parts than an
// unresolved candidate names (its journal lost the final record, and the
// last part was written again under a new name) refuses, as unresolved,
// and replaces neither the candidate nor any part: the earlier creation
// landing later commits a dataset whose parts all exist.
func TestRemoteCandidateKept(t *testing.T) {
	datasettest.RequireJournal(t)
	url := crashBucket(t)
	d := newDestination(t, gcsProvider, nil)
	ws := filepath.Join(t.TempDir(), "work")
	out := d.output("cand")
	open := out.Open
	var w *delayedWriter
	out.Open = func(ctx context.Context, s *dataset.UploadStats) (*dataset.Remote, func() error, error) {
		r, closeFn, err := open(ctx, s)
		if err == nil {
			w = &delayedWriter{Writer: r.Writer}
			r.Writer = w
		}
		return r, closeFn, err
	}
	if _, err := Run(context.Background(), remoteConfig(url, ws, false, out)); !errors.Is(err, dataset.ErrCommitUnresolved) {
		t.Fatal(err)
	}
	pending := bytes.Clone(w.pending)
	b, err := os.ReadFile(journal.Path(ws))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(journal.Path(ws), b[:len(b)-5], 0o644); err != nil { // the final record torn
		t.Fatal(err)
	}
	_, err = Run(context.Background(), remoteConfig(url, ws, true, d.output("cand")))
	if !errors.Is(err, dataset.ErrCandidateMismatch) || !errors.Is(err, dataset.ErrCommitUnresolved) {
		t.Fatalf("resume with another candidate's parts: %v", err)
	}
	if _, ok := d.Object("cand/" + dataset.ManifestName); ok {
		t.Fatal("a manifest was created")
	}
	r, err := os.OpenRoot(ws)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if cand, _ := dataset.ReadCandidate(r); !bytes.Equal(cand, pending) {
		t.Fatal("the unresolved candidate was replaced")
	}
	d.gcs().Put("cand/"+dataset.ManifestName, pending) // the earlier creation lands
	committedPartsExist(t, d, "cand")
}

// stallingOpener holds every open of the manifest until its context ends.
type stallingOpener struct {
	objfile.Opener
	entered chan struct{}
	once    *sync.Once
}

func (o stallingOpener) Open(ctx context.Context, n string) (objfile.Object, error) {
	if n == dataset.ManifestName {
		o.once.Do(func() { close(o.entered) })
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return o.Opener.Open(ctx, n)
}

// TestRemoteResumeBounded: a resume's look at the destination's manifest
// (its preflight) ends at the commit's time bound, whether the read never
// answers or its body stalls after the headers, on the client a run
// builds.
func TestRemoteResumeBounded(t *testing.T) {
	datasettest.RequireJournal(t)
	url := crashBucket(t)
	t.Run("open", func(t *testing.T) {
		d := newDestination(t, gcsProvider, nil)
		ws := filepath.Join(t.TempDir(), "work")
		out := d.output("boundresume")
		crashRemote(t, url, ws, out, false, "candidate-saved", 1)
		open, entered := out.Open, make(chan struct{})
		var once sync.Once
		out.CommitTimeout = 100 * time.Millisecond
		out.Open = func(ctx context.Context, s *dataset.UploadStats) (*dataset.Remote, func() error, error) {
			r, closeFn, err := open(ctx, s)
			if err == nil {
				r.Opener = stallingOpener{r.Opener, entered, &once}
			}
			return r, closeFn, err
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		_, err := Run(ctx, remoteConfig(url, ws, true, out))
		select {
		case <-entered:
		default:
			t.Fatal("the resume never read the manifest")
		}
		if !errors.Is(err, dataset.ErrCommitUnresolved) || ctx.Err() != nil {
			t.Fatalf("%v (the outer deadline: %v)", err, ctx.Err())
		}
	})
	t.Run("stalled body", func(t *testing.T) {
		var stall atomic.Bool
		d := newDestination(t, gcsProvider, func(h http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, q *http.Request) {
				if stall.Load() && q.Method == http.MethodGet && strings.Contains(q.URL.Path, "manifest.json") {
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusOK)
					w.(http.Flusher).Flush()
					<-q.Context().Done()
					return
				}
				h.ServeHTTP(w, q)
			})
		})
		ws := filepath.Join(t.TempDir(), "work")
		out := &RemoteOutput{Location: "gs://" + dstBucket + "/httpbound", ChunkSize: 256 << 10, CommitTimeout: 100 * time.Millisecond}
		t.Setenv("STORAGE_EMULATOR_HOST", d.url)
		crashRemote(t, url, ws, out, false, "candidate-saved", 1)
		stall.Store(true)
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		_, err := Run(ctx, remoteConfig(url, ws, true, out))
		if !errors.Is(err, dataset.ErrCommitUnresolved) || ctx.Err() != nil {
			t.Fatalf("%v (the outer deadline: %v)", err, ctx.Err())
		}
	})
}

// TestRemoteFinalizeRetried: a commit whose orphan cleanup fails (the
// deletes refused) is not finalized: the next resume deletes them and
// finalizes.
func TestRemoteFinalizeRetried(t *testing.T) {
	datasettest.RequireJournal(t)
	url := crashBucket(t)
	ref := filepath.Join(t.TempDir(), "ref")
	if _, err := Run(context.Background(), crashConfig(url, ref, false)); err != nil {
		t.Fatal(err)
	}
	d := newDestination(t, gcsProvider, nil)
	ws := filepath.Join(t.TempDir(), "work")
	crashRemote(t, url, ws, d.output("refin"), false, "upload-completed", 1)
	d.SetFault(func(op, _ string, after bool) int {
		if op == gcsfake.OpDelete && !after {
			return http.StatusForbidden
		}
		return 0
	})
	rep, err := Run(context.Background(), remoteConfig(url, ws, true, d.output("refin")))
	if err != nil {
		t.Fatal(err)
	}
	marker := func() *dataset.RemoteCommitRecord {
		r, err := os.OpenRoot(ws)
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()
		m, err := dataset.ReadCommitMarker(r)
		if err != nil || m == nil {
			t.Fatalf("marker: %v", err)
		}
		return m
	}
	if rep.Output.Remote.OrphansLeft == 0 || marker().Finalized {
		t.Fatalf("left %d, finalized %v", rep.Output.Remote.OrphansLeft, marker().Finalized)
	}
	d.SetFault(nil)
	if _, err := Run(context.Background(), remoteConfig(url, ws, true, d.output("refin"))); !errors.Is(err, ErrComplete) {
		t.Fatalf("the finalizing resume: %v", err)
	}
	if !marker().Finalized {
		t.Fatal("not finalized once the cleanup succeeded")
	}
	d.sameAsLocal(t, "finalized on resume", ref, "refin", true)
}

// TestRemoteLostRaceResumed: a run killed after saving its candidate, whose
// prefix another run then commits, deletes its own uploads when resumed
// (and on a later resume when a delete failed), and reports the lost race.
// Per provider.
func TestRemoteLostRaceResumed(t *testing.T) {
	eachProvider(t, func(t *testing.T, p provider) {
		datasettest.RequireJournal(t)
		url := crashBucket(t)
		d := newDestination(t, p, nil)
		ws := filepath.Join(t.TempDir(), "work")
		crashRemote(t, url, ws, d.output("lostresume"), false, "candidate-saved", 1)
		d.Put("lostresume/"+dataset.ManifestName, []byte(`{"other":1}`))
		ours := func() int {
			n := 0
			for _, name := range d.Names() {
				if strings.HasPrefix(name, "lostresume/data/") {
					n++
				}
			}
			return n
		}
		if ours() == 0 {
			t.Fatal("no part uploaded before the kill")
		}
		d.SetFault(func(op, _ string, after bool) int {
			if op == opDelete && !after {
				return http.StatusForbidden
			}
			return 0
		})
		rep, err := Run(context.Background(), remoteConfig(url, ws, true, d.output("lostresume")))
		if !errors.Is(err, dataset.ErrLostRace) || rep.Output.Remote.OrphansLeft == 0 || ours() == 0 {
			t.Fatalf("%v; %+v", err, rep.Output.Remote)
		}
		d.SetFault(nil)
		rep, err = Run(context.Background(), remoteConfig(url, ws, true, d.output("lostresume")))
		if !errors.Is(err, dataset.ErrLostRace) || rep.Output.Remote.Commit != "lost_race" || rep.Output.Remote.OrphansDeleted == 0 || ours() != 0 {
			t.Fatalf("%v; %+v; %d parts left", err, rep.Output.Remote, ours())
		}
		if o, _ := d.Object("lostresume/" + dataset.ManifestName); string(o) != `{"other":1}` {
			t.Fatal("the winner's manifest changed")
		}
	})
}

// TestRemoteWithProfilesAndSpill: a bucket run with spill on calls the
// listing-done hook (the phase profiles' boundary) once, accounts the
// spill read handles of whatever it spilled, and commits the local run's
// dataset.
func TestRemoteWithProfilesAndSpill(t *testing.T) {
	eachProvider(t, func(t *testing.T, p provider) {
		datasettest.RequireJournal(t)
		url := crashBucket(t)
		ref := filepath.Join(t.TempDir(), "ref")
		if _, err := Run(context.Background(), crashConfig(url, ref, false)); err != nil {
			t.Fatal(err)
		}
		d := newDestination(t, p, nil)
		cfg := remoteConfig(url, filepath.Join(t.TempDir(), "work"), false, d.output("prof"))
		cfg.Limits.Spill.Off = false
		var done atomic.Int64
		cfg.ListingDone = func() { done.Add(1) }
		sum, err := Run(context.Background(), cfg)
		if err != nil {
			t.Fatal(err)
		}
		if done.Load() != 1 {
			t.Fatalf("the listing-done hook ran %d times", done.Load())
		}
		if sp := sum.Resources.Spill; sp == nil || sp.Rows > 0 && sp.ReadHandles.Hits+sp.ReadHandles.Misses == 0 {
			t.Fatalf("no spill read-handle account: %+v", sp)
		}
		d.sameAsLocal(t, "profiles and spill", ref, "prof", true)
	})
}
