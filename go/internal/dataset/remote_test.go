// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package dataset

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"

	"github.com/varveio/swath/go/objfile"
	"github.com/varveio/swath/go/objfile/gcs"
	"github.com/varveio/swath/go/objfile/gcs/gcsfake"
)

func fakeRemote(t *testing.T) (*gcsfake.Server, *Remote) {
	t.Helper()
	f := gcsfake.New("b")
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	c, err := storage.NewClient(context.Background(), option.WithEndpoint(srv.URL+"/storage/v1/"), option.WithoutAuthentication())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	c.SetRetry(storage.WithMaxAttempts(2))
	l, _ := objfile.ParseLocation("gs://b/ds")
	w := gcs.NewWriter(c, "b", gcs.WriteOptions{ChunkSize: 256 << 10, MaxAttempts: 2})
	return f, &Remote{Location: l, Opener: objfile.Root(gcs.New(c, "b"), l.Prefix), Writer: objfile.RootWriter(w, l.Prefix)}
}

// workspace is a prepared work directory, opened as a root.
func workspace(t *testing.T) *os.Root {
	t.Helper()
	ws := filepath.Join(t.TempDir(), "w")
	if err := os.MkdirAll(filepath.Join(ws, Workspace), 0o755); err != nil {
		t.Fatal(err)
	}
	r, err := os.OpenRoot(ws)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { r.Close() })
	if err := PrepareWorkspaceIn(r); err != nil {
		t.Fatal(err)
	}
	return r
}

func committed(t *testing.T, r *os.Root) bool {
	t.Helper()
	m, err := ReadCommitMarker(r)
	if err != nil {
		t.Fatal(err)
	}
	return m != nil
}

// flaky is a Writer whose manifest creations return err without creating
// anything (pending keeps the bytes, as a request still in flight would).
type flaky struct {
	objfile.Writer
	err     error
	calls   atomic.Int64
	pending []byte
}

func (w *flaky) PutIfAbsent(ctx context.Context, name string, b []byte) (objfile.Receipt, error) {
	if name == ManifestName && w.err != nil {
		w.calls.Add(1)
		w.pending = slices.Clone(b)
		return objfile.Receipt{}, w.err
	}
	return w.Writer.PutIfAbsent(ctx, name, b)
}

// TestPublishRemote: the commit's outcomes. The candidate is saved before
// the manifest is created; a creation refused because the run's own
// manifest is there (a lost answer) is the run's commit; another manifest
// is a lost race; a creation that fails in transport and finds nothing is
// created again with the same bytes, and if that never succeeds it is
// unresolved, the candidate kept; a creation the store refuses outright,
// with nothing there, is no commit. The manifest is never replaced.
func TestPublishRemote(t *testing.T) {
	meta := Meta{SchemaID: "s"}
	parts := []Part{{Path: RemotePartName("r", "a", 0), PartInfo: PartInfo{Rows: 1, Bytes: 1, MinKey: "a", MaxKey: "a", SHA256: strings.Repeat("0", 64)}}}
	t.Run("created", func(t *testing.T) {
		f, r := fakeRemote(t)
		ws := workspace(t)
		rc, err := PublishRemote(context.Background(), ws, r, meta, parts, nil, nil)
		if err != nil || rc.Outcome != CommitCreated || rc.MarkerErr != nil {
			t.Fatalf("%+v, %v", rc, err)
		}
		cand, _ := ReadCandidate(ws)
		if o, _ := f.Get("ds/manifest.json"); !slices.Equal(o.Data, cand) {
			t.Fatal("the manifest is not the candidate")
		}
		if !committed(t, ws) {
			t.Fatal("no commit marker")
		}
	})
	t.Run("lost answer", func(t *testing.T) {
		f, r := fakeRemote(t)
		lost := false
		f.SetFault(func(op, _ string, after bool) int {
			if op == gcsfake.OpCreate && after && !lost {
				lost = true
				return http.StatusServiceUnavailable
			}
			return 0
		})
		rc, err := PublishRemote(context.Background(), workspace(t), r, meta, parts, nil, nil)
		if err != nil || rc.Outcome != CommitConfirmed {
			t.Fatalf("%+v, %v", rc, err)
		}
	})
	t.Run("lost race", func(t *testing.T) {
		f, r := fakeRemote(t)
		gen := f.Put("ds/manifest.json", []byte(`{"other":1}`))
		ws := workspace(t)
		_, err := PublishRemote(context.Background(), ws, r, meta, parts, nil, nil)
		var lr *LostRaceError
		if !errors.As(err, &lr) || !errors.Is(err, ErrLostRace) || string(lr.Winner) != `{"other":1}` {
			t.Fatalf("%v", err)
		}
		if o, _ := f.Get("ds/manifest.json"); o.Gen != gen || committed(t, ws) {
			t.Fatal("the winner's manifest changed, or the loser recorded a commit")
		}
	})
	t.Run("in flight then absent", func(t *testing.T) {
		// Every creation times out without creating (a request still in
		// flight); the read-back finds nothing: unresolved, never "not
		// created", and the same bytes were tried again.
		_, r := fakeRemote(t)
		w := &flaky{Writer: r.Writer, err: objfile.RedactError(errors.Join(objfile.ErrUnresolved, context.DeadlineExceeded))}
		r.Writer = w
		ws := workspace(t)
		_, err := PublishRemote(context.Background(), ws, r, meta, parts, nil, nil)
		cand, _ := ReadCandidate(ws)
		if !errors.Is(err, ErrCommitUnresolved) || committed(t, ws) || w.calls.Load() != createAttempts || !slices.Equal(w.pending, cand) {
			t.Fatalf("%v; %d creations", err, w.calls.Load())
		}
	})
	t.Run("in flight then created", func(t *testing.T) {
		// The first creation fails in transport; the second, the same
		// bytes, succeeds: the commit.
		f, r := fakeRemote(t)
		var n atomic.Int64
		f.SetFault(func(op, _ string, after bool) int {
			if op == gcsfake.OpCreate && !after && n.Add(1) <= 2 { // the SDK's two attempts of the first creation
				return http.StatusBadGateway
			}
			return 0
		})
		rc, err := PublishRemote(context.Background(), workspace(t), r, meta, parts, nil, nil)
		if err != nil || rc.Outcome != CommitCreated {
			t.Fatalf("%+v, %v", rc, err)
		}
	})
	t.Run("prior candidate", func(t *testing.T) {
		// An earlier attempt's candidate naming the same parts is created
		// as it is, its publication time included.
		f, r := fakeRemote(t)
		ws := workspace(t)
		w := &flaky{Writer: r.Writer, err: objfile.ErrUnresolved}
		r.Writer = w
		if _, err := PublishRemote(context.Background(), ws, r, meta, parts, nil, nil); !errors.Is(err, ErrCommitUnresolved) {
			t.Fatal(err)
		}
		prior, _ := ReadCandidate(ws)
		r.Writer = w.Writer
		if _, err := PublishRemote(context.Background(), ws, r, meta, parts, prior, nil); err != nil {
			t.Fatal(err)
		}
		if o, _ := f.Get("ds/manifest.json"); !slices.Equal(o.Data, prior) {
			t.Fatal("the prior candidate was not created as it was")
		}
		other := slices.Clone(parts)
		other[0].Rows = 2
		if sameParts(prior, other) {
			t.Fatal("a candidate of other parts is taken for the same")
		}
	})
	t.Run("unresolved read", func(t *testing.T) {
		f, r := fakeRemote(t)
		f.SetFault(func(op, name string, after bool) int {
			if !after && (op == gcsfake.OpCreate || op == gcsfake.OpMetadata) {
				return http.StatusForbidden // not retried
			}
			return 0
		})
		ws := workspace(t)
		_, err := PublishRemote(context.Background(), ws, r, meta, parts, nil, nil)
		if !errors.Is(err, ErrCommitUnresolved) || committed(t, ws) {
			t.Fatalf("%v", err)
		}
		if cand, _ := ReadCandidate(ws); cand == nil {
			t.Fatal("the candidate was not kept")
		}
	})
	t.Run("refused", func(t *testing.T) {
		f, r := fakeRemote(t)
		f.SetFault(func(op, _ string, after bool) int {
			if !after && op == gcsfake.OpCreate {
				return http.StatusForbidden
			}
			return 0
		})
		_, err := PublishRemote(context.Background(), workspace(t), r, meta, parts, nil, nil)
		if err == nil || errors.Is(err, ErrCommitUnresolved) || errors.Is(err, ErrLostRace) {
			t.Fatalf("%v", err)
		}
		if _, ok := f.Get("ds/manifest.json"); ok {
			t.Fatal("created")
		}
	})
}

// TestAllocations: names and creations are durable one a line; a torn
// last line was never acknowledged: it is left out when read, and cut
// (durably) when the log is opened again, so the next name is whole.
// Interrupted again and reopened, every acknowledged name and creation is
// there for cleanup.
func TestAllocations(t *testing.T) {
	ws := workspace(t)
	a, err := OpenAllocations(ws)
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range []string{"data/r/a/part-00000.parquet", "data/r/a/part-00001.parquet", "data/r/a/part-00000.parquet"} {
		if err := a.Allocate(n); err != nil {
			t.Fatal(err)
		}
	}
	if err := a.Created("data/r/a/part-00001.parquet", "7"); err != nil {
		t.Fatal(err)
	}
	a.Close()
	tear := func(s string) {
		f, err := ws.OpenFile(AllocatedRel, os.O_APPEND|os.O_WRONLY, 0)
		if err != nil {
			t.Fatal(err)
		}
		f.WriteString(s)
		f.Close()
	}
	tear("A\tdata/r/a/part-0")
	l, err := ReadAllocations(ws)
	if err != nil || !slices.Equal(l.Names, []string{"data/r/a/part-00000.parquet", "data/r/a/part-00001.parquet"}) || l.Created["data/r/a/part-00001.parquet"] != "7" {
		t.Fatalf("%+v, %v", l, err)
	}
	// Reopened: the torn line is cut before the next name follows.
	a, err = OpenAllocations(ws)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Allocate("data/r/b/part-00002.parquet"); err != nil {
		t.Fatal(err)
	}
	if err := a.Created("data/r/b/part-00002.parquet", "9"); err != nil {
		t.Fatal(err)
	}
	a.Close()
	tear("C\tdata/r/b/part-00002.parquet\t1") // interrupted again
	a, err = OpenAllocations(ws)
	if err != nil {
		t.Fatal(err)
	}
	a.Close()
	l, err = ReadAllocations(ws)
	if err != nil || !slices.Contains(l.Names, "data/r/b/part-00002.parquet") || l.Created["data/r/b/part-00002.parquet"] != "9" || len(l.Names) != 3 {
		t.Fatalf("after reopening: %+v, %v", l, err)
	}
	// A line of another shape is damage, not a name.
	tear("bogus\n")
	if _, err := ReadAllocations(ws); err == nil {
		t.Fatal("a damaged line was accepted")
	}
}

// TestClaims: receipts round-trip by ordinal; a replacement's temporary
// file is ignored; anything else in the directory is refused.
func TestClaims(t *testing.T) {
	ws := workspace(t)
	c := ClaimReceipt{Ordinal: 3, Object: RemotePartName("r", "a", 3), Identity: "7", Part: PartInfo{Rows: 2, Bytes: 9, SHA256: "ab"}, Columns: []string{"key"}, CreatedBy: "x"}
	if err := WriteClaim(ws, c); err != nil {
		t.Fatal(err)
	}
	if err := ws.WriteFile(filepath.Join(ClaimsRel, ".part-00004.parquet.json.1.tmp"), []byte("{"), 0o644); err != nil {
		t.Fatal(err)
	}
	got, err := ReadClaims(ws)
	if err != nil || len(got) != 1 || got[3].Object != c.Object || !slices.Equal(got[3].Columns, c.Columns) {
		t.Fatalf("%+v, %v", got, err)
	}
	if err := ws.WriteFile(filepath.Join(ClaimsRel, "stray"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadClaims(ws); err == nil {
		t.Fatal("a stray entry was accepted")
	}
}

// TestRemoteStateRefusesLinks: the remote state is never reached through a
// link, nor written outside the work directory: a claims directory, a
// remote directory, an allocation log or a candidate replaced by a link
// is refused, and nothing is written where the link points.
func TestRemoteStateRefusesLinks(t *testing.T) {
	for _, rel := range []string{ClaimsRel, RemoteRel, AllocatedRel, CandidateRel} {
		t.Run(filepath.Base(rel), func(t *testing.T) {
			ws := workspace(t)
			if a, err := OpenAllocations(ws); err == nil {
				a.Close()
			}
			if err := SaveCandidate(ws, []byte("{}")); err != nil {
				t.Fatal(err)
			}
			target := filepath.Join(t.TempDir(), "x")
			if err := os.MkdirAll(target, 0o755); err != nil {
				t.Fatal(err)
			}
			if err := ws.RemoveAll(rel); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(target, filepath.Join(ws.Name(), rel)); err != nil {
				t.Fatal(err)
			}
			if err := CheckRemoteLayout(ws); !errors.Is(err, ErrUnsafeLayout) {
				t.Fatalf("layout check: %v", err)
			}
			if _, err := Evidence(ws); err == nil {
				t.Fatal("evidence read through a link")
			}
			errs := []error{WriteClaim(ws, ClaimReceipt{Ordinal: 0, Object: "o"}), SaveCandidate(ws, []byte("{}"))}
			if a, err := OpenAllocations(ws); err == nil {
				errs = append(errs, a.Allocate("n"))
				a.Close()
			} else {
				errs = append(errs, err)
			}
			if !slices.ContainsFunc(errs, func(e error) bool { return e != nil }) {
				t.Fatal("every write through the link succeeded")
			}
			if ents, _ := os.ReadDir(target); len(ents) > 0 {
				t.Fatalf("written outside the work directory: %v", ents)
			}
		})
	}
}

// TestDeleteOwned: only objects this run's uploads created are deleted,
// each at the generation recorded at creation: one another writer put
// there since is left; an allocated name with no recorded creation is
// never touched.
func TestDeleteOwned(t *testing.T) {
	f, r := fakeRemote(t)
	ours := f.Put("ds/a", []byte("a"))
	f.Put("ds/b", []byte("b"))
	replaced := f.Put("ds/c", []byte("ours"))
	f.Put("ds/c", []byte("another writer's replacement"))
	f.Put("ds/d", []byte("a failed CreateOnly found this"))
	gen := func(n int64) string { return strconv.FormatInt(n, 10) }
	l := Ledger{Names: []string{"a", "b", "c", "d", "e"}, Created: map[string]string{"a": gen(ours), "b": "999", "c": gen(replaced), "e": "5"}}
	st := DeleteOwned(context.Background(), r, l, map[string]bool{"b": true})
	if st.Deleted != 1 || st.Gone != 2 || st.Unproven != 1 || st.Failed != 0 {
		t.Fatalf("%+v", st)
	}
	if names := f.Names(); !slices.Equal(names, []string{"ds/b", "ds/c", "ds/d"}) {
		t.Fatalf("left %v", names)
	}
}

// TestCommitMarkerStrict: only a commit record is a commit marker: other
// JSON at its name (a report written there) commits nothing and is refused
// when read.
func TestCommitMarkerStrict(t *testing.T) {
	ws := workspace(t)
	if err := ws.WriteFile(RemoteCommitName, []byte(`{"report_version":6,"outcome":{"status":"failed"}}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if Committed(ws.Name()) || Unfinalized(ws.Name()) {
		t.Fatal("a report at the marker's name is taken for a commit")
	}
	if _, err := ReadCommitMarker(ws); !errors.Is(err, ErrBadMarker) {
		t.Fatalf("read: %v", err)
	}
	if err := WriteCommitMarker(ws, RemoteCommitRecord{Destination: "gs://b/p/", ManifestSHA256: strings.Repeat("a", 64)}); err != nil {
		t.Fatal(err)
	}
	if !Committed(ws.Name()) || !Unfinalized(ws.Name()) {
		t.Fatal("a commit record is not a commit")
	}
	if !Reserved(RemoteCommitName) {
		t.Fatal("the marker's name is not reserved")
	}
}
