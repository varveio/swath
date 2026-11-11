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
	"strings"
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
	l, _ := objfile.ParseLocation("gs://b/ds")
	w := gcs.NewWriter(c, "b", gcs.WriteOptions{ChunkSize: 256 << 10, MaxAttempts: 2})
	return f, &Remote{Location: l, Opener: objfile.Root(gcs.New(c, "b"), l.Prefix), Writer: objfile.RootWriter(w, l.Prefix)}
}

func workspace(t *testing.T) string {
	ws := filepath.Join(t.TempDir(), "w")
	if err := PrepareWorkspace(ws); err != nil {
		t.Fatal(err)
	}
	return ws
}

// TestPublishRemote: the commit's outcomes. The candidate is saved before
// the manifest is created; a creation refused because the run's own
// manifest is there (a lost answer) is the run's commit; another manifest
// is a lost race; a creation that fails while the read-back fails too is
// unresolved; one that fails with nothing there is no commit. The
// manifest is never replaced.
func TestPublishRemote(t *testing.T) {
	meta := Meta{SchemaID: "s"}
	parts := []Part{{Path: RemotePartName("r", "a", 0), PartInfo: PartInfo{Rows: 1, Bytes: 1, MinKey: "a", MaxKey: "a", SHA256: strings.Repeat("0", 64)}}}
	t.Run("created", func(t *testing.T) {
		f, r := fakeRemote(t)
		ws := workspace(t)
		rc, err := PublishRemote(context.Background(), ws, r, meta, parts, nil)
		if err != nil || rc.Outcome != CommitCreated || rc.MarkerErr != nil {
			t.Fatalf("%+v, %v", rc, err)
		}
		cand, _ := ReadCandidate(ws)
		if o, _ := f.Get("ds/manifest.json"); !slices.Equal(o.Data, cand) {
			t.Fatal("the manifest is not the candidate")
		}
		if !Committed(ws) {
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
		rc, err := PublishRemote(context.Background(), workspace(t), r, meta, parts, nil)
		if err != nil || rc.Outcome != CommitConfirmed {
			t.Fatalf("%+v, %v", rc, err)
		}
	})
	t.Run("lost race", func(t *testing.T) {
		f, r := fakeRemote(t)
		gen := f.Put("ds/manifest.json", []byte(`{"other":1}`))
		ws := workspace(t)
		_, err := PublishRemote(context.Background(), ws, r, meta, parts, nil)
		var lr *LostRaceError
		if !errors.As(err, &lr) || !errors.Is(err, ErrLostRace) || string(lr.Winner) != `{"other":1}` {
			t.Fatalf("%v", err)
		}
		if o, _ := f.Get("ds/manifest.json"); o.Gen != gen || Committed(ws) {
			t.Fatal("the winner's manifest changed, or the loser recorded a commit")
		}
	})
	t.Run("unresolved", func(t *testing.T) {
		f, r := fakeRemote(t)
		f.SetFault(func(op, name string, after bool) int {
			if !after && (op == gcsfake.OpCreate || op == gcsfake.OpMetadata) {
				return http.StatusForbidden // not retried
			}
			return 0
		})
		ws := workspace(t)
		_, err := PublishRemote(context.Background(), ws, r, meta, parts, nil)
		if !errors.Is(err, ErrCommitUnresolved) || Committed(ws) {
			t.Fatalf("%v", err)
		}
		if cand, _ := ReadCandidate(ws); cand == nil {
			t.Fatal("the candidate was not kept")
		}
	})
	t.Run("not created", func(t *testing.T) {
		f, r := fakeRemote(t)
		f.SetFault(func(op, _ string, after bool) int {
			if !after && op == gcsfake.OpCreate {
				return http.StatusForbidden
			}
			return 0
		})
		_, err := PublishRemote(context.Background(), workspace(t), r, meta, parts, nil)
		if err == nil || errors.Is(err, ErrCommitUnresolved) || errors.Is(err, ErrLostRace) {
			t.Fatalf("%v", err)
		}
		if _, ok := f.Get("ds/manifest.json"); ok {
			t.Fatal("created")
		}
	})
}

// TestAllocations: names are durable one a line; a torn last line was
// never acknowledged and is left out; a name is listed once.
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
	a.Close()
	f, _ := os.OpenFile(filepath.Join(ws, Workspace, RemoteDir, AllocatedName), os.O_APPEND|os.O_WRONLY, 0)
	f.WriteString("data/r/a/part-0")
	f.Close()
	got, err := ReadAllocations(ws)
	if err != nil || !slices.Equal(got, []string{"data/r/a/part-00000.parquet", "data/r/a/part-00001.parquet"}) {
		t.Fatalf("%v, %v", got, err)
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
	dir := filepath.Join(ws, Workspace, RemoteDir, ClaimsDir)
	os.WriteFile(filepath.Join(dir, ".part-00004.parquet.json.1.tmp"), []byte("{"), 0o644)
	got, err := ReadClaims(ws)
	if err != nil || len(got) != 1 || got[3].Object != c.Object || !slices.Equal(got[3].Columns, c.Columns) {
		t.Fatalf("%+v, %v", got, err)
	}
	os.WriteFile(filepath.Join(dir, "stray"), nil, 0o644)
	if _, err := ReadClaims(ws); err == nil {
		t.Fatal("a stray entry was accepted")
	}
}

// TestDeleteOwned: only names not kept are deleted, each at the
// generation found; gone names are counted.
func TestDeleteOwned(t *testing.T) {
	f, r := fakeRemote(t)
	f.Put("ds/a", []byte("a"))
	f.Put("ds/b", []byte("b"))
	st := DeleteOwned(context.Background(), r, []string{"a", "b", "c"}, map[string]bool{"b": true})
	if st.Deleted != 1 || st.Gone != 1 || st.Failed != 0 {
		t.Fatalf("%+v", st)
	}
	if names := f.Names(); !slices.Equal(names, []string{"ds/b"}) {
		t.Fatalf("left %v", names)
	}
}
