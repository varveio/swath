// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/varveio/swath/internal/filter"
	"github.com/varveio/swath/internal/journal"
	"github.com/varveio/swath/internal/source/azblob"
	"github.com/varveio/swath/internal/source/azblob/azblobtest"
)

// narrowCorpus is a container for the narrowing's command tests: d/NNN
// and d/NNN.tmp under d/, names beside it (d0, dd/x) and after it (e/), and
// names holding a '?' (one that would read as a SAS). want is what
// --include 'd/**' --exclude '*.tmp' keeps, tmp what the exclude drops
// under d/.
func narrowCorpus() (keys, want []string, tmp int) {
	keys = []string{"a", "d0", "dd/x", "d?x/1", "d?sig=1/a", "e"}
	for i := range 1000 {
		k := fmt.Sprintf("d/%04d", i)
		keys, want = append(keys, k), append(want, k)
		if i%10 == 0 {
			keys, tmp = append(keys, k+".tmp"), tmp+1
		}
	}
	for i := range 300 {
		keys = append(keys, fmt.Sprintf("e/%04d", i))
	}
	slices.Sort(keys)
	return keys, want, tmp
}

// listProxy is a replay behind a proxy that records each List Blobs
// request's query.
type listProxy struct {
	url string
	mu  sync.Mutex
	q   []url.Values
}

func newListProxy(t *testing.T, keys []string) *listProxy {
	t.Helper()
	r := azblobtest.Serve(t, keys)
	u, _ := url.Parse(azEndpoint(r))
	px := httputil.NewSingleHostReverseProxy(u)
	p := &listProxy{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if q := req.URL.Query(); q.Get("comp") == "list" {
			p.mu.Lock()
			p.q = append(p.q, q)
			p.mu.Unlock()
		}
		px.ServeHTTP(w, req)
	}))
	t.Cleanup(srv.Close)
	p.url = srv.URL
	return p
}

// take returns the requests recorded since the last take.
func (p *listProxy) take() []url.Values {
	p.mu.Lock()
	defer p.mu.Unlock()
	q := p.q
	p.q = nil
	return q
}

// under fails unless every request asked for prefix or one below it (some
// for prefix itself), each with signature sig ("" for none).
func under(t *testing.T, at string, reqs []url.Values, prefix, sig string) {
	t.Helper()
	exact := false
	for _, q := range reqs {
		if !strings.HasPrefix(q.Get("prefix"), prefix) || q.Get("sig") != sig || sig == "" && q.Has("sig") {
			t.Fatalf("%s: a request for prefix %q, sig %q; want under %q, sig %q", at, q.Get("prefix"), q.Get("sig"), prefix, sig)
		}
		exact = exact || q.Get("prefix") == prefix
	}
	if !exact {
		t.Fatalf("%s: %d requests, none for prefix %q", at, len(reqs), prefix)
	}
}

// narrowReport is the run report's part the narrowing shows in.
type narrowReport struct {
	Output struct {
		RowsEmitted  int64 `json:"rows_emitted"`
		RowsFiltered int64 `json:"rows_filtered"`
	}
	Invocation struct{ Source, Narrowing string }
}

// TestAzblobIncludeNarrows: on Azure an --include with a literal prefix
// lists only that prefix: every List Blobs request asks for it, the rows
// are what the filters keep, the report says narrowed and names the
// narrowed source, rows_filtered counts only rows read under the prefix
// (blobs never visited are not counted), and the manifest's source and
// scope name the narrowed listing while coverage.filters still lists
// every filter and collected is a whole run's.
func TestAzblobIncludeNarrows(t *testing.T) {
	captureStderr(t)
	keys, want, tmp := narrowCorpus()
	px := newListProxy(t, keys)
	ctx := context.Background()
	dir, whole := filepath.Join(t.TempDir(), "ds"), filepath.Join(t.TempDir(), "whole")
	report := filepath.Join(t.TempDir(), "r.json")
	base := []string{"az://replay/corpus", "--endpoint-url", px.url, "--anonymous", "--workers", "4", "--page-size", "7"}
	filters := []string{"--include", "d/**", "--exclude", "*.tmp"}
	if err := run(ctx, slices.Concat(base, filters, []string{"-o", dir, "--report", report}), io.Discard, true); err != nil {
		t.Fatal(err)
	}
	under(t, "narrowed", px.take(), "d/", "")
	if got := datasetKeys(t, dir); !slices.Equal(got, want) {
		t.Fatalf("%d keys, want %d", len(got), len(want))
	}
	var r narrowReport
	readJSON(t, report, &r)
	if r.Invocation.Source != "az://replay/corpus/d/" || r.Invocation.Narrowing != filter.Narrowed ||
		r.Output.RowsEmitted != int64(len(want)) || r.Output.RowsFiltered != int64(tmp) {
		t.Fatalf("report %+v; want %d rows emitted, %d filtered", r, len(want), tmp)
	}
	m := readManifest(t, dir)
	wantFilters := filter.Texts((&filter.Spec{Include: []string{"d/**"}, Exclude: []string{"*.tmp"}}).Descriptions())
	if m.Source.URI != "az://replay/corpus/d/" || m.Source.Container != "corpus" || m.Scope.Kind != "prefix" || m.Scope.Prefix != "d/" ||
		!m.Coverage.Complete || !slices.Equal(filter.Texts(m.Coverage.Filters), wantFilters) {
		t.Fatalf("manifest source %+v, scope %+v, complete %v, filters %q (want %q)", m.Source, m.Scope, m.Coverage.Complete,
			filter.Texts(m.Coverage.Filters), wantFilters)
	}
	if err := run(ctx, append(base, "-o", whole), io.Discard, true); err != nil {
		t.Fatal(err)
	}
	under(t, "whole", px.take(), "", "")
	if mw := readManifest(t, whole); !reflect.DeepEqual(m.Collected, mw.Collected) || mw.Scope.Kind != "whole" {
		t.Fatalf("collected %+v, a whole run's %+v (scope %+v)", m.Collected, mw.Collected, mw.Scope)
	}
}

// TestAzblobIncludeKeepsTheSAS: a SAS in the source URL is kept byte for
// byte when an include narrows it (every request signs with it, under the
// prefix) and is recorded nowhere; a prefix the root cannot carry is
// declined with provider_declined_prefix and the root is listed as given:
// a '?' beside the SAS, and an include whose '?' would read as a SAS,
// which never becomes a credential (the anonymous run sends no sig). The
// rows are the filters' either way.
func TestAzblobIncludeKeepsTheSAS(t *testing.T) {
	stderr := captureStderr(t)
	t.Setenv(azblob.EnvSAS, "")
	t.Setenv(azblob.EnvKey, "")
	keys, want, _ := narrowCorpus()
	px := newListProxy(t, keys)
	ctx := context.Background()
	const sas = "sv=2026-10-06&sig=SECRET1"
	for _, c := range []struct {
		name, root, include string
		anonymous           bool
		source, narrowing   string
		prefix, sig         string
		want                []string
	}{
		{"narrowed", "az://replay/corpus?" + sas, "d/**", false, "az://replay/corpus/d/", filter.Narrowed, "d/", "SECRET1", want},
		{"? beside a SAS", "az://replay/corpus?" + sas, `d\?x/**`, false, "az://replay/corpus", filter.NarrowDeclined, "", "SECRET1", []string{"d?x/1"}},
		{"? that reads as a SAS", "az://replay/corpus", `d\?sig=1/**`, true, "az://replay/corpus", filter.NarrowDeclined, "", "", []string{"d?sig=1/a"}},
	} {
		dir := filepath.Join(t.TempDir(), "ds")
		report := filepath.Join(t.TempDir(), "r.json")
		args := []string{c.root, "--endpoint-url", px.url, "--workers", "4", "--include", c.include, "--exclude", "*.tmp", "-o", dir, "--report", report}
		if c.anonymous {
			args = append(args, "--anonymous")
		}
		if err := run(ctx, args, io.Discard, false); err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		under(t, c.name, px.take(), c.prefix, c.sig)
		if got := datasetKeys(t, dir); !slices.Equal(got, c.want) {
			t.Fatalf("%s: %d keys, want %d", c.name, len(got), len(c.want))
		}
		var r narrowReport
		readJSON(t, report, &r)
		if r.Invocation.Source != c.source || r.Invocation.Narrowing != c.narrowing {
			t.Fatalf("%s: invocation %+v, want source %q, narrowing %q", c.name, r.Invocation, c.source, c.narrowing)
		}
		noSecret(t, c.name, stderr, dir, report)
	}
}

// TestAzblobNarrowedResume: a narrowed run interrupted and resumed by the
// same build lists the same narrowed prefix (its journal's source is the
// narrowed one) and completes with exactly the filters' rows. A journal
// that recorded the whole container for the same command line (as 0.4.0,
// which did not narrow on Azure, wrote it) is refused by the identity
// check, exit 2, with advice a user can act on, and the directory is left
// as it was.
func TestAzblobNarrowedResume(t *testing.T) {
	captureStderr(t)
	keys, want, _ := narrowCorpus()
	px := newListProxy(t, keys)
	ctx := context.Background()
	args := func(dir string) []string {
		return []string{"az://replay/corpus", "--endpoint-url", px.url, "--anonymous", "--include", "d/**", "--exclude", "*.tmp",
			"--part-size", "4KiB", "--part-interval", "0", "--workers", "2", "-o", dir}
	}
	dir := filepath.Join(t.TempDir(), "ds")
	crashRun(t, args(dir), 2)
	st, err := journal.Read(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got := sourceField(st.Header.Identity); got != "az://replay/corpus/d/" {
		t.Fatalf("journal source %q", got)
	}
	under(t, "interrupted", px.take(), "d/", "")
	if err := dispatch(ctx, []string{"resume", dir}, io.Discard); err != nil {
		t.Fatal(err)
	}
	under(t, "resumed", px.take(), "d/", "")
	if got := datasetKeys(t, dir); !slices.Equal(got, want) {
		t.Fatalf("resumed: %d keys, want %d", len(got), len(want))
	}

	old := filepath.Join(t.TempDir(), "ds")
	crashRun(t, args(old), 2)
	px.take()
	st, err = journal.Read(old)
	if err != nil {
		t.Fatal(err)
	}
	id := st.Header.Identity
	id.Source = slices.Clone(id.Source)
	for i := range id.Source {
		if id.Source[i].Name == "source" {
			id.Source[i].Value = "az://replay/corpus"
		}
	}
	j, err := journal.Create(old, id, st.Header.Argv)
	if err != nil {
		t.Fatal(err)
	}
	for _, rec := range st.Records {
		if err := j.Append(rec); err != nil {
			t.Fatal(err)
		}
	}
	j.Close()
	before, _ := os.ReadFile(journal.Path(old))
	err = dispatch(ctx, []string{"resume", old}, io.Discard)
	if exitCode(t, err) != exitUsage || !strings.Contains(err.Error(), "source was az://replay/corpus, now az://replay/corpus/d/") ||
		!strings.Contains(err.Error(), "finish the run with that swath, or start over with swath list --overwrite") {
		t.Fatalf("a 0.4.0 journal resumed: %v", err)
	}
	if after, _ := os.ReadFile(journal.Path(old)); !bytes.Equal(before, after) || len(px.take()) != 0 {
		t.Fatal("the refused resume changed the journal or listed")
	}
}

func sourceField(id journal.Identity) string {
	for _, f := range id.Source {
		if f.Name == "source" {
			return f.Value
		}
	}
	return ""
}

// TestAzblobNarrowedSeedScope: a dataset prior must have the run's scope
// exactly, and an include now narrows it: a narrowed run's dataset does
// not seed the whole container's run, nor a whole run's dataset the
// narrowed run (exit 2 both); it seeds the same narrowed command. A cut
// file records no scope: cuts outside the narrowed prefix are refused,
// and cuts under it seed the whole container's run, which still lists
// every key.
func TestAzblobNarrowedSeedScope(t *testing.T) {
	captureStderr(t)
	keys, want, _ := narrowCorpus()
	px := newListProxy(t, keys)
	ctx := context.Background()
	base := []string{"az://replay/corpus", "--endpoint-url", px.url, "--anonymous", "--workers", "4"}
	filters := []string{"--include", "d/**", "--exclude", "*.tmp"}
	narrowed, whole := filepath.Join(t.TempDir(), "narrowed"), filepath.Join(t.TempDir(), "whole")
	if err := run(ctx, slices.Concat(base, filters, []string{"-o", narrowed}), io.Discard, false); err != nil {
		t.Fatal(err)
	}
	if err := run(ctx, append(base, "-o", whole), io.Discard, false); err != nil {
		t.Fatal(err)
	}
	cuts := func(ks ...string) string {
		p := filepath.Join(t.TempDir(), "cuts.txt")
		if err := os.WriteFile(p, []byte(strings.Join(ks, "\n")+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		return p
	}
	for _, c := range []struct {
		name    string
		filters bool
		seed    string
		refused string // "" accepted
		want    []string
	}{
		{"narrowed dataset, whole run", false, narrowed, `the prior was taken under the prefix "d/", the run lists ""`, nil},
		{"whole dataset, narrowed run", true, whole, `the prior was taken under the prefix "", the run lists "d/"`, nil},
		{"narrowed dataset, narrowed run", true, narrowed, "", want},
		{"whole cuts, narrowed run", true, cuts("a", "d/0500", "e/0100"), `is outside the scope "d/"`, nil},
		{"narrowed cuts, whole run", false, cuts("d/0300", "d/0600"), "", keys},
	} {
		dir := filepath.Join(t.TempDir(), "ds")
		args := append(slices.Clone(base), "--seed-from", c.seed, "-o", dir)
		if c.filters {
			args = append(args, filters...)
		}
		err := run(ctx, args, io.Discard, false)
		px.take()
		if c.refused != "" {
			if exitCode(t, err) != exitUsage || !strings.Contains(err.Error(), c.refused) {
				t.Fatalf("%s: %v, want a refusal naming %q", c.name, err, c.refused)
			}
			continue
		}
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if got := datasetKeys(t, dir); !slices.Equal(got, c.want) {
			t.Fatalf("%s: %d keys, want %d", c.name, len(got), len(c.want))
		}
	}
}
