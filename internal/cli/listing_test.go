// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/pflag"
	seekparquet "github.com/varveio/seekparquet/go"

	"github.com/varveio/swath/internal/filter"
	"github.com/varveio/swath/internal/inventory"
	"github.com/varveio/swath/internal/order"
	"github.com/varveio/swath/internal/registry"
	"github.com/varveio/swath/internal/source"
	"github.com/varveio/swath/internal/source/s3"
	"github.com/varveio/swath/internal/source/s3/s3test"
	"github.com/varveio/swath/internal/totals"
	"github.com/varveio/swath/reader"
)

// TestListingFlag: --listing chooses S3's listing; the default on an R2
// endpoint, given by flag or by AWS_ENDPOINT_URL or AWS_ENDPOINT_URL_S3,
// is the folder walk, told once; --listing range there warns; another
// endpoint lists by key ranges unless asked.
func TestListingFlag(t *testing.T) {
	s3test.NoCredentials(t)
	const r2 = "https://0123abcd.r2.cloudflarestorage.com"
	for _, tc := range []struct {
		name string
		env  map[string]string
		args []string
		tree bool
		warn string
	}{
		{"default", nil, []string{"--endpoint-url", "http://127.0.0.1:1"}, false, ""},
		{"range", nil, []string{"--endpoint-url", "http://127.0.0.1:1", "--listing", "range"}, false, ""},
		{"tree", nil, []string{"--endpoint-url", "http://127.0.0.1:1", "--listing=tree"}, true, source.WarnTreeListing},
		{"r2 flag", nil, []string{"--endpoint-url", r2}, true, source.WarnTreeListing},
		{"r2 env", map[string]string{"AWS_ENDPOINT_URL": r2}, nil, true, source.WarnTreeListing},
		{"r2 env s3", map[string]string{"AWS_ENDPOINT_URL_S3": r2, "AWS_ENDPOINT_URL": "http://127.0.0.1:1"}, nil, true, source.WarnTreeListing},
		{"r2 range", map[string]string{"AWS_ENDPOINT_URL": r2}, []string{"--listing", "range"}, false, source.WarnUnorderedRange},
		{"other env", map[string]string{"AWS_ENDPOINT_URL": "https://storage.example.com"}, nil, false, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("AWS_ENDPOINT_URL", "")
			t.Setenv("AWS_ENDPOINT_URL_S3", "")
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			var warned []string
			src, err := openArgs(t, "s3://bkt", false, func(code, _ string) { warned = append(warned, code) },
				append([]string{"--anonymous", "--s3-region", "auto"}, tc.args...)...)
			if err != nil {
				t.Fatal(err)
			}
			_, isTree := src.(*s3TreeSource)
			if isTree != tc.tree {
				t.Fatalf("opened %T", src)
			}
			// A notice for R2's default only; an explicit tree is told at
			// a dataset run (TestTreeDatasetNotResumable).
			want := tc.warn
			if tc.name == "tree" {
				want = ""
			}
			if (want == "") != (len(warned) == 0) || want != "" && (len(warned) != 1 || warned[0] != want) {
				t.Fatalf("warned %q, want %q", warned, want)
			}
		})
	}
}

// TestListingTreeRefusedElsewhere: GCS and Azure Blob have no folder walk
// yet: --listing tree is refused before any request (exit 2), and an
// unknown listing does not parse.
func TestListingTreeRefusedElsewhere(t *testing.T) {
	for _, root := range []string{"gs://bkt", "az://acct/c"} {
		_, err := openArgs(t, root, false, nil, "--listing", "tree", "--anonymous", "--endpoint-url", "http://127.0.0.1:1")
		if !errors.Is(err, source.ErrUnsupported) || !strings.Contains(err.Error(), "not supported") {
			t.Fatalf("%s: %v", root, err)
		}
	}
	f := factory(t, "s3://bkt")
	fs := pflag.NewFlagSet("t", pflag.ContinueOnError)
	f.RegisterFlags(fs, false)
	if err := fs.Parse([]string{"--listing", "sideways"}); err == nil || !strings.Contains(err.Error(), "auto, range or tree") {
		t.Fatalf("parse: %v", err)
	}
}

// treeKeys are a bucket R2 lists out of byte order.
var treeKeys = []string{"a", "a.jpg", "a.jpg.orig", "a/", "a/0", "a/1/x", "b/", "b/0", "logs", "logs-old/1", "logs/", "logs/2"}

// TestTreeCountMatchesList: swath du and a count of the folder walk see
// what its listing lists: the same objects and bytes.
func TestTreeCountMatchesList(t *testing.T) {
	s3test.NoCredentials(t)
	u := s3test.Serve(t, s3test.NewR2(treeKeys))
	opt := &s3.Options{Endpoint: u, PathStyle: true, Anonymous: true, Region: "us-east-1", Listing: source.ListingTree}
	r, err := inventory.Run(context.Background(), inventory.Config{Source: inventory.SourceConfig{URI: "s3://bkt", Options: opt}})
	if err != nil {
		t.Fatal(err)
	}
	src, err := openS3(context.Background(), registry.Request{Root: "s3://bkt", Options: opt, Workers: 4, Warn: func(string, string) {}})
	if err != nil {
		t.Fatal(err)
	}
	tally := totals.New(totals.Options{Depth: 1})
	if _, _, err := src.Run(context.Background(), registry.Env{Count: true, Totals: tally, Filter: &filter.Spec{}, Order: order.Options{Workers: 4}}); err != nil {
		t.Fatal(err)
	}
	var size int64
	for _, k := range treeKeys {
		size += int64(len(k))
	}
	res := tally.Result()
	if r.Output.RowsEmitted != int64(len(treeKeys)) || len(res) == 0 || res[0].Objects != int64(len(treeKeys)) || res[0].Size != size || src.Listed() != int64(len(treeKeys)) {
		t.Fatalf("listed %d rows, counted %+v, want %d objects, %d bytes", r.Output.RowsEmitted, res, len(treeKeys), size)
	}
}

// TestTreeDatasetNotResumable: a dataset written by the folder walk is
// committed whole, in byte order; the run is told it keeps no journal, and
// swath resume of its directory is refused.
func TestTreeDatasetNotResumable(t *testing.T) {
	s3test.NoCredentials(t)
	u := s3test.Serve(t, s3test.NewR2(treeKeys))
	opt := &s3.Options{Endpoint: u, PathStyle: true, Anonymous: true, Region: "us-east-1", Listing: source.ListingTree}
	dir := filepath.Join(t.TempDir(), "ds")
	var warned []string
	warn := func(code, msg string) { warned = append(warned, code+": "+msg) }
	if _, err := inventory.Run(context.Background(), inventory.Config{Source: inventory.SourceConfig{URI: "s3://bkt", Options: opt},
		Output: inventory.OutputConfig{Dir: dir}, Warn: warn}); err != nil {
		t.Fatal(err)
	}
	if len(warned) != 1 || !strings.Contains(warned[0], "swath resume cannot continue it") {
		t.Fatalf("warned %q", warned)
	}
	d, err := reader.OpenDir(context.Background(), dir, seekparquet.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if id := string(d.Metadata().SchemaID); id != s3.Schema.Info.ID {
		t.Fatalf("schema %s", id)
	}
	_, err = inventory.Run(context.Background(), inventory.Config{Source: inventory.SourceConfig{URI: "s3://bkt", Options: opt},
		Output: inventory.OutputConfig{Dir: dir, Resume: true}, Warn: warn})
	if err == nil || !strings.Contains(err.Error(), "cannot resume") {
		t.Fatalf("resume: %v", err)
	}
}

// TestTreeSourceInterfaces: the folder walk's registered source has the
// live run's interfaces and its own capabilities, and none of the range
// engine's (no resume, seed, reuse, fences or trace).
func TestTreeSourceInterfaces(t *testing.T) {
	s3test.NoCredentials(t)
	src, err := openS3(context.Background(), registry.Request{Root: "s3://bkt", Workers: 2, Warn: func(string, string) {},
		Options: &s3.Options{Endpoint: "http://127.0.0.1:1", Anonymous: true, Region: "us-east-1", Listing: source.ListingTree}})
	if err != nil {
		t.Fatal(err)
	}
	for name, ok := range map[string]bool{
		"Identified": is[source.Identified](src), "Reporter": is[source.Reporter](src), "Live": is[source.Live](src),
		"Advancing": is[source.Advancing](src), "Requested": is[source.Requested](src), "Faulted": is[source.Faulted](src),
		"Priced":        is[source.Priced](src),
		"not Resumable": !is[source.Resumable](src), "not Seeded": !is[source.Seeded](src), "not Traced": !is[source.Traced](src),
		"not Fencing": !is[source.Fencing](src), "not Reusing": !is[source.Reusing](src),
	} {
		if !ok {
			t.Errorf("the tree source: %s does not hold", name)
		}
	}
	if c := src.(source.Capable).Capabilities(); c != (source.Capabilities{Seek: source.SeekTokenOnly, Order: source.OrderNone}) {
		t.Fatalf("capabilities %+v", c)
	}
	if v := identityField(src.(source.Identified).Identity(), "listing"); v != "tree" {
		t.Fatalf("identity listing %q", v)
	}
}

func is[I any](v any) bool { _, ok := v.(I); return ok }
