// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package source_test

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/varveio/swath/go/internal/order"
	"github.com/varveio/swath/go/internal/registry"
	"github.com/varveio/swath/go/internal/source"
	"github.com/varveio/swath/go/internal/source/azblob"
	"github.com/varveio/swath/go/internal/source/azblob/azblobtest"
	"github.com/varveio/swath/go/internal/source/gcs"
	gcsclient "github.com/varveio/swath/go/internal/source/gcs/client"
	"github.com/varveio/swath/go/internal/source/gcs/gcstest"
	"github.com/varveio/swath/go/internal/source/keyrange"
	"github.com/varveio/swath/go/internal/source/s3"
	s3client "github.com/varveio/swath/go/internal/source/s3/client"
	"github.com/varveio/swath/go/internal/source/s3/s3test"
	"github.com/varveio/swath/go/internal/textout"
)

// routedS3 is a supplied S3 client that names its route (s3.Routed);
// unroutedS3 and suppliedGCS are supplied clients that count nothing.
// None is asked anything here.
type (
	routedS3    struct{ s3.API }
	unroutedS3  struct{ s3.API }
	suppliedGCS struct{ gcs.API }
)

func (routedS3) Route(bucket string) string { return "http://store.example/" + bucket }

// TestRegisteredSources opens each object-store provider as the command
// does, through registry.Lookup(root).Open, and checks the registered
// source: every optional interface the pipeline probes for is present
// (an absent one silently turns its capability off), the capabilities are
// the provider's (sequential, capped and supplied-client runs included),
// and a supplied client that counts nothing reports zero activity.
func TestRegisteredSources(t *testing.T) {
	s3test.NoCredentials(t)
	t.Setenv("AWS_REGION", "us-east-1")
	s3ep := s3test.Bucket(t, s3test.Keys(20, 3))
	gcsep := gcstest.Bucket(t, gcstest.Keys(20), 0)
	az := azblobtest.Serve(t, azblobtest.Keys()[:20])
	azRoot := "az://" + az.Account + "/" + az.Container
	azOptions := func(o azblob.Options) *azblob.Options {
		o.Endpoint, o.Anonymous = az.Endpoint[:len(az.Endpoint)-len("/"+az.Account)], true
		return &o
	}
	parallel := source.Capabilities{Resume: true, Seek: source.SeekFabricable, Order: source.OrderBytes}
	sequential := source.Capabilities{Resume: true, Seek: source.SeekTokenOnly, Order: source.OrderBytes}
	noResume := func(c source.Capabilities) source.Capabilities { c.Resume = false; return c }
	for _, c := range []struct {
		name    string
		root    string
		options any
		caps    source.Capabilities
		priced  bool
		counted bool // the built client: its activity is live
	}{
		{"s3", "s3://bkt", &s3.Options{Endpoint: s3ep, Anonymous: true}, parallel, true, true},
		{"s3 versions", "s3://bkt", &s3.Options{Endpoint: s3ep, Anonymous: true, AllVersions: true}, parallel, true, true},
		{"s3 sequential", "s3://bkt", &s3.Options{Endpoint: s3ep, Anonymous: true, Policy: keyrange.Policy{Sequential: true}}, sequential, true, true},
		{"s3 capped", "s3://bkt", &s3.Options{Endpoint: s3ep, Anonymous: true, Policy: keyrange.Policy{Sequential: true, MaxKeys: 5}}, noResume(sequential), true, true},
		{"s3 routed client", "s3://bkt", &s3.Options{Client: routedS3{}}, parallel, true, false},
		{"s3 unrouted client", "s3://bkt", &s3.Options{Client: unroutedS3{}}, noResume(parallel), true, false},
		{"gcs", "gs://bkt", &gcs.Options{Endpoint: gcsep, Anonymous: true}, parallel, true, true},
		{"gcs generations", "gs://bkt", &gcs.Options{Endpoint: gcsep, Anonymous: true, AllVersions: true}, parallel, true, true},
		{"gcs sequential", "gs://bkt", &gcs.Options{Endpoint: gcsep, Anonymous: true, Policy: keyrange.Policy{Sequential: true}}, sequential, true, true},
		{"gcs capped", "gs://bkt", &gcs.Options{Endpoint: gcsep, Anonymous: true, Policy: keyrange.Policy{Sequential: true, MaxKeys: 5}}, noResume(sequential), true, true},
		{"gcs supplied client", "gs://bkt", &gcs.Options{Client: suppliedGCS{}}, noResume(parallel), true, false},
		{"azblob", azRoot, azOptions(azblob.Options{}), parallel, false, true},
		{"azblob sequential", azRoot, azOptions(azblob.Options{Policy: keyrange.Policy{Sequential: true}}), sequential, false, true},
		{"azblob capped", azRoot, azOptions(azblob.Options{Policy: keyrange.Policy{Sequential: true, MaxKeys: 5}}), noResume(sequential), false, true},
		{"azblob supplied client", azRoot, &azblob.Options{Client: azblobtest.NewStore(nil)}, noResume(parallel), false, false},
	} {
		f, err := registry.Lookup(c.root)
		if err != nil {
			t.Fatal(err)
		}
		src, err := f.Open(context.Background(), registry.Request{Root: c.root, Options: c.options, Workers: 2, Warn: func(string) {}})
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		missing := func(name string, ok bool) {
			if !ok {
				t.Errorf("%s: the registered source is not %s", c.name, name)
			}
		}
		_, ok := src.(source.Identified)
		missing("Identified", ok)
		_, ok = src.(source.Reporter)
		missing("Reporter", ok)
		_, ok = src.(source.Live)
		missing("Live", ok)
		_, ok = src.(source.Advancing)
		missing("Advancing", ok)
		_, ok = src.(source.SeedTimed)
		missing("SeedTimed", ok)
		_, ok = src.(source.Contended)
		missing("Contended", ok)
		_, ok = src.(source.Resumable)
		missing("Resumable", ok)
		_, ok = src.(source.Seeded)
		missing("Seeded", ok)
		_, ok = src.(source.Reusing)
		missing("Reusing", ok)
		_, ok = src.(source.Fencing)
		missing("Fencing", ok)
		if _, ok := src.(source.Priced); ok != c.priced {
			t.Errorf("%s: Priced %v, want %v", c.name, ok, c.priced)
		}
		if cp, ok := src.(source.Capable); !ok {
			t.Errorf("%s: the registered source is not Capable", c.name)
		} else if got := cp.Capabilities(); got != c.caps {
			t.Errorf("%s: capabilities %+v, want %+v", c.name, got, c.caps)
		}
		rq, okRq := src.(source.Requested)
		missing("Requested", okRq)
		fa, okFa := src.(source.Faulted)
		missing("Faulted", okFa)
		if !c.counted && okRq && okFa {
			th, to := fa.Faults()
			if rq.Requests() != (source.Requests{}) || th != 0 || to != 0 || src.(source.Live).Answered() != 0 {
				t.Errorf("%s: a supplied client that counts nothing reports activity", c.name)
			}
		}
	}
}

// TestRegisteredRestart checks observable restart through source.Resumable:
// complete keys stay excluded, remaining groups retain their entries in order,
// and built-client activity still reaches the registered source.
func TestRegisteredRestart(t *testing.T) {
	s3test.NoCredentials(t)
	t.Setenv("AWS_REGION", "us-east-1")
	s3keys, gcsKeys, azKeys := s3test.Keys(40, 3), gcstest.Keys(40), azblobtest.Keys()[:40]
	az := azblobtest.Serve(t, azKeys)
	grouped := []string{"a", "b", "c", "d", "e", "f"}
	for _, c := range []struct {
		name, root      string
		options         any
		keys            []string
		groups, counted bool
	}{
		{"s3", "s3://bkt", &s3.Options{Endpoint: s3test.Bucket(t, s3keys), Anonymous: true}, s3keys, false, true},
		{"gcs", "gs://bkt", &gcs.Options{Endpoint: gcstest.Bucket(t, gcsKeys, 0), Anonymous: true}, gcsKeys, false, true},
		{"azblob", "az://" + az.Account + "/" + az.Container, &azblob.Options{Endpoint: strings.TrimSuffix(az.Endpoint, "/"+az.Account), Anonymous: true}, azKeys, false, true},
		{"s3 versions", "s3://bkt", &s3.Options{Client: restartVersions{}, AllVersions: true, Policy: keyrange.Policy{Sequential: true, PageSize: 2}}, grouped, true, false},
		{"gcs generations", "gs://bkt", &gcs.Options{Client: restartGenerations{}, AllVersions: true, Policy: keyrange.Policy{Sequential: true, PageSize: 2}}, grouped, true, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			f, err := registry.Lookup(c.root)
			if err != nil {
				t.Fatal(err)
			}
			src, err := f.Open(context.Background(), registry.Request{Root: c.root, Options: c.options, Workers: 2, Warn: func(string) {}})
			if err != nil {
				t.Fatal(err)
			}
			keys := slices.Sorted(slices.Values(c.keys))
			after := keys[len(keys)/2]
			if err := src.(source.Resumable).Restart(after); err != nil {
				t.Fatal(err)
			}
			var buf bytes.Buffer
			out, err := textout.Open(textout.Config{Format: textout.JSONL, Stdout: &buf})
			if err != nil {
				t.Fatal(err)
			}
			defer out.Abort()
			st, _, err := src.Run(context.Background(), registry.Env{Text: out, Order: order.Options{Workers: 2, Budget: 1 << 20}})
			if err != nil {
				t.Fatal(err)
			}
			if err := out.Commit(); err != nil {
				t.Fatal(err)
			}
			type row struct{ Key, VersionId string }
			var got, want []row
			d := json.NewDecoder(&buf)
			for d.More() {
				var r struct {
					Key        string `json:"key"`
					VersionID  string `json:"version_id"`
					Generation int64  `json:"generation"`
				}
				if err := d.Decode(&r); err != nil {
					t.Fatal(err)
				}
				id := r.VersionID
				if c.groups && r.Generation != 0 {
					id = strconv.FormatInt(r.Generation, 10)
				}
				got = append(got, row{r.Key, id})
			}
			for _, k := range keys[len(keys)/2+1:] {
				if c.groups {
					for _, id := range []string{"1", "2", "3"} {
						want = append(want, row{k, id})
					}
				} else {
					want = append(want, row{Key: k})
				}
			}
			if !slices.Equal(got, want) || st.Rows != int64(len(want)) || src.Listed() != int64(len(want)) {
				t.Fatalf("after %q: rows=%+v want=%+v stats=%+v listed=%d", after, got, want, st, src.Listed())
			}
			if c.groups && st.DistinctKeys != int64(len(keys)-len(keys)/2-1) {
				t.Fatalf("groups=%d stats=%+v", st.DistinctKeys, st)
			}
			if c.counted && (src.(source.Live).Answered() == 0 || src.(source.Requested).Requests().Attempts == 0) {
				t.Error("client activity does not reach the registered source")
			}
		})
	}
}

// Each remaining key has three entries, so pages of two split groups.
type restartVersions struct{ s3.API }

func (restartVersions) ListObjectVersions(_ context.Context, in *s3client.VersionsInput) (*s3client.VersionPage, error) {
	var rows []s3client.Version
	for _, key := range []string{"a", "b", "c", "d", "e", "f"} {
		for _, id := range []string{"1", "2", "3"} {
			if key > in.KeyMarker || key == in.KeyMarker && in.VersionIDMarker != "" && id > in.VersionIDMarker {
				rows = append(rows, s3client.Version{Object: s3client.Object{Key: key}, VersionID: id, DeleteMarker: id == "2"})
			}
		}
	}
	out := &s3client.VersionPage{Versions: rows}
	if len(rows) > in.MaxKeys {
		out.Versions, out.IsTruncated = rows[:in.MaxKeys], true
		last := out.Versions[len(out.Versions)-1]
		out.NextKeyMarker, out.NextVersionIDMarker = last.Key, last.VersionID
	}
	return out, nil
}

type restartGenerations struct{}

func (restartGenerations) List(_ context.Context, in *gcsclient.ListInput) (*gcsclient.Page, error) {
	var rows []gcsclient.Object
	for _, key := range []string{"a", "b", "c", "d", "e", "f"} {
		if key >= in.StartOffset && (in.EndOffset == "" || key < in.EndOffset) {
			for id := int64(1); id <= 3; id++ {
				rows = append(rows, gcsclient.Object{Name: key, Generation: id})
			}
		}
	}
	start := 0
	if in.PageToken != "" {
		start, _ = strconv.Atoi(in.PageToken)
	}
	end := min(len(rows), start+in.MaxResults)
	out := &gcsclient.Page{Items: rows[start:end]}
	if end < len(rows) {
		out.NextPageToken = strconv.Itoa(end)
	}
	return out, nil
}

func TestRegisteredLocators(t *testing.T) {
	s3test.NoCredentials(t)
	t.Setenv("AWS_REGION", "us-east-1")
	az := azblobtest.Serve(t, []string{"a"})
	const prefix = "literal%2F+?x#frag/&=x"
	for _, c := range []struct {
		root, kind, container, endpoint string
		options                         any
	}{
		{"s3://bkt/" + prefix, "s3", "bkt", s3test.Bucket(t, nil), &s3.Options{Anonymous: true}},
		{"gs://bkt/" + prefix, "gcs", "bkt", gcstest.Bucket(t, nil, 0), &gcs.Options{Anonymous: true}},
		{"az://" + az.Account + "/" + az.Container + "/" + prefix, "azblob", az.Container, strings.TrimSuffix(az.Endpoint, "/"+az.Account), &azblob.Options{Client: azblobtest.NewStore(nil)}},
	} {
		t.Run(c.kind, func(t *testing.T) {
			f, err := registry.Lookup(c.root)
			if err != nil {
				t.Fatal(err)
			}
			fs := flag.NewFlagSet("test", flag.ContinueOnError)
			flagged := f.RegisterFlags(fs, false)
			if err := fs.Parse([]string{"--anonymous", "--endpoint", c.endpoint}); err != nil {
				t.Fatal(err)
			}
			for _, opt := range []any{c.options, flagged} {
				src, err := f.Open(context.Background(), registry.Request{Root: c.root, Options: opt, Workers: 2, Warn: func(string) {}})
				if err != nil {
					t.Fatal(err)
				}
				want := source.Locator{Kind: c.kind, Root: c.root, Container: c.container}
				if got := src.Locator(); got != want {
					t.Errorf("locator=%+v want=%+v", got, want)
				}
			}
			if got, err := f.KeyPrefix(c.root); err != nil || got != prefix {
				t.Errorf("prefix=%q err=%v", got, err)
			}
			if f.WithPrefix != nil {
				const replacement = "new/%2F+?q#f&=value"
				root := f.WithPrefix(c.root, replacement)
				if want := strings.TrimSuffix(c.root, prefix) + replacement; root != want {
					t.Errorf("root=%q want=%q", root, want)
				}
				if got, err := f.KeyPrefix(root); err != nil || got != replacement {
					t.Errorf("new prefix=%q err=%v", got, err)
				}
				for _, invalid := range []string{"invalid", strings.Split(c.root, "://")[0] + ":///prefix"} {
					if got := f.WithPrefix(invalid, replacement); got != invalid {
						t.Errorf("invalid root=%q changed to %q", invalid, got)
					}
					if _, err := f.KeyPrefix(invalid); err == nil {
						t.Errorf("invalid root=%q accepted", invalid)
					}
				}
			}
		})
	}
	sasPrefix := "literal%2F+#frag/&=x"
	root := "az://acct/c/" + sasPrefix + "?sv=2026-01-01&sig=secret"
	f, err := registry.Lookup(root)
	if err != nil {
		t.Fatal(err)
	}
	src, err := f.Open(context.Background(), registry.Request{Root: root, Options: &azblob.Options{Client: azblobtest.NewStore(nil)}, Workers: 1, Warn: func(string) {}})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := src.Locator(), (source.Locator{Kind: "azblob", Root: "az://acct/c/" + sasPrefix, Container: "c"}); got != want {
		t.Errorf("SAS locator=%+v want=%+v", got, want)
	}
	if got, want := registry.Redact(root), "az://acct/c/"+sasPrefix+"?"+azblob.RedactedSAS; got != want {
		t.Errorf("root redaction=%q want=%q", got, want)
	}
}

func TestRegisteredEndpointRefusals(t *testing.T) {
	s3test.NoCredentials(t)
	az := azblobtest.Serve(t, []string{"a"})
	for _, c := range []struct {
		root, endpoint string
		options        func(string) any
	}{
		{"s3://bkt", s3test.Bucket(t, []string{"a"}), func(ep string) any { return &s3.Options{Anonymous: true, Endpoint: ep} }},
		{"gs://bkt", gcstest.Bucket(t, []string{"a"}, 0), func(ep string) any { return &gcs.Options{Anonymous: true, Endpoint: ep} }},
		{"az://" + az.Account + "/" + az.Container, strings.TrimSuffix(az.Endpoint, "/"+az.Account), func(ep string) any { return &azblob.Options{Anonymous: true, Endpoint: ep} }},
	} {
		f, err := registry.Lookup(c.root)
		if err != nil {
			t.Fatal(err)
		}
		live := strings.Replace(c.endpoint, "http://", "http://user:s3cret@", 1)
		for _, ep := range []string{live, "http://user:s3cret@127.0.0.1:1", "https://user:s3cret@store.example/base/", "http://:s3cret@h", "http://[broken", "not-a-url", "ftp://store.example"} {
			t.Run(c.root+"/"+ep, func(t *testing.T) {
				fs := flag.NewFlagSet("test", flag.ContinueOnError)
				flagged := f.RegisterFlags(fs, false)
				if err := fs.Parse([]string{"--anonymous", "--endpoint", ep}); err != nil {
					t.Fatal(err)
				}
				for _, opt := range []any{c.options(ep), flagged} {
					_, err := f.Open(context.Background(), registry.Request{Root: c.root, Options: opt, Workers: 1, Warn: func(string) {}})
					if err == nil || strings.Contains(err.Error(), "s3cret") {
						t.Errorf("endpoint refusal: %v", err)
					}
				}
			})
		}
	}
}

func TestRegisteredPoolSizeReported(t *testing.T) {
	az := azblobtest.Serve(t, []string{"a", "b"})
	for _, c := range []struct {
		root    string
		options any
	}{
		{"gs://bkt", &gcs.Options{Anonymous: true, Endpoint: gcstest.Bucket(t, []string{"a", "b"}, 0)}},
		{"az://" + az.Account + "/" + az.Container, &azblob.Options{Anonymous: true, Endpoint: strings.TrimSuffix(az.Endpoint, "/"+az.Account)}},
	} {
		f, err := registry.Lookup(c.root)
		if err != nil {
			t.Fatal(err)
		}
		for workers, want := range map[int]int64{1: 1, 40: 3} {
			src, err := f.Open(context.Background(), registry.Request{Root: c.root, Options: c.options, Workers: workers, Warn: func(string) {}})
			if err != nil {
				t.Fatal(err)
			}
			st, _, err := src.Run(context.Background(), registry.Env{Count: true, Order: order.Options{Workers: workers}})
			if err != nil || st.Rows != 2 {
				t.Fatalf("%s: stats=%+v err=%v", c.root, st, err)
			}
			encoded, err := json.Marshal(src.(source.Reporter).Report().Stats)
			if err != nil {
				t.Fatal(err)
			}
			var report struct {
				Counters struct{ Values map[string]int64 }
			}
			if err := json.Unmarshal(encoded, &report); err != nil {
				t.Fatal(err)
			}
			if got := report.Counters.Values["transport.pool_size"]; got != want || report.Counters.Values["attempts"] == 0 {
				t.Errorf("%s workers=%d: pool=%d want=%d attempts=%d", c.root, workers, got, want, report.Counters.Values["attempts"])
			}
		}
	}
}
