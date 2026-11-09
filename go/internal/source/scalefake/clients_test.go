// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package scalefake_test

import (
	"context"
	"math/rand/v2"
	"net/http/httptest"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/order"
	"github.com/varveio/swath/go/internal/schema"
	"github.com/varveio/swath/go/internal/source/gcs"
	gcsclient "github.com/varveio/swath/go/internal/source/gcs/client"
	"github.com/varveio/swath/go/internal/source/keyrange"
	"github.com/varveio/swath/go/internal/source/objhttp"
	"github.com/varveio/swath/go/internal/source/s3"
	s3client "github.com/varveio/swath/go/internal/source/s3/client"
	"github.com/varveio/swath/go/internal/source/scalefake"
)

func serve(t testing.TB, s scalefake.Shape, opt scalefake.Options) (*scalefake.Keyspace, *scalefake.Server, string) {
	t.Helper()
	k, err := scalefake.New(s)
	if err != nil {
		t.Fatal(err)
	}
	srv := scalefake.NewServer(k, opt)
	h := httptest.NewServer(srv)
	t.Cleanup(h.Close)
	return k, srv, h.URL
}

func isolateAWS(t *testing.T) {
	for k, v := range map[string]string{"AWS_PROFILE": "", "AWS_EC2_METADATA_DISABLED": "true", "AWS_SESSION_TOKEN": "",
		"AWS_ACCESS_KEY_ID": "", "AWS_SECRET_ACCESS_KEY": "", "AWS_CONFIG_FILE": "/nonexistent", "AWS_SHARED_CREDENTIALS_FILE": "/nonexistent"} {
		t.Setenv(k, v)
	}
}

// wireShape has keys that need percent-encoding nowhere but exercise
// every wire field: delimiters, long shared stems, two levels.
var wireShape = scalefake.Shape{Keys: 8000, Fanout: []int{9, 13}, Skew: 1, SegmentPad: 20, KeyLen: 120, Seed: 21}

// TestS3ClientPagesMatchKeyspace: the S3 client, paging ListObjectsV2
// with start-after, delimiter and max-keys, sees exactly the keyspace's
// answer and its metadata, and every key once.
func TestS3ClientPagesMatchKeyspace(t *testing.T) {
	isolateAWS(t)
	k, srv, url := serve(t, wireShape, scalefake.Options{})
	c, err := s3client.New(context.Background(), s3client.Config{Endpoint: url, PathStyle: true, Anonymous: true, Region: "us-east-1"})
	if err != nil {
		t.Fatal(err)
	}
	r := rand.New(rand.NewPCG(1, 2))
	for n := range 30 {
		in := &s3client.ListInput{Bucket: "bkt", MaxKeys: []int{0, 1, 17, 1000}[r.IntN(4)]}
		if r.IntN(2) == 0 {
			in.StartAfter = k.Key(r.Int64N(k.Len()))[:10+r.IntN(30)]
		}
		if r.IntN(2) == 0 {
			in.Delimiter = "/"
		}
		if r.IntN(3) == 0 {
			in.Prefix = k.Key(r.Int64N(k.Len()))[:22]
		}
		if in.MaxKeys > 0 && in.MaxKeys < 100 && in.Delimiter == "" { // small pages only over prefixes
			in.MaxKeys = 1000
		}
		q := scalefake.Query{Prefix: in.Prefix, Delimiter: in.Delimiter, After: in.StartAfter, Max: in.MaxKeys}
		var want []scalefake.Entry
		for {
			p, err := k.List(q)
			if err != nil {
				t.Fatal(err)
			}
			want = append(want, p.Entries...)
			if q.Token = p.Next; q.Token == "" {
				break
			}
		}
		var got []scalefake.Entry
		for {
			p, err := c.ListObjectsV2(context.Background(), in)
			if err != nil {
				t.Fatal(err)
			}
			for _, o := range p.Contents {
				i := k.Search(o.Key, false, 0)
				m := k.Meta(i)
				if o.Size != m.Size || o.Mtime != m.Mtime*1e6 || o.ETag != `"`+m.ETag+`"` || o.StorageClass != "STANDARD" {
					t.Fatalf("object %+v, want %+v", o, m)
				}
				got = append(got, scalefake.Entry{Name: o.Key, Index: i})
			}
			for _, cp := range p.CommonPrefixes { // S3 puts prefixes after keys; a delimiter page here has only prefixes
				got = append(got, scalefake.Entry{Name: cp, Index: -1, Prefix: true})
			}
			if !p.IsTruncated {
				break
			}
			in.ContinuationToken = p.NextContinuationToken
		}
		if !slices.Equal(got, want) {
			t.Fatalf("query %d %+v: %d entries, want %d", n, in, len(got), len(want))
		}
	}
	if st := srv.Stats(false); st.Pages != st.Requests || st.Keys == 0 || st.Inflight != 0 || st.PeakInflight != 1 {
		t.Fatalf("stats %+v", st)
	}
}

// TestGCSClientPagesMatchKeyspace: the same for objects.list, with
// startOffset (inclusive), endOffset and the GCS roll-up rule.
func TestGCSClientPagesMatchKeyspace(t *testing.T) {
	k, _, url := serve(t, wireShape, scalefake.Options{})
	c, err := gcsclient.New(context.Background(), gcsclient.Config{Endpoint: url + "/storage/v1/", Anonymous: true})
	if err != nil {
		t.Fatal(err)
	}
	r := rand.New(rand.NewPCG(3, 4))
	for n := range 30 {
		in := &gcsclient.ListInput{Bucket: "bkt", MaxResults: []int{0, 3, 1000}[r.IntN(3)]}
		if r.IntN(2) == 0 {
			in.StartOffset = k.Key(r.Int64N(k.Len()))[:10+r.IntN(30)]
		}
		if r.IntN(3) == 0 {
			in.EndOffset = k.Key(r.Int64N(k.Len()))
		}
		if r.IntN(2) == 0 {
			in.Delimiter = "/"
		}
		if in.MaxResults == 3 && in.Delimiter == "" { // small pages only over prefixes
			in.MaxResults = 1000
		}
		q := scalefake.Query{Prefix: in.Prefix, Delimiter: in.Delimiter, After: in.StartOffset, Inclusive: true,
			End: in.EndOffset, Max: in.MaxResults, RollUpBelow: true}
		var want []string
		for {
			p, err := k.List(q)
			if err != nil {
				t.Fatal(err)
			}
			for _, e := range p.Entries {
				want = append(want, e.Name)
			}
			if q.Token = p.Next; q.Token == "" {
				break
			}
		}
		var got []string
		for {
			p, err := c.List(context.Background(), in)
			if err != nil {
				t.Fatal(err)
			}
			for _, o := range p.Items {
				m := k.Meta(k.Search(o.Name, false, 0))
				if o.Size != m.Size || o.Updated != m.Mtime*1e6 || o.Metageneration != 1 {
					t.Fatalf("object %+v, want %+v", o, m)
				}
				got = append(got, o.Name)
			}
			got = append(got, p.Prefixes...)
			if p.NextPageToken == "" {
				break
			}
			in.PageToken = p.NextPageToken
		}
		if !slices.Equal(got, want) {
			t.Fatalf("query %d %+v: %d entries, want %d", n, in, len(got), len(want))
		}
	}
}

// checker is a sink that keeps nothing: it checks the rows are strictly
// ascending keys of the keyspace and counts them.
type checker[P any] struct {
	k    *scalefake.Keyspace
	mu   sync.Mutex
	n    int64
	last string
	bad  string
}

func (c *checker[P]) Write(_ context.Context, segs [][]schema.Row[P]) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, s := range segs {
		for _, r := range s {
			if c.bad == "" && (r.Key <= c.last && c.n > 0 || (c.n%997 == 0 && c.k.Key(c.n) != r.Key)) {
				c.bad = "row " + strconv.FormatInt(c.n, 10) + ": " + r.Key + " after " + c.last
			}
			c.last = r.Key
			c.n++
		}
	}
	return nil
}

func (c *checker[P]) check(t testing.TB, what string) {
	t.Helper()
	if c.bad != "" || c.n != c.k.Len() || (c.n > 0 && c.last != c.k.Key(c.n-1)) {
		t.Fatalf("%s: %d rows of %d (%s)", what, c.n, c.k.Len(), c.bad)
	}
}

// scaleKeys is the engine tests' keyspace size: SCALEFAKE_KEYS (say
// 100000000 for the scale tier by hand), else small enough for -race.
func scaleKeys(t testing.TB, def int64) int64 {
	if v := os.Getenv("SCALEFAKE_KEYS"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	if testing.Short() {
		return def / 10
	}
	return def
}

// TestS3EngineOverLazyKeyspace: the S3 provider on the range engine, 32
// workers with the seed and the limiter, lists a skewed deep keyspace
// through latency and 2 % 503s exactly, in order, without a dataset.
func TestS3EngineOverLazyKeyspace(t *testing.T) {
	isolateAWS(t)
	s := scalefake.Shape{Keys: scaleKeys(t, 150_000), Fanout: []int{16, 16, 8}, Skew: 1.5, Seed: 31}
	k, srv, url := serve(t, s, scalefake.Options{Latency: time.Millisecond, Jitter: 2 * time.Millisecond, Fail503: 0.02, FaultSeed: 5})
	const workers = 32
	p, err := s3.New(context.Background(), s3.Options{Bucket: "bkt", Endpoint: url, PathStyle: true, Anonymous: true,
		Region: "us-east-1", Workers: workers, Policy: keyrange.Policy{Seed: true}, Admission: objhttp.Admission{Limiter: true}})
	if err != nil {
		t.Fatal(err)
	}
	c := &checker[s3.Object]{k: k}
	if _, err := order.Run[s3.Range, s3.Object](context.Background(), p, c, &s3.Schema, order.Options{Workers: workers, Budget: 64 << 20}); err != nil {
		t.Fatal(err)
	}
	c.check(t, "s3")
	st := srv.Stats(false)
	t.Logf("%s: %d requests, %d pages, %d keys served (%.3f per key listed), %d throttled, peak inflight %d, %.2f requests per 1k keys",
		s, st.Requests, st.Pages, st.Keys, float64(st.Keys)/float64(k.Len()), st.Throttled, st.PeakInflight, 1000*float64(st.Requests)/float64(k.Len()))
	if st.Throttled == 0 || st.Keys < k.Len() {
		t.Fatalf("stats %+v", st)
	}
}

// TestGCSEngineOverLazyKeyspace: the same through the GCS provider, with
// 1 KiB keys under long shared prefixes.
func TestGCSEngineOverLazyKeyspace(t *testing.T) {
	s := scalefake.Shape{Keys: scaleKeys(t, 30_000), Fanout: []int{8, 8}, SharedPrefix: 64, SegmentPad: 32, KeyLen: 1024, Seed: 32}
	k, srv, url := serve(t, s, scalefake.Options{Latency: time.Millisecond, Fail503: 0.02, FaultSeed: 6})
	const workers = 16
	p, err := gcs.New(context.Background(), gcs.Options{Bucket: "bkt", Endpoint: url + "/storage/v1/", Anonymous: true,
		Workers: workers, Policy: keyrange.Policy{Seed: true}, Admission: objhttp.Admission{Limiter: true}})
	if err != nil {
		t.Fatal(err)
	}
	c := &checker[gcs.Object]{k: k}
	if _, err := order.Run[gcs.Range, gcs.Object](context.Background(), p, c, &gcs.Schema, order.Options{Workers: workers, Budget: 64 << 20}); err != nil {
		t.Fatal(err)
	}
	c.check(t, "gcs")
	if st := srv.Stats(false); st.Throttled == 0 || !strings.Contains(st.Shape, "key-len=1024") {
		t.Fatalf("stats %+v", st)
	}
}
