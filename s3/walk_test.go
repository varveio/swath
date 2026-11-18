// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/varveio/swath"
	"github.com/varveio/swath/internal/source/s3/s3test"
	"github.com/varveio/swath/objfile"
	"github.com/varveio/swath/reader/listing"
)

// served counts the list requests a store is sent, and those that start a
// range: a start-after and no continuation token.
type served struct {
	requests atomic.Int64
	starts   atomic.Int64
}

// serveKeys serves gofakes3's bucket s3test.Name holding keys, each
// object's size its key's length, every answer delayed by delay.
func serveKeys(t *testing.T, keys []string, delay time.Duration) (string, *served) {
	t.Helper()
	s := &served{}
	h := s3test.Handler(s3test.Backend(t, keys))
	if delay > 0 {
		h = s3test.Delayed(h, delay)
	}
	return s3test.Serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if q := r.URL.Query(); q.Get("list-type") == "2" {
			s.requests.Add(1)
			if q.Get("start-after") != "" && q.Get("continuation-token") == "" {
				s.starts.Add(1)
			}
		}
		h.ServeHTTP(w, r)
	})), s
}

// walkKeys are 10,000 objects in 20 folders, and an object at exactly one
// folder's prefix, "p03/".
func walkKeys() []string {
	keys := append(s3test.Keys(10_000, 20), "p03/")
	slices.Sort(keys)
	return keys
}

// under is the keys that begin with prefix.
func under(keys []string, prefix string) []string {
	var out []string
	for _, k := range keys {
		if strings.HasPrefix(k, prefix) {
			out = append(out, k)
		}
	}
	return out
}

// walked is what a walk handed emit: its keys, in the order emitted, and
// how many places it could not read.
type walked struct {
	keys   []string
	unread int
}

// walk walks src with req, keeping what it emits and checking each entry
// as gofakes3 serves it: an object whose size is its key's length.
func walk(t *testing.T, ctx context.Context, src *swath.Source, req listing.WalkRequest) (walked, listing.WalkSummary, error) {
	t.Helper()
	var w walked
	sum, err := src.Walk(ctx, req, func(b listing.WalkBatch) error {
		for _, e := range b.Entries {
			if e.Kind != listing.KindObject || e.Size == nil || *e.Size != int64(len(e.Key)) || e.LastModified == nil {
				return fmt.Errorf("entry %q: kind %v, size %v", e.Key, e.Kind, e.Size)
			}
			w.keys = append(w.keys, string(e.Key))
		}
		w.unread += len(b.Unread)
		return nil
	})
	return w, sum, err
}

// TestWalkEachEntryOnce: a walk hands emit every object under its prefix
// once, the object at exactly the prefix included, through many ranges
// at eight workers and through one request chain at one; it completes
// with nothing unread, and every request it sends reaches the observer on
// its context.
func TestWalkEachEntryOnce(t *testing.T) {
	s3test.NoCredentials(t)
	keys := walkKeys()
	endpoint, srv := serveKeys(t, keys, time.Millisecond)
	src := open(t, endpoint, s3test.Name)

	for _, c := range []struct {
		prefix  string
		workers int
	}{{"", 8}, {"", 1}, {"p03/", 8}, {"p0", 4}, {"none/", 8}} {
		srv.requests.Store(0)
		srv.starts.Store(0)
		obs := &observed{}
		ctx := objfile.WithObserver(context.Background(), obs)
		w, sum, err := walk(t, ctx, src, listing.WalkRequest{Prefix: []byte(c.prefix), Workers: c.workers})
		if err != nil {
			t.Fatalf("%q at %d: %v", c.prefix, c.workers, err)
		}

		slices.Sort(w.keys)
		if want := under(keys, c.prefix); !slices.Equal(w.keys, want) {
			t.Fatalf("%q at %d: %d keys, want %d", c.prefix, c.workers, len(w.keys), len(want))
		}
		ev := sum.Evidence
		if sum.Entries != int64(len(w.keys)) || w.unread != 0 || ev.Stop != listing.StopCompleted ||
			ev.Unlocated == nil || *ev.Unlocated != 0 || ev.Started == nil || ev.Finished == nil {
			t.Errorf("%q at %d: %+v, unread %v, evidence %+v", c.prefix, c.workers, sum, w.unread, ev)
		}
		switch starts := srv.starts.Load(); {
		case c.workers == 1 && starts != 0:
			t.Errorf("sequential: %d ranges started after a key", starts)
		case c.prefix == "" && c.workers > 1 && starts < 4:
			t.Errorf("%q at %d: %d ranges started after a key, want a walk spread over several", c.prefix, c.workers, starts)
		}
		if n := int64(len(obs.seen)); n != srv.requests.Load() {
			t.Errorf("%q at %d: the observer saw %d requests, the store %d", c.prefix, c.workers, n, srv.requests.Load())
		}
	}
}

// TestWalkStopsAtEmitError: an error from emit is returned unchanged; the
// walk sends few requests after it, none once Walk has returned, and emits
// nothing more.
func TestWalkStopsAtEmitError(t *testing.T) {
	s3test.NoCredentials(t)
	endpoint, srv := serveKeys(t, walkKeys(), 5*time.Millisecond)
	src := open(t, endpoint, s3test.Name)

	failed := errors.New("the caller's own failure")
	var returned atomic.Bool
	var after atomic.Int64
	batches := 0
	_, err := src.Walk(context.Background(), listing.WalkRequest{Workers: 8}, func(listing.WalkBatch) error {
		if returned.Load() {
			after.Add(1)
		}
		batches++
		return failed
	})
	returned.Store(true)
	if err != failed {
		t.Fatalf("err %v, want the emit's own", err)
	}
	sent := srv.requests.Load()
	time.Sleep(50 * time.Millisecond)
	if batches != 1 || after.Load() != 0 || srv.requests.Load() != sent || sent > 16 {
		t.Errorf("%d batches, %d after Walk returned; %d requests, %d after", batches, after.Load(), sent, srv.requests.Load()-sent)
	}
}

// TestWalkCancelled: a walk whose context ends returns the context's
// error, with a summary that counts what emit took and says the walk did
// not finish, and emits nothing more.
func TestWalkCancelled(t *testing.T) {
	s3test.NoCredentials(t)
	endpoint, _ := serveKeys(t, walkKeys(), 5*time.Millisecond)
	src := open(t, endpoint, s3test.Name)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var returned atomic.Bool
	var after atomic.Int64
	took := int64(0)
	sum, err := src.Walk(ctx, listing.WalkRequest{Workers: 8}, func(b listing.WalkBatch) error {
		if returned.Load() {
			after.Add(1)
		}
		took += int64(len(b.Entries))
		cancel()
		return nil
	})
	returned.Store(true)
	time.Sleep(20 * time.Millisecond)
	if !errors.Is(err, context.Canceled) || after.Load() != 0 || sum.Entries != took || sum.Evidence.Stop == listing.StopCompleted {
		t.Fatalf("err %v, %d batches after Walk returned, %+v", err, after.Load(), sum)
	}
}

// TestWalkConcurrent: walks of one source at once each emit their own
// prefix's keys, once.
func TestWalkConcurrent(t *testing.T) {
	s3test.NoCredentials(t)
	keys := walkKeys()
	endpoint, _ := serveKeys(t, keys, time.Millisecond)
	src := open(t, endpoint, s3test.Name)

	prefixes := []string{"", "p0", "p1", ""}
	got := make([][]string, len(prefixes))
	errs := make([]error, len(prefixes))
	var wg sync.WaitGroup
	for i, p := range prefixes {
		wg.Go(func() {
			var w walked
			w, _, errs[i] = walk(t, context.Background(), src, listing.WalkRequest{Prefix: []byte(p), Workers: 6})
			got[i] = w.keys
		})
	}
	wg.Wait()
	for i, p := range prefixes {
		slices.Sort(got[i])
		if want := under(keys, p); errs[i] != nil || !slices.Equal(got[i], want) {
			t.Errorf("%q: %d keys, want %d, %v", p, len(got[i]), len(want), errs[i])
		}
	}
}

// TestWalkRefuses: a request of the wrong form is refused before any
// request.
func TestWalkRefuses(t *testing.T) {
	s3test.NoCredentials(t)
	endpoint, srv := serveKeys(t, []string{"a"}, 0)
	src := open(t, endpoint, s3test.Name)
	emit := func(listing.WalkBatch) error { return nil }
	for name, c := range map[string]struct {
		req  listing.WalkRequest
		emit func(listing.WalkBatch) error
		want error
	}{
		"no emit":   {listing.WalkRequest{}, nil, listing.ErrInvalidRequest},
		"a NUL":     {listing.WalkRequest{Prefix: []byte("a\x00")}, emit, listing.ErrInvalidRequest},
		"not UTF-8": {listing.WalkRequest{Prefix: []byte{0xff}}, emit, listing.ErrInvalidRequest},
		"workers":   {listing.WalkRequest{Workers: -1}, emit, listing.ErrInvalidRequest},
	} {
		if _, err := src.Walk(context.Background(), c.req, c.emit); !errors.Is(err, c.want) {
			t.Errorf("%s: %v, want %v", name, err, c.want)
		}
	}
	if _, _, err := src.Summarize(context.Background(), []listing.Target{{Key: nil}}, listing.SummaryOptions{}); !errors.Is(err, listing.ErrInvalidRequest) {
		t.Errorf("summarize an empty exact key: %v", err)
	}
	if n := srv.requests.Load(); n != 0 {
		t.Errorf("%d requests", n)
	}
}

// truth is what the targets of keys hold: each subtree's or exact key's
// count and bytes, the size of each object its key's length.
func truth(keys []string, t listing.Target) (n, size int64) {
	for _, k := range keys {
		if k == string(t.Key) || t.Subtree && strings.HasPrefix(k, string(t.Key)) {
			n++
			size += int64(len(k))
		}
	}
	return n, size
}

// TestSummarize: nested, repeated and disjoint subtree targets are each
// counted in full, each by a walk of its own; each says what is at its own
// key; an exact target is its own entry (gofakes3's HEAD answer names no
// time), or absent.
func TestSummarize(t *testing.T) {
	s3test.NoCredentials(t)
	keys := walkKeys()
	endpoint, srv := serveKeys(t, keys, 0)
	src := open(t, endpoint, s3test.Name)

	targets := []listing.Target{
		{Key: []byte("p03/"), Subtree: true},
		{Key: []byte("p0"), Subtree: true},
		{Key: []byte("p03/obj-0000"), Subtree: true},
		{Key: []byte("p11/"), Subtree: true},
		{Key: []byte("p03/"), Subtree: true},
		{Key: []byte("p03/"), Subtree: false},
		{Key: []byte("p03/missing"), Subtree: false},
		{Key: []byte("zz/"), Subtree: true},
	}
	states := []listing.LookupState{listing.LookupPresent, listing.LookupAbsent, listing.LookupAbsent, listing.LookupAbsent,
		listing.LookupPresent, listing.LookupPresent, listing.LookupAbsent, listing.LookupAbsent}
	out, ev, err := src.Summarize(context.Background(), targets, listing.SummaryOptions{})
	if err != nil || ev.Stop != listing.StopCompleted || len(out) != len(targets) {
		t.Fatalf("%v, %+v, %d answers", err, ev, len(out))
	}
	for i, ts := range out {
		n, size := truth(keys, targets[i])
		if ts.State != states[i] || ts.Entries != n || ts.Bytes == nil || *ts.Bytes != size || ts.Unread != 0 || !ts.Complete {
			t.Errorf("%q subtree=%v: %v %v, %d entries, bytes %v, unread %d, complete %t; want %v, %d, %d",
				targets[i].Key, targets[i].Subtree, ts.State, ts.Reason, ts.Entries, ts.Bytes, ts.Unread, ts.Complete, states[i], n, size)
		}
		if targets[i].Subtree && (n > 0) != (ts.Newest != nil) || ts.State == listing.LookupPresent && (ts.Self == nil || string(ts.Self.Key) != "p03/") {
			t.Errorf("%q: newest %v, self %+v", targets[i].Key, ts.Newest, ts.Self)
		}
	}
	if srv.starts.Load() == 0 && srv.requests.Load() > 30 {
		t.Errorf("%d requests", srv.requests.Load())
	}

	root, _, err := src.Summarize(context.Background(), []listing.Target{{Subtree: true}}, listing.SummaryOptions{Kinds: []listing.Kind{listing.KindFile}})
	if err != nil || root[0].State != listing.LookupUnknown || root[0].Reason == nil || root[0].Entries != 0 || *root[0].Bytes != 0 {
		t.Errorf("the root, counting files: %+v, %v", root, err)
	}
}

// TestWrongRegion: a bucket in another region is ErrNotFound, its message
// naming the region and not the command's flag.
func TestWrongRegion(t *testing.T) {
	s3test.NoCredentials(t)
	endpoint := s3test.Serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Amz-Bucket-Region", "eu-west-1")
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusMovedPermanently)
		fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>PermanentRedirect</Code><Message>elsewhere</Message></Error>`)
	}))
	src := open(t, endpoint, "bkt")
	_, perr := src.Page(context.Background(), listing.PageRequest{Limit: 10})
	_, werr := src.Walk(context.Background(), listing.WalkRequest{}, func(listing.WalkBatch) error { return nil })
	for _, err := range []error{perr, werr} {
		if !errors.Is(err, listing.ErrNotFound) || !strings.Contains(err.Error(), "eu-west-1") || strings.Contains(err.Error(), "--s3-region") {
			t.Errorf("%v", err)
		}
	}
}

// TestWalkRefusesObjectWithoutSize: an object listed without a size is
// ErrInvalidPage to a walk and a Summarize, never a size of zero.
func TestWalkRefusesObjectWithoutSize(t *testing.T) {
	s3test.NoCredentials(t)
	endpoint := s3test.Serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?><ListBucketResult><Name>bkt</Name><IsTruncated>false</IsTruncated>`+
			`<Contents><Key>a/b</Key><LastModified>2024-01-02T03:04:05.000Z</LastModified></Contents></ListBucketResult>`)
	}))
	src := open(t, endpoint, "bkt")
	emit := func(listing.WalkBatch) error { return nil }
	if _, err := src.Walk(context.Background(), listing.WalkRequest{Prefix: []byte("a/")}, emit); !errors.Is(err, listing.ErrInvalidPage) {
		t.Errorf("walk: %v", err)
	}
	_, _, err := src.Summarize(context.Background(), []listing.Target{{Key: []byte("a/"), Subtree: true}}, listing.SummaryOptions{})
	if !errors.Is(err, listing.ErrInvalidPage) {
		t.Errorf("summarize: %v", err)
	}
}
