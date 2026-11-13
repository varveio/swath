// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/replay/latency"
	"github.com/varveio/swath/go/internal/replay/legacy"
	"github.com/varveio/swath/go/internal/replay/metrics"
	"github.com/varveio/swath/go/internal/replay/store"
	"github.com/varveio/swath/go/internal/replay/store/memstore"
)

func gcsConfig(st store.Store, protocols ...string) Config {
	cfg := config(st)
	cfg.Protocols = protocols
	return cfg
}

const gcsList = "/storage/v1/b/bucket/o"

func TestServesGCSObjectsList(t *testing.T) {
	s := start(t, gcsConfig(memstore.Keys(memstore.Options{}, "a/1", "a/2", "b/1"), ProtocolGCS))
	r := get(t, s, gcsList+"?prefix=a%2F&maxResults=1")
	var page struct {
		Kind  string
		Items []struct{ Name, Bucket, Size, Etag string }
		Token string `json:"nextPageToken"`
	}
	if err := json.Unmarshal([]byte(r.body), &page); err != nil || r.status != 200 || r.header.Get("Content-Type") != "application/json; charset=utf-8" {
		t.Fatalf("%d %v %s %v", r.status, r.header, r.body, err)
	}
	if page.Kind != "storage#objects" || len(page.Items) != 1 || page.Items[0].Name != "a/1" || page.Items[0].Bucket != "bucket" ||
		page.Items[0].Etag != "swath-gcs-synthetic-v1" || !strings.HasPrefix(page.Token, "gcs1.") {
		t.Fatalf("%s", r.body)
	}
	r = get(t, s, gcsList+"?prefix=a%2F&pageToken="+page.Token)
	if !strings.Contains(r.body, `"name":"a/2"`) || strings.Contains(r.body, "nextPageToken") {
		t.Errorf("%s", r.body)
	}
	if count(s, metrics.ProtocolHTTPRequests, "gcs", "2xx") != 2 || count(s, metrics.ProtocolObjects, "gcs", "seek") != 1 ||
		count(s, ProviderPath, "gcs", "flat", "bounded_upper") != 2 || count(s, ProviderPath, "gcs", "timestamp_render", "single_utc_day") != 2 {
		t.Errorf("meters: %s", s.reg.AppendMeters(nil))
	}
}

// TestGCSErrorsAreJSONEnvelopes holds each refusal to the status, reason
// header and envelope the Java server sends for it.
func TestGCSErrorsAreJSONEnvelopes(t *testing.T) {
	s := start(t, gcsConfig(memstore.Keys(memstore.Options{}, "a"), ProtocolGCS))
	for _, c := range []struct {
		method, target string
		status         int
		reason, kind   string
	}{
		{"GET", gcsList + "?maxResults=0", 400, "invalid_max_results", "invalid"},
		{"GET", gcsList + "?versions=true", 400, "feature_versions", "invalid"},
		{"GET", gcsList + "?pageToken=gcs1.AAAA", 400, "invalid_page_token", "invalid"},
		{"GET", "/storage/v1/b/other/o", 404, "bucket_not_found", "notFound"},
		{"GET", "/storage/v1/b/%62ucket/o", 404, "bucket_not_found", "notFound"},
		{"GET", gcsList + "/", 404, "bucket_not_found", "notFound"},
		{"POST", gcsList, 405, "wrong_method", "methodNotAllowed"},
		{"POST", "/storage/v1/b/other/o", 405, "wrong_method", "methodNotAllowed"},
	} {
		r := do(t, c.method, fmt.Sprintf("http://127.0.0.1:%d%s", s.Port(), c.target))
		var env struct {
			Error struct {
				Code   int
				Errors []struct{ Reason, Domain string }
			}
		}
		if err := json.Unmarshal([]byte(r.body), &env); err != nil || r.status != c.status || env.Error.Code != c.status ||
			env.Error.Errors[0].Reason != c.kind || env.Error.Errors[0].Domain != "global" || r.header.Get("x-swath-replay-error") != c.reason {
			t.Errorf("%s %s: %d %v %s", c.method, c.target, r.status, r.header, r.body)
		}
		if (c.status == 405) != (r.header.Get("Allow") == "GET") {
			t.Errorf("%s %s: Allow %q", c.method, c.target, r.header.Get("Allow"))
		}
	}
	if r := do(t, "HEAD", fmt.Sprintf("http://127.0.0.1:%d%s", s.Port(), gcsList)); r.status != 405 || r.body != "" || r.header.Get("Content-Length") != "0" {
		t.Errorf("HEAD: %d %v %q", r.status, r.header, r.body)
	}
}

// TestRouteClassification: with GCS alone an unclaimed request is the
// replay's 404; with S3 too it is the S3 route's; an S3 selector on a GCS
// path is ambiguous either way.
func TestRouteClassification(t *testing.T) {
	gcsOnly := start(t, gcsConfig(memstore.Keys(memstore.Options{}, "a"), ProtocolGCS))
	both := start(t, gcsConfig(memstore.Keys(memstore.Options{}, "a"), ProtocolS3, ProtocolGCS))
	for _, c := range []struct {
		s           *Server
		target      string
		status      int
		body, ctype string
	}{
		{gcsOnly, "/bucket?list-type=2", 404, `{"error":"replay_not_found"}`, "application/json"},
		{gcsOnly, gcsList + "?list-type=2", 400, `{"error":"replay_ambiguous_request"}`, "application/json"},
		{gcsOnly, gcsList + "?list%2Dtype", 400, `{"error":"replay_ambiguous_request"}`, "application/json"},
		{both, "/bucket?list-type=2", 200, "<ListBucketResult", "application/xml"},
		{both, gcsList + "?list-type=2", 400, `{"error":"replay_ambiguous_request"}`, "application/json"},
		{both, gcsList, 200, `{"kind":"storage#objects"`, "application/json; charset=utf-8"},
	} {
		r := get(t, c.s, c.target)
		if r.status != c.status || !strings.Contains(r.body, c.body) || r.header.Get("Content-Type") != c.ctype || r.header.Get("x-swath-replay-error") != "" {
			t.Errorf("%s: %d %v %s", c.target, r.status, r.header, r.body)
		}
	}
	if count(gcsOnly, metrics.ProtocolHTTPRequests, "replay", "4xx") != 3 {
		t.Errorf("replay meters: %s", gcsOnly.reg.AppendMeters(nil))
	}
	var snap struct {
		Serving struct {
			Protocols []string
			Profiles  map[string]string
		}
	}
	if err := json.Unmarshal(both.Snapshot(time.Now()), &snap); err != nil || strings.Join(snap.Serving.Protocols, ",") != "gcs,s3" ||
		snap.Serving.Profiles["gcs"] != "json-v1" || snap.Serving.Profiles["s3"] != "list-objects-v2" {
		t.Errorf("serving: %+v %v", snap.Serving, err)
	}
}

func TestGCSAdmissionRefusals(t *testing.T) {
	keys := make([]string, 200)
	for i := range keys {
		keys[i] = fmt.Sprintf("%s%03d", strings.Repeat("x", 100), i)
	}
	cfg := gcsConfig(memstore.Keys(memstore.Options{}, keys...), ProtocolGCS)
	cfg.MaxResponseBytes = 8 << 10
	s := start(t, cfg)
	r := get(t, s, gcsList+"?maxResults=200")
	if r.status != 500 || r.header.Get("x-swath-replay-error") != "response_too_large" || !strings.Contains(r.body, `"reason":"internalError"`) {
		t.Errorf("too large: %d %v %s", r.status, r.header, r.body)
	}
	s.responses.Store(int64(s.cfg.MaxResponses()))
	r = get(t, s, gcsList+"?maxResults=1")
	if r.status != 503 || r.header.Get("x-swath-replay-error") != "response_count_exhausted" || !strings.Contains(r.body, `"reason":"backendError"`) {
		t.Errorf("count: %d %v %s", r.status, r.header, r.body)
	}
	s.responses.Store(0)
	held := s.budget.Limit() - s.budget.Charged()
	s.budget.TryCharge(held)
	if r := get(t, s, gcsList+"?maxResults=1"); r.status != 503 || r.header.Get("x-swath-replay-error") != "response_budget_exhausted" {
		t.Errorf("budget: %d %v", r.status, r.header)
	}
	s.budget.Release(held)
	if r := get(t, s, gcsList+"?maxResults=5"); r.status != 200 {
		t.Errorf("after: %d", r.status)
	}
	waitFor(t, "the budget to come back", func() bool { return s.budget.Charged() == 0 })
	for _, reason := range []string{"response_too_large", "response_count_exhausted", "response_budget_exhausted"} {
		if count(s, metrics.ResponseAdmissionRefused, "gcs", reason) != 1 {
			t.Errorf("%s: %s", reason, s.reg.AppendMeters(nil))
		}
	}
}

// failStore fails every read with err.
type failStore struct {
	store.Store
	err error
}

func (f failStore) Rows(context.Context, []byte, bool, []byte, int, store.Projection) ([]store.Object, error) {
	return nil, f.err
}

func TestGCSFixtureAndStoreFailures(t *testing.T) {
	s := start(t, gcsConfig(memstore.Keys(memstore.Options{}, "a\xffb", "ok"), ProtocolGCS))
	r := get(t, s, gcsList)
	if r.status != 500 || r.header.Get("x-swath-replay-error") != "invalid_utf8_name" || !strings.Contains(r.body, "Fixture incompatible with GCS listing") {
		t.Errorf("%d %v %s", r.status, r.header, r.body)
	}
	if r := get(t, s, gcsList+"?startOffset=o"); r.status != 200 {
		t.Errorf("a page without the bad name: %d", r.status)
	}
	var buf strings.Builder
	log := syncWriter{w: &buf}
	cfg := gcsConfig(failStore{memstore.Keys(memstore.Options{}), errors.New("disk on fire at /secret/path")}, ProtocolGCS)
	cfg.Log = &log
	s = start(t, cfg)
	r = get(t, s, gcsList)
	if r.status != 500 || r.header.Get("x-swath-replay-error") != "internal" || strings.Contains(r.body, "secret") || !strings.Contains(buf.String(), "disk on fire") {
		t.Errorf("%d %v %s", r.status, r.header, r.body)
	}
	disorder := &legacy.DisorderError{Part: "p", Detail: "keys repeat"}
	s = start(t, gcsConfig(failStore{memstore.Keys(memstore.Options{}), disorder}, ProtocolGCS))
	if r := get(t, s, gcsList); r.status != 500 || r.header.Get("x-swath-replay-error") != "fixture_disordered" {
		t.Errorf("disorder: %d %v %s", r.status, r.header, r.body)
	}
}

func TestGCSInjectedDelayUsesTheSharedShapes(t *testing.T) {
	cfg := gcsConfig(memstore.Keys(memstore.Options{}, "a/1", "a/2"), ProtocolGCS)
	cfg.Latency, _ = latency.Parse("worker_page=1ms,pivot_probe=150ms,structure_probe=1ms+100ms/cp", 0, 1)
	s := start(t, cfg)
	for _, c := range []struct {
		query string
		least time.Duration
	}{{"maxResults=1", 150 * time.Millisecond}, {"delimiter=%2F", 100 * time.Millisecond}, {"maxResults=2", 0}} {
		began := time.Now()
		get(t, s, gcsList+"?"+c.query)
		if took := time.Since(began); took < c.least || (c.least == 0 && took > 100*time.Millisecond) {
			t.Errorf("%s took %v", c.query, took)
		}
	}
	if count(s, metrics.RequestStageLatency, "gcs", "delay") < 2 {
		t.Errorf("delay stage: %s", s.reg.AppendMeters(nil))
	}
}

func TestValidateProtocols(t *testing.T) {
	for _, c := range []struct {
		protocols []string
		bucket    string
		ok        bool
	}{
		{[]string{"s3"}, "b", true},
		{[]string{"gcs"}, "my-bucket", true},
		{[]string{"s3", "gcs"}, "my.bucket_1", true},
		{nil, "bkt", false},
		{[]string{"azure"}, "bkt", true},
		{[]string{"ftp"}, "bkt", false},
		{[]string{"s3", "gcs"}, "storage", false},
		{[]string{"gcs"}, "storage", true},
		{[]string{"gcs"}, "b", false},
		{[]string{"gcs"}, "Upper", false},
		{[]string{"gcs"}, "goog-x", false},
		{[]string{"gcs"}, "my-g00gle", false},
		{[]string{"gcs"}, "192.168.1.1", false},
		{[]string{"gcs"}, "192.168.1.256", true},
		{[]string{"gcs"}, strings.Repeat("a", 64), false},
		{[]string{"gcs"}, strings.Repeat("a", 63) + "." + strings.Repeat("b", 63), true},
		{[]string{"gcs"}, "a..b", false},
		{[]string{"gcs"}, "-ab", false},
	} {
		if err := ValidateProtocols(c.protocols, c.bucket); (err == nil) != c.ok {
			t.Errorf("%v %q: %v", c.protocols, c.bucket, err)
		}
	}
}

// TestGCSShapeCountsAndRejectionMeter: each page counts its objects and
// prefixes under its shape, a structure probe's delay charges each
// prefix returned, and a fixture row the API cannot represent counts its
// reason under provider.path.
func TestGCSShapeCountsAndRejectionMeter(t *testing.T) {
	cfg := gcsConfig(memstore.Keys(memstore.Options{}, "a/1", "a/2", "b/1", "c"), ProtocolGCS)
	cfg.Latency, _ = latency.Parse("worker_page=1ms,pivot_probe=1ms,structure_probe=1ms+60ms/cp", 0, 1)
	s := start(t, cfg)
	began := time.Now()
	get(t, s, gcsList+"?delimiter=%2F&maxResults=1")
	if took := time.Since(began); took < 61*time.Millisecond || took > 115*time.Millisecond {
		t.Errorf("one prefix: %v", took)
	}
	began = time.Now()
	get(t, s, gcsList+"?delimiter=%2F")
	if took := time.Since(began); took < 121*time.Millisecond {
		t.Errorf("two prefixes: %v", took)
	}
	get(t, s, gcsList+"?maxResults=1")
	for _, c := range []struct {
		def  metrics.Def
		tags []string
		want int64
	}{
		{metrics.ProtocolObjects, []string{"gcs", "delimiter"}, 1},
		{metrics.ProtocolPrefixes, []string{"gcs", "delimiter"}, 3},
		{metrics.ProtocolObjects, []string{"gcs", "seek"}, 1},
		{metrics.ProtocolPrefixes, []string{"gcs", "seek"}, 0},
	} {
		if got := count(s, c.def, c.tags...); got != c.want {
			t.Errorf("%s %v: %d, want %d", c.def.Name, c.tags, got, c.want)
		}
	}
	bad := start(t, gcsConfig(memstore.Keys(memstore.Options{}, "\xff"), ProtocolGCS))
	get(t, bad, gcsList)
	if count(bad, ProviderPath, "gcs", "fixture_rejected", "invalid_utf8_name") != 1 {
		t.Errorf("rejection meter: %s", bad.reg.AppendMeters(nil))
	}
}
