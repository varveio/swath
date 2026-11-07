// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package evidence

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"testing"
	"time"

	"google.golang.org/api/option"
	storage "google.golang.org/api/storage/v1"
)

// SDK runs. A capture of kind sdk_run is the provider's answers to a
// client library walking a listing on its own: building the requests,
// following nextPageToken, decoding the pages. The replay side runs an
// official client library too, the Go one (google.golang.org/api
// storage/v1; the captures were made with the Java one), pointed at the
// replay server with its endpoint override, anonymous, auto-paginating
// with Pages, and without retries (the generated client makes one
// attempt per call). Every request it sends and every answer is
// recorded, and checked as the conformance suite checks a run: the
// request scope against the capture's, then the two walks' merged
// inventories; the client's own decoded inventory must be that
// inventory too.

// sdkOptions are a capture's portable SDK options from the index.
type sdkOptions struct {
	Prefix      string `json:"prefix"`
	Delimiter   string `json:"delimiter"`
	StartOffset string `json:"startOffset"`
	EndOffset   string `json:"endOffset"`
	PageSize    int64  `json:"pageSize"`
	MaxPages    int    `json:"maxPages"`
}

// recorder is the client's transport: it sends each request to the
// replay server directly and keeps a copy of the request and the answer.
type recorder struct {
	base  http.RoundTripper
	steps []step
}

func (r *recorder) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := r.base.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return nil, err
	}
	resp.Body = io.NopCloser(bytes.NewReader(body))
	var raw [][2]string
	for name, vs := range resp.Header {
		for _, v := range vs {
			raw = append(raw, [2]string{name, v})
		}
	}
	headers := map[string][]string{}
	for name, vs := range req.Header {
		headers[strings.ToLower(name)] = vs
	}
	r.steps = append(r.steps, step{"walk", request{method: req.Method, path: req.URL.EscapedPath(), query: req.URL.RawQuery, headers: headers},
		newExchange(resp.StatusCode, raw, body)})
	return resp, nil
}

// verifySDK runs the capture's walk with the Go client library.
func (e *endpoint) verifySDK(c capture, o sdkOptions) {
	rec := &recorder{base: &http.Transport{Proxy: nil}}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	svc, err := storage.NewService(ctx, option.WithEndpoint(fmt.Sprintf("http://127.0.0.1:%d/storage/v1/", e.port)),
		option.WithoutAuthentication(), option.WithHTTPClient(&http.Client{Transport: rec}))
	if err != nil {
		fail("%v", err)
	}
	call := svc.Objects.List(replayBucket).Projection("full")
	if o.Prefix != "" {
		call.Prefix(o.Prefix)
	}
	if o.Delimiter != "" {
		call.Delimiter(o.Delimiter)
	}
	if o.StartOffset != "" {
		call.StartOffset(o.StartOffset)
	}
	if o.EndOffset != "" {
		call.EndOffset(o.EndOffset)
	}
	if o.PageSize > 0 {
		call.MaxResults(o.PageSize)
	}
	var names, sdkPrefixes []string
	pages := 0
	err = call.Pages(ctx, func(p *storage.Objects) error {
		if pages++; o.MaxPages > 0 && pages > o.MaxPages {
			return fmt.Errorf("more than %d pages", o.MaxPages)
		}
		for _, it := range p.Items {
			names = append(names, it.Name)
		}
		sdkPrefixes = append(sdkPrefixes, p.Prefixes...)
		return nil
	})
	if err != nil {
		fail("replay SDK walk incomplete: %v", err)
	}
	answers := make([]exchange, len(rec.steps))
	for i := range rec.steps {
		answers[i] = rec.steps[i].exchange
	}
	answers = e.mutate(answers)
	walked := make([]step, len(answers))
	for i := range answers {
		walked[i] = step{"walk", rec.steps[min(i, len(rec.steps)-1)].request, answers[i]}
	}
	verifySDKRun(c, walked)
	// The client's own reading of the pages is the provider's inventory.
	var nativeNames, nativePrefixes []string
	for _, s := range c.steps {
		page := readJSON(s.exchange.body)
		for _, it := range items(page) {
			nativeNames = append(nativeNames, text(it, "name"))
		}
		nativePrefixes = append(nativePrefixes, prefixes(page)...)
	}
	equal("SDK decoded names", nativeNames, orEmpty(names))
	equal("SDK decoded prefixes", orEmpty(nativePrefixes), orEmpty(sdkPrefixes))
}

func orEmpty(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

// goTransportForms are the parameters the Go client adds to every call
// that name the API's defaults: the response format and compact output.
var goTransportForms = map[string]string{"alt": "json", "prettyPrint": "false"}

// sdkScope is a query's parameters, decoded and sorted, without the page
// token and without the Go client's transport forms.
func sdkScope(query string) []string {
	var out []string
	for _, part := range strings.Split(query, "&") {
		if part == "" {
			continue
		}
		name, value, _ := strings.Cut(part, "=")
		v, err := url.PathUnescape(value)
		require(err == nil, "replay request query is not percent-encoded")
		if name == "pageToken" || goTransportForms[name] == v {
			continue
		}
		out = append(out, name+"="+v)
	}
	slices.Sort(out)
	return out
}

// sdkToken is a query's page token, decoded; nil without one.
func sdkToken(query string) *string {
	t := queryValue(query, "pageToken")
	if t == nil {
		return nil
	}
	v, err := url.PathUnescape(*t)
	require(err == nil, "replay page token is not percent-encoded")
	return &v
}

// verifySDKRun is the receipt check of an SDK run with a client library
// other than the captured one: every request carries the captured scope
// (compared as decoded parameters, the Go client's alt=json and
// prettyPrint=false aside) and identifies the Go client, then the walks
// are compared.
func verifySDKRun(c capture, replay []step) {
	require(c.kind == "sdk_run", "SDK run receipt needs an SDK run capture")
	require(len(replay) > 0, "replay run is empty")
	nativeFirst := c.steps[0].request
	scope := sdkScope(nativeFirst.query)
	for _, s := range c.steps {
		require(slices.Equal(sdkScope(s.request.query), scope), "native walk scope changed between pages")
	}
	require(receiptToken(nativeFirst.query) == nil && sdkToken(replay[0].request.query) == nil,
		"a walk must start without a continuation token")
	for _, s := range replay {
		require(nativeFirst.method == s.request.method && s.request.path == gcsReplayPath, "replay request method/path differs from captured probe")
		require(slices.Equal(sdkScope(s.request.query), scope), "replay request query differs from captured probe")
		require(noUnexpectedDuplicates(s.request.query, false), "duplicate query field outside duplicate-parameter probe")
		client := s.request.header("x-goog-api-client")
		require(client != nil && strings.Contains(*client, "gl-go/") && strings.Contains(*client, "gdcl/"),
			"replay SDK product/version token differs")
	}
	var nativePages, replayPages []capturedPage
	for _, s := range c.steps {
		nativePages = append(nativePages, capturedPage{receiptToken(s.request.query), s.exchange})
	}
	for _, s := range replay {
		replayPages = append(replayPages, capturedPage{sdkToken(s.request.query), s.exchange})
	}
	assertGCSWalk(nativePages, replayPages, "{bucket}", replayBucket, true, 1024, 1_000_000)
}

// TestSDKMakesOneAttempt: the client the SDK runs use sends one request
// for a failing call (no retry policy), so a replay error reaches the
// comparison as the replay answered it.
func TestSDKMakesOneAttempt(t *testing.T) {
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(503)
		io.WriteString(w, `{"error":{"code":503,"message":"Replay server busy","errors":[{"message":"Replay server busy","domain":"global","reason":"backendError"}]}}`)
	}))
	defer srv.Close()
	svc, err := storage.NewService(context.Background(), option.WithEndpoint(srv.URL+"/storage/v1/"), option.WithoutAuthentication())
	if err != nil {
		t.Fatal(err)
	}
	err = svc.Objects.List("bkt").Pages(context.Background(), func(*storage.Objects) error { return nil })
	if err == nil || calls != 1 || !strings.Contains(err.Error(), "backendError") {
		t.Errorf("%d calls, %v", calls, err)
	}
}
