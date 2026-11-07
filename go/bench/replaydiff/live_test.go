// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/replay/server"
	"github.com/varveio/swath/go/internal/replay/store/memstore"
)

// liveKeys is a small tree with every role the provider matrix binds.
func liveKeys() []string {
	var keys []string
	for i := range 8 {
		keys = append(keys, fmt.Sprintf("data/small/f%02d.csv", i))
	}
	for i := range 1003 {
		keys = append(keys, fmt.Sprintf("data/big/%04d.bin", i))
	}
	for i := range 1000 {
		keys = append(keys, fmt.Sprintf("data/exact/%04d.bin", i))
	}
	return append(keys, "data/tree/", "data/tree/a/1.txt", "data/tree/a/2.txt", "data/tree/a/3.txt", "data/tree/b/1.txt",
		"data/tree/b/2.txt", "data/tree/c.txt", "data/tree/readme.md", "data/x.csv", "data/x.csv.gz", "data/x.csv.gz.md5", "other/z")
}

func startGoProtocols(t *testing.T, protocols []string, keys ...string) string {
	t.Helper()
	srv, err := server.New(server.Config{Bucket: "bkt", Host: "127.0.0.1", MetricsPort: -1, MaxConcurrentRequests: 64,
		ResponseBufferBudget: 1 << 30, MaxResponseBytes: 1 << 26, StopTimeout: time.Second, IdleTimeout: time.Minute,
		WriteTimeout: time.Minute, SlowRequestLog: -1, Store: memstore.Keys(memstore.Options{}, keys...),
		ServingMode: "MEMORY", FixtureIdentity: "f", Protocols: protocols})
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.Listen(); err != nil {
		t.Fatal(err)
	}
	go srv.Serve()
	t.Cleanup(func() { srv.Shutdown(time.Second) })
	return fmt.Sprintf("http://127.0.0.1:%d", srv.Port())
}

// fakeProvider stands for a live provider in front of a Go server: its
// tokens are opaque strings of its own (mapped to the server's), its
// answers carry the provider's request-id and transport headers, and
// edit, when set, alters a body.
type fakeProvider struct {
	backend, tokenParam string
	tokenRe             *regexp.Regexp
	extra               http.Header
	edit                func(query string, body []byte) []byte
	mu                  sync.Mutex
	tokens              map[string]string // opaque -> backend token
}

func (f *fakeProvider) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	query := r.URL.RawQuery
	var parts []string
	sent, echo := "", ""
	for _, p := range strings.Split(query, "&") {
		if name, v, _ := strings.Cut(p, "="); name == f.tokenParam {
			f.mu.Lock()
			real, ok := f.tokens[v]
			f.mu.Unlock()
			if ok {
				p, sent = name+"="+real, v
			}
		}
		parts = append(parts, p)
	}
	resp, err := http.Get(f.backend + r.URL.EscapedPath() + "?" + strings.Join(parts, "&"))
	if err != nil {
		http.Error(w, err.Error(), 502)
		return
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	body = f.tokenRe.ReplaceAllFunc(body, func(m []byte) []byte {
		sub := f.tokenRe.FindSubmatch(m)
		f.mu.Lock()
		defer f.mu.Unlock()
		opaque := fmt.Sprintf("OPAQUE%d", len(f.tokens))
		f.tokens[campaignEscape(opaque, "")] = campaignEscape(string(sub[2]), "")
		return bytes.Replace(m, sub[2], []byte(opaque), 1)
	})
	if sent != "" {
		// The echo is the token as the client sent it.
		decoded, _ := url.QueryUnescape(sent)
		echo = "<ContinuationToken>" + decoded + "</ContinuationToken>"
		body = regexp.MustCompile(`<ContinuationToken>[^<]*</ContinuationToken>`).ReplaceAll(body, []byte(echo))
	}
	if f.edit != nil {
		body = f.edit(query, body)
	}
	for k, v := range f.extra {
		w.Header()[k] = v
	}
	w.Header().Set("Content-Type", resp.Header.Get("Content-Type"))
	w.Header().Set("Date", resp.Header.Get("Date"))
	w.Header().Set("Content-Length", fmt.Sprint(len(body)))
	w.WriteHeader(resp.StatusCode)
	w.Write(body)
}

func runFakeLive(t *testing.T, protocol string, edit func(string, []byte) []byte) *report {
	t.Helper()
	keys := liveKeys()
	protocols := []string{server.ProtocolS3}
	f := &fakeProvider{tokenParam: "continuation-token", tokenRe: regexp.MustCompile(`(<NextContinuationToken>)([^<]*)<`),
		extra: http.Header{"X-Amz-Request-Id": {"R1"}, "X-Amz-Id-2": {"H1"}, "Server": {"AmazonS3"}, "X-Amz-Bucket-Region": {"us-east-1"}},
		edit:  edit, tokens: map[string]string{}}
	if protocol == "gcs" {
		protocols = []string{server.ProtocolGCS}
		f.tokenParam, f.tokenRe = "pageToken", regexp.MustCompile(`("nextPageToken":")([^"]*)"`)
		f.extra = http.Header{"X-Guploader-Uploadid": {"U1"}, "Vary": {"Origin", "X-Origin"}, "Server": {"UploadServer"},
			"Cache-Control": {"private, max-age=0"}}
	}
	f.backend = startGoProtocols(t, protocols, keys...)
	live := httptest.NewServer(f)
	t.Cleanup(live.Close)
	client := &http.Client{}
	got := &target{name: "go", base: startGoProtocols(t, protocols, keys...), client: client}
	ref := &target{name: "live", base: live.URL, client: client}
	o := options{bucket: "bkt", parallel: 4, maxTop: 5, walkSizes: "7,1000", liveWalkPages: 300, livePrefix: "data/"}
	listed, _, err := listScope(got, protocol, "bkt", "data/")
	if err != nil {
		t.Fatal(err)
	}
	rep := &report{}
	lr := &liveRun{d: &driver{o: o, ref: ref, got: got, rep: rep}, protocol: protocol, scope: "data/",
		roles: deriveRoles(listed, "data/"), tokens: map[string][2][]string{}}
	lr.run(listed)
	return rep
}

// TestLiveModesAgreeWithAFaithfulProvider: against a provider whose
// tokens, request ids and transport headers are its own but whose
// listings are the fixture's, every sent case and walk passes on both
// routes, each side following its own tokens, the token cases get their
// tokens from both sides' walks, and every case either is sent or says
// why not. Against one that changes a single listing, the change is
// found.
func TestLiveModesAgreeWithAFaithfulProvider(t *testing.T) {
	for _, protocol := range []string{"s3", "gcs"} {
		rep := runFakeLive(t, protocol, nil)
		cases := len(s3ProviderCases())
		if protocol == "gcs" {
			cases = len(gcsProviderCases())
		}
		sent := 0
		for _, c := range rep.Coverage {
			if strings.Contains(c, ": sent ") {
				sent++
			}
		}
		if len(rep.Coverage) != cases || sent < cases*3/5 {
			t.Errorf("%s: %d of %d cases sent: %v", protocol, sent, cases, rep.Coverage)
		}
		// This provider knows only the tokens it issued; the replay
		// decodes an altered token of its own to a position.
		expected := map[string]bool{protocol + "-07r": true, protocol + "-07s": true}
		for _, r := range rep.Results {
			if r.Pass == expected[r.Name] {
				t.Errorf("%s %s %q: pass %v %s %+v", protocol, r.Kind, r.Name, r.Pass, r.Error, r.diff)
			}
		}
		if rep.Failed != len(expected) || rep.Passed < sent-len(expected) {
			t.Fatalf("%s: %d passed, %d failed, skipped %v", protocol, rep.Passed, rep.Failed, rep.Skipped)
		}
		for _, s := range rep.Skipped {
			if !strings.Contains(s, "not base64") {
				t.Errorf("%s: skipped %s", protocol, s)
			}
		}
		t.Logf("%s: %d sent, %d compared, %d skipped", protocol, sent, rep.Compared, len(rep.Skipped))
	}
	edit := func(query string, body []byte) []byte {
		if strings.Contains(query, "start-after=data/small/f01.csv") || strings.Contains(query, "startOffset=data/small/f01.csv") {
			return bytes.Replace(body, []byte("f02.csv"), []byte("f02.csw"), 1)
		}
		return body
	}
	for _, protocol := range []string{"s3", "gcs"} {
		rep := runFakeLive(t, protocol, edit)
		if rep.Failed <= 2 {
			t.Errorf("%s: a changed name compared equal", protocol)
		}
	}
}

// TestS3LiveComparatorPolicy pins what the S3 comparator sets aside
// (request ids, dates, the tokens' values, Owner values) and what it
// does not (a scalar, an unclassified header, a token echo that is not
// the side's own, the error code).
func TestS3LiveComparatorPolicy(t *testing.T) {
	page := func(token, owner, keyCount string) []byte {
		return []byte(`<?xml version="1.0" encoding="UTF-8"?>` + "\n" + `<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">` +
			`<Name>b</Name><Prefix></Prefix><NextContinuationToken>` + token + `</NextContinuationToken><KeyCount>` + keyCount +
			`</KeyCount><MaxKeys>1</MaxKeys><IsTruncated>true</IsTruncated><Contents><Key>a&#x1;</Key>` +
			`<LastModified>2026-01-01T00:00:00.000Z</LastModified><ETag>"e"</ETag><Size>1</Size><Owner><ID>` + owner +
			`</ID></Owner><StorageClass>STANDARD</StorageClass></Contents></ListBucketResult>`)
	}
	h := func(kv ...string) http.Header {
		x := http.Header{"Content-Type": {"application/xml"}}
		for i := 0; i < len(kv); i += 2 {
			x[kv[i]] = []string{kv[i+1]}
		}
		return x
	}
	live := &response{200, h("X-Amz-Request-Id", "R", "X-Amz-Id-2", "H", "Date", "Sat, 07 Nov 2026 11:54:47 GMT"), page("1ueG=", "abc", "1")}
	same := &response{200, h("X-Amz-Request-Id", "S", "Date", "Sun, 08 Nov 2026 00:00:00 GMT"), page("v2:AA", "synthetic", "1")}
	if d := s3Live(live, same, "b", "", ""); !d.OK {
		t.Errorf("equal pages differ: %s", d.Detail)
	}
	for name, c := range map[string]struct {
		got           *response
		refTok, gotTo string
		want          string
	}{
		"key count":     {&response{200, h(), page("v2:AA", "x", "2")}, "", "", "KeyCount differs"},
		"unclassified":  {&response{200, h("X-Other", "1"), page("v2:AA", "x", "1")}, "", "", "unclassified HTTP header x-other"},
		"token echo":    {&response{200, h(), page("v2:AA", "x", "1")}, "", "v2:AB", "replay ContinuationToken echo differs"},
		"date spelling": {&response{200, h("Date", "Sat, 7 Nov 2026 11:54:47 GMT"), page("v2:AA", "x", "1")}, "", "", "not padded GMT"},
	} {
		if d := s3Live(live, c.got, "b", c.refTok, c.gotTo); d.OK || !strings.Contains(d.Detail, c.want) {
			t.Errorf("%s: %+v", name, d)
		}
	}
	e1 := &response{400, h(), []byte(`<Error><Code>InvalidArgument</Code><Message>S3 words</Message><RequestId>R</RequestId></Error>`)}
	e2 := &response{400, h(), []byte(`<Error><Code>InvalidArgument</Code><Message>replay words</Message></Error>`)}
	if d := s3Live(e1, e2, "b", "", ""); !d.OK {
		t.Errorf("errors with the same code: %s", d.Detail)
	}
	e2.Body = []byte(`<Error><Code>InvalidToken</Code></Error>`)
	if d := s3Live(e1, e2, "b", "", ""); d.OK {
		t.Error("different error codes compared equal")
	}
}

// TestGCSLiveComparatorMetadata: with a native fixture the stored fields
// are compared exactly; with a synthetic one the replay's fixed values
// are required and the provider's are not compared.
func TestGCSLiveComparatorMetadata(t *testing.T) {
	item := func(etag, gen, md5 string) string {
		s := `{"kind":"storage#object","bucket":"b","name":"a","size":"1","updated":"2026-01-01T00:00:00.123456Z","etag":"` + etag +
			`","generation":"` + gen + `","metageneration":"1","storageClass":"STANDARD","contentType":"application/octet-stream"`
		if md5 != "" {
			s += `,"md5Hash":"` + md5 + `"`
		}
		return s + "}"
	}
	page := func(it string) []byte { return []byte(`{"kind":"storage#objects","items":[` + it + `]}`) }
	h := http.Header{"Content-Type": {"application/json; charset=UTF-8"}}
	live := &response{200, h, page(strings.Replace(item("CJ+", "17", "bWQ="), `"kind":"storage#object",`, `"kind":"storage#object","id":"x","timeCreated":"2026-01-01T00:00:00Z",`, 1))}
	native := &response{200, h, page(item("CJ+", "17", "bWQ="))}
	synthetic := &response{200, h, page(item("swath-gcs-synthetic-v1", "1", ""))}
	if d := gcsLive(live, native, "b", false, gcsNative); !d.OK {
		t.Errorf("native: %s", d.Detail)
	}
	if d := gcsLive(live, synthetic, "b", false, gcsSynthetic); !d.OK {
		t.Errorf("synthetic: %s", d.Detail)
	}
	if d := gcsLive(live, synthetic, "b", false, gcsNative); d.OK || !strings.Contains(d.Detail, "etag 0") {
		t.Errorf("a synthetic etag passed as native: %+v", d)
	}
	if d := gcsLive(live, &response{200, h, page(item("CJ+", "17", ""))}, "b", false, gcsNative); d.OK || !strings.Contains(d.Detail, "md5Hash 0") {
		t.Errorf("a missing md5Hash passed: %+v", d)
	}
	e := func(reason string) *response {
		return &response{400, h, []byte(`{"error":{"code":400,"message":"m","errors":[{"message":"m","domain":"global","reason":"` + reason + `"}]}}`)}
	}
	if d := gcsLive(e("invalid"), e("invalid"), "b", false, gcsNative); !d.OK {
		t.Errorf("same reason: %s", d.Detail)
	}
	if d := gcsLive(e("invalid"), e("required"), "b", false, gcsNative); d.OK {
		t.Error("different reasons compared equal")
	}
}

func TestMutateToken(t *testing.T) {
	for _, c := range []struct{ token, mutation, want string }{
		{"abcA", "flip", "abcB"}, {"abcd", "flip", "abcA"}, {"abcdefgh", "truncate", "abcd"}, {"abc", "truncate", "a"},
		{"AAAA", "flip-decoded", "AAAB"}, {"AA-_", "flip-decoded", "AA--"}, {"AAAAAAAB=", "flip-decoded", "AAAAAAAA"},
	} {
		if got, err := mutateToken(c.token, c.mutation); err != nil || got != c.want {
			t.Errorf("%s %s = %q %v, want %q", c.mutation, c.token, got, err, c.want)
		}
	}
	if _, err := mutateToken("v2:AA", "flip-decoded"); err == nil {
		t.Error("a token that is not base64 was flipped")
	}
}
