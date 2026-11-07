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
	"os"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/replay/s3"
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
	return runFakeLiveThrough(t, protocol, edit, nil, false, nil)
}

// runFakeLiveThrough is runFakeLive with the provider behind rec, when
// set (the live driver then runs with -live-record), and with the
// scripted matrix when scripted is set, and pins as -live-pins.
func runFakeLiveThrough(t *testing.T, protocol string, edit func(string, []byte) []byte, rec *testRecorder, scripted bool, pins map[string]string) *report {
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
	var h http.Handler = f
	if rec != nil {
		rec.next, h = f, rec
	}
	live := httptest.NewServer(h)
	t.Cleanup(live.Close)
	client := &http.Client{}
	got := &target{name: "go", base: startGoProtocols(t, protocols, keys...), client: client}
	ref := &target{name: "live", base: live.URL, client: client, recorder: rec != nil}
	o := options{bucket: "bkt", parallel: 4, maxTop: 5, walkSizes: "7,1000", liveWalkPages: 300, livePrefix: "data/",
		liveScripted: scripted, liveRecord: rec != nil, maxPrefixes: 5, seed: 1}
	if rec != nil {
		o.walkSizes = "1000" // the scope walks at 7 add pages, not shapes
	}
	listed, _, err := listScope(got, protocol, "bkt", "data/")
	if err != nil {
		t.Fatal(err)
	}
	rep := &report{}
	lr := &liveRun{d: &driver{o: o, ref: ref, got: got, rep: rep}, protocol: protocol, scope: "data/",
		roles: deriveRoles(listed, "data/"), tokens: map[string][2][]string{}, pins: pins}
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
		// This provider knows only the tokens it issued and serves an
		// unknown one as an empty page. The GCS route decodes an altered
		// token of its own to a position; the S3 route refuses any
		// alteration (its token carries a checksum), so of the S3 token
		// mutations only the decoded-byte flip, sent now that the token is
		// base64 as a whole, differs: a documented difference, listed
		// apart, not a failure.
		expected := map[string]bool{"gcs-07r": true, "gcs-07s": true}
		documented := 0
		if protocol == "s3" {
			expected = map[string]bool{}
			documented = 1
			if len(rep.Residuals) != 1 || !strings.HasPrefix(rep.Residuals[0], "s3-07x: an issued token with a decoded byte flipped") {
				t.Errorf("s3: documented %q", rep.Residuals)
			}
		}
		if len(rep.Residuals) != documented {
			t.Errorf("%s: documented %q", protocol, rep.Residuals)
		}
		for _, r := range rep.Results {
			if r.Pass == expected[r.Name] {
				t.Errorf("%s %s %q: pass %v %s %+v", protocol, r.Kind, r.Name, r.Pass, r.Error, r.diff)
			}
		}
		if rep.Failed != len(expected) || rep.Passed < sent-len(expected)-documented {
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
		// More than the token differences of the faithful run.
		if base := map[string]int{"s3": 0, "gcs": 2}[protocol]; rep.Failed <= base {
			t.Errorf("%s: a changed name compared equal", protocol)
		}
	}
}

// TestS3LiveComparatorPolicy pins what the S3 comparator sets aside
// (request ids, dates, the tokens' values, Owner values, an error's
// RequestId and HostId values) and what it does not (a scalar, an
// unclassified header, a token echo that is not the side's own, a key
// written raw where S3 writes a reference, the error document's other
// elements as written).
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
		"raw control":   {&response{200, h(), bytes.Replace(page("v2:AA", "x", "1"), []byte("&#x1;"), []byte("\x01"), 1)}, "", "", "key text as written differs"},
	} {
		if d := s3Live(live, c.got, "b", c.refTok, c.gotTo); d.OK || !strings.Contains(d.Detail, c.want) {
			t.Errorf("%s: %+v", name, d)
		}
	}
	doc := func(message, value, requestID, hostID string) []byte {
		return []byte(`<?xml version="1.0" encoding="UTF-8"?>` + "\n" + `<Error><Code>InvalidArgument</Code><Message>` + message +
			`</Message><ArgumentName>prefix</ArgumentName><ArgumentValue>` + value + `</ArgumentValue><RequestId>` + requestID +
			`</RequestId><HostId>` + hostID + `</HostId></Error>`)
	}
	e1 := &response{400, h(), doc("S3 words", "a&#0;", "R", "H")}
	e2 := &response{400, h(), doc("S3 words", "a&#0;", "S3LISTINGREPLAY", "S3LISTINGREPLAY")}
	if d := s3Live(e1, e2, "b", "", ""); !d.OK {
		t.Errorf("errors differing in RequestId and HostId: %s", d.Detail)
	}
	for _, body := range [][]byte{
		doc("replay words", "a&#0;", "R", "H"),
		doc("S3 words", "a&#x0;", "R", "H"),
		bytes.Replace(doc("S3 words", "a&#0;", "R", "H"), []byte("<HostId>H</HostId>"), nil, 1),
		[]byte(`<Error><Code>InvalidToken</Code></Error>`),
	} {
		e2.Body = body
		if d := s3Live(e1, e2, "b", "", ""); d.OK {
			t.Errorf("%s compared equal", body)
		}
	}
}

// TestGCSLiveComparatorMetadata: with a native fixture the stored fields
// are compared exactly (timeCreated where the replay lists it); with a
// synthetic one the replay's fixed values are required and the
// provider's are not compared.
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
	// A recording that stored timeCreated lists it, compared as an instant
	// with six fractional digits; one that did not lists none.
	withCreated := func(v string) *response {
		return &response{200, h, page(strings.Replace(item("CJ+", "17", "bWQ="), `"updated"`, `"timeCreated":"`+v+`","updated"`, 1))}
	}
	if d := gcsLive(live, withCreated("2026-01-01T00:00:00.000000Z"), "b", false, gcsNative); !d.OK {
		t.Errorf("native timeCreated: %s", d.Detail)
	}
	if d := gcsLive(live, withCreated("2026-01-01T00:00:00.001000Z"), "b", false, gcsNative); d.OK || !strings.Contains(d.Detail, "timeCreated 0") {
		t.Errorf("another timeCreated passed: %+v", d)
	}
	if d := gcsLive(live, withCreated("2026-01-01T00:00:00Z"), "b", false, gcsNative); d.OK || !strings.Contains(d.Detail, "six UTC fractional digits") {
		t.Errorf("a timeCreated without six digits passed: %+v", d)
	}
	if d := gcsLive(live, withCreated("2026-01-01T00:00:00.000000Z"), "b", false, gcsSynthetic); d.OK {
		t.Error("a synthetic profile listing timeCreated passed")
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

func TestReadPins(t *testing.T) {
	path := t.TempDir() + "/pins.json"
	if err := os.WriteFile(path, []byte(`{"gcs-01a":"match","gcs-03p":{"mismatch":"GCS HTTP status differs: expected=200 actual=400"}}`), 0o644); err != nil {
		t.Fatal(err)
	}
	pins, err := readPins(path)
	if err != nil || len(pins) != 1 || pins["gcs-03p"] != "GCS HTTP status differs: expected=200 actual=400" {
		t.Errorf("%v %v", pins, err)
	}
	// A pinned mismatch is a documented difference, listed apart; any
	// other difference of the same case fails.
	lr := &liveRun{d: &driver{rep: &report{}}, protocol: "gcs", pins: pins}
	res := result{Name: "gcs-03p"}
	ref, got := &response{Status: 200}, &response{Status: 400}
	lr.finish(&res, judgement{protocol: "gcs", caseID: "gcs-03p", ref: ref, got: got, df: diff{Detail: pins["gcs-03p"]}}, nil)
	other := result{Name: "gcs-03p"}
	lr.finish(&other, judgement{protocol: "gcs", caseID: "gcs-03p", ref: ref, got: got,
		df: diff{Detail: "GCS HTTP status differs: expected=200 actual=500"}}, nil)
	if r := lr.d.rep.Results; len(r) != 1 || r[0].Pass || lr.d.rep.Failed != 1 {
		t.Errorf("%+v", r)
	}
	if r := lr.d.rep.Residuals; len(r) != 1 || !strings.Contains(r[0], "pins this mismatch for gcs-03p") {
		t.Errorf("%q", r)
	}
}

// TestListScopeDecodesListedNames: discovery reads the names of an
// encoding-type=url listing in the response grammar, where a space is
// '+' and a '+' is %2B, so names with spaces beside names with literal
// plus signs (and the escape-like "%20" and "%2B" as text) come back as
// stored, in rising byte order, and a scope with a space finds its
// subtree.
func TestListScopeDecodesListedNames(t *testing.T) {
	keys := []string{"a b/1", "a b/2", "a!/z", "a%20b/y", "a%2Bb/x", "a+b/1", "a+b/c d+e", "data/x"}
	got := &target{name: "go", base: startGoProtocols(t, []string{server.ProtocolS3}, keys...), client: &http.Client{}}
	names, _, err := listScope(got, "s3", "bkt", "")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(names, "|") != strings.Join(keys, "|") {
		t.Fatalf("listed %q, want %q", names, keys)
	}
	if sub := under(names, "a b/"); len(sub) != 2 || sub[0] != "a b/1" {
		t.Errorf("under a b/: %q", sub)
	}
	scoped, _, err := listScope(got, "s3", "bkt", "a b/")
	if err != nil || strings.Join(scoped, "|") != "a b/1|a b/2" {
		t.Errorf("scope a b/: %q %v", scoped, err)
	}
}

// TestGCSLiveRejectsTrailingData: a JSON answer is one object and
// whitespace: trailing whitespace is accepted; an additional value, a
// malformed suffix or a stray closing brace after the object fails the
// comparison, on either side.
func TestGCSLiveRejectsTrailingData(t *testing.T) {
	h := http.Header{"Content-Type": {"application/json; charset=UTF-8"}}
	const page = `{"kind":"storage#objects"}`
	ref := &response{200, h, []byte(page)}
	for _, suffix := range []string{"", " ", "\n", " \r\n\t "} {
		if d := gcsLive(ref, &response{200, h, []byte(page + suffix)}, "b", false, gcsNative); !d.OK {
			t.Errorf("%q: %s", suffix, d.Detail)
		}
	}
	for _, suffix := range []string{"garbage", "{}", `"x"`, " 1", "}", "]", " ,", "\x00"} {
		if d := gcsLive(ref, &response{200, h, []byte(page + suffix)}, "b", false, gcsNative); d.OK || !strings.Contains(d.Detail, "trailing data") {
			t.Errorf("got side %q: %+v", suffix, d)
		}
		if d := gcsLive(&response{200, h, []byte(page + suffix)}, ref, "b", false, gcsNative); d.OK {
			t.Errorf("reference side %q passed", suffix)
		}
	}
}

// TestDocumentedDifferences: each documented difference is matched by
// the request's shape and both answers' shapes, so the same request
// answered any other way, or a near request, still fails.
func TestDocumentedDifferences(t *testing.T) {
	xmlHeader := func() http.Header { return http.Header{"Content-Type": {"application/xml"}} }
	page := func(extra, contents string) *response {
		return &response{Status: 200, Header: xmlHeader(), Body: []byte(`<?xml version="1.0" encoding="UTF-8"?>` + "\n" +
			`<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>bkt</Name><Prefix>a/</Prefix>` + extra +
			`<KeyCount>1</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>` + contents + `</ListBucketResult>`)}
	}
	contents := func(key, owner string) string {
		return `<Contents><Key>` + key + `</Key><LastModified>2026-01-02T03:04:05.000Z</LastModified><ETag>"e"</ETag><Size>1</Size>` +
			owner + `<StorageClass>STANDARD</StorageClass></Contents>`
	}
	v1 := &response{Status: 200, Header: xmlHeader(), Body: []byte(`<?xml version="1.0" encoding="UTF-8"?>` + "\n" +
		`<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>bkt</Name><Prefix>a/</Prefix><Marker>a/0</Marker>` +
		`<MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>` + contents("a/1", "") + `</ListBucketResult>`)}
	refusal := func(arg string) *response {
		return &response{Status: 400, Header: xmlHeader(), Body: s3.AppendError(nil, &s3.Error{Status: 400, Code: "InvalidArgument",
			Message: "m", Details: []s3.Detail{{Name: "ArgumentName", Value: arg}}})}
	}
	owner := `<Owner><ID>abc</ID></Owner>`
	gcsJSON := func(status int, body string) *response {
		return &response{Status: status, Header: http.Header{"Content-Type": {"application/json; charset=UTF-8"}}, Body: []byte(body)}
	}
	gcsPage := gcsJSON(200, `{"kind":"storage#objects"}`)
	gcsRefusal := func(reason string) *response {
		return gcsJSON(400, `{"error":{"code":400,"message":"m","errors":[{"domain":"global","reason":"`+reason+`","message":"m"}]}}`)
	}
	for _, c := range []struct {
		name, protocol, query, mutation string
		ref, got                        *response
		want                            string // a prefix of the reason; "" for not documented
	}{
		{"V1 refused", "s3", "prefix=a/&marker=a/0", "", v1, refusal("list-type"), "ListObjects V1"},
		{"V1 list-type=3", "s3", "list-type=3&prefix=a/", "", v1, refusal("list-type"), "ListObjects V1"},
		{"V1 refused otherwise", "s3", "prefix=a/&marker=a/0", "", v1, refusal("marker"), ""},
		{"V1 answered", "s3", "prefix=a/&marker=a/0", "", v1, page("", contents("a/1", "")), ""},
		{"V2 refused", "s3", "list-type=2&prefix=a/", "", page("", contents("a/1", "")), refusal("list-type"), ""},
		{"owner missing", "s3", "list-type=2&prefix=a/&fetch-owner=true", "", page("", contents("a/1", owner)), page("", contents("a/1", "")), "fetch-owner=true over a recording"},
		{"owner missing, key differs", "s3", "list-type=2&prefix=a/&fetch-owner=true", "", page("", contents("a/1", owner)), page("", contents("a/2", "")), ""},
		{"owner missing, not asked exactly", "s3", "list-type=2&prefix=a/&fetch-owner=TRUE", "", page("", contents("a/1", owner)), page("", contents("a/1", "")), ""},
		{"owner where none asked", "s3", "list-type=2&prefix=a/", "", page("", contents("a/1", owner)), page("", contents("a/1", "")), ""},
		{"decoded flip refused", "s3", "list-type=2&prefix=a/", "flip-decoded", page("", contents("a/1", "")), refusal("continuation-token"), "an issued token with a decoded byte flipped"},
		{"decoded flip answered", "s3", "list-type=2&prefix=a/", "flip-decoded", page("", contents("a/1", "")), page("", contents("a/2", "")), ""},
		{"last character flip refused", "s3", "list-type=2&prefix=a/", "flip", page("", contents("a/1", "")), refusal("continuation-token"), ""},
		{"softDeleted refused", "gcs", "prefix=a/&softDeleted=true", "", gcsPage, gcsRefusal("invalid"), "softDeleted=true"},
		{"softDeleted refused otherwise", "gcs", "prefix=a/&softDeleted=true", "", gcsPage, gcsRefusal("required"), ""},
		{"softDeleted=false refused", "gcs", "prefix=a/&softDeleted=false", "", gcsPage, gcsRefusal("invalid"), ""},
	} {
		var df diff
		if c.protocol == "gcs" {
			df = gcsLive(c.ref, c.got, "bkt", false, gcsSynthetic)
		} else {
			df = s3Live(c.ref, c.got, "bkt", "", "")
		}
		if df.OK {
			t.Fatalf("%s: the answers agree", c.name)
		}
		why := documentedDifference(judgement{protocol: c.protocol, bucket: "bkt", query: c.query, mutation: c.mutation,
			ref: c.ref, got: c.got, df: df}, nil)
		if c.want == "" && why != "" || !strings.HasPrefix(why, c.want) {
			t.Errorf("%s: %q (%s)", c.name, why, df.Detail)
		}
	}
}
