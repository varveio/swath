// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package evidence

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// The conformance suite's own tests of its comparator and receipt checks,
// held here against the Go port of them.

const (
	profileNative = `{"kind":"storage#objects","items":[{"kind":"storage#object","bucket":"live",
 "name":"a/1","size":"12","updated":"2026-01-01T00:00:00.123Z","etag":"native-etag",
 "generation":"42","metageneration":"3","storageClass":"STANDARD","contentType":"text/plain",
 "id":"live/a/1/42","timeCreated":"2025-12-01T00:00:00Z"}],"prefixes":["b/"]}`
	profileReplay = `{"kind":"storage#objects","items":[{"kind":"storage#object","bucket":"replay",
 "name":"a/1","size":"12","updated":"2026-01-01T00:00:00.123000Z",
 "etag":"synthetic","generation":"1","metageneration":"1","storageClass":"STANDARD",
 "contentType":"application/octet-stream"}],"prefixes":["b/"]}`
	httpDateValue = "Wed, 21 Oct 2026 17:24:08 GMT"
)

// catch runs f and returns the mismatch it raised, "" for none.
func catch(f func()) (msg string) {
	defer func() {
		if r := recover(); r != nil {
			msg = r.(*mismatch).msg
		}
	}()
	f()
	return ""
}

func comparePages(native, replay string, full bool) string {
	return catch(func() { assertPage([]byte(native), []byte(replay), "live", "replay", full) })
}

func TestProfileComparatorAcceptsAndRefuses(t *testing.T) {
	if msg := comparePages(profileNative, profileReplay, false); msg != "" {
		t.Fatalf("the profile pair: %s", msg)
	}
	for _, c := range []struct {
		native, replay, want string
	}{
		{profileNative, strings.Replace(profileReplay, `"size":"12"`, `"size":"13"`, 1), "size 0 differs"},
		{profileNative, strings.Replace(profileReplay, `"size":"12"`, `"size":12`, 1), "size must be text"},
		{profileNative, strings.Replace(profileReplay, `"name":"a/1"`, `"name":"a/2"`, 1), "name 0 differs"},
		{profileNative, strings.Replace(profileReplay, `"etag":"synthetic"`, `"etag":"synthetic","owner":"x"`, 1), "replay item has unknown field owner"},
		{profileNative, strings.Replace(profileReplay, `"contentType":"application/octet-stream"`, `"contentType":"application/octet-stream","futureField":true`, 1), "unknown field"},
		{profileNative, strings.Replace(profileReplay, `"generation":"1"`, `"generation":"2"`, 1), "generation 0 differs"},
		{profileNative, strings.Replace(profileReplay, `"bucket":"replay"`, `"bucket":"wrong"`, 1), "replay bucket 0 differs"},
		{profileNative, strings.Replace(profileReplay, "00.123000Z", "00.124000Z", 1), "updated 0 differs"},
		{strings.Replace(profileNative, `"name":"a/1"`, `"name":"a/1","surprise":1`, 1), profileReplay, "native item has unknown field surprise"},
		{profileNative, strings.Replace(profileReplay, `"prefixes":["b/"]`, `"prefixes":["b/","b/"]`, 1), "invalid or duplicate GCS prefix"},
		{profileNative, strings.Replace(profileReplay, `"prefixes":["b/"]`, `"prefixes":["b/"],"nextPageToken":"opaque"`, 1), "terminal pagination differs"},
		{profileNative, strings.Replace(profileReplay, `"name":"a/1"`, `"name":"a/1","name":"a/1"`, 1), "duplicate field"},
		{profileNative, profileReplay + " {}", "trailing token"},
	} {
		if msg := comparePages(c.native, c.replay, false); !strings.Contains(msg, c.want) {
			t.Errorf("%q, want %q", msg, c.want)
		}
	}
}

func TestFullProjectionRequiresEmptyACLAndNoACLForbidsIt(t *testing.T) {
	withACL := strings.Replace(profileReplay, `"contentType":"application/octet-stream"`, `"contentType":"application/octet-stream","acl":[]`, 1)
	if msg := comparePages(profileNative, withACL, true); msg != "" {
		t.Error(msg)
	}
	if msg := comparePages(profileNative, withACL, false); msg != "replay noAcl projection must omit acl" {
		t.Error(msg)
	}
	if msg := comparePages(profileNative, profileReplay, true); msg != "replay full ACL must be an empty array" {
		t.Error(msg)
	}
}

func walkItem(bucket, name string, replay bool) string {
	gen := "2"
	if replay {
		gen = "1"
	}
	return `{"kind":"storage#object","bucket":"` + bucket + `","name":"` + name + `","size":"12",` +
		`"updated":"2026-01-01T00:00:00.123000Z","etag":"x","generation":"` + gen + `","metageneration":"1",` +
		`"storageClass":"STANDARD","contentType":"application/octet-stream"}`
}

func walkPage(items, token string) []byte {
	if token == "" {
		return []byte(`{"kind":"storage#objects","items":[` + items + `]}`)
	}
	return []byte(`{"kind":"storage#objects","items":[` + items + `],"nextPageToken":"` + token + `"}`)
}

func jsonExchange(status int, contentType string, body []byte) exchange {
	return newExchange(status, [][2]string{{"Content-Type", contentType}}, body)
}

// TestWalksCompareInventoriesAcrossPageBoundaries: two native pages and
// one replay page with the same ordered inventory agree; a wrong request
// token, a repeated prefix across pages, a repeated token and a non-JSON
// content type do not.
func TestWalksCompareInventoriesAcrossPageBoundaries(t *testing.T) {
	native := []capturedPage{
		{nil, jsonExchange(200, "application/json", walkPage(walkItem("live", "a/1", false), "native-1"))},
		{strp("native-1"), jsonExchange(200, "application/json", walkPage(walkItem("live", "a/2", false), ""))},
	}
	replay := []capturedPage{{nil, jsonExchange(200, "application/json; charset=utf-8",
		walkPage(walkItem("replay", "a/1", true)+","+walkItem("replay", "a/2", true), ""))}}
	run := func(n, r []capturedPage) string {
		return catch(func() { assertGCSWalk(n, r, "live", "replay", false, 3, 10) })
	}
	if msg := run(native, replay); msg != "" {
		t.Fatal(msg)
	}
	wrong := []capturedPage{native[0], {strp("wrong"), native[1].exchange}}
	if msg := run(wrong, replay); !strings.Contains(msg, "GCS request token at page 1 differs") {
		t.Error(msg)
	}
	plain := []capturedPage{{nil, jsonExchange(200, "text/plain", replay[0].exchange.body)}}
	if msg := run(native, plain); !strings.Contains(msg, "Content-Type") {
		t.Error(msg)
	}
	prefixes := []capturedPage{
		{nil, jsonExchange(200, "application/json", []byte(`{"kind":"storage#objects","prefixes":["a/"],"nextPageToken":"native-1"}`))},
		{strp("native-1"), jsonExchange(200, "application/json", []byte(`{"kind":"storage#objects","prefixes":["a/"]}`))},
	}
	one := []capturedPage{{nil, jsonExchange(200, "application/json", []byte(`{"kind":"storage#objects","prefixes":["a/"]}`))}}
	if msg := run(prefixes, one); !strings.Contains(msg, "cross-page repeated GCS prefix") {
		t.Error(msg)
	}
	loop := []capturedPage{
		{nil, jsonExchange(200, "application/json", walkPage(walkItem("live", "a/1", false), "loop"))},
		{strp("loop"), jsonExchange(200, "application/json", walkPage(walkItem("live", "a/2", false), "loop"))},
		{strp("loop"), jsonExchange(200, "application/json", walkPage(walkItem("live", "a/3", false), ""))},
	}
	if msg := run(loop, replay); !strings.Contains(msg, "stopped early or repeated token") {
		t.Error(msg)
	}
	if msg := run(native, replay[:0]); !strings.Contains(msg, "page bound") {
		t.Error(msg)
	}
}

// TestExchangeHeadersAreClassified: the provider's transport and volatile
// headers are accepted when present and well formed; an empty Vary value,
// a malformed Expires, an empty classified header, an unclassified header
// and a Content-Length that is not the body's are refused.
func TestExchangeHeadersAreClassified(t *testing.T) {
	nativeBody := []byte(`{"error":{"code":400,"message":"native text","errors":[{"reason":"invalid"}]}}`)
	replayBody := []byte(`{"error":{"code":400,"message":"replay text","errors":[{"reason":"invalid"}]}}`)
	headers := map[string][]string{
		"Content-Type": {"application/json; charset=UTF-8"}, "Date": {httpDateValue}, "X-GUploader-UploadID": {"upload-id-digest"},
		"x-goog-gcs-base-ts": {"1729530000"}, "Expires": {"Mon, 01 Jan 1990 00:00:00 GMT"}, "Alt-Svc": {`h3=":443"`},
		"Pragma": {"no-cache"}, "Vary": {"Origin", "X-Origin"}, "Cache-Control": {"no-cache, no-store, max-age=0, must-revalidate"},
		"Server": {"UploadServer"},
	}
	build := func(h map[string][]string) exchange {
		var raw [][2]string
		for k, vs := range h {
			for _, v := range vs {
				raw = append(raw, [2]string{k, v})
			}
		}
		return newExchange(400, raw, nativeBody)
	}
	replay := jsonExchange(400, "application/json; charset=utf-8", replayBody)
	if msg := catch(func() { assertGCS(build(headers), replay, "live", "replay", false) }); msg != "" {
		t.Fatal(msg)
	}
	for _, c := range []struct {
		name, value, want string
	}{
		{"Vary", "", "vary must be nonempty"},
		{"Expires", "0", "HTTP expires spelling is not padded GMT"},
		{"x-goog-gcs-base-ts", "", "x-goog-gcs-base-ts must be nonempty"},
		{"x-new-header", "unclassified", "unclassified HTTP header x-new-header"},
		{"content-length", "999", "HTTP Content-Length differs from body"},
	} {
		h := map[string][]string{}
		for k, v := range headers {
			h[k] = v
		}
		if c.name == "Vary" {
			h["Vary"] = []string{"Origin", ""}
		} else {
			h[c.name] = []string{c.value}
		}
		if msg := catch(func() { assertGCS(build(h), replay, "live", "replay", false) }); msg != c.want {
			t.Errorf("%s: %q, want %q", c.name, msg, c.want)
		}
	}
	if msg := catch(func() {
		assertGCS(build(headers), jsonExchange(400, "application/json", []byte(`{"error":{"code":400,"message":"x","errors":[{"reason":"notFound"}]}}`)), "live", "replay", false)
	}); msg != "GCS error reason differs: expected=invalid actual=notFound" {
		t.Error(msg)
	}
	empty := []byte(`{"kind":"storage#objects"}`)
	ok := newExchange(200, [][2]string{{"content-type", "application/json"}, {"content-length", strconv.Itoa(len(empty))}}, empty)
	if msg := catch(func() {
		assertGCS(ok, jsonExchange(200, "application/json; charset=utf-8", empty), "live", "replay", false)
	}); msg != "" {
		t.Error(msg)
	}
}

// writeRunCapture writes an sdk_run capture of two pages under dir.
func writeRunCapture(t *testing.T, dir string) string {
	t.Helper()
	b64 := func(s []byte) string { j, _ := json.Marshal(s); return strings.Trim(string(j), `"`) }
	step := func(query string, body []byte) map[string]any {
		return map[string]any{"phase": "walk", "exchange": map[string]any{
			"request":  map[string]any{"method": "GET", "path": "/storage/v1/b/{bucket}/o", "query": query, "headers": []any{map[string]string{"name": "user-agent", "value": "gcloud-java/2.73.0"}}},
			"response": map[string]any{"status": 200, "headers": []any{map[string]string{"name": "content-type", "value": "application/json"}}, "body_base64": b64(body)},
		}}
	}
	doc := map[string]any{"schema_version": "provider-capture-run-v1", "evidence_kind": "sdk_run", "provider": "gcs", "probe_id": "gcs-09",
		"steps": []any{step("prefix=a", walkPage(walkItem("{bucket}", "a", false), "native-1")), step("prefix=a&pageToken=native-1", walkPage(walkItem("{bucket}", "b", false), ""))}}
	b, _ := json.Marshal(doc)
	path := filepath.Join(dir, "capture.json")
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

// TestSDKRunReceiptNeedsEveryStep: an SDK run is checked as a whole
// walk: the same inventory on other page boundaries passes; a walk that
// stops a page early, or whose inventory is duplicated, short or
// reordered, does not; nor does a request without the Go client's
// identity, or a run checked as one exchange.
func TestSDKRunReceiptNeedsEveryStep(t *testing.T) {
	c := readCapture(writeRunCapture(t, t.TempDir()))
	goClient := map[string][]string{"x-goog-api-client": {"gl-go/1.26 gdcl/0.311.0"}}
	rstep := func(query string, body []byte, headers map[string][]string) step {
		return step{"walk", request{method: "GET", path: gcsReplayPath, query: query + "&alt=json&prettyPrint=false", headers: headers},
			jsonExchange(200, "application/json; charset=utf-8", body)}
	}
	two := []step{
		rstep("prefix=a", walkPage(walkItem(replayBucket, "a", true), "local"), goClient),
		rstep("prefix=a&pageToken=local", walkPage(walkItem(replayBucket, "b", true), ""), goClient),
	}
	run := func(r []step) string { return catch(func() { verifySDKRun(c, r) }) }
	if msg := run(two); msg != "" {
		t.Fatal(msg)
	}
	if msg := run(two[:1]); !strings.Contains(msg, "nonterminal") {
		t.Errorf("one page early: %q", msg)
	}
	one := func(names ...string) []step {
		var items []string
		for _, n := range names {
			items = append(items, walkItem(replayBucket, n, true))
		}
		return []step{rstep("prefix=a", walkPage(strings.Join(items, ","), ""), goClient)}
	}
	if msg := run(one("a", "b")); msg != "" {
		t.Errorf("other page boundaries: %s", msg)
	}
	for _, names := range [][]string{{"a", "b", "b"}, {"a"}, {"b", "a"}} {
		if msg := run(one(names...)); msg == "" {
			t.Errorf("%v accepted", names)
		}
	}
	javaClient := []step{rstep("prefix=a", walkPage(walkItem(replayBucket, "a", true)+","+walkItem(replayBucket, "b", true), ""), map[string][]string{"x-goog-api-client": {"gl-java/25 gdcl/2.7.2"}})}
	if msg := run(javaClient); msg != "replay SDK product/version token differs" {
		t.Errorf("another client: %q", msg)
	}
	other := []step{rstep("prefix=b", walkPage(walkItem(replayBucket, "a", true)+","+walkItem(replayBucket, "b", true), ""), goClient)}
	if msg := run(other); msg != "replay request query differs from captured probe" {
		t.Errorf("another scope: %q", msg)
	}
	// A parameter the capture did not send is a different scope, empty
	// or not, known or not; only the Go client's alt=json and
	// prettyPrint=false are its own.
	for _, extra := range []string{"versions=", "unknownParam=", "alt=xml", "prettyPrint=true", "fields="} {
		r := []step{rstep("prefix=a&"+extra, walkPage(walkItem(replayBucket, "a", true)+","+walkItem(replayBucket, "b", true), ""), goClient)}
		if msg := run(r); msg != "replay request query differs from captured probe" {
			t.Errorf("%s: %q", extra, msg)
		}
	}
	if msg := catch(func() { verifyRun(c, two) }); msg != "GCS walk receipt needs a walk capture" {
		t.Errorf("as a walk: %q", msg)
	}
}

// TestRetryRunCapturesAreRefusedRatherThanWalked: a retry run has no
// verifier; it is refused before any request (port 0 has no server).
func TestRetryRunCapturesAreRefusedRatherThanWalked(t *testing.T) {
	path := filepath.Join(t.TempDir(), "gcs-10.json")
	if err := os.WriteFile(path, []byte(`{"schema_version":"provider-capture-run-v1","evidence_kind":"retry_run","provider":"gcs","probe_id":"gcs-10","steps":[{"phase":"walk","exchange":{"request":{"method":"GET","path":"/storage/v1/b/{bucket}/o","query":""},"response":{"status":503,"body_base64":""}}},{"phase":"walk","exchange":{"request":{"method":"GET","path":"/storage/v1/b/{bucket}/o","query":""},"response":{"status":200,"body_base64":""}}}]}`), 0o644); err != nil {
		t.Fatal(err)
	}
	e := &endpoint{port: 0, cases: map[string]caseRecord{"gcs-10": {ID: "gcs-10"}}, mutate: func(p []exchange) []exchange { return p }}
	if err := e.verify("gcs-10", path); err == nil || err.Error() != "no replay verifier for retry_run captures" {
		t.Errorf("%v", err)
	}
}
