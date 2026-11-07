// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package evidence

import (
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
)

// The GCS profile comparator: the field policy a replay page is held to
// against the provider's page, independent of the replay code. Its checks
// and their messages are the conformance suite's (the pins in
// replay-expectations.json name a mismatch by the start of its message),
// so every check below keeps the suite's order and wording.

// mismatch is a comparison failure; its message becomes the outcome.
type mismatch struct{ msg string }

func (m *mismatch) Error() string { return m.msg }

func fail(format string, args ...any) { panic(&mismatch{fmt.Sprintf(format, args...)}) }

func require(ok bool, msg string) {
	if !ok {
		fail("%s", msg)
	}
}

// javaString renders a compared value the way the suite's messages do:
// null, booleans, numbers and text as themselves, a list as [a, b], an
// instant in ISO form with the fraction in groups of three digits.
func javaString(v any) string {
	switch x := v.(type) {
	case nil:
		return "null"
	case *string:
		if x == nil {
			return "null"
		}
		return *x
	case []string:
		return "[" + strings.Join(x, ", ") + "]"
	case time.Time:
		s := x.UTC().Format("2006-01-02T15:04:05")
		switch ns := x.Nanosecond(); {
		case ns == 0:
		case ns%1_000_000 == 0:
			s += fmt.Sprintf(".%03d", ns/1_000_000)
		case ns%1_000 == 0:
			s += fmt.Sprintf(".%06d", ns/1_000)
		default:
			s += fmt.Sprintf(".%09d", ns)
		}
		return s + "Z"
	}
	return fmt.Sprint(v)
}

func equal(label string, expected, actual any) {
	same := false
	switch e := expected.(type) {
	case []string:
		same = slices.Equal(e, actual.([]string))
	case time.Time:
		same = e.Equal(actual.(time.Time))
	case *string:
		a := actual.(*string)
		same = (e == nil) == (a == nil) && (e == nil || *e == *a)
	default:
		same = expected == actual
	}
	if !same {
		fail("%s differs: expected=%s actual=%s", label, javaString(expected), javaString(actual))
	}
}

func strp(s string) *string { return &s }

// exchange is one HTTP answer: status, headers by lower-case name, body.
type exchange struct {
	status  int
	headers map[string][]string
	body    []byte
}

func newExchange(status int, raw [][2]string, body []byte) exchange {
	h := map[string][]string{}
	for _, kv := range raw {
		h[strings.ToLower(kv[0])] = append(h[strings.ToLower(kv[0])], kv[1])
	}
	return exchange{status, h, body}
}

func (e exchange) single(name string) *string {
	v, ok := e.headers[name]
	if !ok {
		return nil
	}
	if len(v) != 1 {
		fail("duplicate HTTP header %s", name)
	}
	return &v[0]
}

var (
	gcsHeaders = map[string]bool{"content-type": true, "date": true, "x-goog-request-id": true,
		"content-length": true, "server": true, "cache-control": true, "vary": true, "transfer-encoding": true,
		"x-swath-replay-error": true, "x-guploader-uploadid": true, "x-goog-gcs-base-ts": true, "expires": true,
		"alt-svc": true, "pragma": true}
	nonemptyHeaders = []string{"server", "cache-control", "transfer-encoding", "x-swath-replay-error",
		"x-guploader-uploadid", "x-goog-gcs-base-ts", "alt-svc", "pragma"}
	httpDate = regexp.MustCompile(`^[A-Za-z]{3}, [0-9]{2} [A-Za-z]{3} [0-9]{4} [0-9]{2}:[0-9]{2}:[0-9]{2} GMT$`)
)

func checkHeaders(e exchange, allowed map[string]bool, contentType string) {
	for _, name := range slices.Sorted(func(yield func(string) bool) {
		for k := range e.headers {
			if !yield(k) {
				return
			}
		}
	}) {
		if !allowed[name] {
			fail("unclassified HTTP header %s", name)
		}
	}
	ct := e.single("content-type")
	if ct == nil || strings.TrimSpace(strings.SplitN(strings.ToLower(*ct), ";", 2)[0]) != contentType {
		fail("HTTP Content-Type differs from %s", contentType)
	}
	if l := e.single("content-length"); l != nil {
		n, err := strconv.ParseInt(*l, 10, 64)
		if err != nil {
			fail("HTTP Content-Length is malformed")
		}
		if n < 0 || n != int64(len(e.body)) {
			fail("HTTP Content-Length differs from body")
		}
	}
	for _, name := range []string{"date", "expires"} {
		if d := e.single(name); d != nil {
			if !httpDate.MatchString(*d) {
				fail("HTTP %s spelling is not padded GMT", name)
			}
			if _, err := time.Parse(time.RFC1123, *d); err != nil {
				fail("HTTP %s is not an RFC 1123 date", name)
			}
		}
	}
	for _, id := range []string{"x-goog-request-id", "x-ms-request-id", "x-amz-request-id", "x-amz-id-2"} {
		if _, ok := e.headers[id]; ok && *e.single(id) == "" {
			fail("%s must be nonempty", id)
		}
	}
	for _, name := range nonemptyHeaders {
		if _, ok := e.headers[name]; ok && *e.single(name) == "" {
			fail("%s must be nonempty", name)
		}
	}
	for _, v := range e.headers["vary"] {
		if v == "" {
			fail("vary must be nonempty")
		}
	}
}

func readJSON(b []byte) *node {
	n, err := parseNode(b)
	if err != nil {
		fail("%v", err)
	}
	return n
}

// assertGCS compares one exchange: status, classified headers, then the
// page or the error envelope's first reason.
func assertGCS(native, replay exchange, nativeBucket, replayBucket string, full bool) {
	equal("GCS HTTP status", native.status, replay.status)
	checkHeaders(native, gcsHeaders, "application/json")
	checkHeaders(replay, gcsHeaders, "application/json")
	if native.status == 200 {
		assertPage(native.body, replay.body, nativeBucket, replayBucket, full)
		return
	}
	equal("GCS error reason", gcsErrorReason(native), gcsErrorReason(replay))
}

func gcsErrorReason(e exchange) string {
	root := readJSON(e.body)
	if root.kind != 'o' || len(root.keys) != 1 || !root.has("error") {
		fail("unknown GCS error envelope field")
	}
	er := root.get("error")
	for _, f := range er.keys {
		if f != "code" && f != "message" && f != "errors" {
			fail("unknown GCS error field %s", f)
		}
	}
	code, ok := er.get("code").integral()
	if er.kind != 'o' || !ok || code != int64(e.status) || er.get("message").kind != 's' {
		fail("invalid GCS error envelope")
	}
	errs := er.get("errors")
	if errs.kind != 'a' || len(errs.elems) == 0 || errs.elems[0].get("reason").kind != 's' {
		fail("GCS error reason missing")
	}
	for _, d := range errs.elems {
		if d.kind != 'o' {
			fail("GCS error detail must be an object")
		}
		for _, f := range d.keys {
			if !slices.Contains([]string{"domain", "reason", "message", "location", "locationType"}, f) {
				fail("unknown GCS error detail field %s", f)
			}
		}
	}
	return errs.elems[0].get("reason").str
}

var (
	pageFields       = []string{"kind", "items", "prefixes", "nextPageToken"}
	replayItemFields = []string{"kind", "bucket", "name", "size", "updated", "etag", "generation", "metageneration", "storageClass", "contentType", "acl"}
	nativeOnlyFields = []string{"id", "selfLink", "mediaLink", "timeCreated", "md5Hash", "crc32c", "owner", "metadata",
		"timeStorageClassUpdated", "componentCount", "cacheControl", "contentEncoding", "contentDisposition",
		"contentLanguage", "kmsKeyName", "customerEncryption", "temporaryHold", "eventBasedHold",
		"retentionExpirationTime", "customTime", "softDeleteTime", "hardDeleteTime", "timeFinalized"}
	sixDigitUTC = regexp.MustCompile(`^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}Z$`)
	decimalText = regexp.MustCompile(`^(0|[1-9][0-9]*)$`)
)

func readPage(body []byte, replay bool) *node {
	page := readJSON(body)
	checkPage(page, replay)
	items(page)
	prefixes(page)
	pageToken(page)
	return page
}

func checkPage(page *node, replay bool) {
	if page.kind != 'o' {
		fail("GCS page must be an object")
	}
	label := "native page"
	if replay {
		label = "replay page"
	}
	fields(page, pageFields, label)
	equal("page kind", "storage#objects", text(page, "kind"))
	if replay && page.has("items") && len(page.get("items").elems) == 0 {
		fail("replay must omit empty items array")
	}
	if replay && page.has("prefixes") && len(page.get("prefixes").elems) == 0 {
		fail("replay must omit empty prefixes array")
	}
}

func items(page *node) []*node {
	a := page.get("items")
	if a == missing {
		return nil
	}
	if a.kind != 'a' {
		fail("GCS items must be an array")
	}
	return a.elems
}

func prefixes(page *node) []string {
	a := page.get("prefixes")
	if a == missing {
		return []string{}
	}
	if a.kind != 'a' {
		fail("GCS prefixes must be an array")
	}
	out := []string{}
	seen := map[string]bool{}
	for _, v := range a.elems {
		if v.kind != 's' || seen[v.str] {
			fail("invalid or duplicate GCS prefix")
		}
		seen[v.str] = true
		out = append(out, v.str)
	}
	return out
}

func pageToken(page *node) *string {
	t := page.get("nextPageToken")
	if t == missing {
		return nil
	}
	if t.kind != 's' || t.str == "" {
		fail("invalid GCS nextPageToken")
	}
	return &t.str
}

func fields(n *node, allowed []string, label string) {
	if n.kind != 'o' {
		fail("%s must be an object", label)
	}
	for _, f := range n.keys {
		if !slices.Contains(allowed, f) {
			fail("%s has unknown field %s", label, f)
		}
	}
}

func text(n *node, name string) string {
	v := n.get(name)
	if v.kind != 's' {
		fail("%s must be text", name)
	}
	return v.str
}

func decimal(n *node, name string) string {
	v := text(n, name)
	if !decimalText.MatchString(v) {
		fail("%s must be a decimal string", name)
	}
	return v
}

func instant(n *node, name string) time.Time {
	t, err := time.Parse(time.RFC3339Nano, text(n, name))
	if err != nil {
		fail("%s must be RFC3339 timestamp", name)
	}
	return t
}

func assertPage(nativeBody, replayBody []byte, nativeBucket, replayBucket string, full bool) {
	nativePage, replayPage := readPage(nativeBody, false), readPage(replayBody, true)
	equal("page kind", text(nativePage, "kind"), text(replayPage, "kind"))
	ni, ri := items(nativePage), items(replayPage)
	equal("item count", len(ni), len(ri))
	for i := range ni {
		n, r := ni[i], ri[i]
		fields(n, append(slices.Clone(replayItemFields), nativeOnlyFields...), "native item")
		fields(r, replayItemFields, "replay item")
		equal("item kind "+strconv.Itoa(i), "storage#object", text(n, "kind"))
		equal("replay item kind "+strconv.Itoa(i), "storage#object", text(r, "kind"))
		equal("native bucket "+strconv.Itoa(i), nativeBucket, text(n, "bucket"))
		equal("replay bucket "+strconv.Itoa(i), replayBucket, text(r, "bucket"))
		equal("name "+strconv.Itoa(i), text(n, "name"), text(r, "name"))
		equal("size "+strconv.Itoa(i), decimal(n, "size"), decimal(r, "size"))
		if !sixDigitUTC.MatchString(text(r, "updated")) {
			fail("replay updated must use six UTC fractional digits")
		}
		equal("updated "+strconv.Itoa(i), instant(n, "updated"), instant(r, "updated"))
		if text(r, "etag") == "" {
			fail("etag must be nonempty")
		}
		equal("generation "+strconv.Itoa(i), "1", decimal(r, "generation"))
		equal("metageneration "+strconv.Itoa(i), "1", decimal(r, "metageneration"))
		equal("storageClass "+strconv.Itoa(i), "STANDARD", text(r, "storageClass"))
		equal("contentType "+strconv.Itoa(i), "application/octet-stream", text(r, "contentType"))
		if text(n, "etag") == "" {
			fail("etag must be nonempty")
		}
		decimal(n, "generation")
		decimal(n, "metageneration")
		if n.has("storageClass") {
			text(n, "storageClass")
		}
		if n.has("contentType") {
			text(n, "contentType")
		}
		if full {
			if acl := r.get("acl"); acl.kind != 'a' || len(acl.elems) != 0 {
				fail("replay full ACL must be an empty array")
			}
		} else if r.has("acl") {
			fail("replay noAcl projection must omit acl")
		}
		if r.has("timeCreated") || r.has("owner") || r.has("crc32c") {
			fail("replay contains profile-omitted native metadata")
		}
	}
	equal("prefixes", prefixes(nativePage), prefixes(replayPage))
	equal("terminal pagination", pageToken(nativePage) == nil, pageToken(replayPage) == nil)
}

// capturedPage is one page of a walk: the token its request carried (as
// sent in the query) and the answer.
type capturedPage struct {
	requestToken *string
	exchange     exchange
}

// assertGCSWalk compares two complete walks, each following its own
// tokens: every page a 200 with classified headers, each token chain
// unbroken and terminal, then the merged inventories as one page.
func assertGCSWalk(nativePages, replayPages []capturedPage, nativeBucket, replayBucket string, full bool, maxPages, maxEntries int) {
	for _, p := range nativePages {
		requireSuccessHeaders(p.exchange)
	}
	for _, p := range replayPages {
		requireSuccessHeaders(p.exchange)
	}
	n := aggregate(nativePages, false, maxPages, maxEntries)
	r := aggregate(replayPages, true, maxPages, maxEntries)
	assertPage(n.encode(nil), r.encode(nil), nativeBucket, replayBucket, full)
}

func requireSuccessHeaders(e exchange) {
	if e.status != 200 {
		fail("walk page has non-200 HTTP status")
	}
	checkHeaders(e, gcsHeaders, "application/json")
}

func aggregate(pages []capturedPage, replay bool, maxPages, maxEntries int) *node {
	if len(pages) == 0 || len(pages) > maxPages || maxEntries < 0 {
		fail("GCS walk page bound violated")
	}
	merged := &node{kind: 'o', fields: map[string]*node{}}
	merged.set("kind", &node{kind: 's', str: "storage#objects"})
	all := &node{kind: 'a'}
	pre := &node{kind: 'a'}
	emitted := map[string]bool{}
	var expected *string
	seen := map[string]bool{}
	for i, p := range pages {
		equal("GCS request token at page "+strconv.Itoa(i), expected, p.requestToken)
		page := readPage(p.exchange.body, replay)
		all.elems = append(all.elems, items(page)...)
		for _, pr := range prefixes(page) {
			if emitted[pr] {
				fail("cross-page repeated GCS prefix is unsupported by replay profile")
			}
			emitted[pr] = true
			pre.elems = append(pre.elems, &node{kind: 's', str: pr})
		}
		if len(all.elems)+len(pre.elems) > maxEntries {
			fail("GCS walk entry bound violated")
		}
		expected = pageToken(page)
		if i+1 < len(pages) {
			if expected == nil || seen[*expected] {
				fail("GCS walk stopped early or repeated token")
			}
			seen[*expected] = true
		}
	}
	if expected != nil {
		fail("GCS walk ended with nonterminal token")
	}
	if len(all.elems) > 0 {
		merged.set("items", all)
	}
	if len(pre.elems) > 0 {
		merged.set("prefixes", pre)
	}
	return merged
}
