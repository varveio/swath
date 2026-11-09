// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"

	"github.com/varveio/swath/go/internal/source/objhttp"
	"github.com/varveio/swath/go/internal/utf16order"
)

const (
	listHead = "\xef\xbb\xbf<?xml version=\"1.0\" encoding=\"utf-8\"?><EnumerationResults ServiceEndpoint=\"https://acct.blob.core.windows.net/\" ContainerName=\"c\"><Blobs>"
	listTail = "</Blobs><NextMarker /></EnumerationResults>"
)

// page is a listing body of blobs named names (BOM, declaration, an empty
// final marker unless next is set).
func page(next string, names ...string) string {
	var b strings.Builder
	b.WriteString(listHead)
	for _, n := range names {
		fmt.Fprintf(&b, "<Blob><Name>%s</Name><Properties><Creation-Time>Wed, 21 Oct 2026 17:27:04 GMT</Creation-Time>"+
			"<Last-Modified>Wed, 21 Oct 2026 17:27:05 GMT</Last-Modified><Etag>0x8DF2F9886625F76</Etag><Content-Length>5</Content-Length>"+
			"<Content-Type>application/octet-stream</Content-Type><Content-MD5>2KAlPH8W1jwF0SkOJToFzg==</Content-MD5>"+
			"<BlobType>BlockBlob</BlobType><AccessTier>Hot</AccessTier></Properties><OrMetadata /></Blob>", n)
	}
	if next == "" {
		b.WriteString(listTail)
	} else {
		b.WriteString("</Blobs><NextMarker>" + next + "</NextMarker></EnumerationResults>")
	}
	return b.String()
}

// azureError writes an Azure error answer: x-ms-error-code (when code is
// set) and the <Error> body.
func azureError(w http.ResponseWriter, status int, code string) {
	if code != "" {
		w.Header().Set(headerErrorCode, code)
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	fmt.Fprintf(w, "\xef\xbb\xbf<?xml version=\"1.0\" encoding=\"utf-8\"?><Error><Code>%s</Code><Message>m</Message></Error>", code)
}

func newTestClient(t *testing.T, cfg Config) *Client {
	t.Helper()
	if cfg.Account == "" {
		cfg.Account = "acct"
	}
	c, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

type failingCredential struct{ err error }

func (f failingCredential) GetToken(context.Context, policy.TokenRequestOptions) (azcore.AccessToken, error) {
	return azcore.AccessToken{}, f.err
}

// TestClassify: every Azure answer a listing request can end in has its
// class, through the client as a run meets it (one attempt, so nothing is
// retried away): an answer worth repeating comes back wrapped in
// ExhaustedError and counts as a throttle or a timeout where it is one.
// The error code decides before the status: AuthenticationFailed is a 403,
// ServerBusy a 503, OperationTimedOut a 500. Generic transport, timeout,
// exhaustion and cancellation behaviour is objhttptest.Contract's
// (TestRetryContract).
func TestClassify(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.Query().Get("prefix")
		if status, code, ok := strings.Cut(p, " "); ok {
			n, _ := strconv.Atoi(status)
			azureError(w, n, code)
			return
		}
		switch p {
		case "garbled":
			fmt.Fprint(w, listHead+"<Blob><Name>a")
		case "not-xml":
			fmt.Fprint(w, "\xef\xbb\xbfnull")
		case "error-envelope":
			fmt.Fprint(w, "\xef\xbb\xbf<?xml version=\"1.0\" encoding=\"utf-8\"?><Error><Code>InternalError</Code></Error>")
		case "bad-size":
			fmt.Fprint(w, listHead+"<Blob><Name>a</Name><Properties><Content-Length>x</Content-Length></Properties></Blob>"+listTail)
		case "descending":
			fmt.Fprint(w, page("", "b", "a"))
		case "redirect":
			http.Redirect(w, r, "/acct/other", http.StatusTemporaryRedirect)
		}
	}))
	defer srv.Close()
	for _, x := range []struct {
		status            int
		code              string
		class             objhttp.Class
		exhausted         bool // retried: the one attempt exhausts it
		throttle, timeout bool // counted
	}{
		{401, codeNoAuthentication, objhttp.ClassAuth, false, false, false},
		{403, codeAuthFailed, objhttp.ClassAuth, false, false, false},
		{400, codeInvalidAuthInfo, objhttp.ClassAuth, false, false, false},
		{401, "", objhttp.ClassAuth, false, false, false},
		{403, codeAuthorization, objhttp.ClassAccessDenied, false, false, false},
		{403, codePermissionMismatch, objhttp.ClassAccessDenied, false, false, false},
		{403, "AuthorizationSourceIPMismatch", objhttp.ClassAccessDenied, false, false, false},
		{404, codeContainerNotFound, objhttp.ClassNoSuchBucket, false, false, false},
		{503, codeServerBusy, objhttp.ClassThrottle, true, true, false},
		{429, "", objhttp.ClassThrottle, true, true, false},
		{500, codeOperationTimedOut, objhttp.ClassTimeout, true, false, true},
		{408, "", objhttp.ClassTimeout, true, false, true},
		{500, codeInternalError, objhttp.ClassServer, true, false, false},
		{502, "", objhttp.ClassServer, true, false, false},
		{503, "", objhttp.ClassServer, true, false, false},
		{504, "", objhttp.ClassServer, true, false, false},
		{500, "", objhttp.ClassServer, false, false, false},               // a 500 Azure does not ask to retry
		{501, "NotImplemented", objhttp.ClassServer, false, false, false}, // nor another 5xx
		{400, "InvalidQueryParameterValue", objhttp.ClassOther, false, false, false},
	} {
		c := newTestClient(t, Config{Endpoint: srv.URL, Anonymous: true, MaxAttempts: 1})
		_, err := c.List(context.Background(), &ListInput{Container: "c", Prefix: fmt.Sprintf("%d %s", x.status, x.code)})
		var ex *ExhaustedError
		n := c.Counters()
		if got := Classify(err); got != x.class || errors.As(err, &ex) != x.exhausted || (n.Throttles == 1) != x.throttle || (n.Timeouts == 1) != x.timeout {
			t.Errorf("%d %s: class %q exhausted=%v throttles %d timeouts %d (%v)", x.status, x.code, got, errors.As(err, &ex), n.Throttles, n.Timeouts, err)
		}
	}
	// Bodies: a 200 that is not a listing envelope is a parse failure,
	// retried; a parsed page out of order is invalid; a redirect is never
	// followed.
	c := newTestClient(t, Config{Endpoint: srv.URL, Anonymous: true, MaxAttempts: 1})
	for prefix, want := range map[string]objhttp.Class{"garbled": objhttp.ClassParse, "not-xml": objhttp.ClassParse,
		"error-envelope": objhttp.ClassParse, "bad-size": objhttp.ClassParse, "descending": objhttp.ClassInvalidPage, "redirect": objhttp.ClassRedirect} {
		_, err := c.List(context.Background(), &ListInput{Container: "c", Prefix: prefix})
		var ex *ExhaustedError
		if got := Classify(err); got != want || errors.As(err, &ex) != (want == objhttp.ClassParse) {
			t.Errorf("%s: class %q (%v), want %q", prefix, got, err, want)
		}
	}
	// Credentials: a refused grant, and token failures that may pass.
	resp := func(s int) *http.Response { return &http.Response{StatusCode: s} }
	for _, x := range []struct {
		name  string
		err   error
		class objhttp.Class
	}{
		{"refused grant", &azidentity.AuthenticationFailedError{RawResponse: resp(400)}, objhttp.ClassAuth},
		{"unauthorized client", &azidentity.AuthenticationFailedError{RawResponse: resp(401)}, objhttp.ClassAuth},
		{"token endpoint 503", &azidentity.AuthenticationFailedError{RawResponse: resp(503)}, objhttp.ClassTransport},
		{"token endpoint 429", &azidentity.AuthenticationFailedError{RawResponse: resp(429)}, objhttp.ClassTransport},
		{"no answer", &azidentity.AuthenticationFailedError{}, objhttp.ClassTransport},
		{"token endpoint unreachable", &net.OpError{Op: "dial", Err: errors.New("connection refused")}, objhttp.ClassTransport},
		{"token exchange timeout", fmt.Errorf("msal: %w", context.DeadlineExceeded), objhttp.ClassTimeout},
		{"credential unavailable", errors.New("EnvironmentCredential: missing environment variable AZURE_TENANT_ID"), objhttp.ClassAuth},
	} {
		e := newTestClient(t, Config{Endpoint: srv.URL, Credential: failingCredential{x.err}, MaxAttempts: 1})
		_, err := e.List(context.Background(), &ListInput{Container: "c"})
		var ce *CredentialError
		var ex *ExhaustedError
		if Classify(err) != x.class || !errors.As(err, &ce) || errors.As(err, &ex) != (x.class != objhttp.ClassAuth) {
			t.Errorf("%s: class %q exhausted=%v (%v), want %q", x.name, Classify(err), errors.As(err, &ex), err, x.class)
		}
	}
	// A shared key that is not base64 refuses the client.
	if _, err := New(context.Background(), Config{Account: "acct", SharedKey: "not base64!"}); Classify(err) != objhttp.ClassAuth {
		t.Errorf("bad key: class %q (%v)", Classify(err), err)
	}
	if got := Classify(nil); got != "" {
		t.Errorf("nil: class %q", got)
	}
}

// TestRetryAfter: a throttle's Retry-After (seconds or an HTTP date) holds
// the next attempt at least that long, past the drawn backoff.
func TestRetryAfter(t *testing.T) {
	now := time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)
	for v, want := range map[string]time.Duration{"": 0, "2": 2 * time.Second, "0": 0, "-1": 0, "x": 0,
		"Sun, 08 Nov 2026 12:00:03 GMT": 3 * time.Second, "Sun, 08 Nov 2026 11:59:00 GMT": 0,
		"99999999999999999999": time.Duration(math.MaxInt64 / time.Second * time.Second), "9223372036854775807": time.Duration(math.MaxInt64 / time.Second * time.Second),
		"Fri, 31 Dec 9999 23:59:59 GMT": time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC).Sub(now)} {
		if got := retryAfter(v, now); got != want {
			t.Errorf("Retry-After %q: %s, want %s", v, got, want)
		}
	}
	var n atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if n.Add(1) == 1 {
			w.Header().Set(headerRetryAfter, "1")
			azureError(w, 503, codeServerBusy)
			return
		}
		fmt.Fprint(w, page("", "a"))
	}))
	defer srv.Close()
	c := newTestClient(t, Config{Endpoint: srv.URL, Anonymous: true, BaseBackoff: time.Millisecond, MaxBackoff: time.Millisecond})
	start := time.Now()
	if _, err := c.List(context.Background(), &ListInput{Container: "c"}); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d < time.Second {
		t.Fatalf("retried after %s, before the 1 s Retry-After", d)
	}
	if got := c.Counters(); got.Attempts != 2 || got.Throttles != 1 || got.Retries != 1 {
		t.Fatalf("counters %+v", got)
	}
}

// TestInvalidPage: a parsed page that breaks the contract is an invalid
// page, not retried: names that do not ascend in UTF-16 order (byte order
// is not enough: U+E000 after U+1F600 ascends), a repeated name, a marker
// equal to the one sent. A truncated body (Azure has no truncation flag:
// an absent marker is the end) never reads as a shorter final page: it is
// a parse failure, retried.
func TestInvalidPage(t *testing.T) {
	var body atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, body.Load().(string))
	}))
	defer srv.Close()
	c := newTestClient(t, Config{Endpoint: srv.URL, Anonymous: true, MaxAttempts: 2, BaseBackoff: time.Millisecond})
	full := page("m2", "a", "b")
	for _, x := range []struct {
		name, body, marker string
		class              objhttp.Class
	}{
		{"descending", page("", "b", "a"), "", objhttp.ClassInvalidPage},
		{"repeated name", page("", "a", "a"), "", objhttp.ClassInvalidPage},
		{"byte order only", page("", "u\ue000", "u\U0001F600"), "", objhttp.ClassInvalidPage},
		{"repeated marker", page("m1", "a"), "m1", objhttp.ClassInvalidPage},
		{"truncated body", full[:len(full)-len("</EnumerationResults>")], "", objhttp.ClassParse},
		{"cut in an entry", full[:len(listHead)+20], "", objhttp.ClassParse},
		{"UTF-16 order", page("", "u\U0001F600", "u\ue000", "u\ufffd"), "", ""},
		{"a new marker", page("m2", "a"), "m1", ""},
	} {
		body.Store(x.body)
		p, err := c.List(context.Background(), &ListInput{Container: "c", Marker: x.marker})
		if got := Classify(err); got != x.class {
			t.Errorf("%s: class %q (%v), page %+v", x.name, got, err, p)
		}
		if x.class == objhttp.ClassInvalidPage && !errors.Is(err, ErrInvalidPage) {
			t.Errorf("%s: %v does not wrap ErrInvalidPage", x.name, err)
		}
	}
}

// TestCompareNames: compareNames is UTF-16 code-unit order on valid UTF-8,
// against the code units themselves.
func TestCompareNames(t *testing.T) {
	names := []string{"", "a", "a/", "a\x00", "ab", "\u007f", "\u0080", "\u07ff", "\u0800", "\ud7ff", "\ue000", "\uefff",
		"\uf000", "\ufffd", "\ufffe", "\uffff", "\U00010000", "\U0001F600", "\U0010FFFF", "x\U0001F600y", "x\ue000", "x\uffffz"}
	for _, a := range names {
		for _, b := range names {
			if got, want := utf16order.Compare(a, b), utf16Less(a, b); got != want {
				t.Errorf("utf16order.Compare(%+q, %+q) = %d, UTF-16 says %d", a, b, got, want)
			}
		}
	}
}

// TestEncodedNameDecoding: <Name Encoded="true"> is percent-decoded once
// with path semantics: '+' stays '+', %2B is '+', %25 is '%' (not decoded
// twice); control characters, U+FFFE and 4-byte scalars come through; a
// malformed escape or bytes that are not UTF-8 are an invalid page.
func TestEncodedNameDecoding(t *testing.T) {
	for _, c := range []struct{ wire, want string }{
		{"a+b", "a+b"},
		{"a%2Bb", "a+b"},
		{"a%2541", "a%41"},
		{"ctl%01", "ctl\x01"},
		{"x%EF%BF%BE", "x\ufffe"},
		{"%F0%9F%98%80", "\U0001F600"},
		{"probe%2Fu%2F%EF%BF%BF", "probe/u/\uffff"},
	} {
		got, err := decodeName(wireName{Encoded: true, Text: c.wire})
		if err != nil || got != c.want {
			t.Errorf("%q: %+q, %v; want %+q", c.wire, got, err, c.want)
		}
	}
	for _, bad := range []string{"a%2", "a%zz", "%", "%C3", "%FF%FE"} {
		if _, err := decodeName(wireName{Encoded: true, Text: bad}); !errors.Is(err, ErrInvalidPage) {
			t.Errorf("%q: %v, want an invalid page", bad, err)
		}
	}
	// Not encoded: the text as XML gave it, '%' and '+' literal.
	if got, _ := decodeName(wireName{Text: "a%2B+b"}); got != "a%2B+b" {
		t.Errorf("plain name %q", got)
	}
	// Through the parser, mixed with plain names and prefixes in wire order.
	body := listHead + `<Blob><Name>a+%</Name></Blob><BlobPrefix><Name Encoded="true">b%2B%25%01/</Name></BlobPrefix>` +
		`<Blob><Name Encoded="true">c%EF%BF%BE</Name></Blob><Blob><Name>d&amp;&#13;</Name></Blob>` + listTail
	p, err := parse(context.Background(), []byte(body))
	if err != nil {
		t.Fatal(err)
	}
	want := []Item{{Name: "a+%"}, {Name: "b+%\x01/", Prefix: true}, {Name: "c\ufffe"}, {Name: "d&\r"}}
	if len(p.Items) != len(want) {
		t.Fatalf("%+v", p.Items)
	}
	for i := range want {
		if p.Items[i] != want[i] {
			t.Errorf("item %d: %+v, want %+v", i, p.Items[i], want[i])
		}
	}
	if _, err := parse(context.Background(), []byte(listHead+`<Blob><Name Encoded="true">a%G1</Name></Blob>`+listTail)); !errors.Is(err, ErrInvalidPage) {
		t.Errorf("malformed escape through the parser: %v", err)
	}
}

// TestEnvelopeRetried: a 200 whose body is not a listing envelope (an
// <Error>, another root, no XML, nothing, an EnumerationResults without
// Blobs or with an Error inside, content after its close) is retried like
// a garbled body, counted as a parse failure, until a real page comes; an
// empty listing (an empty Blobs) is a legitimate final page.
func TestEnvelopeRetried(t *testing.T) {
	bad := []string{"", "\xef\xbb\xbf", "null", "\xef\xbb\xbf<?xml version=\"1.0\"?><Error><Code>InternalError</Code></Error>",
		"<Other/>", "\xef\xbb\xbf<?xml version=\"1.0\"?>",
		// the right root, but not a listing: no Blobs, an Error inside,
		// corruption after the close
		"\xef\xbb\xbf<EnumerationResults/>",
		"\xef\xbb\xbf<EnumerationResults><NextMarker /></EnumerationResults>",
		"\xef\xbb\xbf<EnumerationResults><Error><Code>InternalError</Code></Error></EnumerationResults>",
		listHead + "</Blobs><Error><Code>InternalError</Code></Error><NextMarker /></EnumerationResults>",
		listHead + "<Error><Code>InternalError</Code></Error>" + listTail,
		page("") + "<Blobs>",
		page("") + "garbage",
		page("") + "<EnumerationResults><Blobs/></EnumerationResults>"}
	var n atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("prefix") == "empty" {
			fmt.Fprint(w, page(""))
			return
		}
		if i := int(n.Add(1)) - 1; i < len(bad) {
			fmt.Fprint(w, bad[i])
			return
		}
		fmt.Fprint(w, page("", "a"))
	}))
	defer srv.Close()
	c := newTestClient(t, Config{Endpoint: srv.URL, Anonymous: true, MaxAttempts: len(bad) + 1, BaseBackoff: time.Millisecond, MaxBackoff: 2 * time.Millisecond})
	p, err := c.List(context.Background(), &ListInput{Container: "c"})
	if err != nil || len(p.Items) != 1 || p.Items[0].Name != "a" {
		t.Fatalf("page %+v, err %v", p, err)
	}
	if got := c.Counters(); got.ParseFailures != int64(len(bad)) || got.Retries != int64(len(bad)) {
		t.Fatalf("counters %+v, want %d parse failures retried", got, len(bad))
	}
	p, err = c.List(context.Background(), &ListInput{Container: "c", Prefix: "empty"})
	if err != nil || len(p.Items) != 0 || p.NextMarker != "" {
		t.Fatalf("empty listing: %+v, %v", p, err)
	}
}

// TestRedirectRefused: a 3xx is a redirect failure, never followed, also
// through a supplied HTTP client that would follow it; its message never
// quotes the target, which may carry the SAS.
func TestRedirectRefused(t *testing.T) {
	var n atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n.Add(1)
		http.Redirect(w, r, "https://elsewhere.example/acct/c?"+r.URL.RawQuery, http.StatusFound)
	}))
	defer srv.Close()
	for name, hc := range map[string]*http.Client{"built": nil, "supplied": {}} {
		n.Store(0)
		c := newTestClient(t, Config{Endpoint: srv.URL, SAS: "?sv=2026-10-06&sp=rl&sig=S3cr3t%2Bsig", HTTPClient: hc})
		_, err := c.List(context.Background(), &ListInput{Container: "c"})
		if Classify(err) != objhttp.ClassRedirect || n.Load() != 1 {
			t.Errorf("%s: class %q after %d requests (%v)", name, Classify(err), n.Load(), err)
		}
		if strings.Contains(err.Error(), "elsewhere.example") || strings.Contains(err.Error(), "S3cr3t") {
			t.Errorf("%s: %v", name, err)
		}
	}
}

// TestRequestShape: the one request the client sends: the container path
// under the account segment of a non-default endpoint, restype and comp,
// the other parameters only when set (spaces as %20, '+' as %2B), never
// endBefore or include; x-ms-version pinned, a fresh
// x-ms-client-request-id per attempt, Accept and the user agent.
func TestRequestShape(t *testing.T) {
	var mu sync.Mutex
	var reqs []*http.Request
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		reqs = append(reqs, r)
		first := len(reqs) == 1
		mu.Unlock()
		if first {
			azureError(w, 500, codeInternalError)
			return
		}
		fmt.Fprint(w, page(""))
	}))
	defer srv.Close()
	c := newTestClient(t, Config{Endpoint: srv.URL + "/", Anonymous: true, BaseBackoff: time.Millisecond})
	if _, err := c.List(context.Background(), &ListInput{Container: "c", Prefix: "a b+c/\u00e9", Delimiter: "/", StartFrom: "a b+c/\u00e9/x",
		Marker: "m", MaxResults: 10}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.List(context.Background(), &ListInput{Container: "c"}); err != nil {
		t.Fatal(err)
	}
	if len(reqs) != 3 {
		t.Fatalf("%d requests", len(reqs))
	}
	r := reqs[0]
	if r.URL.Path != "/acct/c" || strings.Contains(r.URL.RawQuery, "+") ||
		r.URL.RawQuery != "comp=list&delimiter=%2F&marker=m&maxresults=10&prefix=a%20b%2Bc%2F%C3%A9&restype=container&startFrom=a%20b%2Bc%2F%C3%A9%2Fx" {
		t.Errorf("request %s ?%s", r.URL.Path, r.URL.RawQuery)
	}
	if q := reqs[2].URL.RawQuery; q != "comp=list&restype=container" {
		t.Errorf("bare request ?%s", q)
	}
	if reqs[0].Header.Get(headerClientRequestID) == reqs[1].Header.Get(headerClientRequestID) {
		t.Error("a retry repeated the client request id")
	}
	for _, r := range reqs {
		if r.Header.Get(headerVersion) != "2026-10-06" || r.Header.Get(headerClientRequestID) == "" || r.Header.Get(headerAccept) != "application/xml" ||
			r.Header.Get(headerUserAgent) != "swath-go" || r.Header.Get(headerAuthorization) != "" || r.Header.Get(headerDate) != "" {
			t.Errorf("headers %v", r.Header)
		}
	}
	if got := c.Route("c"); got != srv.URL+"/acct/c" {
		t.Errorf("route %s", got)
	}
	d := newTestClient(t, Config{Account: "myacct", Anonymous: true})
	if got := d.Route("c"); got != "https://myacct.blob.core.windows.net/c" {
		t.Errorf("default route %s", got)
	}
	for _, bad := range []Config{{Account: "Bad"}, {Account: "ab"}, {Account: "acct", Endpoint: "ftp://h"}, {Account: "acct", Endpoint: "http://h/?sv=1"}} {
		bad.Anonymous = true
		if _, err := New(context.Background(), bad); err == nil {
			t.Errorf("%+v accepted", bad)
		}
	}
}

// TestBodyCap: a page past the body cap is asked again with maxresults
// halved, marker and query unchanged, so the walk still lists every key
// exactly once with bounded memory; only a single entry past the cap is a
// parse failure. The fake serves entries of 200 KiB (a long Content-Type)
// against a cap lowered to 1 MiB, so pages hold at most 4 entries.
func TestBodyCap(t *testing.T) {
	const keys, entry = 30, 200 << 10
	ct := strings.Repeat("x", entry)
	var mu sync.Mutex
	var sizes []int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		n, _ := strconv.Atoi(q.Get("maxresults"))
		from, _ := strconv.Atoi(strings.TrimPrefix(q.Get("marker"), "m"))
		if q.Get("prefix") != "p/" || n < 0 {
			t.Errorf("query %s", r.URL.RawQuery)
		}
		mu.Lock()
		sizes = append(sizes, n)
		mu.Unlock()
		if q.Get("delimiter") == "huge" {
			n = 1 // one entry past any cap
			ct = strings.Repeat("x", 2<<20)
		}
		if n == 0 {
			n = MaxPageSize
		}
		var b strings.Builder
		b.WriteString(listHead)
		to := min(from+n, keys)
		for i := from; i < to; i++ {
			fmt.Fprintf(&b, "<Blob><Name>p/%03d</Name><Properties><Content-Type>%s</Content-Type></Properties></Blob>", i, ct)
		}
		next := ""
		if to < keys {
			next = fmt.Sprintf("m%d", to)
		}
		fmt.Fprintf(&b, "</Blobs><NextMarker>%s</NextMarker></EnumerationResults>", next)
		fmt.Fprint(w, b.String())
	}))
	defer srv.Close()
	c := newTestClient(t, Config{Endpoint: srv.URL, Anonymous: true, MaxAttempts: 1})
	c.maxBody = 1 << 20
	seen := map[string]int{}
	in := ListInput{Container: "c", Prefix: "p/"}
	for pages := 0; ; pages++ {
		if pages > keys {
			t.Fatal("the walk does not end")
		}
		p, err := c.List(context.Background(), &in)
		if err != nil {
			t.Fatal(err)
		}
		for _, it := range p.Items {
			seen[it.Name]++
			if len(it.ContentType) != entry {
				t.Fatalf("%s: content type of %d bytes", it.Name, len(it.ContentType))
			}
		}
		if p.NextMarker == "" {
			break
		}
		in.Marker = p.NextMarker
	}
	if len(seen) != keys {
		t.Fatalf("%d keys listed, want %d", len(seen), keys)
	}
	for k, n := range seen {
		if n != 1 {
			t.Fatalf("%s listed %d times", k, n)
		}
	}
	// Each page is first asked without maxresults (the server's 5,000),
	// then halved until it fits: 0, 2500, ..., 9, 4.
	if sizes[0] != 0 || slices.Min(sizes) != 0 || !slices.Contains(sizes, 4) || slices.Contains(sizes, 2) || c.BodyCapShrinks() == 0 {
		t.Fatalf("maxresults sent %v, %d shrinks", sizes, c.BodyCapShrinks())
	}
	_, err := c.List(context.Background(), &ListInput{Container: "c", Prefix: "p/", Delimiter: "huge"})
	if Classify(err) != objhttp.ClassParse || !strings.Contains(err.Error(), "one entry") {
		t.Fatalf("a single entry past the cap: %v", err)
	}
	if MaxBody != 64<<20 {
		t.Fatalf("MaxBody %d", MaxBody)
	}
}

// TestAccountInfo: Get Account Information on the container path reads
// x-ms-is-hns-enabled; a missing header and a refusal are errors (the
// namespace is unknown), never flat.
func TestAccountInfo(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.RawQuery != "comp=properties&restype=account" || r.URL.Path != "/acct/"+r.URL.Path[len("/acct/"):] {
			t.Errorf("request %s ?%s", r.URL.Path, r.URL.RawQuery)
		}
		switch r.URL.Path {
		case "/acct/flat":
			w.Header().Set(headerHNSEnabled, "false")
		case "/acct/hns":
			w.Header().Set(headerHNSEnabled, "true")
		case "/acct/denied":
			azureError(w, 403, codeAuthorization)
			return
		}
		w.Header().Set("x-ms-account-kind", "StorageV2")
	}))
	defer srv.Close()
	c := newTestClient(t, Config{Endpoint: srv.URL, Anonymous: true, MaxAttempts: 1})
	for container, want := range map[string]bool{"flat": false, "hns": true} {
		if got, err := c.AccountInfo(context.Background(), container); err != nil || got != want {
			t.Errorf("%s: %v, %v", container, got, err)
		}
	}
	if _, err := c.AccountInfo(context.Background(), "unknown"); !errors.Is(err, ErrHNSUnknown) {
		t.Errorf("no header: %v", err)
	}
	if _, err := c.AccountInfo(context.Background(), "denied"); Classify(err) != objhttp.ClassAccessDenied {
		t.Errorf("denied: class %q (%v)", Classify(err), err)
	}
}

// TestRetryAfterCapped: a Retry-After past the request timeout (a day in
// seconds, a far-future date) is waited only up to the timeout and
// counted; the attempt budget still ends the request, exhausted and
// requeued.
func TestRetryAfterCapped(t *testing.T) {
	for _, v := range []string{"86400", "9223372036854775807", "Fri, 31 Dec 9999 23:59:59 GMT"} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set(headerRetryAfter, v)
			azureError(w, 503, codeServerBusy)
		}))
		c := newTestClient(t, Config{Endpoint: srv.URL, Anonymous: true, MaxAttempts: 3, RequestTimeout: 200 * time.Millisecond,
			BaseBackoff: time.Millisecond, MaxBackoff: time.Millisecond})
		start := time.Now()
		_, err := c.List(context.Background(), &ListInput{Container: "c"})
		d := time.Since(start)
		srv.Close()
		var ex *ExhaustedError
		if !errors.As(err, &ex) || ex.Attempts != 3 || !objhttp.Requeue(Classify)(err) {
			t.Errorf("%s: %v, want three attempts exhausted and requeued", v, err)
		}
		if d < 400*time.Millisecond || d > 3*time.Second {
			t.Errorf("%s: took %s, want two waits of the 200 ms timeout", v, d)
		}
		if n := c.RetryAfterCapped(); n != 3 {
			t.Errorf("%s: retry_after_capped %d, want 3", v, n)
		}
	}
}
