// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
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
// objhttp.ExhaustedError and counts as a throttle or a timeout where it is one.
// The error code decides before the status: AuthenticationFailed is a 403,
// ServerBusy a 503, OperationTimedOut a 500. Generic transport, timeout,
// exhaustion and cancellation behaviour is objhttptest.Contract's
// (TestRetryContract); the 200 bodies that are not pages, and redirects,
// are TestScriptedAnswers'.
func TestClassify(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		status, code, _ := strings.Cut(r.URL.Query().Get("prefix"), " ")
		n, _ := strconv.Atoi(status)
		azureError(w, n, code)
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
		{404, codeContainerNotFound, objhttp.ClassOther, false, false, false},
		{503, codeServerBusy, objhttp.ClassThrottle, true, true, false},
		{429, "", objhttp.ClassThrottle, true, true, false},
		{500, codeOperationTimedOut, objhttp.ClassTimeout, true, false, true},
		{408, "", objhttp.ClassTimeout, true, false, true},
		{500, codeInternalError, objhttp.ClassOther, true, false, false},
		{502, "", objhttp.ClassOther, true, false, false},
		{503, "", objhttp.ClassOther, true, false, false},
		{504, "", objhttp.ClassOther, true, false, false},
		{500, "", objhttp.ClassOther, false, false, false},               // a 500 Azure does not ask to retry
		{501, "NotImplemented", objhttp.ClassOther, false, false, false}, // nor another 5xx
		{400, "InvalidQueryParameterValue", objhttp.ClassOther, false, false, false},
	} {
		c := newTestClient(t, Config{Endpoint: srv.URL, Anonymous: true, Retry: objhttp.Retry{MaxAttempts: 1}})
		_, err := c.List(context.Background(), &ListInput{Container: "c", Prefix: fmt.Sprintf("%d %s", x.status, x.code)})
		var f *objhttp.Failure
		if !errors.As(err, &f) || f.Status != x.status || f.Code != x.code || f.Transient != x.exhausted || f.Throttle != x.throttle || f.Timeout != x.timeout {
			t.Fatalf("%d %s: normalized failure %+v (%v)", x.status, x.code, f, err)
		}
		var ex *objhttp.ExhaustedError
		n := c.Counters()
		if got := objhttp.Classify(err); got != x.class || errors.As(err, &ex) != x.exhausted || (n.Throttles == 1) != x.throttle || (n.Timeouts == 1) != x.timeout {
			t.Errorf("%d %s: class %q exhausted=%v throttles %d timeouts %d (%v)", x.status, x.code, got, errors.As(err, &ex), n.Throttles, n.Timeouts, err)
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
		{"token endpoint 503", &azidentity.AuthenticationFailedError{RawResponse: resp(503)}, objhttp.ClassOther},
		{"token endpoint 429", &azidentity.AuthenticationFailedError{RawResponse: resp(429)}, objhttp.ClassOther},
		{"no answer", &azidentity.AuthenticationFailedError{}, objhttp.ClassOther},
		{"token endpoint unreachable", &net.OpError{Op: "dial", Err: errors.New("connection refused")}, objhttp.ClassOther},
		{"token exchange timeout", fmt.Errorf("msal: %w", context.DeadlineExceeded), objhttp.ClassTimeout},
		{"credential unavailable", errors.New("EnvironmentCredential: missing environment variable AZURE_TENANT_ID"), objhttp.ClassAuth},
	} {
		e := newEntraClient(t, Config{Endpoint: srv.URL, Retry: objhttp.Retry{MaxAttempts: 1}}, failingCredential{x.err})
		_, err := e.List(context.Background(), &ListInput{Container: "c"})
		var ce *objhttp.CredentialError
		var ex *objhttp.ExhaustedError
		if objhttp.Classify(err) != x.class || !errors.As(err, &ce) || errors.As(err, &ex) != (x.class != objhttp.ClassAuth) {
			t.Errorf("%s: class %q exhausted=%v (%v), want %q", x.name, objhttp.Classify(err), errors.As(err, &ex), err, x.class)
		}
	}
	// A shared key that is not base64 refuses the client.
	if _, err := New(context.Background(), Config{Account: "acct", SharedKey: "not base64!"}); objhttp.Classify(err) != objhttp.ClassAuth {
		t.Errorf("bad key: class %q (%v)", objhttp.Classify(err), err)
	}
	if got := objhttp.Classify(nil); got != "" {
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
	c := newTestClient(t, Config{Endpoint: srv.URL, Anonymous: true, Retry: objhttp.Retry{BaseBackoff: time.Millisecond, MaxBackoff: time.Millisecond}})
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
// is not enough: U+E000 after U+1F600 ascends), a repeated name. A body
// cut at an element boundary (Azure has no truncation flag: an absent
// marker is the end) never reads as a shorter final page: it is a parse
// failure, retried. A descending page, a marker equal to the one sent and
// a cut inside an entry are TestScriptedAnswers'.
func TestInvalidPage(t *testing.T) {
	var body atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, body.Load().(string))
	}))
	defer srv.Close()
	c := newTestClient(t, Config{Endpoint: srv.URL, Anonymous: true, Retry: objhttp.Retry{MaxAttempts: 2, BaseBackoff: time.Millisecond}})
	full := page("m2", "a", "b")
	for _, x := range []struct {
		name, body, marker string
		class              objhttp.Class
	}{
		{"repeated name", page("", "a", "a"), "", objhttp.ClassOther},
		{"byte order only", page("", "u\ue000", "u\U0001F600"), "", objhttp.ClassOther},
		{"truncated body", full[:len(full)-len("</EnumerationResults>")], "", objhttp.ClassParse},
		{"UTF-16 order", page("", "u\U0001F600", "u\ue000", "u\ufffd"), "", ""},
		{"a new marker", page("m2", "a"), "m1", ""},
	} {
		body.Store(x.body)
		p, err := c.List(context.Background(), &ListInput{Container: "c", Marker: x.marker})
		if got := objhttp.Classify(err); got != x.class {
			t.Errorf("%s: class %q (%v), page %+v", x.name, got, err, p)
		}
		if x.class == objhttp.ClassOther && !errors.Is(err, objhttp.ErrInvalidPage) {
			t.Errorf("%s: %v does not wrap ErrInvalidPage", x.name, err)
		}
	}
}

// TestRequestShape: the List Blobs options the client maps: the container
// path under the account segment of a non-default endpoint, each set input
// as its query parameter (startFrom included, which the SDK's flat pager
// drops), an unset one absent, never include or endBefore, no credentials
// when anonymous; the route; and the configurations New refuses. The
// version, Accept and User-Agent headers are TestServiceVersionPinned's.
func TestRequestShape(t *testing.T) {
	var mu sync.Mutex
	var reqs []*http.Request
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		reqs = append(reqs, r)
		mu.Unlock()
		fmt.Fprint(w, page(""))
	}))
	defer srv.Close()
	c := newTestClient(t, Config{Endpoint: srv.URL + "/", Anonymous: true})
	full := &ListInput{Container: "c", Prefix: "a b+c/\u00e9", Delimiter: "/", StartFrom: "a b+c/\u00e9/x", Marker: "m", MaxResults: 10}
	for _, in := range []*ListInput{full, {Container: "c"}} {
		if _, err := c.List(context.Background(), in); err != nil {
			t.Fatal(err)
		}
	}
	want := []map[string]string{
		{"restype": "container", "comp": "list", "prefix": full.Prefix, "delimiter": "/", "startFrom": full.StartFrom, "marker": "m", "maxresults": "10"},
		{"restype": "container", "comp": "list"},
	}
	for i, r := range reqs {
		got := map[string]string{}
		for k, v := range r.URL.Query() {
			got[k] = strings.Join(v, ",")
		}
		if r.URL.Path != "/acct/c" || !maps.Equal(got, want[i]) || r.Header.Get(headerAuthorization) != "" {
			t.Errorf("request %d: %s %v, Authorization %q", i, r.URL.Path, got, r.Header.Get(headerAuthorization))
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
		c := newTestClient(t, Config{Endpoint: srv.URL, Anonymous: true, Retry: objhttp.Retry{MaxAttempts: 3, BaseBackoff: time.Millisecond, MaxBackoff: time.Millisecond}, RequestTimeout: 200 * time.Millisecond})
		start := time.Now()
		_, err := c.List(context.Background(), &ListInput{Container: "c"})
		d := time.Since(start)
		srv.Close()
		var ex *objhttp.ExhaustedError
		if !errors.As(err, &ex) || ex.Attempts != 3 || !objhttp.Requeue(err) {
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
