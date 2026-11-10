// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"reflect"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"

	"github.com/varveio/swath/go/internal/buildinfo"
	"github.com/varveio/swath/go/internal/source/objhttp"
	"github.com/varveio/swath/go/internal/utf16order"
)

// reqLog serves every request with answer and keeps the requests and the
// length of each one's body.
type reqLog struct {
	mu     sync.Mutex
	reqs   []*http.Request
	bodies []int
}

func (c *reqLog) server(t *testing.T, answer func(w http.ResponseWriter, r *http.Request)) *httptest.Server {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
		}
		c.mu.Lock()
		c.reqs, c.bodies = append(c.reqs, r.Clone(context.Background())), append(c.bodies, len(body))
		c.mu.Unlock()
		answer(w, r)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func (c *reqLog) last() *http.Request {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.reqs[len(c.reqs)-1]
}

const testKey = "c2VjcmV0LWtleS1mb3ItdGVzdHM="

// TestServiceVersionPinned: azblob refuses ClientOptions.APIVersion (pin
// can go when it stops refusing), so the client's flat, hierarchy and
// account-information requests carry exactly one x-ms-version, Version,
// set by pin, with swath's User-Agent product token once, before the SDK's
// own, signed under Shared Key; the listings ask for XML (Accept; account
// information sends none, TestWireSnapshot).
func TestServiceVersionPinned(t *testing.T) {
	over, err := container.NewClientWithNoCredential("http://127.0.0.1:1/acct/c", &container.ClientOptions{ClientOptions: azcore.ClientOptions{
		APIVersion: Version, Retry: policy.RetryOptions{MaxRetries: -1}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := over.NewListBlobsFlatPager(nil).NextPage(context.Background()); err == nil || !strings.Contains(err.Error(), "doesn't support overriding its API version") {
		t.Fatalf("ClientOptions.APIVersion: %v (if the SDK takes it now, pin can go)", err)
	}

	var cp reqLog
	srv := cp.server(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("comp") == "list" {
			fmt.Fprint(w, page(""))
		} else {
			w.Header().Set("x-ms-is-hns-enabled", "false")
		}
	})
	for _, cfg := range []Config{{Anonymous: true}, {SharedKey: testKey}, {SAS: testSAS}} {
		cfg.Endpoint, cfg.Account, cfg.MaxAttempts = srv.URL, "acct", 1
		c, err := New(context.Background(), cfg)
		if err != nil {
			t.Fatal(err)
		}
		for _, call := range []struct {
			listing bool
			do      func() error
		}{
			{true, func() error {
				_, err := c.List(context.Background(), &ListInput{Container: "c", StartFrom: "s"})
				return err
			}},
			{true, func() error {
				_, err := c.List(context.Background(), &ListInput{Container: "c", Delimiter: "/", StartFrom: "s"})
				return err
			}},
			{false, func() error { _, err := c.AccountInfo(context.Background(), "c"); return err }},
		} {
			if err := call.do(); err != nil {
				t.Fatal(err)
			}
			req := cp.last()
			var versions []string
			for k, v := range req.Header {
				if strings.EqualFold(k, "x-ms-version") {
					versions = append(versions, v...)
				}
			}
			agent, token := req.Header.Get("User-Agent"), buildinfo.UserAgent()
			if !slices.Equal(versions, []string{Version}) || !strings.HasPrefix(agent, token+" ") ||
				!strings.Contains(agent, "azsdk-go-azblob/") || strings.Count(agent, token) != 1 {
				t.Errorf("%+v %s: x-ms-version %q, User-Agent %q", cfg, req.URL.RawQuery, versions, agent)
			}
			if accept := req.Header.Values("Accept"); call.listing && !slices.Equal(accept, []string{"application/xml"}) {
				t.Errorf("%+v %s: Accept %q", cfg, req.URL.RawQuery, accept)
			}
			if cfg.SharedKey != "" {
				verifySharedKey(t, req, "acct", cfg.SharedKey)
			}
		}
	}
}

// TestStartFromSent: the client sends StartFrom (the SDK's own option)
// exactly once, escaped, flat and with a delimiter, on a first request and
// on a continuation (the marker and the original seek both sent), and
// nothing when it is unset; Shared Key signs it.
func TestStartFromSent(t *testing.T) {
	var cp reqLog
	srv := cp.server(t, func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, page("")) })
	sf := "s t+u/"
	c, err := New(context.Background(), Config{Endpoint: srv.URL, Account: "acct", SharedKey: testKey, Retry: objhttp.Retry{MaxAttempts: 1}})
	if err != nil {
		t.Fatal(err)
	}
	for _, delim := range []string{"", "/"} {
		for _, in := range []ListInput{
			{Container: "c", Delimiter: delim, Prefix: "p q"},
			{Container: "c", Delimiter: delim, Prefix: "p q", StartFrom: sf},
			{Container: "c", Delimiter: delim, Prefix: "p q", StartFrom: sf, Marker: "m"},
		} {
			if _, err := c.List(context.Background(), &in); err != nil {
				t.Fatal(err)
			}
			req := cp.last()
			q := req.URL.Query()
			for k, want := range map[string]string{"startFrom": in.StartFrom, "marker": in.Marker, "prefix": in.Prefix} {
				var vs []string
				if want != "" {
					vs = []string{want}
				}
				if !slices.Equal(q[k], vs) {
					t.Errorf("%+v: %s %q, want %q (query %s)", in, k, q[k], vs, req.URL.RawQuery)
				}
			}
			if strings.Contains(req.URL.RawQuery, "+") {
				t.Errorf("%+v: query %s holds an unescaped '+'", in, req.URL.RawQuery)
			}
			verifySharedKey(t, req, "acct", testKey)
		}
	}
}

// verifySharedKey checks req's Shared Key signature (Authorize with Shared
// Key, computed here from the specification, independently of the SDK): every
// x-ms-* header and query parameter the request carries is signed.
func verifySharedKey(t *testing.T, req *http.Request, account, key string) {
	t.Helper()
	k, err := base64.StdEncoding.DecodeString(key)
	if err != nil {
		t.Fatal(err)
	}
	var xms []string
	for k, v := range req.Header {
		if n := strings.ToLower(k); strings.HasPrefix(n, "x-ms-") {
			xms = append(xms, n+":"+strings.Join(v, ","))
		}
	}
	sort.Strings(xms)
	res := "/" + account + req.URL.EscapedPath()
	q := req.URL.Query()
	names := slices.Sorted(func(yield func(string) bool) {
		for n := range q {
			if !yield(n) {
				return
			}
		}
	})
	for _, n := range names {
		vs := slices.Sorted(slices.Values(q[n]))
		res += "\n" + strings.ToLower(n) + ":" + strings.Join(vs, ",")
	}
	h := req.Header.Get
	sts := strings.Join([]string{req.Method, h("Content-Encoding"), h("Content-Language"), "", h("Content-MD5"), h("Content-Type"), "",
		h("If-Modified-Since"), h("If-Match"), h("If-None-Match"), h("If-Unmodified-Since"), h("Range"), strings.Join(xms, "\n"), res}, "\n")
	mac := hmac.New(sha256.New, k)
	mac.Write([]byte(sts))
	sig := base64.StdEncoding.EncodeToString(mac.Sum(nil))
	if want := "SharedKey " + account + ":" + sig; h("Authorization") != want {
		t.Errorf("Authorization %q, want %q (string to sign %q)", h("Authorization"), want, sts)
	}
}

// TestSDKDecoding pins the client's reading of a page through the SDK's
// decoder and the envelope check (the accepted readings list.go names):
// an Encoded literal '+' reads as a space (%2B, Azure's form, as '+'), a
// malformed escape is a parse failure, a namespaced root and a
// comment after the root refused (an <Error> element in a listing, and data
// after it, both refuse), a hierarchy page out of order across its two
// kinds is merged into order, and one out of order within a kind is an
// invalid page.
func TestSDKDecoding(t *testing.T) {
	type result struct {
		names []string
		class objhttp.Class
	}
	cases := []struct {
		name      string
		delimiter string
		body      string
		want      result
	}{
		{"Encoded %2B", "", listHead + `<Blob><Name Encoded="true">a%EF%BF%BE%2Bb%20c</Name></Blob>` + listTail,
			result{names: []string{"a\ufffe+b c"}}},
		{"Encoded literal plus", "", listHead + `<Blob><Name Encoded="true">a%EF%BF%BE+b</Name></Blob>` + listTail,
			result{names: []string{"a\ufffe b"}}},
		{"Encoded malformed escape", "", listHead + `<Blob><Name Encoded="true">a%G1</Name></Blob>` + listTail,
			result{class: objhttp.ClassParse}},
		{"Error element inside", "", listHead + `<Blob><Name>a</Name></Blob></Blobs><Error><Code>X</Code></Error><NextMarker/></EnumerationResults>`,
			result{class: objhttp.ClassParse}},
		{"Error element instead of the entries", "", `<EnumerationResults><Blobs/><Error><Code>InternalError</Code></Error><NextMarker/></EnumerationResults>`,
			result{class: objhttp.ClassParse}},
		{"prefixed Error element inside", "", listHead + `<Blob><Name>a</Name></Blob></Blobs><e:Error xmlns:e="urn:test"/><NextMarker/></EnumerationResults>`,
			result{class: objhttp.ClassParse}},
		{"Error in a blob name", "", page("", "Error-report"),
			result{names: []string{"Error-report"}}},
		{"ErrorCode element is not an Error", "", listHead + `<Blob><Name>a</Name><Properties><ErrorCode>x</ErrorCode></Properties></Blob>` + listTail,
			result{names: []string{"a"}}},
		{"data after the root", "", page("", "a") + "<junk/>",
			result{class: objhttp.ClassParse}},
		{"prefixed root with trailing junk", "", `<e:EnumerationResults xmlns:e="urn:test"><Blobs/><NextMarker/></e:EnumerationResults>junk</EnumerationResults>`,
			result{class: objhttp.ClassParse}},
		{"prefixed root", "", `<e:EnumerationResults xmlns:e="urn:test"><Blobs/><NextMarker/></e:EnumerationResults>`,
			result{class: objhttp.ClassParse}},
		{"default namespace on root", "", `<EnumerationResults xmlns="urn:test"><Blobs/><NextMarker/></EnumerationResults>`,
			result{class: objhttp.ClassParse}},
		{"a second root", "", page("", "a") + page("", "b"),
			result{class: objhttp.ClassParse}},
		{"whitespace after the root", "", page("", "a") + "\r\n ",
			result{names: []string{"a"}}},
		{"comment after the root", "", page("", "a") + "<!-- c -->",
			result{class: objhttp.ClassParse}},
		{"hierarchy out of order across kinds", "/", listHead + `<Blob><Name>b</Name></Blob><BlobPrefix><Name>a/</Name></BlobPrefix>` + listTail,
			result{names: []string{"a/", "b"}}},
		{"hierarchy out of order within a kind", "/", listHead + `<Blob><Name>b</Name></Blob><Blob><Name>a</Name></Blob><BlobPrefix><Name>c/</Name></BlobPrefix>` + listTail,
			result{class: objhttp.ClassOther}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, c.body) }))
			defer srv.Close()
			cl := newTestClient(t, Config{Endpoint: srv.URL, Anonymous: true, Retry: objhttp.Retry{MaxAttempts: 1}})
			var got result
			p, err := cl.List(context.Background(), &ListInput{Container: "c", Delimiter: c.delimiter})
			if err != nil {
				got.class = objhttp.Classify(err)
			} else {
				for _, it := range p.Items {
					got.names = append(got.names, it.Name)
				}
			}
			if !reflect.DeepEqual(got, c.want) {
				t.Errorf("%+v (%v), want %+v", got, err, c.want)
			} else if c.want.class == objhttp.ClassOther && !errors.Is(err, objhttp.ErrInvalidPage) {
				t.Fatalf("%v does not wrap ErrInvalidPage", err)
			} else if c.want.class == objhttp.ClassParse && !errors.Is(err, objhttp.ErrSyntax) {
				t.Errorf("%v does not wrap ErrSyntax", err)
			}
		})
	}
}

// TestAccountInfoExact: Get Account Information reads x-ms-is-hns-enabled
// exactly: "true" and "false" are the answer, any other value (another
// spelling, "1", "0") or none is unknown, never flat, whatever the SDK's
// ParseBool would make of it; a refusal is an error, never flat.
func TestAccountInfoExact(t *testing.T) {
	type answer struct {
		hns     bool
		unknown bool
	}
	for _, c := range []struct {
		header string
		want   answer
	}{
		{"true", answer{hns: true}},
		{"false", answer{}},
		{"FALSE", answer{unknown: true}},
		{"", answer{unknown: true}},
		{"yes", answer{unknown: true}},
		{"0", answer{unknown: true}},
	} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.RawQuery != "comp=properties&restype=account" || r.URL.Path != "/acct/c" {
				t.Errorf("request %s ?%s", r.URL.Path, r.URL.RawQuery)
			}
			if c.header != "" {
				w.Header().Set("x-ms-is-hns-enabled", c.header)
			}
		}))
		cl := newTestClient(t, Config{Endpoint: srv.URL, Anonymous: true, Retry: objhttp.Retry{MaxAttempts: 1}})
		hns, err := cl.AccountInfo(context.Background(), "c")
		got := answer{hns: hns, unknown: errors.Is(err, ErrHNSUnknown)}
		if err != nil && !got.unknown {
			t.Errorf("%q: %v", c.header, err)
		}
		if got != c.want {
			t.Errorf("%q: %+v (%v), want %+v", c.header, got, err, c.want)
		}
		srv.Close()
	}
	// A refusal is an error with its class, never flat; the request goes to
	// the container path under the account segment.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.RawQuery != "comp=properties&restype=account" || r.URL.Path != "/acct/denied" {
			t.Errorf("request %s ?%s", r.URL.Path, r.URL.RawQuery)
		}
		azureError(w, 403, codeAuthorization)
	}))
	defer srv.Close()
	cl := newTestClient(t, Config{Endpoint: srv.URL, Anonymous: true, Retry: objhttp.Retry{MaxAttempts: 1}})
	if hns, err := cl.AccountInfo(context.Background(), "denied"); hns || objhttp.Classify(err) != objhttp.ClassAccessDenied {
		t.Errorf("denied: hns %v, class %q (%v)", hns, objhttp.Classify(err), err)
	}
}

// TestMergeOrder: merging a valid hierarchy page's blobs and prefixes,
// each in UTF-16 code-unit order, restores the one sequence, over random
// splits of the hostile corpus.
func TestMergeOrder(t *testing.T) {
	names := hostileKeys()
	slices.SortFunc(names, utf16order.Compare)
	rng := rand.New(rand.NewPCG(1, 2))
	for range 200 {
		n := 1 + rng.IntN(64)
		start := rng.IntN(len(names) - n)
		want := names[start : start+n]
		seg := &container.BlobHierarchyListSegment{}
		for _, name := range want {
			if rng.IntN(2) == 0 {
				seg.BlobPrefixes = append(seg.BlobPrefixes, &container.BlobPrefix{Name: &name})
			} else {
				seg.BlobItems = append(seg.BlobItems, &container.BlobItem{Name: &name})
			}
		}
		out, err := merge(seg, nil)
		if err != nil {
			t.Fatal(err)
		}
		var got []string
		for _, it := range out.Items {
			got = append(got, it.Name)
		}
		if !slices.Equal(got, want) {
			t.Fatalf("merged %q, want %q", got, want)
		}
		if err := out.Validate(""); err != nil {
			t.Fatal(err)
		}
	}
}

// TestRoute: Route is the account root and the container, SAS-free, and
// every request's URL starts with it (the SAS after it), on the default
// endpoint and on endpoints with and without a path.
func TestRoute(t *testing.T) {
	var cp reqLog
	srv := cp.server(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("comp") == "list" {
			fmt.Fprint(w, page(""))
		}
	})
	u, _ := url.Parse(srv.URL)
	for ep, want := range map[string]string{
		"":                                   "https://acct.blob.core.windows.net/cont",
		"https://acct.blob.core.windows.net": "https://acct.blob.core.windows.net/acct/cont",
		"http://127.0.0.1:10000":             "http://127.0.0.1:10000/acct/cont",
		"http://127.0.0.1:10000/emu/path/":   "http://127.0.0.1:10000/emu/path/acct/cont",
		"https://h.example:8443/a%2Fb":       "https://h.example:8443/a/b/acct/cont",
	} {
		for _, sas := range []string{"", testSAS} {
			cfg := Config{Endpoint: ep, Account: "acct", SAS: sas, Anonymous: sas == "", Retry: objhttp.Retry{MaxAttempts: 1}, HTTPClient: &http.Client{Transport: toFake{u}}}
			sdk := newTestClient(t, cfg)
			route := sdk.Route("cont")
			if route != want {
				t.Fatalf("%q: route %q, want %q", ep, route, want)
			}
			if _, err := sdk.List(context.Background(), &ListInput{Container: "cont"}); err != nil {
				t.Fatal(err)
			}
			if _, err := sdk.AccountInfo(context.Background(), "cont"); err != nil && !errors.Is(err, ErrHNSUnknown) {
				t.Fatal(err)
			}
			for _, req := range cp.reqs[len(cp.reqs)-2:] {
				sent := req.URL
				sent.Scheme, sent.Host = "http", req.Header.Get("X-Original-Host")
				if ru, _ := url.Parse(route); ru.Scheme == "https" {
					sent.Scheme = "https"
				}
				if s := sent.String(); !strings.HasPrefix(s, route+"?") {
					t.Errorf("%q: sent %s, route %s", ep, s, route)
				}
				if sas != "" && !strings.Contains(sent.RawQuery, "sig=TopSecretSig") {
					t.Errorf("%q: the SAS not sent: %s", ep, sent.RawQuery)
				}
			}
		}
	}
}

// fakeToken is an Entra ID credential that issues tok, or fails with err.
type fakeToken struct {
	tok string
	err error
}

func (f fakeToken) GetToken(context.Context, policy.TokenRequestOptions) (azcore.AccessToken, error) {
	return azcore.AccessToken{Token: f.tok, ExpiresOn: time.Now().Add(time.Hour)}, f.err
}

// TestBearer: an Entra ID token is fetched before the attempt and sent as
// Authorization: Bearer; a 401 challenge is an answer, not re-sent; a
// token that cannot be had is a credential error before any attempt.
func TestBearer(t *testing.T) {
	var cp reqLog
	srv := cp.server(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "challenge") {
			w.Header().Set("WWW-Authenticate", `Bearer authorization_uri="https://login.example/t", error="insufficient_claims", claims="e30="`)
			echoError(w, r, 401, "InvalidAuthenticationInfo")
			return
		}
		fmt.Fprint(w, page(""))
	})
	c, err := newClient(context.Background(), Config{Endpoint: srv.URL, Account: "acct", Retry: objhttp.Retry{MaxAttempts: 3}}, withEntra(fakeToken{tok: "tok"}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.List(context.Background(), &ListInput{Container: "c"}); err != nil {
		t.Fatal(err)
	}
	if a := cp.last().Header.Get("Authorization"); a != "Bearer tok" {
		t.Fatalf("Authorization %q", a)
	}
	n := len(cp.reqs)
	_, err = c.List(context.Background(), &ListInput{Container: "challenge"})
	if objhttp.Classify(err) != objhttp.ClassAuth || len(cp.reqs) != n+1 {
		t.Fatalf("challenge: %v (class %q), %d requests", err, objhttp.Classify(err), len(cp.reqs)-n)
	}
	bad, err := newClient(context.Background(), Config{Endpoint: srv.URL, Account: "acct", Retry: objhttp.Retry{MaxAttempts: 1}}, withEntra(fakeToken{err: errors.New("refused")}))
	if err != nil {
		t.Fatal(err)
	}
	n = len(cp.reqs)
	_, err = bad.List(context.Background(), &ListInput{Container: "c"})
	if _, ok := errors.AsType[*objhttp.CredentialError](err); !ok || len(cp.reqs) != n || bad.Counters().Attempts != 0 {
		t.Fatalf("token failure: %v, %d requests, %d attempts", err, len(cp.reqs)-n, bad.Counters().Attempts)
	}
}

// TestRedactedRefusal: a 200 the SDK refuses for a header that quotes the
// SAS (a malformed Date echoing the query) is a parse failure, retried,
// whose error never holds the signature.
func TestRedactedRefusal(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		dec, _ := url.QueryUnescape(r.URL.RawQuery)
		w.Header()["Date"] = []string{"x " + r.URL.RawQuery + " " + dec}
		fmt.Fprint(w, page("", "a"))
	}))
	t.Cleanup(srv.Close)
	c, err := New(context.Background(), Config{Endpoint: srv.URL, Account: "acct", SAS: testSAS, Retry: objhttp.Retry{MaxAttempts: 3, BaseBackoff: time.Millisecond, MaxBackoff: time.Millisecond}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.List(context.Background(), &ListInput{Container: "c"})
	ex, ok := errors.AsType[*objhttp.ExhaustedError](err)
	if !ok || ex.Attempts != 3 || objhttp.Classify(err) != objhttp.ClassParse || c.Counters().ParseFailures != 3 {
		t.Fatalf("%v: class %q, parse failures %d", err, objhttp.Classify(err), c.Counters().ParseFailures)
	}
	if sasLeak(err.Error()) != "" || !strings.Contains(err.Error(), "REDACTED") {
		t.Fatalf("not redacted: %v", err)
	}
}

// TestConcurrentClient: 16 goroutines list distinct ranges (prefixes,
// startFrom seeks, page sizes) through one client while a proxy throttles
// every seventh request: each gets its own range's names, in order, as a
// fault-free walk at the server's page size lists them.
func TestConcurrentClient(t *testing.T) {
	var keys []string
	for i := range 1600 {
		keys = append(keys, fmt.Sprintf("f/%04d", i))
	}
	ep := replayServer(t, keys)
	target, _ := url.Parse(ep)
	rp := httputil.NewSingleHostReverseProxy(target)
	var n atomic.Int64
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Query().Get("comp") == "list" && n.Add(1)%7 == 0 {
			w.Header().Set("Retry-After", "0")
			echoError(w, req, 503, "ServerBusy")
			return
		}

		rp.ServeHTTP(w, req)
	}))
	t.Cleanup(proxy.Close)
	sdk, err := New(context.Background(), Config{Endpoint: proxy.URL, Account: replayAccount, Anonymous: true, Workers: 32,
		Retry: objhttp.Retry{BaseBackoff: time.Millisecond, MaxBackoff: time.Millisecond}})
	if err != nil {
		t.Fatal(err)
	}
	ref, err := New(context.Background(), Config{Endpoint: ep, Account: replayAccount, Anonymous: true})
	if err != nil {
		t.Fatal(err)
	}
	inputs := make([]ListInput, 16)
	for i := range inputs {
		in := ListInput{Container: replayContainer, Prefix: fmt.Sprintf("f/%02d", i), MaxResults: 5 + i%3}
		if i%2 == 1 {
			in.StartFrom = fmt.Sprintf("f/%02d50", i)
		}
		inputs[i] = in
	}
	names := func(pages []ListOutput) []string {
		var out []string
		for _, p := range pages {
			for _, it := range p.Items {
				out = append(out, it.Name)
			}
		}
		return out
	}
	got := make([][]string, len(inputs))
	var wg sync.WaitGroup
	for i, in := range inputs {
		wg.Go(func() {
			for {
				p, err := sdk.List(context.Background(), &in)
				if err != nil {
					t.Errorf("%+v: %v", in, err)
					return
				}
				got[i] = append(got[i], names([]ListOutput{*p})...)
				if p.NextMarker == "" {
					return
				}
				in.Marker = p.NextMarker
			}
		})
	}
	wg.Wait()
	for i, in := range inputs {
		in.MaxResults = 0
		if want := names(walk(t, ref, in)); !slices.Equal(got[i], want) || len(want) == 0 {
			t.Errorf("%+v: %d names, want %d", in, len(got[i]), len(want))
		}
	}
	if sdk.Counters().Retries == 0 || sdk.Counters().Throttles == 0 {
		t.Fatalf("retries %d, throttles %d: the faults were not met", sdk.Counters().Retries, sdk.Counters().Throttles)
	}
}

func TestEndpointRefusal(t *testing.T) {
	for _, ep := range []string{"http://%zz/", "http://host/%zz", "ftp://host/", "host/path", "http:///path"} {
		t.Run(ep, func(t *testing.T) {
			c, err := New(t.Context(), Config{Endpoint: ep, Account: "acct", Anonymous: true})
			if c != nil || err == nil || !strings.Contains(err.Error(), "endpoint") {
				t.Fatalf("client %v, error %v", c, err)
			}
		})
	}
}
