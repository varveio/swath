// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package sdkclient

import (
	"bytes"
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
	"net/url"
	"os"
	"os/exec"
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
	"github.com/varveio/swath/go/internal/source/azblob/azblobtest"
	"github.com/varveio/swath/go/internal/source/azblob/client"
	"github.com/varveio/swath/go/internal/source/objhttp"
	"github.com/varveio/swath/go/internal/utf16order"
)

// capture serves every request with answer and keeps the requests.
type capture struct {
	mu   sync.Mutex
	reqs []*http.Request
}

func (c *capture) server(t *testing.T, answer func(w http.ResponseWriter, r *http.Request)) *httptest.Server {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.mu.Lock()
		c.reqs = append(c.reqs, r.Clone(context.Background()))
		c.mu.Unlock()
		answer(w, r)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func (c *capture) last() *http.Request {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.reqs[len(c.reqs)-1]
}

const testKey = "c2VjcmV0LWtleS1mb3ItdGVzdHM="

// TestServiceVersionPinned: azblob v1.7.0 sends its own service version,
// which the replay (answering only the captured 2026-06-06 and 2026-10-06)
// refuses, and refuses ClientOptions.APIVersion; the client's requests
// carry exactly one x-ms-version, client.Version, with the hand-rolled
// client's User-Agent and Accept, signed under Shared Key.
func TestServiceVersionPinned(t *testing.T) {
	r := azblobtest.Serve(t, []string{"a", "b"})
	plain, err := container.NewClientWithNoCredential(r.Endpoint+"/"+r.Container, &container.ClientOptions{ClientOptions: azcore.ClientOptions{
		Retry: policy.RetryOptions{MaxRetries: -1}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := plain.NewListBlobsFlatPager(nil).NextPage(context.Background()); err == nil {
		t.Fatal("the replay answered the SDK's own service version: the pinned version may no longer be needed")
	} else if re, ok := errors.AsType[*azcore.ResponseError](err); !ok || re.StatusCode/100 != 4 {
		t.Fatalf("the SDK's own version: %v", err)
	}
	over, err := container.NewClientWithNoCredential(r.Endpoint+"/"+r.Container, &container.ClientOptions{ClientOptions: azcore.ClientOptions{
		APIVersion: client.Version, Retry: policy.RetryOptions{MaxRetries: -1}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := over.NewListBlobsFlatPager(nil).NextPage(context.Background()); err == nil || !strings.Contains(err.Error(), "doesn't support overriding its API version") {
		t.Fatalf("ClientOptions.APIVersion: %v", err)
	}

	var cp capture
	srv := cp.server(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("comp") == "list" {
			fmt.Fprint(w, page(""))
		} else {
			w.Header().Set("x-ms-is-hns-enabled", "false")
		}
	})
	for _, cfg := range []client.Config{{Anonymous: true}, {SharedKey: testKey}, {SAS: testSAS}} {
		cfg.Endpoint, cfg.Account, cfg.MaxAttempts = srv.URL, "acct", 1
		c, err := New(context.Background(), cfg)
		if err != nil {
			t.Fatal(err)
		}
		for _, call := range []func() error{
			func() error {
				_, err := c.List(context.Background(), &client.ListInput{Container: "c", StartFrom: "s"})
				return err
			},
			func() error { _, err := c.AccountInfo(context.Background(), "c"); return err },
		} {
			if err := call(); err != nil {
				t.Fatal(err)
			}
			req := cp.last()
			var versions []string
			for k, v := range req.Header {
				if strings.EqualFold(k, "x-ms-version") {
					versions = append(versions, v...)
				}
			}
			if !slices.Equal(versions, []string{client.Version}) || req.Header.Get("User-Agent") != buildinfo.Tool || req.Header.Get("Accept") != "application/xml" {
				t.Errorf("%+v: x-ms-version %q, User-Agent %q, Accept %q", cfg, versions, req.Header.Get("User-Agent"), req.Header.Get("Accept"))
			}
			if cfg.SharedKey != "" {
				verifySharedKey(t, req, "acct", cfg.SharedKey)
			}
		}
	}
}

// TestStartFromSent: azblob v1.7.0's pagers do not send StartFrom; the
// client's do, flat and with a delimiter, before Shared Key signs.
func TestStartFromSent(t *testing.T) {
	var cp capture
	srv := cp.server(t, func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, page("")) })
	sf := "s t+u/\ue000"
	plain, err := container.NewClientWithNoCredential(srv.URL+"/acct/c", &container.ClientOptions{ClientOptions: azcore.ClientOptions{Retry: policy.RetryOptions{MaxRetries: -1}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := plain.NewListBlobsFlatPager(&container.ListBlobsFlatOptions{StartFrom: &sf}).NextPage(context.Background()); err != nil {
		t.Fatal(err)
	}
	if cp.last().URL.Query().Has("startFrom") {
		t.Error("the SDK's flat pager sends StartFrom now: pin need not")
	}
	if _, err := plain.NewListBlobsHierarchyPager("/", &container.ListBlobsHierarchyOptions{StartFrom: &sf}).NextPage(context.Background()); err != nil {
		t.Fatal(err)
	}
	if cp.last().URL.Query().Has("startFrom") {
		t.Error("the SDK's hierarchy pager sends StartFrom now: pin need not")
	}
	c, err := New(context.Background(), client.Config{Endpoint: srv.URL, Account: "acct", SharedKey: testKey, MaxAttempts: 1})
	if err != nil {
		t.Fatal(err)
	}
	for _, delim := range []string{"", "/"} {
		if _, err := c.List(context.Background(), &client.ListInput{Container: "c", StartFrom: sf, Delimiter: delim, Prefix: "p q"}); err != nil {
			t.Fatal(err)
		}
		req := cp.last()
		if q := req.URL.Query(); q.Get("startFrom") != sf || q.Get("prefix") != "p q" || strings.Contains(req.URL.RawQuery, "+") && !strings.Contains(req.URL.RawQuery, "%2B") {
			t.Errorf("delimiter %q: query %s", delim, req.URL.RawQuery)
		}
		verifySharedKey(t, req, "acct", testKey)
	}
}

// verifySharedKey checks req's Shared Key signature (Authorize with Shared
// Key, as the client package's sharedkey.go and the SDK compute it): every
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

// TestSDKDecoding pins where the SDK's decoding differs from the
// hand-rolled parser's (the accepted differences the package comment
// names), from the same body: an Encoded literal '+' reads as a space
// (%2B, Azure's form, as '+' in both), a malformed escape is a parse
// failure, a second <Blobs> is read and a comment after the root refused
// (an <Error> element in a listing, and data after it, both refuse), a
// hierarchy page out of order across its two kinds is
// merged into order, and a malformed date the row does not keep, or a
// malformed Date header, fails the page.
func TestSDKDecoding(t *testing.T) {
	type result struct {
		names []string
		class objhttp.Class
	}
	cases := []struct {
		name      string
		delimiter string
		date      string // the Date header, when set
		body      string
		hand, sdk result
	}{
		{"Encoded %2B", "", "", listHead + `<Blob><Name Encoded="true">a%EF%BF%BE%2Bb%20c</Name></Blob>` + listTail,
			result{names: []string{"a\ufffe+b c"}}, result{names: []string{"a\ufffe+b c"}}},
		{"Encoded literal plus", "", "", listHead + `<Blob><Name Encoded="true">a%EF%BF%BE+b</Name></Blob>` + listTail,
			result{names: []string{"a\ufffe+b"}}, result{names: []string{"a\ufffe b"}}},
		{"Encoded malformed escape", "", "", listHead + `<Blob><Name Encoded="true">a%G1</Name></Blob>` + listTail,
			result{class: objhttp.ClassInvalidPage}, result{class: objhttp.ClassParse}},
		{"Error element inside", "", "", listHead + `<Blob><Name>a</Name></Blob></Blobs><Error><Code>X</Code></Error><NextMarker/></EnumerationResults>`,
			result{class: objhttp.ClassParse}, result{class: objhttp.ClassParse}},
		{"Error element instead of the entries", "", "", `<EnumerationResults><Blobs/><Error><Code>InternalError</Code></Error><NextMarker/></EnumerationResults>`,
			result{class: objhttp.ClassParse}, result{class: objhttp.ClassParse}},
		{"prefixed Error element inside", "", "", listHead + `<Blob><Name>a</Name></Blob></Blobs><e:Error xmlns:e="urn:test"/><NextMarker/></EnumerationResults>`,
			result{class: objhttp.ClassParse}, result{class: objhttp.ClassParse}},
		{"Error in a blob name", "", "", page("", "Error-report"),
			result{names: []string{"Error-report"}}, result{names: []string{"Error-report"}}},
		{"ErrorCode element is not an Error", "", "", listHead + `<Blob><Name>a</Name><Properties><ErrorCode>x</ErrorCode></Properties></Blob>` + listTail,
			result{names: []string{"a"}}, result{names: []string{"a"}}},
		{"data after the root", "", "", page("", "a") + "<junk/>",
			result{class: objhttp.ClassParse}, result{class: objhttp.ClassParse}},
		{"prefixed root with trailing junk", "", "", `<e:EnumerationResults xmlns:e="urn:test"><Blobs/><NextMarker/></e:EnumerationResults>junk</EnumerationResults>`,
			result{class: objhttp.ClassParse}, result{class: objhttp.ClassParse}},
		{"prefixed root", "", "", `<e:EnumerationResults xmlns:e="urn:test"><Blobs/><NextMarker/></e:EnumerationResults>`,
			result{}, result{class: objhttp.ClassParse}},
		{"default namespace on root", "", "", `<EnumerationResults xmlns="urn:test"><Blobs/><NextMarker/></EnumerationResults>`,
			result{}, result{class: objhttp.ClassParse}},
		{"a second root", "", "", page("", "a") + page("", "b"),
			result{class: objhttp.ClassParse}, result{class: objhttp.ClassParse}},
		{"whitespace after the root", "", "", page("", "a") + "\r\n ",
			result{names: []string{"a"}}, result{names: []string{"a"}}},
		{"comment after the root", "", "", page("", "a") + "<!-- c -->",
			result{names: []string{"a"}}, result{class: objhttp.ClassParse}},
		{"two Blobs", "", "", listHead + `<Blob><Name>a</Name></Blob></Blobs><Blobs><Blob><Name>b</Name></Blob></Blobs><NextMarker/></EnumerationResults>`,
			result{class: objhttp.ClassParse}, result{names: []string{"a", "b"}}},
		{"hierarchy out of order across kinds", "/", "", listHead + `<Blob><Name>b</Name></Blob><BlobPrefix><Name>a/</Name></BlobPrefix>` + listTail,
			result{class: objhttp.ClassInvalidPage}, result{names: []string{"a/", "b"}}},
		{"hierarchy out of order within a kind", "/", "", listHead + `<Blob><Name>b</Name></Blob><Blob><Name>a</Name></Blob><BlobPrefix><Name>c/</Name></BlobPrefix>` + listTail,
			result{class: objhttp.ClassInvalidPage}, result{class: objhttp.ClassInvalidPage}},
		{"malformed Date header", "", "someday", page("", "a"), result{names: []string{"a"}}, result{class: objhttp.ClassParse}},
		{"malformed date not kept", "", "", listHead + `<Blob><Name>a</Name><Properties><DeletedTime>soon</DeletedTime></Properties></Blob>` + listTail,
			result{names: []string{"a"}}, result{class: objhttp.ClassParse}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if c.date != "" {
					w.Header()["Date"] = []string{c.date}
				}
				io.WriteString(w, c.body)
			}))
			defer srv.Close()
			hand, sdk := pair(t, client.Config{Endpoint: srv.URL, Account: "acct", Anonymous: true, MaxAttempts: 1})
			var pages []*client.ListOutput
			for i, l := range []lister{hand, sdk} {
				var got result
				p, err := l.List(context.Background(), &client.ListInput{Container: "c", Delimiter: c.delimiter})
				pages = append(pages, p)
				if err != nil {
					got.class = client.Classify(err)
				} else {
					for _, it := range p.Items {
						got.names = append(got.names, it.Name)
					}
				}
				if want := []result{c.hand, c.sdk}[i]; !reflect.DeepEqual(got, want) {
					t.Errorf("%T: %+v (%v), want %+v", l, got, err, want)
				} else if want.class == objhttp.ClassParse && !errors.Is(err, objhttp.ErrSyntax) {
					t.Errorf("%T: %v does not wrap ErrSyntax", l, err)
				}
			}
			if c.hand.class == "" && reflect.DeepEqual(c.hand, c.sdk) && !reflect.DeepEqual(pages[0], pages[1]) {
				t.Errorf("pages differ: hand %+v, SDK %+v", pages[0], pages[1])
			}
		})
	}
}

// TestAccountInfo: both clients read x-ms-is-hns-enabled exactly:
// "true" and "false" are the answer, any other value (another spelling,
// "1", "0") or none is unknown, never flat, whatever the SDK's ParseBool
// would make of it.
func TestAccountInfo(t *testing.T) {
	type answer struct {
		hns     bool
		unknown bool
	}
	for _, c := range []struct {
		header    string
		hand, sdk answer
	}{
		{"true", answer{hns: true}, answer{hns: true}},
		{"false", answer{}, answer{}},
		{"True", answer{unknown: true}, answer{unknown: true}},
		{"FALSE", answer{unknown: true}, answer{unknown: true}},
		{"", answer{unknown: true}, answer{unknown: true}},
		{"yes", answer{unknown: true}, answer{unknown: true}},
		{"1", answer{unknown: true}, answer{unknown: true}},
		{"0", answer{unknown: true}, answer{unknown: true}},
		{"tRuE", answer{unknown: true}, answer{unknown: true}},
	} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.RawQuery != "comp=properties&restype=account" || r.URL.Path != "/acct/c" {
				t.Errorf("request %s ?%s", r.URL.Path, r.URL.RawQuery)
			}
			if c.header != "" {
				w.Header().Set("x-ms-is-hns-enabled", c.header)
			}
		}))
		hand, sdk := pair(t, client.Config{Endpoint: srv.URL, Account: "acct", Anonymous: true, MaxAttempts: 1})
		for i, l := range []interface {
			AccountInfo(context.Context, string) (bool, error)
		}{hand, sdk} {
			hns, err := l.AccountInfo(context.Background(), "c")
			got := answer{hns: hns, unknown: errors.Is(err, client.ErrHNSUnknown)}
			if err != nil && !got.unknown {
				t.Errorf("%q %T: %v", c.header, l, err)
			}
			if want := []answer{c.hand, c.sdk}[i]; got != want {
				t.Errorf("%q %T: %+v (%v), want %+v", c.header, l, got, err, want)
			}
		}
		srv.Close()
	}
}

// TestMergeOrder: merging a valid hierarchy page's blobs and prefixes,
// each in UTF-16 code-unit order, restores the one sequence, over random
// splits of the hostile corpus.
func TestMergeOrder(t *testing.T) {
	names := corpus()
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

// toFake sends every request to the fake server, keeping the host it was
// addressed to in X-Original-Host.
type toFake struct{ to *url.URL }

func (t toFake) RoundTrip(req *http.Request) (*http.Response, error) {
	req = req.Clone(req.Context())
	req.Header.Set("X-Original-Host", req.URL.Host)
	req.URL.Scheme, req.URL.Host, req.Host = t.to.Scheme, t.to.Host, t.to.Host
	return http.DefaultTransport.RoundTrip(req)
}

// TestRoute: the SDK client's Route is the hand-rolled client's, SAS-free,
// and every request's URL starts with it (the SAS after it), on the
// default endpoint and on endpoints with and without a path.
func TestRoute(t *testing.T) {
	var cp capture
	srv := cp.server(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("comp") == "list" {
			fmt.Fprint(w, page(""))
		}
	})
	u, _ := url.Parse(srv.URL)
	for _, ep := range []string{"", "https://acct.blob.core.windows.net", "http://127.0.0.1:10000", "http://127.0.0.1:10000/emu/path/", "https://h.example:8443/a%2Fb"} {
		for _, sas := range []string{"", testSAS} {
			cfg := client.Config{Endpoint: ep, Account: "acct", SAS: sas, Anonymous: sas == "", MaxAttempts: 1, HTTPClient: &http.Client{Transport: toFake{u}}}
			hand, sdk := pair(t, cfg)
			route := sdk.Route("cont")
			if want := hand.Route("cont"); route != want || strings.Contains(route, "?") {
				t.Fatalf("%q: route %q, the hand-rolled client's %q", ep, route, want)
			}
			if _, err := sdk.List(context.Background(), &client.ListInput{Container: "cont"}); err != nil {
				t.Fatal(err)
			}
			if _, err := sdk.AccountInfo(context.Background(), "cont"); err != nil && !errors.Is(err, client.ErrHNSUnknown) {
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
	var cp capture
	srv := cp.server(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "challenge") {
			w.Header().Set("WWW-Authenticate", `Bearer authorization_uri="https://login.example/t", error="insufficient_claims", claims="e30="`)
			azureError(w, r, 401, "InvalidAuthenticationInfo")
			return
		}
		fmt.Fprint(w, page(""))
	})
	c, err := New(context.Background(), client.Config{Endpoint: srv.URL, Account: "acct", Credential: fakeToken{tok: "tok"}, MaxAttempts: 3})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.List(context.Background(), &client.ListInput{Container: "c"}); err != nil {
		t.Fatal(err)
	}
	if a := cp.last().Header.Get("Authorization"); a != "Bearer tok" {
		t.Fatalf("Authorization %q", a)
	}
	n := len(cp.reqs)
	_, err = c.List(context.Background(), &client.ListInput{Container: "challenge"})
	if client.Classify(err) != objhttp.ClassAuth || len(cp.reqs) != n+1 {
		t.Fatalf("challenge: %v (class %q), %d requests", err, client.Classify(err), len(cp.reqs)-n)
	}
	bad, err := New(context.Background(), client.Config{Endpoint: srv.URL, Account: "acct", Credential: fakeToken{err: errors.New("refused")}, MaxAttempts: 1})
	if err != nil {
		t.Fatal(err)
	}
	n = len(cp.reqs)
	_, err = bad.List(context.Background(), &client.ListInput{Container: "c"})
	if _, ok := errors.AsType[*client.CredentialError](err); !ok || len(cp.reqs) != n || bad.Counters().Attempts != 0 {
		t.Fatalf("token failure: %v, %d requests, %d attempts", err, len(cp.reqs)-n, bad.Counters().Attempts)
	}
}

// TestNamespaceInfo: Filesystem Get Properties is the core's HEAD on the
// account's Data Lake host (the default endpoint only); with Shared Key it
// is not asked.
func TestNamespaceInfo(t *testing.T) {
	var cp capture
	srv := cp.server(t, func(w http.ResponseWriter, r *http.Request) { w.Header().Set("x-ms-namespace-enabled", "false") })
	u, _ := url.Parse(srv.URL)
	c, err := New(context.Background(), client.Config{Account: "acct", SAS: testSAS, MaxAttempts: 1, HTTPClient: &http.Client{Transport: toFake{u}}})
	if err != nil {
		t.Fatal(err)
	}
	if hns, err := c.NamespaceInfo(context.Background(), "c"); err != nil || hns {
		t.Fatalf("%v, %v", hns, err)
	}
	if req := cp.last(); req.Method != http.MethodHead || req.Header.Get("X-Original-Host") != "acct.dfs.core.windows.net" {
		t.Fatalf("%s %s", req.Method, req.Header.Get("X-Original-Host"))
	}
	k, err := New(context.Background(), client.Config{Account: "acct", SharedKey: testKey, MaxAttempts: 1, HTTPClient: &http.Client{Transport: toFake{u}}})
	if err != nil {
		t.Fatal(err)
	}
	n := len(cp.reqs)
	if _, err := k.NamespaceInfo(context.Background(), "c"); !errors.Is(err, client.ErrNoDFSEndpoint) || len(cp.reqs) != n {
		t.Fatalf("Shared Key: %v, %d requests", err, len(cp.reqs)-n)
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
	c, err := New(context.Background(), client.Config{Endpoint: srv.URL, Account: "acct", SAS: testSAS, MaxAttempts: 3,
		BaseBackoff: time.Millisecond, MaxBackoff: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.List(context.Background(), &client.ListInput{Container: "c"})
	ex, ok := errors.AsType[*client.ExhaustedError](err)
	if !ok || ex.Attempts != 3 || client.Classify(err) != objhttp.ClassParse || c.Counters().ParseFailures != 3 {
		t.Fatalf("%v: class %q, parse failures %d", err, client.Classify(err), c.Counters().ParseFailures)
	}
	if strings.Contains(err.Error(), secret) || !strings.Contains(err.Error(), "REDACTED") {
		t.Fatalf("not redacted: %v", err)
	}
}

// TestLogScrubbed starts with azcore's environment-enabled stderr listener
// in a fresh process: New must replace it before any SDK request logs the
// SAS. No test listener is installed, so this covers production wiring as
// well as scrubbing, independently of other tests' initialization order.
func TestLogScrubbed(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestLogScrubProcess$")
	cmd.Env = append(os.Environ(), "SWATH_TEST_LOGSCRUB=1", "AZURE_SDK_GO_LOGGING=all")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	stdout, err := cmd.Output()
	if err != nil {
		t.Fatalf("log scrub helper: %v\nstdout: %s\nstderr: %s", err, stdout, stderr.String())
	}
	logged := stderr.String()
	if logged == "" {
		t.Fatal("the SDK did not log to stderr")
	}
	sas, err := url.ParseQuery(testSAS)
	if err != nil {
		t.Fatal(err)
	}
	for _, sig := range []string{sas.Get("sig"), url.QueryEscape(sas.Get("sig"))} {
		if strings.Contains(logged, sig) {
			t.Errorf("stderr contains the signature %q: %s", sig, logged)
		}
	}
	for _, want := range []string{"REDACTED", "REQUEST ERROR", "AuthorizationFailure"} {
		if !strings.Contains(logged, want) {
			t.Errorf("stderr does not contain %q: %s", want, logged)
		}
	}
}

// TestLogScrubProcess is run only by TestLogScrubbed's child process so
// azcore's environment handling and scrubLogs' sync.Once both start fresh.
func TestLogScrubProcess(t *testing.T) {
	if os.Getenv("SWATH_TEST_LOGSCRUB") != "1" {
		t.Skip("subprocess helper for TestLogScrubbed")
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { azureError(w, r, 403, "AuthorizationFailure") }))
	t.Cleanup(srv.Close)
	down := httptest.NewServer(http.NotFoundHandler())
	down.Close() // a connection refused
	for _, ep := range []string{srv.URL, down.URL} {
		c, err := New(context.Background(), client.Config{Endpoint: ep, Account: "acct", SAS: testSAS, MaxAttempts: 1})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.List(context.Background(), &client.ListInput{Container: "c"}); err == nil || strings.Contains(err.Error(), secret) {
			t.Fatalf("%s: %v", ep, err)
		}
	}
}

// TestConcurrentClient: 16 goroutines list distinct ranges (prefixes,
// startFrom seeks, page sizes) through one SDK client while a proxy
// throttles every seventh request and the body cap makes the large pages
// halve: each gets its own range's names, in order, as a fault-free
// hand-rolled walk lists them.
func TestConcurrentClient(t *testing.T) {
	r := azblobtest.Serve(t, azblobtest.Keys())
	ep := strings.TrimSuffix(r.Endpoint, "/"+r.Account)
	target, _ := url.Parse(ep)
	var n atomic.Int64
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Query().Get("comp") == "list" && n.Add(1)%7 == 0 {
			w.Header().Set("Retry-After", "0")
			azureError(w, req, 503, "ServerBusy")
			return
		}
		out, err := http.DefaultTransport.RoundTrip(withHost(req, target))
		if err != nil {
			w.WriteHeader(502)
			return
		}
		defer out.Body.Close()
		for k, v := range out.Header {
			w.Header()[k] = v
		}
		w.WriteHeader(out.StatusCode)
		io.Copy(w, out.Body)
	}))
	t.Cleanup(proxy.Close)
	sdk, err := New(context.Background(), client.Config{Endpoint: proxy.URL, Account: r.Account, Anonymous: true, Workers: 32,
		BaseBackoff: time.Millisecond, MaxBackoff: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	const cap = 6 << 10 // a page of 5 fits, one of 40 does not
	sdk.SetBodyCap(cap)
	for _, g := range sdk.guards {
		g.MaxBody = cap
	}
	ref, err := client.New(context.Background(), client.Config{Endpoint: ep, Account: r.Account, Anonymous: true})
	if err != nil {
		t.Fatal(err)
	}
	inputs := make([]client.ListInput, 16)
	for i := range inputs {
		in := client.ListInput{Container: r.Container, Prefix: fmt.Sprintf("f/%02d", i), MaxResults: 5 + i%3}
		if i%2 == 1 {
			in.StartFrom = fmt.Sprintf("f/%02d50", i)
		}
		if i == 0 {
			in.MaxResults = 40
		}
		inputs[i] = in
	}
	names := func(pages []client.ListOutput) []string {
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
				got[i] = append(got[i], names([]client.ListOutput{*p})...)
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
	if sdk.BodyCapShrinks() == 0 || sdk.Counters().Retries == 0 || sdk.Counters().Throttles == 0 {
		t.Fatalf("shrinks %d, retries %d, throttles %d: the faults were not met", sdk.BodyCapShrinks(), sdk.Counters().Retries, sdk.Counters().Throttles)
	}
}

// withHost is req addressed to target, for a forwarding proxy.
func withHost(req *http.Request, target *url.URL) *http.Request {
	out := req.Clone(req.Context())
	out.RequestURI = ""
	out.URL.Scheme, out.URL.Host, out.Host = target.Scheme, target.Host, target.Host
	return out
}
