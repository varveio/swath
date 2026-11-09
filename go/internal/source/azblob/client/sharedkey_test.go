// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
)

// testKey is an account key (base64 of 64 bytes, as Azure issues them);
// not a real one.
const testKey = "dGVzdC1rZXktZm9yLXN3YXRoLXNoYXJlZC1rZXktdmVjdG9ycy0wMTIzNDU2Nzg5YWJjZGVmZ2hpams="

// recorder answers every request with an empty listing and keeps a copy of
// each request as signed.
type recorder struct {
	mu   sync.Mutex
	reqs []*http.Request
}

func (r *recorder) Do(req *http.Request) (*http.Response, error) {
	r.mu.Lock()
	r.reqs = append(r.reqs, req.Clone(context.Background()))
	r.mu.Unlock()
	h := http.Header{"Content-Type": {"application/xml"}, "X-Ms-Is-Hns-Enabled": {"false"}}
	return &http.Response{StatusCode: 200, Header: h, Body: io.NopCloser(strings.NewReader(page(""))), Request: req}, nil
}

// TestSharedKeyMatchesSDK derives the vectors from the official SDK: its
// container client signs real requests (flat and hierarchy listings with
// query values that need escaping, markers, startFrom, maxresults, and Get
// Account Information) with its SharedKeyCredential; the signer here,
// given each request as the SDK sent it, must produce the same
// Authorization. Both endpoint forms: the service's, and the emulator's,
// whose path starts with the account, so the canonical resource names it
// twice.
func TestSharedKeyMatchesSDK(t *testing.T) {
	for _, ep := range []struct{ account, container string }{
		{"acct", "https://acct.blob.core.windows.net/cont"},
		{"devstoreaccount1", "http://127.0.0.1:10000/devstoreaccount1/cont"},
	} {
		cred, err := container.NewSharedKeyCredential(ep.account, testKey)
		if err != nil {
			t.Fatal(err)
		}
		rec := &recorder{}
		cc, err := container.NewClientWithSharedKeyCredential(ep.container, cred, &container.ClientOptions{
			ClientOptions: azcore.ClientOptions{Transport: rec, Retry: policy.RetryOptions{MaxRetries: -1}}})
		if err != nil {
			t.Fatal(err)
		}
		ctx := context.Background()
		str := func(s string) *string { return &s }
		n := int32(7)
		flat := cc.NewListBlobsFlatPager(&container.ListBlobsFlatOptions{Prefix: str("a b+c%d/\u00e9?&=#;,"), Marker: str("m/1+2 3"), MaxResults: &n})
		if _, err := flat.NextPage(ctx); err != nil {
			t.Fatal(err)
		}
		hier := cc.NewListBlobsHierarchyPager("/", &container.ListBlobsHierarchyOptions{Prefix: str("p/"), StartFrom: str("p/x y\U0001F600")})
		if _, err := hier.NextPage(ctx); err != nil {
			t.Fatal(err)
		}
		if _, err := cc.GetAccountInfo(ctx, nil); err != nil {
			t.Fatal(err)
		}
		k, err := newSharedKey(ep.account, testKey)
		if err != nil {
			t.Fatal(err)
		}
		if len(rec.reqs) != 3 {
			t.Fatalf("%d requests", len(rec.reqs))
		}
		for _, req := range rec.reqs {
			want := req.Header.Get(headerAuthorization)
			if req.Header.Get(headerDate) == "" || !strings.HasPrefix(want, "SharedKey "+ep.account+":") {
				t.Fatalf("SDK request headers %v", req.Header)
			}
			req.Header.Del(headerAuthorization)
			if err := k.sign(req); err != nil {
				t.Fatal(err)
			}
			if got := req.Header.Get(headerAuthorization); got != want {
				sts, _ := k.stringToSign(req)
				t.Errorf("%s ?%s: %s, the SDK %s; string to sign:\n%s", req.URL.Path, req.URL.RawQuery, got, want, sts)
			}
		}
		res, _ := k.canonicalResource(rec.reqs[2].URL)
		if prefix := "/" + ep.account + "/"; ep.account == "devstoreaccount1" && !strings.HasPrefix(res, prefix+"devstoreaccount1/cont\n") {
			t.Errorf("emulator canonical resource %q", res)
		}
	}
}

// TestSharedKeyRequests: a client with a shared key signs each attempt it
// sends, with its own x-ms-date, over the query as it arrives (a server
// recomputing the signature agrees), and sends no other credential.
func TestSharedKeyRequests(t *testing.T) {
	k, _ := newSharedKey("acct", testKey)
	var mu sync.Mutex
	n := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		n++
		first := n == 1
		mu.Unlock()
		got := r.Header.Get(headerAuthorization)
		if r.Header.Get(headerDate) == "" || r.URL.Query().Has("sig") {
			t.Errorf("request %v ?%s", r.Header, r.URL.RawQuery)
		}
		r2 := r.Clone(context.Background())
		r2.Header.Del(headerAuthorization)
		if err := k.sign(r2); err != nil || r2.Header.Get(headerAuthorization) != got {
			t.Errorf("signature %q, recomputed %q (%v)", got, r2.Header.Get(headerAuthorization), err)
		}
		if first {
			azureError(w, 503, codeServerBusy)
			return
		}
		w.Header().Set(headerHNSEnabled, "false")
		fmt.Fprint(w, page(""))
	}))
	defer srv.Close()
	// A shared key precedes a token credential.
	c := newTestClient(t, Config{Endpoint: srv.URL, SharedKey: testKey, Credential: failingCredential{}, BaseBackoff: 1})
	if _, err := c.List(context.Background(), &ListInput{Container: "c", Prefix: "a b+c/\u00e9", StartFrom: "a b+c/\u00e9x", MaxResults: 3}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.AccountInfo(context.Background(), "c"); err != nil {
		t.Fatal(err)
	}
	if src, anon := c.Credentials(); src != "shared key" || anon || n != 3 {
		t.Fatalf("credentials %q anonymous=%v, %d requests", src, anon, n)
	}
	var buf bytes.Buffer
	fmt.Fprint(&buf, c.Counters())
	if strings.Contains(buf.String(), testKey) {
		t.Fatal("the key in the counters")
	}
}
