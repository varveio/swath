// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package azure_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
)

// The SDK runs (azure-10a, 10b, 10c) are replayed by the official Azure
// SDK for Go's paginators against the Go server: the SDK builds each
// request, follows NextMarker and decodes the XML (Encoded names
// included). Its transport fixes what the captured run fixed through the
// Java SDK's options and is otherwise transparent: the service version
// (the Go SDK has no option for it) and the deterministic client request
// id prefix-N of call N. It records every exchange, which is then held to
// the captured run as the reference does: the request scope of every
// call, the inventory over the walk, and the SDK's decoded items against
// the native pages.

// sdkTransport sends the SDK's requests to the server and records them.
type sdkTransport struct {
	version, idPrefix string
	// startFrom, when set, is added to a call that lacks it: the flat
	// paginator of the SDK release used here drops its StartFrom option
	// (the hierarchy paginator keeps it). Later releases send it, but
	// they require Apache Arrow and newer compression modules than the
	// production build uses, so the test adds what they would send.
	startFrom string
	mu        sync.Mutex
	steps     []replayStep
}

func (tr *sdkTransport) Do(req *http.Request) (*http.Response, error) {
	tr.mu.Lock()
	n := len(tr.steps) + 1
	tr.mu.Unlock()
	// The SDK keeps its x-ms-* names in lower case, outside the canonical
	// key Header.Set uses: replace every spelling, or the request would
	// carry the header twice.
	set := func(name, value string) {
		for k := range req.Header {
			if strings.EqualFold(k, name) {
				delete(req.Header, k)
			}
		}
		req.Header[name] = []string{value}
	}
	if tr.version != "" {
		set("x-ms-version", tr.version)
	}
	if tr.idPrefix != "" {
		set("x-ms-client-request-id", tr.idPrefix+"-"+strconv.Itoa(n))
	}
	if tr.startFrom != "" && !req.URL.Query().Has("startFrom") {
		q := req.URL.Query()
		q.Set("startFrom", tr.startFrom)
		req.URL.RawQuery = q.Encode()
	}
	resp, err := http.DefaultTransport.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return nil, err
	}
	resp.Body = io.NopCloser(bytes.NewReader(body))
	var rh, sh [][2]string
	for name, vs := range req.Header {
		for _, v := range vs {
			rh = append(rh, [2]string{strings.ToLower(name), v})
		}
	}
	for name, vs := range resp.Header {
		for _, v := range vs {
			sh = append(sh, [2]string{strings.ToLower(name), v})
		}
	}
	if resp.ContentLength >= 0 && resp.Header.Get("Content-Length") == "" {
		sh = append(sh, [2]string{"content-length", strconv.FormatInt(resp.ContentLength, 10)})
	}
	tr.mu.Lock()
	tr.steps = append(tr.steps, replayStep{newRequest(req.Method, req.URL.EscapedPath(), req.URL.RawQuery, rh),
		exchange{resp.StatusCode, lowerKeys(sh), body}})
	tr.mu.Unlock()
	return resp, nil
}

func lowerKeys(kv [][2]string) map[string][]string {
	h := map[string][]string{}
	for _, p := range kv {
		h[p[0]] = append(h[p[0]], p[1])
	}
	return h
}

// sdkOptions are a captured SDK run's options.
type sdkOptions struct {
	ClientRequestIDPrefix string `json:"clientRequestIdPrefix"`
	MaxPages              int    `json:"maxPages"`
	PageSize              int32  `json:"pageSize"`
	Prefix                string `json:"prefix"`
	Delimiter             string `json:"delimiter"`
	StartFrom             string `json:"startFrom"`
	ServiceVersion        string `json:"serviceVersion"`
	Walk                  string `json:"walk"`
}

// sdkItem is one item the SDK decoded: a blob or a prefix.
type sdkItem struct {
	prefix       bool
	name         string
	size         int64
	lastModified time.Time
}

// sdkClient is a container client on the replay's local account path with
// retries off.
func sdkClient(endpoint string, tr *sdkTransport, retries int32) *container.Client {
	client, err := azblob.NewClientWithNoCredential(endpoint, &azblob.ClientOptions{ClientOptions: azcore.ClientOptions{
		Transport: tr, Retry: policy.RetryOptions{MaxRetries: retries, RetryDelay: time.Millisecond, MaxRetryDelay: time.Millisecond},
	}})
	if err != nil {
		fail("SDK client: %v", err)
	}
	return client.ServiceClient().NewContainerClient(replayBucket)
}

// sdkWalk runs the SDK paginator the options name to its end.
func sdkWalk(cc *container.Client, o sdkOptions) ([]sdkItem, error) {
	var items []sdkItem
	blob := func(name *string, size *int64, lm *time.Time) {
		it := sdkItem{name: deref(name)}
		if size != nil {
			it.size = *size
		}
		if lm != nil {
			it.lastModified = lm.UTC()
		}
		items = append(items, it)
	}
	maxPages := max(o.MaxPages, 1)
	ptr := func(s string) *string {
		if s == "" {
			return nil
		}
		return &s
	}
	switch o.Walk {
	case "flat":
		pager := cc.NewListBlobsFlatPager(&container.ListBlobsFlatOptions{Prefix: ptr(o.Prefix), MaxResults: &o.PageSize, StartFrom: ptr(o.StartFrom)})
		for pages := 0; pager.More(); pages++ {
			if pages == maxPages {
				return items, fmt.Errorf("page limit")
			}
			resp, err := pager.NextPage(context.Background())
			if err != nil {
				return items, err
			}
			for _, b := range resp.Segment.BlobItems {
				blob(b.Name, b.Properties.ContentLength, b.Properties.LastModified)
			}
		}
	case "hierarchy":
		pager := cc.NewListBlobsHierarchyPager(o.Delimiter, &container.ListBlobsHierarchyOptions{Prefix: ptr(o.Prefix), MaxResults: &o.PageSize, StartFrom: ptr(o.StartFrom)})
		for pages := 0; pager.More(); pages++ {
			if pages == maxPages {
				return items, fmt.Errorf("page limit")
			}
			resp, err := pager.NextPage(context.Background())
			if err != nil {
				return items, err
			}
			// The SDK returns a page's prefixes apart from its blobs; the
			// item order across kinds is checked on the XML.
			for _, p := range resp.Segment.BlobPrefixes {
				items = append(items, sdkItem{prefix: true, name: deref(p.Name)})
			}
			for _, b := range resp.Segment.BlobItems {
				blob(b.Name, b.Properties.ContentLength, b.Properties.LastModified)
			}
		}
	default:
		return nil, fmt.Errorf("unknown SDK walk %q", o.Walk)
	}
	return items, nil
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// verifySDK runs the captured SDK walk through the Go SDK and compares.
func (e *endpoint) verifySDK(c capture, raw json.RawMessage) {
	var o sdkOptions
	if err := json.Unmarshal(raw, &o); err != nil || o.ClientRequestIDPrefix == "" {
		fail("SDK options lack a client request id prefix")
	}
	tr := &sdkTransport{version: o.ServiceVersion, idPrefix: o.ClientRequestIDPrefix}
	if o.Walk == "flat" {
		tr.startFrom = o.StartFrom
	}
	items, err := sdkWalk(sdkClient(e.replayEndpoint(), tr, -1), o)
	if err != nil {
		fail("replay SDK walk incomplete: %v", err)
	}
	require(len(tr.steps) > 0, "Azure replay run is empty")
	for _, s := range tr.steps {
		assertMapped(c.steps[0].request.path, s.request.path, nativeEndpoint, e.replayEndpoint())
	}
	assertSDKRequests(c, tr.steps, e.replayPath())
	var nativePages, replayPages []capturedPage
	for _, s := range c.steps {
		nativePages = append(nativePages, azurePage(s.request, s.exchange))
	}
	for _, s := range tr.steps {
		replayPages = append(replayPages, azurePage(s.request, normalizedResponse(s.exchange, sp(opaqueDigest(*s.request.header("x-ms-client-request-id"))))))
	}
	assertAzureWalk(nativePages, replayPages, nativeEndpoint, e.replayEndpoint(), nativeContainer, replayBucket,
		c.steps[0].request.header("x-ms-version"), 1024, 1_000_000)
	assertSDKItems(nativePages, items)
}

// assertSDKRequests is the run's request check for SDK calls: each call
// is a GET of the replay path whose parameters other than the marker are
// the captured first call's (the Go SDK orders and escapes them its own
// way, so they are compared decoded and unordered), with the captured
// service version, the captured client request id digest for each call
// the native run also made, and the Go SDK's product token in its user
// agent (the captured run's is the Java SDK's).
func assertSDKRequests(c capture, replay []replayStep, replayPath string) {
	nativeFirst := c.steps[0].request
	scope := queryParams(withoutField(nativeFirst.query, "marker"))
	require(token(nativeFirst.query, "marker") == nil && token(replay[0].request.query, "marker") == nil,
		"a walk must start without a continuation token")
	for i, s := range replay {
		r := s.request
		require(nativeFirst.method == r.method && r.path == replayPath, "replay request method/path differs from captured probe")
		require(slices.Equal(queryParams(withoutField(r.query, "marker")), scope), "replay request query differs from captured probe")
		require(noUnexpectedDuplicates(r.query, false), "duplicate query field outside duplicate-parameter probe")
		require(show(nativeFirst.header("x-ms-version")) == show(r.header("x-ms-version")), "replay request service version differs")
		ua := r.header("user-agent")
		require(ua != nil && strings.Contains(*ua, "azsdk-go-azblob/"), "replay SDK product/version token differs")
		id := r.header("x-ms-client-request-id")
		require(id != nil, "replay request client ID differs")
		if i < len(c.steps) {
			require(show(c.steps[i].request.header("x-ms-client-request-id")) == opaqueDigest(*id), "replay request client ID differs")
		}
	}
}

// queryParams is a query's parameters, form-decoded, sorted.
func queryParams(q string) []string {
	var out []string
	for _, p := range strings.Split(q, "&") {
		if p == "" {
			continue
		}
		k, v, _ := strings.Cut(p, "=")
		dk, err1 := url.QueryUnescape(k)
		dv, err2 := url.QueryUnescape(v)
		if err1 != nil || err2 != nil {
			fail("replay request query differs from captured probe")
		}
		out = append(out, dk+"="+dv)
	}
	slices.Sort(out)
	return out
}

// assertSDKItems holds what the SDK decoded to the native pages: the same
// blobs in order and the same prefixes in order (the SDK hands a page's
// prefixes apart from its blobs, and page boundaries may differ between
// the two walks, so the order across kinds is the XML comparison's), each
// name decoded (an Encoded name percent-decoded), each blob's size and
// modification time.
func assertSDKItems(native []capturedPage, items []sdkItem) {
	var want []sdkItem
	for _, p := range native {
		for _, e := range parsePage(p.exchange.body, false).entries {
			name := e.name
			if e.encoded != nil {
				d, err := url.PathUnescape(name)
				if err != nil {
					fail("native Encoded name is not percent-encoded")
				}
				name = d
			}
			if e.kind == "BlobPrefix" {
				want = append(want, sdkItem{prefix: true, name: name})
				continue
			}
			size, _ := strconv.ParseInt(decimalField(e.properties, "Content-Length"), 10, 64)
			want = append(want, sdkItem{name: name, size: size, lastModified: lastModified(e.properties)})
		}
	}
	for _, kind := range []bool{true, false} {
		var w, g []string
		for _, it := range want {
			if it.prefix == kind {
				w = append(w, fmt.Sprintf("%+v", it))
			}
		}
		for _, it := range items {
			if it.prefix == kind {
				g = append(g, fmt.Sprintf("%+v", it))
			}
		}
		equal(fmt.Sprintf("SDK items (prefixes %v) count", kind), len(w), len(g))
		for i := range w {
			equal(fmt.Sprintf("SDK item (prefixes %v) %d", kind, i), w[i], g[i])
		}
	}
}
