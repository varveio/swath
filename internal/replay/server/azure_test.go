// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/varveio/swath/internal/replay/metrics"
	"github.com/varveio/swath/internal/replay/store/memstore"
)

func azureConfig(protocols ...string) Config {
	cfg := config(memstore.Keys(memstore.Options{Rollup: true}, "a/1", "a/2", "b"))
	cfg.Protocols, cfg.AzureAccount = protocols, DefaultAzureAccount
	return cfg
}

func azureDo(t *testing.T, s *Server, method, target string, header ...string) response {
	t.Helper()
	req, _ := http.NewRequest(method, fmt.Sprintf("http://127.0.0.1:%d%s", s.Port(), target), nil)
	for i := 0; i+1 < len(header); i += 2 {
		req.Header.Add(header[i], header[i+1])
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var b strings.Builder
	buf := make([]byte, 4096)
	for {
		n, err := resp.Body.Read(buf)
		b.Write(buf[:n])
		if err != nil {
			break
		}
	}
	return response{resp.StatusCode, resp.Header, b.String()}
}

const bom = "\xef\xbb\xbf"

// TestAzureRouting pins the routing of the replay wire contract: the Azure
// selector claims its path before the method is checked, a request with
// both the Azure and the S3 selector is a replay 400, every other request
// stays on the S3 route, and with S3 off an unclaimed request is a replay
// 404.
func TestAzureRouting(t *testing.T) {
	s := start(t, azureConfig("s3", "azure"))
	const list = "/replay/bucket?restype=container&comp=list"
	ok := azureDo(t, s, "GET", list+"&delimiter=/", "x-ms-version", "2026-10-06", "x-ms-client-request-id", "c-1")
	if ok.status != 200 || !strings.HasPrefix(ok.body, bom+"<?xml") ||
		!strings.Contains(ok.body, fmt.Sprintf(`ServiceEndpoint="http://127.0.0.1:%d/replay/"`, s.Port())) ||
		!strings.Contains(ok.body, "<BlobPrefix><Name>a/</Name></BlobPrefix><Blob><Name>b</Name>") ||
		ok.header.Get("x-ms-version") != "2026-10-06" || ok.header.Get("x-ms-client-request-id") != "c-1" ||
		!strings.HasPrefix(ok.header.Get("x-ms-request-id"), "replay-") || ok.header.Get("Date") == "" {
		t.Fatalf("%+v", ok)
	}
	post := azureDo(t, s, "POST", list, "x-ms-version", "2026-06-06")
	if post.status != 405 || post.header.Get("x-ms-error-code") != "UnsupportedHttpVerb" || post.header.Get("allow") != "GET" ||
		post.header.Get("x-ms-version") != "2026-06-06" || !strings.HasPrefix(post.body, bom) {
		t.Fatalf("POST %+v", post)
	}
	head := azureDo(t, s, "HEAD", list, "x-ms-version", "2026-06-06")
	if head.status != 400 || head.body != "" || head.header.Get("x-swath-replay-error") != "container_head_unsupported" {
		t.Fatalf("HEAD %+v", head)
	}
	if r := azureDo(t, s, "GET", list+"&list-type=2"); r.status != 400 || r.body != `{"error":"replay_ambiguous_request"}` {
		t.Fatalf("ambiguous %+v", r)
	}
	// Not claimed: another account, a second path segment, no selector.
	for _, target := range []string{"/other/bucket?restype=container&comp=list", "/replay/bucket/x?comp=list", "/bucket?list-type=2"} {
		if r := azureDo(t, s, "GET", target); r.header.Get("x-amz-request-id") == "" {
			t.Errorf("%s: not the S3 route: %+v", target, r)
		}
	}
	if r := azureDo(t, s, "GET", "/replay/other?restype=container&comp=list", "x-ms-version", "2026-06-06"); r.status != 404 ||
		r.header.Get("x-ms-error-code") != "ContainerNotFound" {
		t.Fatalf("other container %+v", r)
	}
	if count(s, ProviderPath, "azure", "request_refused", "wrong_method") != 1 ||
		count(s, metrics.ProtocolHTTPRequests, "azure", "2xx") != 1 {
		t.Error("azure meters")
	}
	snap := string(s.Snapshot(s.started))
	for _, want := range []string{`"protocols":["azure","s3"]`, `"azure_account":"replay"`,
		`"ordering_profile":"unsigned-utf8-byte-order;azure=utf16-code-unit-order"`,
		`"profiles":{"azure":"blob-list-flat-2026-06-06,2026-10-06","s3":"list-objects-v2"}`} {
		if !strings.Contains(snap, want) {
			t.Errorf("snapshot lacks %s", want)
		}
	}

	only := start(t, azureConfig("azure"))
	if r := azureDo(t, only, "GET", "/bucket?list-type=2"); r.status != 404 || r.body != `{"error":"replay_not_found"}` ||
		r.header.Get("Content-Type") != "application/json" {
		t.Fatalf("azure only %+v", r)
	}
}

// TestAzureMarkerWalk walks the route with its own markers.
func TestAzureMarkerWalk(t *testing.T) {
	s := start(t, azureConfig("azure"))
	var names []string
	marker := ""
	for range 10 {
		target := "/replay/bucket?restype=container&comp=list&maxresults=1"
		if marker != "" {
			target += "&marker=" + marker
		}
		r := azureDo(t, s, "GET", target, "x-ms-version", "2026-06-06")
		if r.status != 200 {
			t.Fatalf("%+v", r)
		}
		i := strings.Index(r.body, "<Name>")
		names = append(names, r.body[i+6:i+6+strings.Index(r.body[i+6:], "<")])
		j := strings.Index(r.body, "<NextMarker>") + len("<NextMarker>")
		marker = r.body[j : j+strings.Index(r.body[j:], "<")]
		if marker == "" {
			break
		}
	}
	if strings.Join(names, ",") != "a/1,a/2,b" {
		t.Fatalf("%v", names)
	}
	bad := azureDo(t, s, "GET", "/replay/bucket?restype=container&comp=list&marker=az1.AAAA", "x-ms-version", "2026-06-06")
	if bad.status != 400 || bad.header.Get("x-swath-replay-error") != "invalid_marker" {
		t.Fatalf("%+v", bad)
	}
}

func TestValidateAzure(t *testing.T) {
	for _, c := range []struct {
		mutate func(*Config)
		ok     bool
	}{
		{func(*Config) {}, true},
		{func(c *Config) { c.AzureAccount = "Replay" }, false},
		{func(c *Config) { c.AzureAccount = "ab" }, false},
		{func(c *Config) { c.Bucket = "Bucket" }, false},
		{func(c *Config) { c.Bucket = "a--b" }, false},
		{func(c *Config) { c.Host = "0.0.0.0" }, false},
		{func(c *Config) { c.Host = "::" }, false},
		{func(c *Config) { c.Host = "0.0.0.0"; c.AdvertisedHost = "replay.local" }, true},
		{func(c *Config) { c.Protocols = []string{"s3"} }, false}, // an account without the route
		{func(c *Config) { c.Protocols = []string{"s3"}; c.AzureAccount = "" }, true},
		{func(c *Config) { c.Protocols = []string{"s3"}; c.AzureAccount = ""; c.AzureHNS = true }, false},
	} {
		cfg := azureConfig("s3", "azure")
		c.mutate(&cfg)
		if err := cfg.Validate(); (err == nil) != c.ok {
			t.Errorf("%+v: %v", cfg.Protocols, err)
		}
	}
}

// TestAzureErrorEchoUsesTheClientIDRule pins the error echo to the same
// length rule as the refusal: 512 raw 0xE9 bytes are echoed, 513 are not.
func TestAzureErrorEchoUsesTheClientIDRule(t *testing.T) {
	s := start(t, azureConfig("azure"))
	for n, echoed := range map[int]bool{512: true, 513: false} {
		id := strings.Repeat("\xe9", n)
		r := azureDo(t, s, "GET", "/replay/bucket?restype=container&comp=list", "x-ms-version", "bad", "x-ms-client-request-id", id)
		if r.status != 400 || (r.header.Get("x-ms-client-request-id") == id) != echoed {
			t.Errorf("%d bytes: %d echo %d bytes", n, r.status, len(r.header.Get("x-ms-client-request-id")))
		}
	}
	r := azureDo(t, s, "GET", "/replay/bucket?restype=container&comp=list", "x-ms-version", "2026-06-06",
		"x-ms-client-request-id", strings.Repeat("\xe9", 513))
	if r.status != 400 || r.header.Get("x-swath-replay-error") != "invalid_client_request_id" || r.header.Get("x-ms-client-request-id") != "" {
		t.Errorf("%+v", r.header)
	}
}

// TestAzureEngagementMeters pins the provider.path meters the server
// publishes for an encoded name and an empty window.
func TestAzureEngagementMeters(t *testing.T) {
	cfg := config(memstore.Keys(memstore.Options{Rollup: true}, "u/\uffff", "u/\ufffe/x"))
	cfg.Protocols, cfg.AzureAccount = []string{"azure"}, DefaultAzureAccount
	s := start(t, cfg)
	for _, q := range []string{"prefix=u/&delimiter=/", "prefix=a&startFrom=b"} {
		if r := azureDo(t, s, "GET", "/replay/bucket?restype=container&comp=list&"+q, "x-ms-version", "2026-06-06"); r.status != 200 {
			t.Fatalf("%s: %+v", q, r)
		}
	}
	for _, c := range []struct{ path, reason string }{
		{"encoded_name", "blob_xml_forbidden"}, {"encoded_name", "prefix_xml_forbidden"}, {"empty_window", "lower_bound_past_prefix"},
	} {
		if n := count(s, ProviderPath, "azure", c.path, c.reason); n != 1 {
			t.Errorf("%s %s: %d", c.path, c.reason, n)
		}
	}
}

// TestAzureAccountInformation pins Get Account Information on every path
// the route claims, existing container or not, by GET and HEAD: 200 with an
// empty body and the flat account's headers, the HNS answer under
// AzureHNS, and the route's error envelopes for a wrong method, a missing
// or unsupported version and a wrong selector.
func TestAzureAccountInformation(t *testing.T) {
	const query = "?restype=account&comp=properties"
	for _, hns := range []bool{false, true} {
		cfg := azureConfig("s3", "azure")
		cfg.AzureHNS = hns
		s := start(t, cfg)
		for _, path := range []string{"/replay", "/replay/", "/replay/bucket", "/replay/bucket/", "/replay/other", "/replay/other/"} {
			for _, method := range []string{"GET", "HEAD"} {
				r := azureDo(t, s, method, path+query+"&timeout=30", "x-ms-version", "2026-10-06", "x-ms-client-request-id", "c-2")
				checkAccountInformation(t, fmt.Sprintf("hns=%v %s %s", hns, method, path), r, hns)
			}
		}
	}

	s := start(t, azureConfig("s3", "azure"))
	for _, c := range []struct {
		method, target string
		header         []string
		status         int
		code, reason   string
	}{
		{"POST", "/replay" + query, []string{"x-ms-version", "2026-06-06"}, 405, "UnsupportedHttpVerb", "wrong_method"},
		{"PUT", "/replay/bucket" + query, []string{"x-ms-version", "2026-06-06"}, 405, "UnsupportedHttpVerb", "wrong_method"},
		{"GET", "/replay" + query, nil, 400, "InvalidHeaderValue", "invalid_header_value"},
		{"GET", "/replay/bucket" + query, []string{"x-ms-version", "2019-12-12"}, 400, "InvalidHeaderValue", "invalid_header_value"},
		{"GET", "/replay" + query, []string{"x-ms-version", "2026-06-06", "x-ms-version", "2026-06-06"}, 400, "InvalidHeaderValue", "duplicate_header"},
		{"GET", "/replay?restype=account&comp=list", []string{"x-ms-version", "2026-06-06"}, 400, "InvalidQueryParameterValue", "invalid_selector"},
		{"GET", "/replay" + query + "&prefix=a", []string{"x-ms-version", "2026-06-06"}, 400, "UnsupportedQueryParameter", "unsupported_query_parameter"},
		{"GET", "/replay" + query + "&sig=a&sig=b", []string{"x-ms-version", "2026-06-06"}, 400, "InvalidQueryParameterValue", "duplicate_query_parameter"},
		{"GET", "/replay" + query + "&timeout=0", []string{"x-ms-version", "2026-06-06"}, 400, "InvalidQueryParameterValue", "invalid_timeout"},
	} {
		r := azureDo(t, s, c.method, c.target, c.header...)
		if r.status != c.status || r.header.Get("x-ms-error-code") != c.code || r.header.Get("x-swath-replay-error") != c.reason ||
			!strings.HasPrefix(r.body, bom+"<?xml") || r.header.Get("x-ms-is-hns-enabled") != "" {
			t.Errorf("%s %s %q: %+v", c.method, c.target, c.header, r)
		}
		if c.status == 405 && r.header.Get("allow") != "GET, HEAD" {
			t.Errorf("%s %s: Allow %q", c.method, c.target, r.header.Get("allow"))
		}
	}
	if r := azureDo(t, s, "GET", "/replay"+query+"&list-type=2"); r.status != 400 || r.body != `{"error":"replay_ambiguous_request"}` {
		t.Fatalf("ambiguous %+v", r)
	}
	// Without the account-information selector the account paths and a
	// container path with a trailing slash stay on the S3 route.
	for _, target := range []string{"/replay?list-type=2", "/replay?restype=container&comp=list", "/replay/?restype=container&comp=list",
		"/replay/bucket/?restype=container&comp=list"} {
		if r := azureDo(t, s, "GET", target); r.header.Get("x-amz-request-id") == "" {
			t.Errorf("%s: not the S3 route: %+v", target, r)
		}
	}
	if count(s, ProviderPath, "azure", "request_refused", "wrong_method") != 2 {
		t.Error("refusal meters")
	}
}

func checkAccountInformation(t *testing.T, what string, r response, hns bool) {
	t.Helper()
	if r.status != 200 || r.body != "" || r.header.Get("Content-Length") != "0" || r.header.Get("Content-Type") != "" ||
		r.header.Get("x-ms-is-hns-enabled") != fmt.Sprint(hns) || r.header.Get("x-ms-account-kind") != "StorageV2" ||
		r.header.Get("x-ms-sku-name") != "Standard_LRS" || r.header.Get("x-ms-version") != "2026-10-06" ||
		r.header.Get("x-ms-client-request-id") != "c-2" || !strings.HasPrefix(r.header.Get("x-ms-request-id"), "replay-") ||
		r.header.Get("Date") == "" {
		t.Errorf("%s: %+v", what, r)
	}
}

// TestAzureSASQueries sends both operations a SAS-shaped query: the SAS
// parameters are accepted and ignored, an unknown parameter beside them
// is still refused.
func TestAzureSASQueries(t *testing.T) {
	const sas = "sv=2026-06-06&ss=b&srt=sco&sp=rl&se=2026-12-31T00%3A00%3A00Z&st=2026-01-01T00%3A00%3A00Z&spr=https" +
		"&sr=c&si=policy&sip=127.0.0.1&skoid=o&sktid=t&skt=2026-01-01&ske=2026-12-31&sks=b&skv=2026-06-06" +
		"&saoid=a&suoid=u&scid=c&ses=e&sdd=1&rscc=no-cache&rscd=inline&rsce=gzip&rscl=en&rsct=text%2Fplain&sig=c2ln"
	s := start(t, azureConfig("azure"))
	list := azureDo(t, s, "GET", "/replay/bucket?restype=container&comp=list&"+sas, "x-ms-version", "2026-06-06")
	if list.status != 200 || !strings.Contains(list.body, "<Name>a/1</Name>") {
		t.Errorf("list: %+v", list)
	}
	account := azureDo(t, s, "GET", "/replay/bucket?restype=account&comp=properties&"+sas, "x-ms-version", "2026-10-06",
		"x-ms-client-request-id", "c-2")
	checkAccountInformation(t, "account", account, false)
	for _, q := range []string{"restype=container&comp=list", "restype=account&comp=properties"} {
		if r := azureDo(t, s, "GET", "/replay/bucket?"+q+"&"+sas+"&sigx=1", "x-ms-version", "2026-06-06"); r.status != 400 ||
			r.header.Get("x-swath-replay-error") != "unsupported_query_parameter" {
			t.Errorf("%s with an unknown parameter: %+v", q, r)
		}
	}
}
