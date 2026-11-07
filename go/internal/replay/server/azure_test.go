// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/varveio/swath/go/internal/replay/metrics"
	"github.com/varveio/swath/go/internal/replay/store/memstore"
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
