// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/varveio/swath/internal/replay/metrics"
	"github.com/varveio/swath/internal/replay/store/memstore"
)

// rawGet sends target as the request line, byte for byte.
func rawGet(t *testing.T, s *Server, method, target string) (*http.Response, string) {
	t.Helper()
	conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", s.Port()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(10 * time.Second))
	fmt.Fprintf(conn, "%s %s HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n", method, target)
	resp, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: method})
	if err != nil {
		t.Fatalf("%q: %v", target, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp, string(body)
}

// TestRejectsAmbiguousTargets holds the replay wire contract's verdict on
// raw request targets, each taken from the Java server: the ones its HTTP
// layer refuses get 400 with an HTML page, no caching and no S3 headers,
// and are not counted; the unusual ones it lets through reach the S3
// route.
func TestRejectsAmbiguousTargets(t *testing.T) {
	s := start(t, config(memstore.Keys(memstore.Options{}, "a")))
	for _, target := range []string{
		"//bucket?list-type=2", "/bucket%2Fchild?list-type=2", "/bucket%2fchild?list-type=2", "/%2Fbucket?list-type=2",
		"/bucket/x%2Fy?list-type=2", "/bucket//x?list-type=2", "/bucket/x//?list-type=2", "/;/bucket?list-type=2",
		"/bucket/;p/x?list-type=2", "/bucket/%2e%2e/x?list-type=2", "/bucket/.%2e?list-type=2", "/%2e/bucket?list-type=2",
		"/bucket/%2E?list-type=2", "/bucket/..;/x?list-type=2", "/../bucket?list-type=2", "/bucket/../../x?list-type=2",
		"/bucket/x/../../..?list-type=2", "/%2e%2e?list-type=2", "/bucket%00?list-type=2", "/bucket%25?list-type=2",
		"/bucket%5C?list-type=2", "/bucket%5c?list-type=2", "/bucket%01?list-type=2", "/bucket%7F?list-type=2",
		"/bucket%0A?list-type=2", "/bucket\\x?list-type=2", "/bucket\xc3\xa9?list-type=2", "/bucket\xff?list-type=2",
		"/bucket\"?list-type=2", "/bucket<?list-type=2", "/bucket>?list-type=2", "/bucket`?list-type=2", "/bucket{?list-type=2",
		"/bucket|?list-type=2", "/bucket}?list-type=2", "/bucket^?list-type=2", "/bucket[?list-type=2", "/bucket]?list-type=2",
		"/bucket#frag?list-type=2", "/bucket?list-type=2#frag", "/bucket%C0%AF?list-type=2", "/bucket%FF?list-type=2",
		"/bucket%ED%A0%80?list-type=2", "*", "http://127.0.0.1//bucket?list-type=2", "http://127.0.0.1/bucket%2F?list-type=2",
		"http://user@127.0.0.1/bucket?list-type=2",
	} {
		for _, method := range []string{"GET", "HEAD"} {
			resp, body := rawGet(t, s, method, target)
			if resp.StatusCode != 400 || resp.Header.Get("Content-Type") != "text/html;charset=iso-8859-1" ||
				resp.Header.Get("Cache-Control") != "must-revalidate,no-cache,no-store" || resp.Header.Get("x-amz-request-id") != "" ||
				(method == "HEAD") != (body == "") {
				t.Errorf("%s %q: %d %v %q", method, target, resp.StatusCode, resp.Header, body)
			}
		}
	}
	if n := count(s, metrics.HTTPRequests); n != 0 {
		t.Errorf("%d refused targets counted", n)
	}
	for target, status := range map[string]int{
		"/bucket/../bucket?list-type=2": 200, "/bucket/.?list-type=2": 200, "/bucket/..?list-type=2": 200,
		"/bucket/a/..?list-type=2": 200, "/./bucket?list-type=2": 404, "/bucket;p?list-type=2": 404, "/;bucket?list-type=2": 404,
		"/bucket/x;/?list-type=2": 200, "/bucket/%3B?list-type=2": 200, "/bucket/a%2Eb?list-type=2": 200,
		"/bucket%20x?list-type=2": 404, "/bucket%C3%A9?list-type=2": 404, "/bucket/%3Fx?list-type=2": 200,
		"/bucket%3F?list-type=2": 404, "/bucket!$&'()*+,=:@~?list-type=2": 404, "http://127.0.0.1/bucket?list-type=2": 200,
		"HTTP://127.0.0.1/bucket?list-type=2": 200, "ftp://127.0.0.1/bucket?list-type=2": 200, "http:/bucket?list-type=2": 200,
		"http://127.0.0.1?list-type=2": 404, "/bucket?list-type=2&prefix=\xc3\xa9": 200, "/bucket?list-type=2&prefix=\"<>`{|}^[]\\": 200,
		"/bucket?list-type=2&prefix=%": 200, "/bucket?": 400, "/": 404,
	} {
		resp, body := rawGet(t, s, "GET", target)
		if resp.StatusCode != status || resp.Header.Get("Content-Type") != "application/xml" || resp.Header.Get("x-amz-request-id") != "S3LISTINGREPLAY" {
			t.Errorf("%q: %d %v %s", target, resp.StatusCode, resp.Header, body)
		}
	}
}
