// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/varveio/swath/internal/replay/store/memstore"
)

// rawHead sends head as the whole request and returns the status and
// Content-Type of the answer.
func rawHead(t *testing.T, s *Server, head string) (int, string) {
	t.Helper()
	conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", s.Port()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(10 * time.Second))
	go io.WriteString(conn, head)
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatalf("no answer to a %d-byte head: %v", len(head), err)
	}
	resp.Body.Close()
	return resp.StatusCode, resp.Header.Get("Content-Type")
}

// padded is a listing request head of exactly total bytes, grown in an
// X-Pad header (or, with inLine, in the prefix parameter of the request
// line).
func padded(total int, inLine bool) string {
	head := func(pad string) string {
		if inLine {
			return "GET /bucket?list-type=2&max-keys=1&prefix=" + pad + " HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n"
		}
		return "GET /bucket?list-type=2&max-keys=1 HTTP/1.1\r\nHost: 127.0.0.1\r\nX-Pad: " + pad + "\r\nConnection: close\r\n\r\n"
	}
	return head(strings.Repeat("a", total-len(head(""))))
}

// TestRequestHeadLimits holds the contract's request-head limits, each
// boundary measured on the Java server: with the S3 route alone a head of
// 8,192 bytes is served and one more byte is 431 Request Header Fields Too
// Large, whether the bytes are in a header or the request line, and a
// request line beyond the limit is 414 URI Too Long; with a native route
// enabled the limit is 65,536. The answers are the HTML refusal, and a
// 70 KiB header (the review's case) is refused either way.
func TestRequestHeadLimits(t *testing.T) {
	for _, c := range []struct {
		protocols []string
		limit     int
	}{{[]string{ProtocolS3}, 8192}, {[]string{ProtocolS3, ProtocolGCS}, 65536}} {
		cfg := config(memstore.Keys(memstore.Options{}, "a"))
		cfg.Protocols = c.protocols
		s := start(t, cfg)
		for _, inLine := range []bool{false, true} {
			if st, _ := rawHead(t, s, padded(c.limit, inLine)); st != 200 {
				t.Errorf("%v line=%v: a %d-byte head got %d", c.protocols, inLine, c.limit, st)
			}
			if st, ct := rawHead(t, s, padded(c.limit+1, inLine)); st != 431 || ct != "text/html;charset=iso-8859-1" {
				t.Errorf("%v line=%v: a %d-byte head got %d %s", c.protocols, inLine, c.limit+1, st, ct)
			}
		}
		// Bytes net/http drops or normalizes before a handler sees them
		// count as sent: a Transfer-Encoding header (with its chunked
		// body after the head), a Trailer header, whitespace around a
		// value.
		for name, shaped := range map[string]func(int) string{
			"chunked": func(total int) string { return shapedHead(total, "Transfer-Encoding: chunked\r\n", "", "0\r\n\r\n") },
			"trailer": func(total int) string {
				return shapedHead(total, "Transfer-Encoding: chunked\r\nTrailer: X-T\r\n", "", "0\r\nX-T: v\r\n\r\n")
			},
			"whitespace": func(total int) string { return shapedHead(total, "X-Space:      v      \r\n", "   ", "") },
		} {
			if st, _ := rawHead(t, s, shaped(c.limit)); st != 200 {
				t.Errorf("%v %s: a %d-byte head got %d", c.protocols, name, c.limit, st)
			}
			if st, ct := rawHead(t, s, shaped(c.limit+1)); st != 431 || ct != "text/html;charset=iso-8859-1" {
				t.Errorf("%v %s: a %d-byte head got %d %s", c.protocols, name, c.limit+1, st, ct)
			}
		}
		if st, ct := rawHead(t, s, padded(c.limit+1000, true)); st != 414 || ct != "text/html;charset=iso-8859-1" {
			t.Errorf("%v: a request line past the limit got %d %s", c.protocols, st, ct)
		}
		if st, ct := rawHead(t, s, padded(70<<10, false)); st != 431 || ct != "text/html;charset=iso-8859-1" {
			t.Errorf("%v: a 70 KiB header got %d %s", c.protocols, st, ct)
		}
	}
}

// shapedHead is a listing request whose head (up to and including its
// blank line) is exactly total bytes: extra header lines, an X-Pad header
// grown to fit (its value wrapped in padWS on both sides), and body after
// the head.
func shapedHead(total int, extra, padWS, body string) string {
	head := func(pad string) string {
		return "GET /bucket?list-type=2&max-keys=1 HTTP/1.1\r\nHost: 127.0.0.1\r\n" + extra +
			"X-Pad:" + padWS + pad + padWS + "\r\nConnection: close\r\n\r\n"
	}
	return head(strings.Repeat("a", total-len(head("")))) + body
}

// TestRawHeadsStayAlignedOnAConnection: on one keep-alive connection,
// pipelined requests of different head sizes are each measured by their
// own bytes: a small head, one of exactly the limit, a small one, all
// served, then one over the limit refused.
func TestRawHeadsStayAlignedOnAConnection(t *testing.T) {
	s := start(t, config(memstore.Keys(memstore.Options{}, "a")))
	// keep makes a head of total bytes ask to stay open: "keep-alive" is
	// five bytes longer than "close".
	keep := func(total int) string {
		return strings.Replace(padded(total-5, false), "Connection: close\r\n", "Connection: keep-alive\r\n", 1)
	}
	small := "GET /bucket?list-type=2&max-keys=1 HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n"
	heads := small + keep(8192) + small + keep(8193)
	conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", s.Port()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(10 * time.Second))
	go io.WriteString(conn, heads)
	br := bufio.NewReader(conn)
	for i, want := range []int{200, 200, 200, 431} {
		resp, err := http.ReadResponse(br, nil)
		if err != nil {
			t.Fatalf("answer %d: %v", i, err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode != want {
			t.Errorf("answer %d: %d, want %d", i, resp.StatusCode, want)
		}
	}
}

// TestBlankLinesBetweenRequestsAreNotAHead: net/http discards the CR and
// LF a client sends after a bodyless POST before its next request; the
// head counter must too, or the stray bytes take the next request's place
// in the queue. A POST with Content-Length: 0, then CRLFCRLF, then a GET
// head one byte over the limit: 405 for the POST, 431 for the GET (and a
// GET of exactly the limit is served), as on the Java server.
func TestBlankLinesBetweenRequestsAreNotAHead(t *testing.T) {
	s := start(t, config(memstore.Keys(memstore.Options{}, "a")))
	post := "POST /bucket?list-type=2 HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 0\r\n\r\n"
	for _, c := range []struct {
		total, want int
	}{{8192, 200}, {8193, 431}} {
		conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", s.Port()))
		if err != nil {
			t.Fatal(err)
		}
		conn.SetDeadline(time.Now().Add(10 * time.Second))
		go io.WriteString(conn, post+"\r\n\r\n"+padded(c.total, false))
		br := bufio.NewReader(conn)
		for i, want := range []int{405, c.want} {
			resp, err := http.ReadResponse(br, nil)
			if err != nil {
				t.Fatalf("%d-byte GET, answer %d: %v", c.total, i, err)
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode != want {
				t.Errorf("%d-byte GET, answer %d: %d, want %d", c.total, i, resp.StatusCode, want)
			}
		}
		conn.Close()
	}
}

// TestEveryLineEndingFramesTheHead: net/http accepts a header line ending
// in CRLF or LF and a blank line of CRLF or LF; the head counter frames
// all four, so a head of exactly the limit is served and one byte more is
// 431, as on the Java server, which accepts the same four.
func TestEveryLineEndingFramesTheHead(t *testing.T) {
	for _, c := range []struct {
		protocols []string
		limit     int
	}{{[]string{ProtocolS3}, 8192}, {[]string{ProtocolS3, ProtocolGCS}, 65536}} {
		cfg := config(memstore.Keys(memstore.Options{}, "a"))
		cfg.Protocols = c.protocols
		s := start(t, cfg)
		for _, e := range []struct{ name, mid, last, blank string }{
			{"crlf", "\r\n", "\r\n", "\r\n"}, {"lf last line, crlf blank", "\r\n", "\n", "\r\n"},
			{"crlf last line, lf blank", "\r\n", "\r\n", "\n"}, {"all lf", "\n", "\n", "\n"},
		} {
			head := func(total int) string {
				h := func(pad string) string {
					return "GET /bucket?list-type=2&max-keys=1 HTTP/1.1" + e.mid + "Host: 127.0.0.1" + e.mid + "Connection: close" + e.mid +
						"X-Pad: " + pad + e.last + e.blank
				}
				return h(strings.Repeat("a", total-len(h(""))))
			}
			if st, _ := rawHead(t, s, head(c.limit)); st != 200 {
				t.Errorf("%v %s: a %d-byte head got %d", c.protocols, e.name, c.limit, st)
			}
			if st, _ := rawHead(t, s, head(c.limit+1)); st != 431 {
				t.Errorf("%v %s: a %d-byte head got %d", c.protocols, e.name, c.limit+1, st)
			}
		}
	}
}

// TestAnUnframedHeadIsRefused: on a connection that counts heads, a
// request whose head the counter has no size for is refused 431, never
// measured from its parsed form.
func TestAnUnframedHeadIsRefused(t *testing.T) {
	h := &headCounter{}
	r := httptest.NewRequest("GET", "/bucket?list-type=2", nil)
	r = r.WithContext(context.WithValue(r.Context(), headCounterKey{}, h))
	if status, _ := headProblem(r, headLimitS3); status != http.StatusRequestHeaderFieldsTooLarge {
		t.Errorf("unframed: %d", status)
	}
	h.feed([]byte("GET /bucket?list-type=2 HTTP/1.1\r\nHost: x\r\n\r\n"))
	r = httptest.NewRequest("GET", "/bucket?list-type=2", nil)
	r = r.WithContext(context.WithValue(r.Context(), headCounterKey{}, h))
	if status, _ := headProblem(r, headLimitS3); status != 0 {
		t.Errorf("framed: %d", status)
	}
}
