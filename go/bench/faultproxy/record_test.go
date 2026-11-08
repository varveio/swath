// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/source/objhttp/faulttest"
)

// TestRecordWritesEveryExchange: each forwarded exchange is one JSON
// line with the raw target split into path and query, the client's
// headers with credentials redacted, the answer's status, headers and
// body (text when UTF-8, base64 otherwise), and an injected throttle is
// recorded as answered.
func TestRecordWritesEveryExchange(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		if r.URL.Query().Get("prefix") == "bin" {
			w.Write([]byte{0xff, 0x00})
			return
		}
		w.Write([]byte("<ListBucketResult><Name>b</Name></ListBucketResult>"))
	}))
	defer upstream.Close()
	p, err := faulttest.New(upstream.URL, faulttest.Plan{Windows: []faulttest.Window{{N0: 2, N1: 3, Fraction: 1, Fault: faulttest.Throttle}}})
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	rec := newRecorder(p, upstream.URL, &out, 1<<20)
	front := httptest.NewServer(rec)
	defer front.Close()
	for _, q := range []string{"list-type=2&prefix=a%2Fb", "list-type=2&prefix=bin", "list-type=2"} {
		req, _ := http.NewRequest("GET", front.URL+"/b?"+q, nil)
		req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=secret")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
	var got []Exchange
	sc := bufio.NewScanner(&out)
	for sc.Scan() {
		var x Exchange
		if err := json.Unmarshal(sc.Bytes(), &x); err != nil {
			t.Fatal(err)
		}
		got = append(got, x)
	}
	if len(got) != 3 {
		t.Fatalf("%d lines", len(got))
	}
	x := got[0]
	if x.Seq != 1 || x.Request.Method != "GET" || x.Request.Path != "/b" || x.Request.Query != "list-type=2&prefix=a%2Fb" ||
		x.Request.URL != upstream.URL+"/b?list-type=2&prefix=a%2Fb" || x.Response.Status != 200 || x.Response.Body == nil ||
		*x.Response.Body != "<ListBucketResult><Name>b</Name></ListBucketResult>" || x.StartedAt == "" || x.CapturedAt == "" {
		t.Errorf("%+v", x)
	}
	for _, h := range x.Request.Headers {
		if h.Name == "Authorization" && h.Value != "{redacted}" {
			t.Errorf("credential written: %v", h)
		}
	}
	if b := got[1].Response; b.Body != nil || b.BodyBase64 == nil || *b.BodyBase64 != "/wA=" {
		t.Errorf("binary body: %+v", b)
	}
	if got[2].Response.Status != 503 {
		t.Errorf("injected throttle recorded as %d", got[2].Response.Status)
	}
}

func readExchanges(t *testing.T, r io.Reader) []Exchange {
	t.Helper()
	var got []Exchange
	sc := bufio.NewScanner(r)
	sc.Buffer(nil, 1<<24)
	for sc.Scan() {
		var x Exchange
		if err := json.Unmarshal(sc.Bytes(), &x); err != nil {
			t.Fatal(err)
		}
		got = append(got, x)
	}
	return got
}

// gateWriter holds its first Write until open is closed (or a second
// passes), and notes whether early was closed while it waited.
type gateWriter struct {
	w           io.Writer
	open, early chan struct{}
	once        sync.Once
	sawEarly    bool
}

func (g *gateWriter) Write(b []byte) (int, error) {
	g.once.Do(func() {
		select {
		case <-g.open:
		case <-g.early:
		case <-time.After(time.Second):
		}
		select {
		case <-g.early:
			g.sawEarly = true
		default:
		}
	})
	return g.w.Write(b)
}

// TestRecordPersistsBeforeRelease: an answer reaches the client only
// after its exchange is written, so with the first page's write held the
// client has no answer, and the continuation the client then sends with
// the issued token is recorded after the page that issued it. The first
// page is 64 KiB with a Content-Length, which net/http sends on while the
// handler still runs.
func TestRecordPersistsBeforeRelease(t *testing.T) {
	pad := strings.Repeat("x", 64<<10)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := "<ListBucketResult><KeyCount>1</KeyCount></ListBucketResult>"
		if r.URL.Query().Get("continuation-token") == "" {
			body = "<ListBucketResult><NextContinuationToken>T1</NextContinuationToken><Pad>" + pad + "</Pad></ListBucketResult>"
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		io.WriteString(w, body)
	}))
	defer upstream.Close()
	p, err := faulttest.New(upstream.URL, faulttest.Plan{})
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	gate := &gateWriter{w: &out, open: make(chan struct{}), early: make(chan struct{})}
	front := httptest.NewServer(newRecorder(p, upstream.URL, gate, 1<<20))
	defer front.Close()
	get := func(q string) string {
		resp, err := http.Get(front.URL + "/b?" + q)
		if err != nil {
			t.Error(err)
			return ""
		}
		defer resp.Body.Close()
		b, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Error(err)
		}
		return string(b)
	}
	first := make(chan string)
	go func() {
		b := get("list-type=2")
		close(gate.early)
		first <- b
	}()
	time.Sleep(200 * time.Millisecond) // the client has every chance to read the answer
	close(gate.open)
	body := <-first
	if gate.sawEarly {
		t.Fatal("the client had the first page's answer before its exchange was written")
	}
	tok := body[strings.Index(body, "<NextContinuationToken>")+len("<NextContinuationToken>") : strings.Index(body, "</NextContinuationToken>")]
	get("list-type=2&continuation-token=" + tok)
	got := readExchanges(t, &out)
	if len(got) != 2 || got[0].Seq != 1 || got[0].Request.Query != "list-type=2" ||
		got[1].Seq != 2 || got[1].Request.Query != "list-type=2&continuation-token=T1" {
		t.Fatalf("recorded out of order: %+v", got)
	}
}

// TestRecordDrainsOnShutdown: a reset is recorded after the client saw
// its connection close; a shutdown right then waits for that exchange
// (its handler held here after the reset, as a descheduled one would
// be) to be written before the file is closed, and the last exchange is
// in the file.
func TestRecordDrainsOnShutdown(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "<ListBucketResult></ListBucketResult>")
	}))
	defer upstream.Close()
	p, err := faulttest.New(upstream.URL, faulttest.Plan{Windows: []faulttest.Window{{N0: 0, N1: 1, Fraction: 1, Fault: faulttest.Reset}}})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "capture.jsonl")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	open := make(chan struct{})
	held := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p.ServeHTTP(w, r)
		<-open
	})
	rec := newRecorder(held, upstream.URL, f, 1<<20)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: rec}
	go srv.Serve(ln)
	client := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}
	if resp, err := client.Get("http://" + ln.Addr().String() + "/b?list-type=2"); err == nil {
		resp.Body.Close()
		t.Fatal("the reset answered")
	}
	stopped := make(chan error)
	go func() { stopped <- stop(srv, rec, f, 5*time.Second) }()
	select {
	case err := <-stopped:
		t.Fatalf("shutdown returned while an exchange was still to be written: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(open)
	if err := <-stopped; err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	got := readExchanges(t, bytes.NewReader(b))
	if len(got) != 1 || got[0].Response.Status != 0 {
		t.Fatalf("%+v", got)
	}
	// Closed, the recorder admits nothing more.
	w := httptest.NewRecorder()
	rec.ServeHTTP(w, httptest.NewRequest("GET", "/b?list-type=2", nil))
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("after close: %d", w.Code)
	}
}

// TestRecordBodyLimit: an answer over the recording's body limit, cut
// short by the proxy in front of the provider (Plan.MaxBody) or written
// whole by the handler it records, is recorded as exceeded with no body,
// the client is answered a marked 502, never a cut 200, an answer at the
// limit is recorded whole, and the shutdown reports the count.
func TestRecordBodyLimit(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n, _ := strconv.Atoi(r.URL.Query().Get("n"))
		io.WriteString(w, strings.Repeat("<", n))
	}))
	defer upstream.Close()
	const limit = 1000
	p, err := faulttest.New(upstream.URL, faulttest.Plan{MaxBody: limit})
	if err != nil {
		t.Fatal(err)
	}
	direct := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("direct") != "" {
			io.WriteString(w, strings.Repeat("y", 600))
			io.WriteString(w, strings.Repeat("y", 600))
			return
		}
		p.ServeHTTP(w, r)
	})
	var out bytes.Buffer
	rec := newRecorder(direct, upstream.URL, &out, limit)
	front := httptest.NewServer(rec)
	defer front.Close()
	for _, c := range []struct {
		q    string
		code int
	}{{"n=1000", 200}, {"n=1001", 502}, {"direct=1", 502}} {
		resp, err := http.Get(front.URL + "/b?" + c.q)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != c.code || c.code == 502 && (resp.Header.Get(faulttest.BodyLimitHeader) != "1000" || !strings.Contains(string(b), "over 1000 bytes")) {
			t.Errorf("%s: %d %q %.80q", c.q, resp.StatusCode, resp.Header, b)
		}
	}
	raw := append([]byte(nil), out.Bytes()...)
	got := readExchanges(t, &out)
	if len(got) != 3 {
		t.Fatalf("%d exchanges", len(got))
	}
	if x := got[0]; x.LimitExceeded != "" || x.Response.Body == nil || len(*x.Response.Body) != limit {
		t.Errorf("at the limit: %+v", x.LimitExceeded)
	}
	if !bytes.Contains(raw, []byte(`"body":"<<<`)) {
		t.Errorf("a body's '<' is escaped in the line: %.300s", raw)
	}
	for _, x := range got[1:] {
		if x.LimitExceeded == "" || x.Response.Body != nil || x.Response.BodyBase64 != nil {
			t.Errorf("over the limit: %+v", x)
		}
	}
	if got[2].Response.Status != 200 {
		t.Errorf("the handler's own status: %d", got[2].Response.Status)
	}
	if err := rec.Close(context.Background()); err == nil || !strings.Contains(err.Error(), "2 answers were over the body limit") {
		t.Errorf("close: %v", err)
	}
}

// TestRecordReleasesWithAContentType: the client is sent the provider's
// body with a Content-Type set and X-Content-Type-Options: nosniff,
// never a sniffed type: the provider's own type, application/octet-stream
// when it named none (the body below would sniff as text/html), and
// text/plain for an HTML type; the recording keeps the provider's
// headers as they were.
func TestRecordReleasesWithAContentType(t *testing.T) {
	const page = "<html><body><script>x</script></body></html>"
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch ct := r.URL.Query().Get("ct"); ct {
		case "":
			w.Header()["Content-Type"] = nil // no type, none sniffed upstream
		default:
			w.Header().Set("Content-Type", ct)
		}
		io.WriteString(w, page)
	}))
	defer upstream.Close()
	p, err := faulttest.New(upstream.URL, faulttest.Plan{})
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	front := httptest.NewServer(newRecorder(p, upstream.URL, &out, 1<<20))
	defer front.Close()
	for _, c := range []struct{ ct, want, recorded string }{
		{"application/xml", "application/xml", "application/xml"},
		{"", "application/octet-stream", ""},
		{"text/html; charset=utf-8", "text/plain; charset=utf-8", "text/html; charset=utf-8"},
		{"TEXT/HTML", "text/plain; charset=utf-8", "TEXT/HTML"},
	} {
		out.Reset()
		resp, err := http.Get(front.URL + "/b?ct=" + url.QueryEscape(c.ct))
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if got := resp.Header.Get("Content-Type"); got != c.want || resp.Header.Get("X-Content-Type-Options") != "nosniff" || string(b) != page {
			t.Errorf("%q: served %q nosniff=%q body %q", c.ct, got, resp.Header.Get("X-Content-Type-Options"), b)
		}
		x := readExchanges(t, &out)
		recorded := ""
		for _, h := range x[0].Response.Headers {
			if h.Name == "Content-Type" {
				recorded = h.Value
			}
			if h.Name == "X-Content-Type-Options" {
				t.Errorf("%q: the recording holds the proxy's nosniff", c.ct)
			}
		}
		if recorded != c.recorded {
			t.Errorf("%q: recorded %q", c.ct, recorded)
		}
	}
}

// TestRecordAnnotations: a request's X-Record-* headers, in any
// spelling, are written as the exchange's annotations, not among its
// request headers, and never reach the provider; the client's other
// headers are forwarded as before.
func TestRecordAnnotations(t *testing.T) {
	var mu sync.Mutex
	var seen []http.Header
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen = append(seen, r.Header.Clone())
		mu.Unlock()
		w.Header().Set("Content-Type", "application/xml")
		w.Write([]byte("<ListBucketResult/>"))
	}))
	defer upstream.Close()
	p, err := faulttest.New(upstream.URL, faulttest.Plan{})
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	rec := newRecorder(p, upstream.URL, &out, 1<<20)
	front := httptest.NewServer(rec)
	defer front.Close()
	req, _ := http.NewRequest("GET", front.URL+"/b?list-type=2", nil)
	req.Header["x-record-case"] = []string{"s3-07r"}
	req.Header.Set("X-Record-Token", "flip abc%3D")
	req.Header.Set("X-Other", "kept")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	xs := readExchanges(t, &out)
	if len(xs) != 1 || len(seen) != 1 {
		t.Fatalf("%d exchanges, %d forwarded", len(xs), len(seen))
	}
	want := []Header{{"X-Record-Case", "s3-07r"}, {"X-Record-Token", "flip abc%3D"}}
	if got := xs[0].Annotations; len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Errorf("annotations %v, want %v", got, want)
	}
	for _, h := range xs[0].Request.Headers {
		if strings.HasPrefix(strings.ToLower(h.Name), "x-record-") {
			t.Errorf("annotation recorded as a request header: %v", h)
		}
	}
	for k := range seen[0] {
		if strings.HasPrefix(strings.ToLower(k), "x-record-") {
			t.Errorf("annotation forwarded: %s", k)
		}
	}
	if seen[0].Get("X-Other") != "kept" {
		t.Errorf("other header not forwarded: %v", seen[0])
	}
}
