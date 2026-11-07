// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
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
	rec := newRecorder(p, upstream.URL, &out)
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
	front := httptest.NewServer(newRecorder(p, upstream.URL, gate))
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
	rec := newRecorder(held, upstream.URL, f)
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
