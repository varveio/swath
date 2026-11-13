// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/varveio/swath/go/internal/source/objhttp"
)

// evidenceArchive is the published evidence of the October 2026 live
// campaign (sanitised captures of real Azure responses), read in memory.
const evidenceArchive = "../../../../../docs/replay-provider-evidence.tar.gz"

type captureExchange struct {
	Request struct {
		Method string `json:"method"`
		Query  string `json:"query"`
	} `json:"request"`
	Response struct {
		Status  int `json:"status"`
		Headers []struct {
			Name  string `json:"name"`
			Value string `json:"value"`
		} `json:"headers"`
		Body string `json:"body_base64"`
	} `json:"response"`
}

type capture struct {
	id    string
	steps []captureExchange
}

// readCaptures returns every Azure capture in the archive, a walk's steps
// in order.
func readCaptures(t *testing.T) []capture {
	t.Helper()
	f, err := os.Open(evidenceArchive)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	var out []capture
	tr := tar.NewReader(gz)
	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if h.Typeflag != tar.TypeReg || path.Dir(h.Name) != "azure/captures" {
			continue
		}
		b, err := io.ReadAll(tr)
		if err != nil {
			t.Fatal(err)
		}
		if strings.HasSuffix(h.Name, ".gz") {
			r, err := gzip.NewReader(bytes.NewReader(b))
			if err != nil {
				t.Fatal(err)
			}
			if b, err = io.ReadAll(r); err != nil {
				t.Fatal(err)
			}
		}
		var raw struct {
			ProbeID string `json:"probe_id"`
			captureExchange
			Steps []struct {
				Exchange captureExchange `json:"exchange"`
			} `json:"steps"`
		}
		if err := json.Unmarshal(b, &raw); err != nil {
			t.Fatalf("%s: %v", h.Name, err)
		}
		c := capture{id: raw.ProbeID}
		if len(raw.Steps) == 0 {
			c.steps = []captureExchange{raw.captureExchange}
		}
		for _, s := range raw.Steps {
			c.steps = append(c.steps, s.Exchange)
		}
		out = append(out, c)
	}
	if len(out) == 0 {
		t.Fatal("no Azure captures in the evidence archive")
	}
	return out
}

// sendable reads a captured request as the ListInput the client would send
// it from, or false when the client never sends that shape: only GET, only
// the parameters it sends (endBefore, include, timeout and others are
// never sent), each once and non-empty, and maxresults as the decimal the
// client writes.
func sendable(ex captureExchange) (ListInput, bool) {
	if ex.Request.Method != http.MethodGet {
		return ListInput{}, false
	}
	var in ListInput
	seen := map[string]bool{}
	for _, part := range strings.Split(ex.Request.Query, "&") {
		k, v, _ := strings.Cut(part, "=")
		k, err1 := url.PathUnescape(k)
		v, err2 := url.PathUnescape(v)
		if err1 != nil || err2 != nil || seen[k] {
			return ListInput{}, false
		}
		seen[k] = true
		switch k {
		case "restype":
			if v != "container" {
				return ListInput{}, false
			}
		case "comp":
			if v != "list" {
				return ListInput{}, false
			}
		case "prefix", "delimiter", "startFrom", "marker":
			if v == "" {
				return ListInput{}, false
			}
			*map[string]*string{"prefix": &in.Prefix, "delimiter": &in.Delimiter, "startFrom": &in.StartFrom, "marker": &in.Marker}[k] = v
		case "maxresults":
			n, err := strconv.Atoi(v)
			if err != nil || n <= 0 || strconv.Itoa(n) != v {
				return ListInput{}, false
			}
			in.MaxResults = n
		default:
			return ListInput{}, false
		}
	}
	return in, seen["restype"] && seen["comp"]
}

// captureServer answers each request with the next captured response, and
// checks that the client sent the captured query (as decoded pairs).
func captureServer(t *testing.T, id string, steps []captureExchange) *httptest.Server {
	n := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if n >= len(steps) {
			t.Errorf("%s: request %d past the capture", id, n)
			w.WriteHeader(599)
			return
		}
		ex := steps[n]
		n++
		if got, want := pairs(r.URL.RawQuery), pairs(ex.Request.Query); !slices.Equal(got, want) {
			t.Errorf("%s step %d: query %q, captured %q", id, n, got, want)
		}
		writeCaptured(w, ex)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// writeCaptured writes ex's captured response: its headers but the
// framing ones, its status and its body.
func writeCaptured(w http.ResponseWriter, ex captureExchange) {
	for _, h := range ex.Response.Headers {
		if n := strings.ToLower(h.Name); n != "content-length" && n != "transfer-encoding" {
			w.Header().Add(h.Name, h.Value)
		}
	}
	w.WriteHeader(ex.Response.Status)
	b, _ := base64.StdEncoding.DecodeString(ex.Response.Body)
	w.Write(b)
}

// pairs is a query's decoded name=value pairs, sorted.
func pairs(q string) []string {
	var out []string
	for _, part := range strings.Split(q, "&") {
		k, v, _ := strings.Cut(part, "=")
		k, _ = url.PathUnescape(k)
		v, _ = url.PathUnescape(v)
		out = append(out, k+"="+v)
	}
	slices.Sort(out)
	return out
}

var (
	entryRe  = regexp.MustCompile(`<(Blob|BlobPrefix)><Name( Encoded="true")?>([^<]*)</Name>`)
	markerRe = regexp.MustCompile(`<NextMarker>([^<]*)</NextMarker>|<NextMarker ?/>`)
)

// expectedEntries reads a captured body's entries and marker with a
// regular expression, apart from the client's decoder: names unescaped
// for the five XML entities and the CR reference, Encoded ones
// percent-decoded.
func expectedEntries(t *testing.T, body []byte) (names []string, prefixes []bool, marker string) {
	t.Helper()
	unescape := strings.NewReplacer("&lt;", "<", "&gt;", ">", "&quot;", `"`, "&apos;", "'", "&#13;", "\r", "&amp;", "&")
	for _, m := range entryRe.FindAllSubmatch(body, -1) {
		name := unescape.Replace(string(m[3]))
		if len(m[2]) > 0 {
			var err error
			if name, err = url.PathUnescape(string(m[3])); err != nil {
				t.Fatal(err)
			}
		}
		names = append(names, name)
		prefixes = append(prefixes, string(m[1]) == "BlobPrefix")
	}
	if m := markerRe.FindSubmatch(body); m != nil {
		marker = string(m[1])
	}
	return names, prefixes, marker
}

// captureClass is the class a captured error answer must take.
func captureClass(status int, code string) objhttp.Class {
	switch {
	case code == "AuthenticationFailed":
		return objhttp.ClassAuth
	case status == 400:
		return objhttp.ClassOther
	}
	return "unexpected"
}

// TestCaptures serves every capture whose request the client can send
// from a local server and holds the client to it: the query it sends, the
// items in wire order (names, kinds), the marker and, for an error, the
// class. Walks are followed step by step with the client's own markers.
func TestCaptures(t *testing.T) {
	var served, pagesSeen, errorsSeen, skipped int
	features := map[string]bool{}
	for _, c := range readCaptures(t) {
		in, ok := sendable(c.steps[0])
		for _, s := range c.steps[1:] {
			if _, more := sendable(s); !more {
				ok = false
			}
		}
		if !ok {
			// A request the client never sends: its error answer is
			// still a real one, served to a plain request.
			if ex := c.steps[0]; len(c.steps) == 1 && ex.Response.Status != http.StatusOK {
				errorsSeen++
				checkErrorAnswer(t, c.id, ex, &ListInput{Container: "c", Prefix: "p/"}, features)
			} else {
				skipped++
			}
			continue
		}
		served++
		srv := captureServer(t, c.id, c.steps)
		cl, err := New(context.Background(), Config{Endpoint: srv.URL, Account: "acct", Anonymous: true, Retry: objhttp.Retry{MaxAttempts: 1}})
		if err != nil {
			t.Fatal(err)
		}
		in.Container = "c"
		for i, ex := range c.steps {
			if i > 0 {
				next, _ := sendable(ex)
				if next.Marker != in.Marker {
					t.Fatalf("%s step %d: the client continues with %q, the capture with %q", c.id, i, in.Marker, next.Marker)
				}
			}
			body, _ := base64.StdEncoding.DecodeString(ex.Response.Body)
			if !bytes.HasPrefix(body, bom) {
				t.Errorf("%s: a body without the BOM", c.id)
			}
			if ex.Response.Status != http.StatusOK {
				errorsSeen++
				checkErrorAnswer(t, c.id, ex, &in, features)
				break
			}
			p, err := cl.List(context.Background(), &in)
			if err != nil {
				t.Fatalf("%s step %d: %v", c.id, i, err)
			}
			pagesSeen++
			names, prefixes, marker := expectedEntries(t, body)
			if len(p.Items) != len(names) || p.NextMarker != marker {
				t.Fatalf("%s step %d: %d items next %q, captured %d next %q", c.id, i, len(p.Items), p.NextMarker, len(names), marker)
			}
			for j, it := range p.Items {
				if it.Name != names[j] || it.Prefix != prefixes[j] {
					t.Fatalf("%s step %d item %d: %q prefix=%v, captured %q prefix=%v", c.id, i, j, it.Name, it.Prefix, names[j], prefixes[j])
				}
				if !it.Prefix && (it.LastModified == 0 || it.CreationTime == 0 || it.ETag == "" || it.BlobType == "") {
					t.Fatalf("%s step %d item %d: properties not read: %+v", c.id, i, j, it)
				}
			}
			switch {
			case bytes.Contains(body, []byte(`Encoded="true"`)):
				features["Encoded names"] = true
			}
			if bytes.Contains(body, []byte("<NextMarker />")) {
				features["empty <NextMarker />"] = true
			}
			// maxresults above 5,000 is echoed but answered with at most
			// 5,000 entries, so the body cap holds.
			if len(p.Items) > MaxPageSize || int64(len(body)) > MaxBody {
				t.Errorf("%s: maxresults %d: %d items in %d bytes", c.id, in.MaxResults, len(p.Items), len(body))
			}
			if in.MaxResults > MaxPageSize && len(p.Items) == MaxPageSize && p.NextMarker != "" {
				features["maxresults echo above 5,000"] = true
			}
			in.Marker = p.NextMarker
		}
	}
	for _, f := range []string{"Encoded names", "empty <NextMarker />", "error without x-ms-error-code", "maxresults echo above 5,000"} {
		if !features[f] {
			t.Errorf("no served capture has %s", f)
		}
	}
	t.Logf("%d captures served (%d pages, %d errors), %d of shapes the client never sends", served, pagesSeen, errorsSeen, skipped)
}

// checkErrorAnswer serves one captured error answer to in and checks its
// class, and that a body's code is read when the header is absent.
func checkErrorAnswer(t *testing.T, id string, ex captureExchange, in *ListInput, features map[string]bool) {
	t.Helper()
	code := ""
	for _, h := range ex.Response.Headers {
		if strings.EqualFold(h.Name, headerErrorCode) {
			code = h.Value
		}
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { writeCaptured(w, ex) }))
	defer srv.Close()
	cl, err := New(context.Background(), Config{Endpoint: srv.URL, Account: "acct", Anonymous: true, Retry: objhttp.Retry{MaxAttempts: 1}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = cl.List(context.Background(), in)
	var e *Error
	if !errors.As(err, &e) || objhttp.Classify(err) != captureClass(ex.Response.Status, code) {
		t.Errorf("%s: %d %q: class %q (%v)", id, ex.Response.Status, code, objhttp.Classify(err), err)
		return
	}
	if code == "" {
		features["error without x-ms-error-code"] = true
		if e.Code == "" {
			t.Errorf("%s: the body's code was not read: %v", id, err)
		}
	}
}
