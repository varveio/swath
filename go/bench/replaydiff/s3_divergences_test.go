// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/varveio/swath/go/internal/replay/s3"
)

// TestS3PlanRules holds the driver's reading of a request to the probed
// rules: what the Java server is asked instead, what the Go server is
// sent, and which refusal the Go server owes.
func TestS3PlanRules(t *testing.T) {
	goTok := s3.EncodeToken([]byte("a/1"), false)
	for _, c := range []struct {
		query, java, goQuery string
		refusal              *s3Refusal
		driverToken          bool
	}{
		{query: "list-type=2&prefix=a%2F", java: "list-type=2&prefix=a%2F"},
		{query: "list-type=2&prefix=a%ZZ&max-keys=2", java: "list-type=2&max-keys=2"},
		{query: "list-type=2&max-keys=1&max-keys=2", java: "list-type=2&max-keys=1"},
		{query: "list-type=2&max-keys=+2", java: "list-type=2&max-keys=%202"},
		{query: "list-type=2&prefix=a+b", java: "list-type=2&prefix=a%20b"},
		{query: "list-type=2&continuation-token=", refusal: refuseToken},
		{query: "list-type=2&continuation-token=v1:a2V5", refusal: refuseToken},
		{query: "list-type=2&continuation-token=" + goTok, java: "list-type=2&continuation-token=v2%3AAGEvMQ"},
		{query: "list-type=2&continuation-token=v2:AGEvMQ", driverToken: true, java: "list-type=2&continuation-token=v2:AGEvMQ",
			goQuery: "list-type=2&continuation-token=" + goTok},
		{query: "list-type=2&encoding-type=bogus", refusal: refuseEncoding},
		{query: "list-type=2&encoding-type=URL", java: "list-type=2&encoding-type=url"},
		{query: "list-type=2&marker=", refusal: refuseMarker},
		{query: "list-type=2&marker=x&encoding-type=bogus", refusal: refuseMarker},
		{query: "list-type=2&encoding-type=bogus&continuation-token=", refusal: refuseEncoding},
		{query: "list-type=2&start-after=a%00", refusal: refuseNUL},
		{query: "list-type=2&prefix=a%00", refusal: refuseNUL},
		{query: "list-type=2&start-after=a%00&continuation-token=v2:AGEvMQ", driverToken: true, java: "list-type=2&start-after=a%00&continuation-token=v2:AGEvMQ",
			goQuery: "list-type=2&start-after=a%00&continuation-token=" + goTok},
		{query: "list-type=2&start-after=a%FF", java: "list-type=2&start-after=a%EF%BF%BD"},
		{query: "list-type=2&delimiter=%00", java: "list-type=2&delimiter=%00"},
	} {
		p := planS3(c.query, c.driverToken)
		goQuery := c.goQuery
		if goQuery == "" {
			goQuery = c.query
		}
		if p.refusal != c.refusal || c.refusal == nil && p.java != c.java || p.goQuery != goQuery {
			t.Errorf("%s: java %q go %q refusal %v; want java %q go %q refusal %v", c.query, p.java, p.goQuery, p.refusal, c.java, goQuery, c.refusal)
		}
	}
}

// TestS3PlanFixes: the echo and encoding rules applied to a Java page.
func TestS3PlanFixes(t *testing.T) {
	page := []byte(`<ListBucketResult><Name>b</Name><Prefix>a%20b%2A</Prefix><KeyCount>1</KeyCount><EncodingType>url</EncodingType>` +
		`<Contents><Key>a%20b%2Ac%7E</Key></Contents></ListBucketResult>`)
	p := planS3("list-type=2&prefix=a%20b*&start-after=&encoding-type=Url", false)
	var reasons []string
	exp := expectedS3(&response{Status: 200, Body: page}, p, "/b", &reasons)
	want := `<ListBucketResult><Name>b</Name><Prefix>a+b*</Prefix><StartAfter></StartAfter><KeyCount>1</KeyCount><EncodingType>Url</EncodingType>` +
		`<Contents><Key>a+b*c%7E</Key></Contents></ListBucketResult>`
	if string(exp.Body) != want {
		t.Errorf("got  %s\nwant %s", exp.Body, want)
	}
	if len(reasons) != 3 {
		t.Errorf("reasons %q", reasons)
	}
	// The Java server's own max-keys refusal comes first.
	mk := []byte(`<Error><Code>InvalidArgument</Code><Message>max-keys must be an integer</Message></Error>`)
	p = planS3("list-type=2&max-keys=x&marker=", false)
	if exp := expectedS3(&response{Status: 400, Body: mk}, p, "/b", &reasons); !bytes.Equal(exp.Body, mk) {
		t.Errorf("max-keys refusal replaced: %s", exp.Body)
	}
	if exp := expectedS3(&response{Status: 200, Body: page, Header: http.Header{}}, p, "/b", &reasons); exp.Status != 400 ||
		!strings.Contains(string(exp.Body), "<Message>Marker unsupported with REST.GET.BUCKET in list-type=2</Message><Resource>/b</Resource>") {
		t.Errorf("marker: %d %s", exp.Status, exp.Body)
	}
}

// The driver's UTF-8 replacement is the route's, on random bytes.
func TestUTF8ReplacedMatchesTheRoute(t *testing.T) {
	r := rand.New(rand.NewPCG(7, 7))
	for range 20000 {
		b := make([]byte, 1+r.IntN(8))
		for i := range b {
			b[i] = byte(1 + r.IntN(255))
			if r.IntN(3) == 0 {
				b[i] = 0x80 | byte(r.IntN(64))
			}
		}
		req, err := s3.ParseRequest("b", "list-type=2&start-after="+s3.PercentEncode(b))
		if err != nil {
			t.Fatal(err)
		}
		if got := utf8Replaced(b); !bytes.Equal(got, req.StartAfter) {
			t.Fatalf("%x: driver %q, route %q", b, got, req.StartAfter)
		}
	}
}

func TestTokenTranslation(t *testing.T) {
	for _, c := range []struct {
		java      string
		boundary  string
		inclusive bool
	}{{"v2:AGEvMQ", "a/1", false}, {"v2:AWEw", "a0", true}, {"v2:AGE=", "a", false}, {"v1:a2V5", "key", false}} {
		b, inc, ok := javaToken(c.java)
		if !ok || string(b) != c.boundary || inc != c.inclusive {
			t.Errorf("%s: %q %v %v", c.java, b, inc, ok)
		}
	}
	if javaFormToken([]byte("a/1"), false) != "v2:AGEvMQ" {
		t.Error(javaFormToken([]byte("a/1"), false))
	}
	for _, bad := range []string{"v2:", "v2:Ag", "zz:AA", ""} {
		if _, _, ok := javaToken(bad); ok {
			t.Errorf("%q decoded", bad)
		}
	}
}

func TestDivergenceCases(t *testing.T) {
	keys := [][]byte{[]byte("a/b/c"), []byte("a/b/d e"), []byte("a/x"), []byte("b/é"), []byte("c")}
	seen := map[string]bool{}
	for _, r := range append(s3Matrix(keys, rand.New(rand.NewPCG(1, 2)), 100), s3DivergenceCases(keys)...) {
		if seen[r.Name] {
			t.Errorf("duplicate %q", r.Name)
		}
		seen[r.Name] = true
	}
	for _, n := range []string{"s3 live 01 empty start-after", "s3 live 07 flat token with a delimiter", "s3 live 12 %FF in start-after"} {
		if !seen[n] {
			t.Errorf("missing %q", n)
		}
	}
	if seed, first, ok := flatSeed(keys); !ok || seed != "" || first != "a/" {
		t.Errorf("flat seed %q %q %v", seed, first, ok)
	}
}

// TestDivergenceAssertsTheGoSide: against a reference named java, a
// request the probed rules cover passes when the Go answer is the
// expectation and is listed as a documented divergence with the Java
// answer; a Go answer that moves fails.
func TestDivergenceAssertsTheGoSide(t *testing.T) {
	const javaPage = `<?xml version="1.0" encoding="UTF-8"?><ListBucketResult><Name>bkt</Name><Prefix></Prefix><KeyCount>0</KeyCount></ListBucketResult>`
	serve := func(body string) string {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/xml")
			w.Write([]byte(body))
		}))
		t.Cleanup(srv.Close)
		return srv.URL
	}
	java := &target{name: "java", base: serve(javaPage), client: http.DefaultClient}
	for _, c := range []struct {
		goBody string
		pass   bool
	}{
		{strings.Replace(javaPage, "</Prefix>", "</Prefix><StartAfter></StartAfter>", 1), true},
		{javaPage, false},
	} {
		rep := &report{}
		d := &driver{o: options{bucket: "bkt", parallel: 1}, ref: java, got: &target{name: "go", base: serve(c.goBody), client: http.DefaultClient},
			comp: bytesComparator{}, rep: rep}
		d.runCases("case", []request{{Name: "empty start-after", Method: "GET", Query: "list-type=2&start-after="}})
		if (rep.Passed == 1) != c.pass || (len(rep.Divergences) == 1) != c.pass {
			t.Errorf("go %s: passed %d failed %d divergences %v", c.goBody, rep.Passed, rep.Failed, rep.Divergences)
		}
	}
}
