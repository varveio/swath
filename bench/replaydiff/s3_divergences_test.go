// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/varveio/swath/internal/replay/s3"
	"github.com/varveio/swath/internal/replay/server"
	"github.com/varveio/swath/internal/replay/store"
	"github.com/varveio/swath/internal/replay/store/memstore"
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
		{query: "list-type=2&encoding-type=bogus", refusal: refuseEncoding.withArgument("encoding-type", []byte("bogus"))},
		{query: "list-type=2&encoding-type=URL", java: "list-type=2&encoding-type=url"},
		{query: "list-type=2&marker=", refusal: refuseMarker},
		{query: "list-type=2&marker=x&encoding-type=bogus", refusal: refuseMarker},
		{query: "list-type=2&encoding-type=bogus&continuation-token=", refusal: refuseEncoding.withArgument("encoding-type", []byte("bogus"))},
		{query: "list-type=2&encoding-type=%01%FF", refusal: refuseEncoding.withArgument("encoding-type", []byte("\x01\uFFFD"))},
		{query: "list-type=2&start-after=a%00", refusal: refuseNUL.withArgument("key-marker", []byte("a\x00"))},
		{query: "list-type=2&prefix=a%00", refusal: refuseNUL.withArgument("prefix", []byte("a\x00"))},
		{query: "list-type=2&prefix=a%00&start-after=b%00", refusal: refuseNUL.withArgument("prefix", []byte("a\x00"))},
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
		if !reflect.DeepEqual(p.refusal, c.refusal) || c.refusal == nil && p.java != c.java || p.goQuery != goQuery {
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
	exp := expectedS3(&response{Status: 200, Body: page}, p, request{Method: "GET"}, "", "b", &reasons)
	want := `<ListBucketResult><Name>b</Name><Prefix>a+b*</Prefix><StartAfter></StartAfter><KeyCount>1</KeyCount><EncodingType>Url</EncodingType>` +
		`<Contents><Key>a+b*c%7E</Key></Contents></ListBucketResult>`
	if string(exp.Body) != want {
		t.Errorf("got  %s\nwant %s", exp.Body, want)
	}
	if len(reasons) != 3 {
		t.Errorf("reasons %q", reasons)
	}
	// The Java server's own max-keys refusal comes first, in live S3's
	// document.
	const tail = "<RequestId>S3LISTINGREPLAY</RequestId><HostId>S3LISTINGREPLAY</HostId></Error>"
	mk := []byte(`<Error><Code>InvalidArgument</Code><Message>max-keys must be an integer</Message><Resource>/b</Resource></Error>`)
	q := "list-type=2&max-keys=x%01&marker="
	p = planS3(q, false)
	if exp := expectedS3(&response{Status: 400, Body: mk}, p, request{Method: "GET", Query: q}, q, "b", &reasons); exp.Status != 400 ||
		!strings.HasSuffix(string(exp.Body), "<Message>Provided max-keys not an integer or within integer range</Message>"+
			"<ArgumentName>max-keys</ArgumentName><ArgumentValue>x&#1;</ArgumentValue>"+tail) {
		t.Errorf("max-keys refusal: %s", exp.Body)
	}
	if exp := expectedS3(&response{Status: 200, Body: page, Header: http.Header{}}, p, request{Method: "GET", Query: q}, q, "b", &reasons); exp.Status != 400 ||
		!strings.HasSuffix(string(exp.Body), "<Message>Marker unsupported with REST.GET.BUCKET in list-type=2</Message><ArgumentName>marker</ArgumentName>"+tail) {
		t.Errorf("marker: %d %s", exp.Status, exp.Body)
	}
	// Control characters in a Java page become references.
	ctl := []byte("<ListBucketResult><Prefix>a\x01\t\x1f\x7f</Prefix><Delimiter>\x00</Delimiter></ListBucketResult>")
	reasons = nil
	if exp := expectedS3(&response{Status: 200, Body: ctl}, planS3("list-type=2", false), request{}, "list-type=2", "b", &reasons); string(exp.Body) !=
		"<ListBucketResult><Prefix>a&#x1;\t&#x1f;\x7f</Prefix><Delimiter>&#x0;</Delimiter></ListBucketResult>" || len(reasons) != 1 {
		t.Errorf("control characters: %q %q", exp.Body, reasons)
	}
}

// TestJavaFirstRefusals: the refusals the Java server makes first, in
// live S3's document.
func TestJavaFirstRefusals(t *testing.T) {
	javaErr := func(status int, code, msg string) *response {
		return &response{Status: status, Body: []byte("<Error><Code>" + code + "</Code><Message>" + msg + "</Message><Resource>/x</Resource></Error>")}
	}
	const tail = "<RequestId>S3LISTINGREPLAY</RequestId><HostId>S3LISTINGREPLAY</HostId></Error>"
	for _, c := range []struct {
		exp  *response
		r    request
		want string
	}{
		{javaErr(404, "NoSuchBucket", "The specified bucket does not exist"), request{Method: "GET", Path: "/no%2Dsuch", Query: "list-type=2"},
			"<Code>NoSuchBucket</Code><Message>The specified bucket does not exist</Message><BucketName>no-such</BucketName>"},
		{javaErr(404, "NoSuchBucket", "x"), request{Method: "GET", Raw: "http://{host}/other?list-type=2"},
			"<Code>NoSuchBucket</Code><Message>The specified bucket does not exist</Message><BucketName>other</BucketName>"},
		{javaErr(404, "NoSuchBucket", "x"), request{Method: "GET", Path: "/", Query: "list-type=2"},
			"<Code>NoSuchBucket</Code><Message>The specified bucket does not exist</Message>"},
		{javaErr(405, "MethodNotAllowed", "method not allowed"), request{Method: "POST", Query: "list-type=2"},
			"<Code>MethodNotAllowed</Code><Message>The specified method is not allowed against this resource.</Message><Method>POST</Method><ResourceType>BUCKET</ResourceType>"},
		{javaErr(400, "InvalidArgument", "only ListObjectsV2 is supported"), request{Method: "GET", Query: "max-keys=1"},
			"<Code>InvalidArgument</Code><Message>only ListObjectsV2 is supported</Message><ArgumentName>list-type</ArgumentName>"},
		{javaErr(400, "InvalidArgument", "only ListObjectsV2 is supported"), request{Method: "GET", Query: "list-type=1"},
			"<Code>InvalidArgument</Code><Message>only ListObjectsV2 is supported</Message><ArgumentName>list-type</ArgumentName><ArgumentValue>1</ArgumentValue>"},
		{javaErr(400, "InvalidArgument", "max-keys must be non-negative"), request{Method: "GET", Query: "list-type=2&max-keys=-1"},
			"<Code>InvalidArgument</Code><Message>Argument maxKeys must be an integer between 0 and 2147483647</Message><ArgumentName>maxKeys</ArgumentName><ArgumentValue>-1</ArgumentValue>"},
	} {
		query, _ := requestQuery(c.r)
		got := javaFirstRefusal(c.exp, c.r, query, "bkt")
		if got == nil || string(got.body()) != `<?xml version="1.0" encoding="UTF-8"?>`+"\n"+`<Error>`+c.want+tail {
			t.Errorf("%+v: %v", c.r, got)
		}
	}
	if javaFirstRefusal(javaErr(400, "InvalidArgument", "something else"), request{}, "", "bkt") != nil {
		t.Error("other refusal taken")
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
// answer; a Go answer that moves fails. The Java server writes no newline
// after the XML declaration and the Go route must write one (live S3
// does), without that making a request a divergence or a token-bytes
// difference.
func TestDivergenceAssertsTheGoSide(t *testing.T) {
	const javaPage = `<?xml version="1.0" encoding="UTF-8"?><ListBucketResult><Name>bkt</Name><Prefix></Prefix><KeyCount>0</KeyCount></ListBucketResult>`
	goPage := strings.Replace(javaPage, "?>", "?>\n", 1)
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
		query, goBody string
		pass          bool
		divergences   int
	}{
		{"list-type=2&start-after=", strings.Replace(goPage, "</Prefix>", "</Prefix><StartAfter></StartAfter>", 1), true, 1},
		{"list-type=2&start-after=", strings.Replace(javaPage, "</Prefix>", "</Prefix><StartAfter></StartAfter>", 1), false, 0},
		{"list-type=2&start-after=", goPage, false, 0},
		{"list-type=2", goPage, true, 0},
		{"list-type=2", javaPage, false, 0},
	} {
		rep := &report{}
		d := &driver{o: options{bucket: "bkt", parallel: 1}, ref: java, got: &target{name: "go", base: serve(c.goBody), client: http.DefaultClient},
			comp: bytesComparator{}, rep: rep}
		d.runCases("case", []request{{Name: "case", Method: "GET", Query: c.query}})
		if (rep.Passed == 1) != c.pass || len(rep.Divergences) != c.divergences || len(rep.TokenBytes) != 0 {
			t.Errorf("%s, go %q: passed %d failed %d divergences %v token bytes %v", c.query, c.goBody, rep.Passed, rep.Failed, rep.Divergences, rep.TokenBytes)
		}
	}
}

// TestDeclNewline: the newline is inserted after the declaration only
// where it is missing, and nothing else is touched.
func TestDeclNewline(t *testing.T) {
	for in, want := range map[string]string{
		javaDecl + "<a/>":       javaDecl + "\n<a/>",
		javaDecl + "\n<a/>":     javaDecl + "\n<a/>",
		javaDecl:                javaDecl + "\n",
		"<a/>":                  "<a/>",
		"":                      "",
		" " + javaDecl + "<a/>": " " + javaDecl + "<a/>",
	} {
		if got := string(declNewline([]byte(in))); got != want {
			t.Errorf("%q: %q, want %q", in, got, want)
		}
	}
}

// TestOwnerOrderIsDerived serves an owner-bearing fixture from the Go
// route and, as the reference named java, the same answers in the Java
// server's form: no newline after the XML declaration and each Owner
// after the StorageClass in front of it (the order
// TestEmptyAndNullStringsEndToEnd recorded from the Java server). A
// fetch-owner=true listing passes only because the driver moves the
// Owner on the Java side, and is listed as a documented divergence for
// that reason; a listing without owners is not a divergence; a Go answer
// in the Java order fails. The reference reads fetch-owner=TRUE as true,
// as the Java server does, and the Go route lists without owners, as live
// S3 does: the driver drops the Java page's owners, a divergence of its
// own reason, and without that the request fails.
func TestOwnerOrderIsDerived(t *testing.T) {
	sp := func(s string) *string { return &s }
	size, mod := int64(7), time.UnixMicro(1_767_225_600_000_000)
	objs := []store.Object{
		{Key: []byte("a1"), Size: &size, LastModified: &mod, ETag: sp("e1"), StorageClass: sp("STANDARD"), OwnerID: sp("id&1"),
			ChecksumAlgorithm: sp("CRC32"), ChecksumType: sp("FULL_OBJECT")},
		{Key: []byte("a2"), Size: &size, LastModified: &mod, ETag: sp("e2"), StorageClass: sp(""), OwnerID: sp(""), OwnerDisplayName: sp("n<2")},
		{Key: []byte("b/k3"), Size: &size, LastModified: &mod, OwnerID: sp("id3")},       // no StorageClass: nothing to move
		{Key: []byte("c"), Size: &size, LastModified: &mod, StorageClass: sp("GLACIER")}, // no owner
	}
	st, err := memstore.New(objs, memstore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	srv, err := server.New(server.Config{Bucket: "bkt", Host: "127.0.0.1", MetricsPort: -1, MaxConcurrentRequests: 64,
		ResponseBufferBudget: 1 << 30, MaxResponseBytes: 1 << 26, StopTimeout: time.Second, IdleTimeout: time.Minute,
		WriteTimeout: time.Minute, SlowRequestLog: -1, Store: st, ServingMode: "MEMORY", FixtureIdentity: "same", Protocols: []string{"s3"}})
	if err == nil {
		err = srv.Listen()
	}
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve()
	t.Cleanup(func() { srv.Shutdown(time.Second) })
	goBase := fmt.Sprintf("http://127.0.0.1:%d", srv.Port())

	// javaForm rewrites a Go answer in the Java server's form, by string
	// surgery independent of the driver's regular expression.
	javaForm := func(body []byte, newline bool) []byte {
		b := string(body)
		if !newline {
			b = strings.Replace(b, javaDecl+"\n", javaDecl, 1)
		}
		var out strings.Builder
		for {
			i := strings.Index(b, "<Owner>")
			if i < 0 {
				out.WriteString(b)
				return []byte(out.String())
			}
			j := strings.Index(b[i:], "</Owner>") + i + len("</Owner>")
			owner, rest := b[i:j], b[j:]
			out.WriteString(b[:i])
			if strings.HasPrefix(rest, "<StorageClass>") {
				k := strings.Index(rest, "</StorageClass>") + len("</StorageClass>")
				out.WriteString(rest[:k])
				rest = rest[k:]
			}
			out.WriteString(owner)
			b = rest
		}
	}
	stub := func(newline bool) string {
		h := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// The Java server reads fetch-owner case-insensitively.
			resp, err := http.Get(goBase + r.URL.Path + "?" + anyCaseTrue.ReplaceAllString(r.URL.RawQuery, "fetch-owner=true"))
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadGateway)
				return
			}
			defer resp.Body.Close()
			body, _ := io.ReadAll(resp.Body)
			for k, v := range resp.Header {
				if k != "Content-Length" {
					w.Header()[k] = v
				}
			}
			w.WriteHeader(resp.StatusCode)
			w.Write(javaForm(body, newline))
		}))
		t.Cleanup(h.Close)
		return h.URL
	}
	java := &target{name: "java", base: stub(false), client: http.DefaultClient}

	// The rewrite is the Java server's order on the probed shape.
	page := javaForm([]byte(javaDecl+"\n<Size>1</Size><Owner><ID></ID><DisplayName></DisplayName></Owner><StorageClass></StorageClass></Contents>"), false)
	if string(page) != javaDecl+"<Size>1</Size><StorageClass></StorageClass><Owner><ID></ID><DisplayName></DisplayName></Owner></Contents>" {
		t.Fatalf("java form: %s", page)
	}

	owned := []string{"list-type=2&fetch-owner=true", "list-type=2&fetch-owner=true&delimiter=%2F", "list-type=2&fetch-owner=true&encoding-type=url",
		"list-type=2&fetch-owner=true&max-keys=1"}
	plain := []string{"list-type=2", "list-type=2&fetch-owner=false", "list-type=2&fetch-owner=true&prefix=c", "list-type=2&fetch-owner=yes"}
	anyCase := []string{"list-type=2&fetch-owner=TRUE", "list-type=2&fetch-owner=True&delimiter=%2F"}
	run := func(got *target, queries []string) *report {
		rep := &report{}
		d := &driver{o: options{bucket: "bkt", parallel: 1}, ref: java, got: got, comp: bytesComparator{}, rep: rep}
		var cases []request
		for _, q := range queries {
			cases = append(cases, request{Name: q, Method: "GET", Query: q})
		}
		d.runCases("case", cases)
		return rep
	}
	goRoute := &target{name: "go", base: goBase, client: http.DefaultClient}
	if rep := run(goRoute, owned); rep.Passed != len(owned) || rep.Failed != 0 || len(rep.Divergences) != len(owned) {
		t.Errorf("owned: passed %d failed %d divergences %q", rep.Passed, rep.Failed, rep.Divergences)
	} else {
		for _, l := range rep.Divergences {
			if !strings.Contains(l, "Owner before StorageClass") {
				t.Errorf("divergence without the owner reason: %s", l)
			}
		}
	}
	if rep := run(goRoute, plain); rep.Passed != len(plain) || rep.Failed != 0 || len(rep.Divergences) != 0 {
		t.Errorf("plain: passed %d failed %d divergences %q", rep.Passed, rep.Failed, rep.Divergences)
	}
	if rep := run(goRoute, anyCase); rep.Passed != len(anyCase) || rep.Failed != 0 || len(rep.Divergences) != len(anyCase) {
		t.Errorf("any case: passed %d failed %d divergences %q", rep.Passed, rep.Failed, rep.Divergences)
	} else {
		for _, l := range rep.Divergences {
			if !strings.HasSuffix(l, "(as live S3: Owner only for fetch-owner=true exactly)") {
				t.Errorf("not the fetch-owner divergence alone: %s", l)
			}
		}
	}
	javaOrder := &target{name: "go", base: stub(true), client: http.DefaultClient}
	if rep := run(javaOrder, owned); rep.Failed != len(owned) {
		t.Errorf("a Go answer in the Java order: passed %d failed %d", rep.Passed, rep.Failed)
	}
}

var anyCaseTrue = regexp.MustCompile(`(?i)fetch-owner=true\b`)
