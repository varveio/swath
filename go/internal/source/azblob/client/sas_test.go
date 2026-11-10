// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/source"
	"github.com/varveio/swath/go/internal/source/objhttp"
)

// testSAS is a SAS whose signature is easy to find in a message. Its
// parameters are not in sorted order: the SDK sends them sorted in among
// the request's, so the SAS never reaches the wire as one string.
const testSAS = "sv=2026-10-06&ss=b&srt=co&sp=rl&se=2026-12-01T00%3A00%3A00Z&sig=TopSecretSig%2Bpart%3D"

// sasLeak is the first form of a testSAS parameter (name and value: as
// sent, decoded, or escaped once more) or of its signature found in text;
// "" when none is.
func sasLeak(text string) string {
	if strings.Contains(text, "TopSecretSig") {
		return "TopSecretSig"
	}
	q, _ := url.ParseQuery(testSAS)
	for k, vs := range q {
		for _, v := range vs {
			enc := url.QueryEscape(v)
			for _, f := range []string{k + "=" + enc, k + "=" + v, k + "%3D" + url.QueryEscape(enc), k + "%3D" + enc} {
				if strings.Contains(text, f) {
					return f
				}
			}
		}
	}
	return ""
}

// TestSASOnEveryRequest: the SAS (with or without '?') is sent with the
// listing and account-information queries, every parameter as given, and
// nothing else authenticates the request.
func TestSASOnEveryRequest(t *testing.T) {
	var queries []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		queries = append(queries, r.URL.RawQuery)
		if r.Header.Get(headerAuthorization) != "" || r.Header.Get("x-ms-date") != "" {
			t.Errorf("headers %v", r.Header)
		}
		if r.URL.Query().Get("comp") == "properties" {
			w.Header().Set(headerHNSEnabled, "false")
			return
		}
		fmt.Fprint(w, page(""))
	}))
	defer srv.Close()
	for _, sas := range []string{testSAS, "?" + testSAS} {
		queries = nil
		// The SAS precedes a shared key and a token credential.
		c := newEntraClient(t, Config{Endpoint: srv.URL, SAS: sas, SharedKey: "a2V5"}, failingCredential{})
		if _, err := c.List(context.Background(), &ListInput{Container: "c", Prefix: "p"}); err != nil {
			t.Fatal(err)
		}
		if _, err := c.AccountInfo(context.Background(), "c"); err != nil {
			t.Fatal(err)
		}
		want, _ := url.ParseQuery(testSAS)
		for _, q := range queries {
			got, _ := url.ParseQuery(q)
			for k, v := range want {
				if !slices.Equal(got[k], v) {
					t.Errorf("query %q lacks the SAS's %s", q, k)
				}
			}
		}
		if src, anon := c.Credentials(); src != "shared access signature" || anon || strings.Contains(c.Route("c"), "sig") {
			t.Errorf("credentials %q anonymous=%v route %s", src, anon, c.Route("c"))
		}
	}
}

// TestSASRedacted: no parameter of the SAS appears in an error: not in a
// transport error's URL, a 400's query echo (the SDK's sorted query, as
// sent, decoded or escaped again), a redirect target, nor a message that
// quotes the signature; the 400's parsed query keeps the request's own
// parameters only.
func TestSASRedacted(t *testing.T) {
	sig, _ := url.QueryUnescape("TopSecretSig%2Bpart%3D")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Query().Get("prefix") {
		case "400":
			w.Header().Set(headerErrorCode, "InvalidQueryParameterValue")
			w.WriteHeader(400)
			var msg strings.Builder
			xml.EscapeText(&msg, []byte("bad sig "+sig+" in "+r.URL.RawQuery))
			fmt.Fprintf(w, "\xef\xbb\xbf<Error><Code>InvalidQueryParameterValue</Code><Message>%s</Message></Error>", msg.String())
		case "400-escaped", "400-decoded":
			// the query quoted escaped once more, or decoded
			q := url.QueryEscape(r.URL.RawQuery)
			if strings.HasSuffix(r.URL.Query().Get("prefix"), "decoded") {
				q, _ = url.QueryUnescape(r.URL.RawQuery)
			}
			w.Header().Set(headerErrorCode, "InvalidQueryParameterValue")
			w.WriteHeader(400)
			var msg strings.Builder
			xml.EscapeText(&msg, []byte("bad query "+q))
			fmt.Fprintf(w, "\xef\xbb\xbf<Error><Code>InvalidQueryParameterValue</Code><Message>%s</Message></Error>", msg.String())
		case "403":
			w.Header().Set(headerErrorCode, codeAuthFailed)
			w.WriteHeader(403)
			fmt.Fprintf(w, "\xef\xbb\xbf<Error><Code>AuthenticationFailed</Code><Message>Signature %s did not match</Message></Error>", url.QueryEscape(sig))
		case "json-code":
			// No x-ms-error-code: azcore reads JSON too. Values differ from
			// the configured SAS, so each parameter must be scrubbed by name.
			w.WriteHeader(403)
			json.NewEncoder(w).Encode(map[string]any{"error": map[string]string{"code": "sig=UnexpectedSecret&se=other-expiry&sp=other-permissions&ss=other-service&srt=other-resource&sv=other-version"}})
		case "long-code":
			w.Header().Set(headerErrorCode, strings.Repeat("x", maxErrorBody+1))
			w.WriteHeader(403)
		case "xml-cdata-code", "xml-entity-code":
			w.WriteHeader(403)
			code := "<![CDATA[sig=UnexpectedSecret&se=other-expiry]]>"
			if r.URL.Query().Get("prefix") == "xml-entity-code" {
				code = "sig&#61;UnexpectedSecret&amp;se=other-expiry"
			}
			fmt.Fprintf(w, "<Error><Code>%s</Code></Error>", code)
		case "redirect":
			http.Redirect(w, r, "http://elsewhere.example/?"+r.URL.RawQuery, http.StatusMovedPermanently)
		case "bad-location":
			// net/http would quote a Location it cannot parse in its error
			w.Header().Set("Location", "https://elsewhere/%ZZ?"+r.URL.RawQuery)
			w.WriteHeader(http.StatusFound)
		case "slow":
			select {
			case <-time.After(5 * time.Second):
			case <-r.Context().Done():
			}
		}
	}))
	defer srv.Close()
	c := newTestClient(t, Config{Endpoint: srv.URL, SAS: testSAS, Retry: objhttp.Retry{MaxAttempts: 1}, RequestTimeout: 100 * time.Millisecond})
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	addr := ln.Addr().String()
	ln.Close()
	down := newTestClient(t, Config{Endpoint: "http://" + addr, SAS: testSAS, Retry: objhttp.Retry{MaxAttempts: 1}})
	for _, x := range []struct {
		c      *Client
		prefix string
	}{{c, "400"}, {c, "400-escaped"}, {c, "400-decoded"}, {c, "403"}, {c, "json-code"}, {c, "long-code"}, {c, "xml-cdata-code"}, {c, "xml-entity-code"}, {c, "redirect"}, {c, "bad-location"}, {c, "slow"}, {down, "refused"}} {
		_, err := x.c.List(context.Background(), &ListInput{Container: "c", Prefix: x.prefix})
		if err == nil {
			t.Fatalf("%s: no error", x.prefix)
		}
		if msg := err.Error(); sasLeak(msg) != "" || strings.Contains(msg, "elsewhere") {
			t.Errorf("%s: the SAS (%q) or a redirect target leaks: %s", x.prefix, sasLeak(msg), msg)
		}
		if strings.HasSuffix(x.prefix, "code") {
			e, ok := errors.AsType[*Error](err)
			if !ok {
				t.Fatalf("%s: no Azure answer: %v", x.prefix, err)
			}
			if e.Code == "" || len(e.Code) > maxErrorBody || strings.Contains(e.Code, "UnexpectedSecret") || strings.Contains(e.Code, "other-") {
				t.Errorf("%s: code is empty, unbounded or leaks SAS parameters (length %d): %.200s", x.prefix, len(e.Code), e.Code)
			}
			if f := objhttp.Normalize(err); f == nil || f.Code != e.Code {
				t.Errorf("%s: normalized code differs from the scrubbed answer: %+v", x.prefix, f)
			}
			if x.prefix == "json-code" && e.Code != "sig=REDACTED&se=REDACTED&sp=REDACTED&ss=REDACTED&srt=REDACTED&sv=REDACTED" {
				t.Errorf("%s: code not scrubbed by parameter name: %s", x.prefix, e.Code)
			}
		}
		if e, ok := errors.AsType[*Error](err); ok && strings.HasPrefix(x.prefix, "400") {
			q, _ := url.ParseQuery(e.Query)
			sas, _ := url.ParseQuery(testSAS)
			for k := range sas {
				if q.Has(k) {
					t.Errorf("%s: the parsed query %q keeps the SAS's %s", x.prefix, e.Query, k)
				}
			}
			if q.Get("prefix") != x.prefix || q.Get("comp") != "list" || !strings.Contains(e.Message, "comp") {
				t.Errorf("%s: the query or message lost the request's own parameters: %q, %q", x.prefix, e.Query, e.Message)
			}
		}
		if strings.Contains(x.prefix, "redirect") || x.prefix == "bad-location" {
			var f *objhttp.Failure
			if !errors.As(err, &f) || f.Status/100 != 3 || f.Transient {
				t.Errorf("%s: redirect failure %+v (%v)", x.prefix, f, err)
			}
		}
		if _, err := x.c.AccountInfo(context.Background(), x.prefix); err != nil && sasLeak(err.Error()) != "" {
			t.Errorf("%s: account information leaks the SAS: %v", x.prefix, err)
		}
	}
	// The 400's message was read (the fixture is well-formed XML) and
	// scrubbed, not dropped.
	if _, err := c.List(context.Background(), &ListInput{Container: "c", Prefix: "400"}); !strings.Contains(err.Error(), "prefix=400") ||
		!strings.Contains(err.Error(), "bad sig REDACTED in comp=list") {
		t.Errorf("a 400 names its query and its scrubbed message: %v", err)
	}
	// Recursive redaction: a SAS quoted deep inside a wrapped error.
	u, _ := url.Parse(srv.URL + "/acct/c?comp=list")
	inner := &url.Error{Op: "Get", URL: srv.URL + "?" + testSAS, Err: fmt.Errorf("wrapped: %w", &url.Error{Op: "Get", URL: "x?" + testSAS, Err: fmt.Errorf("location %q", "https://e/?sig="+sig)})}
	if msg := c.redact(fmt.Errorf("outer: %w", inner), u).Error(); sasLeak(msg) != "" || strings.Contains(msg, "se=2026") {
		t.Errorf("nested errors leak: %s", msg)
	}
	if _, err := New(context.Background(), Config{Account: "acct", SAS: "%zz&sig=TopSecretSig"}); err == nil || strings.Contains(err.Error(), "TopSecretSig") {
		t.Errorf("malformed SAS: %v", err)
	}
}

// TestScrubSASParameters: every parameter of the configured SAS is found
// by name in text, whatever order the SAS was given in and the text holds
// it in, and however it was encoded (as sent, lower-case escapes, decoded,
// escaped once more); its value is replaced and the rest of the text kept.
func TestScrubSASParameters(t *testing.T) {
	// the configured SAS, in yet another order
	sas := "sig=TopSecretSig%2Bpart%3D&se=2026-12-01T00%3A00%3A00Z&sp=rl&sv=2026-10-06&srt=co&ss=b"
	c := newTestClient(t, Config{Account: "acct", SAS: "?" + sas})
	for _, x := range []struct{ in, want string }{
		{ // the SDK's sorted query, as sent
			"q comp=list&prefix=p&restype=container&se=2026-12-01T00%3A00%3A00Z&sig=TopSecretSig%2Bpart%3D&sp=rl&srt=co&ss=b&sv=2026-10-06 end",
			"q comp=list&prefix=p&restype=container&se=REDACTED&sig=REDACTED&sp=REDACTED&srt=REDACTED&ss=REDACTED&sv=REDACTED end",
		},
		{ // re-encoded: lower-case escapes, the expiry and signature escaped differently
			"?ss=b&se=2026-12-01T00%3a00%3a00Z&prefix=p&sig=TopSecretSig%2bpart%3d&sv=2026-10-06&sp=rl&srt=co",
			"?ss=REDACTED&se=REDACTED&prefix=p&sig=REDACTED&sv=REDACTED&sp=REDACTED&srt=REDACTED",
		},
		{ // decoded
			"(comp=list&se=2026-12-01T00:00:00Z&sig=TopSecretSig+part=&sp=rl&srt=co&ss=b&sv=2026-10-06&prefix=p q)",
			"(comp=list&se=REDACTED&sig=REDACTED&sp=REDACTED&srt=REDACTED&ss=REDACTED&sv=REDACTED&prefix=p q)",
		},
		{ // escaped once more (the query quoted inside another URL)
			"u=" + url.QueryEscape("comp=list&se=2026-12-01T00%3A00%3A00Z&sp=rl&sv=2026-10-06&prefix=p"),
			"u=comp%3Dlist%26se%3DREDACTED%26sp%3DREDACTED%26sv%3DREDACTED%26prefix%3Dp",
		},
		{ // not a SAS parameter: kept
			"spr=https&prefix=sp&marker=se",
			"spr=https&prefix=sp&marker=se",
		},
	} {
		if got := c.scrub(x.in); got != x.want {
			t.Errorf("scrub(%q)\n got  %q\n want %q", x.in, got, x.want)
		}
	}
}

// secretTransport returns an error whose text quotes the SAS independently
// of net/http's request URL, forcing redact to preserve the network signals.
type secretTransport struct{ timeout bool }

func (s secretTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if s.timeout {
		<-req.Context().Done()
	}
	return nil, &secretNetError{timeout: s.timeout}
}

type secretNetError struct{ timeout bool }

func (e *secretNetError) Error() string   { return "transport failure: " + testSAS }
func (e *secretNetError) Timeout() bool   { return e.timeout }
func (e *secretNetError) Temporary() bool { return false }

func TestScrubbedTransportSignals(t *testing.T) {
	for _, timeout := range []bool{false, true} {
		t.Run(fmt.Sprint(timeout), func(t *testing.T) {
			c := newTestClient(t, Config{Endpoint: "http://storage.test", SAS: testSAS, RequestTimeout: 20 * time.Millisecond,
				Retry: objhttp.Retry{MaxAttempts: 2, BaseBackoff: time.Millisecond, MaxBackoff: time.Millisecond}, HTTPClient: &http.Client{Transport: secretTransport{timeout}}})
			safe := c.route("c")
			redacted := c.redact(fmt.Errorf("send: %w", &secretNetError{timeout: timeout}), &safe)
			var ne net.Error
			var scrubbed *scrubbedNetError
			if !errors.As(redacted, &scrubbed) || !errors.As(redacted, &ne) || ne.Timeout() != timeout || !objhttp.Transport(redacted) || sasLeak(redacted.Error()) != "" {
				t.Fatalf("redacted error: %v", redacted)
			}
			_, err := c.List(t.Context(), &ListInput{Container: "c"})
			f := objhttp.Normalize(err)
			counts := c.Counters()
			wantClass := objhttp.ClassOther
			if timeout {
				wantClass = objhttp.ClassTimeout
			}
			if err == nil || !objhttp.Requeue(err) || !f.Transient || f.Timeout != timeout || f.Class != wantClass || counts.Attempts != 2 || counts.Retries != 1 || (counts.Timeouts != 0 && !timeout || timeout && counts.Timeouts != 2) || sasLeak(err.Error()) != "" {
				t.Fatalf("error %v, failure %+v, counters %+v", err, f, counts)
			}
		})
	}
}

// TestSASReservedParameters: a SAS naming a parameter of swath's requests
// is refused when the client is built (unsupported: the caller's to fix),
// whatever the name's case or escaping, with an empty, repeated or absent
// value, first or last, with or without '?'; the message names the
// parameter alone, never a value or the SAS. Ordinary SAS fields and
// unknown ones pass, and Anonymous, which ignores the SAS, is not refused.
func TestSASReservedParameters(t *testing.T) {
	build := func(cfg Config) error {
		cfg.Account = "acct"
		_, err := newClient(context.Background(), cfg, withEntra(failingCredential{}))
		return err
	}
	for _, p := range []string{"comp", "restype", "prefix", "delimiter", "marker", "maxresults", "startFrom", "endBefore",
		"include", "showonly", "timeout", "resource"} {
		escaped := fmt.Sprintf("%%%02X", p[0]) + p[1:]
		for _, sas := range []string{
			testSAS + "&" + p + "=TopSecretSig", testSAS + "&" + strings.ToUpper(p) + "=TopSecretSig", testSAS + "&" + escaped + "=TopSecretSig",
			testSAS + "&" + p + "=", testSAS + "&" + p, testSAS + "&" + p + "=a&" + p + "=TopSecretSig",
			p + "=TopSecretSig&" + testSAS, "?" + p + "=TopSecretSig&" + testSAS,
		} {
			err := build(Config{SAS: sas})
			if !errors.Is(err, source.ErrUnsupported) || !strings.Contains(err.Error(), "names "+p+",") || sasLeak(err.Error()) != "" {
				t.Errorf("%s: %v, want the refusal naming %s alone", sas, err, p)
			}
		}
	}
	for _, sas := range []string{testSAS, "?" + testSAS, testSAS + "&sdd=2&ses=x&skoid=o&unknown=v&compx=list&xprefix=p"} {
		if err := build(Config{SAS: sas}); err != nil {
			t.Errorf("%s: %v", sas, err)
		}
	}
	if err := build(Config{Anonymous: true, SAS: testSAS + "&comp=list"}); err != nil {
		t.Errorf("anonymous: %v", err)
	}
}
