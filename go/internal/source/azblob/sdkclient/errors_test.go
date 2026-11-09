// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package sdkclient

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/source/azblob/client"
	"github.com/varveio/swath/go/internal/source/objhttp"
	"github.com/varveio/swath/go/internal/source/objhttp/objhttptest"
)

const (
	listHead = "\xef\xbb\xbf<?xml version=\"1.0\" encoding=\"utf-8\"?><EnumerationResults ServiceEndpoint=\"https://acct.blob.core.windows.net/\" ContainerName=\"c\"><Blobs>"
	listTail = "</Blobs><NextMarker /></EnumerationResults>"
	// testSAS is a fake SAS: its signature must never reach an error.
	testSAS = "sv=2026-10-06&sp=rl&sig=TopSecretSig%2Bx%3D"
	secret  = "TopSecretSig"
)

// page is a listing body of blobs named names (BOM, declaration, an empty
// final marker unless next is set).
func page(next string, names ...string) string {
	var b strings.Builder
	b.WriteString(listHead)
	for _, n := range names {
		fmt.Fprintf(&b, "<Blob><Name>%s</Name><Properties><Creation-Time>Wed, 21 Oct 2026 17:27:04 GMT</Creation-Time>"+
			"<Last-Modified>Wed, 21 Oct 2026 17:27:05 GMT</Last-Modified><Etag>0x8DF2F9886625F76</Etag><Content-Length>5</Content-Length>"+
			"<Content-Type>application/octet-stream</Content-Type><Content-MD5>2KAlPH8W1jwF0SkOJToFzg==</Content-MD5>"+
			"<BlobType>BlockBlob</BlobType><AccessTier>Hot</AccessTier></Properties><OrMetadata /></Blob>", n)
	}
	b.WriteString("</Blobs>")
	if next != "" {
		fmt.Fprintf(&b, "<NextMarker>%s</NextMarker>", next)
	} else {
		b.WriteString("<NextMarker />")
	}
	b.WriteString("</EnumerationResults>")
	return b.String()
}

// azureError writes an Azure error answer that quotes the request's query
// (SAS included, as sent and decoded) in its message, as some answers do.
func azureError(w http.ResponseWriter, r *http.Request, status int, code string) {
	if code != "" {
		w.Header().Set("x-ms-error-code", code)
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	quote := ""
	if r != nil {
		// as sent and decoded: the signature both ways
		dec, _ := url.QueryUnescape(r.URL.RawQuery)
		quote = r.URL.RawQuery + " " + dec
	}
	fmt.Fprintf(w, "\xef\xbb\xbf<?xml version=\"1.0\" encoding=\"utf-8\"?><Error><Code>%s</Code><Message>m ", code)
	xml.EscapeText(w, []byte(quote))
	fmt.Fprint(w, "</Message></Error>")
}

type contractClient struct{ *Client }

func (c contractClient) List(ctx context.Context) error {
	_, err := c.Client.List(ctx, &client.ListInput{Container: "c"})
	return err
}

// TestRetryContract: the SDK client passes the shared retry and
// classification contract with Azure's answers.
func TestRetryContract(t *testing.T) {
	wire := objhttptest.Wire{
		OK:       func(w http.ResponseWriter) { fmt.Fprint(w, page("")) },
		Throttle: func(w http.ResponseWriter) { azureError(w, nil, 503, "ServerBusy") },
		Server:   func(w http.ResponseWriter) { azureError(w, nil, 500, "InternalError") },
		Denied:   func(w http.ResponseWriter) { azureError(w, nil, 403, "AuthorizationFailure") },
		Garbled:  func(w http.ResponseWriter) { fmt.Fprint(w, listHead+"<Blob><Na") },
	}
	objhttptest.Contract(t, wire, func(s objhttptest.Settings) objhttptest.Client {
		c, err := New(context.Background(), client.Config{Endpoint: s.Endpoint, Account: "acct", Anonymous: true, RequestTimeout: s.Timeout,
			MaxAttempts: s.Retry.MaxAttempts, BaseBackoff: s.Retry.BaseBackoff, MaxBackoff: s.Retry.MaxBackoff, Limiter: s.Retry.Limiter})
		if err != nil {
			t.Fatal(err)
		}
		return contractClient{c}
	}, client.Classify)
}

// closeCount counts the response bodies its transport hands out and their
// closes.
type closeCount struct {
	next           http.RoundTripper
	bodies, closes atomic.Int64
}

func (c *closeCount) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := c.next.RoundTrip(req)
	if err == nil {
		c.bodies.Add(1)
		resp.Body = &counted{ReadCloser: resp.Body, n: &c.closes}
	}
	return resp, err
}

type counted struct {
	io.ReadCloser
	once sync.Once
	n    *atomic.Int64
}

func (c *counted) Close() error {
	c.once.Do(func() { c.n.Add(1) })
	return c.ReadCloser.Close()
}

// outcome is what one List did, compared across the clients.
type outcome struct {
	Class    objhttp.Class
	Requeue  bool
	Err      string // *client.Error text, or the error's class-relevant sentinel
	Answer   *client.Error
	Page     *client.ListOutput
	Counts   objhttp.Counts
	Shrinks  int64
	Capped   int64
	Requests int64
}

// TestScriptedAnswers sends the same scripted answers to both clients
// (each through a supplied HTTP client that counts bodies and closes, with
// a SAS whose signature the answers quote): the same class, retry decision,
// *client.Error (status, code, message, query, wait), page, counters and
// requests; every body closed once; the signature in no error.
func TestScriptedAnswers(t *testing.T) {
	big := page("", "p/0", "p/1", "p/2", "p/3")
	cases := []struct {
		name     string
		attempts int
		in       client.ListInput
		cap      int64 // the body cap, 0: MaxBody
		handler  func(n int, w http.ResponseWriter, r *http.Request)
	}{
		{"429 with Retry-After, then a page", 3, client.ListInput{}, 0, func(n int, w http.ResponseWriter, r *http.Request) {
			if n == 1 {
				w.Header().Set("Retry-After", "0")
				azureError(w, r, 429, "")
				return
			}
			fmt.Fprint(w, page("", "a", "b"))
		}},
		{"503 ServerBusy past the timeout exhausted", 2, client.ListInput{}, 0, func(n int, w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Retry-After", "3600")
			azureError(w, r, 503, "ServerBusy")
		}},
		{"500 InternalError exhausted", 2, client.ListInput{}, 0, func(n int, w http.ResponseWriter, r *http.Request) { azureError(w, r, 500, "InternalError") }},
		{"500 OperationTimedOut exhausted", 2, client.ListInput{}, 0, func(n int, w http.ResponseWriter, r *http.Request) {
			azureError(w, r, 500, "OperationTimedOut")
		}},
		{"500 other code not retried", 3, client.ListInput{}, 0, func(n int, w http.ResponseWriter, r *http.Request) { azureError(w, r, 500, "Unknown") }},
		{"500 without a code header", 3, client.ListInput{}, 0, func(n int, w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(500)
			fmt.Fprint(w, "\xef\xbb\xbf<?xml version=\"1.0\"?><Error><Code>InternalError</Code><Message>x</Message></Error>")
		}},
		{"403", 3, client.ListInput{}, 0, func(n int, w http.ResponseWriter, r *http.Request) { azureError(w, r, 403, "AuthorizationFailure") }},
		{"401 with a challenge, not re-sent", 3, client.ListInput{}, 0, func(n int, w http.ResponseWriter, r *http.Request) {
			w.Header().Set("WWW-Authenticate", `Bearer authorization_uri=https://login.example/t, resource_id=https://storage.azure.com`)
			azureError(w, r, 401, "InvalidAuthenticationInfo")
		}},
		{"404", 3, client.ListInput{}, 0, func(n int, w http.ResponseWriter, r *http.Request) { azureError(w, r, 404, "ContainerNotFound") }},
		{"400 names the query", 3, client.ListInput{Prefix: "p q+", StartFrom: "s", Marker: "m"}, 0, func(n int, w http.ResponseWriter, r *http.Request) {
			azureError(w, r, 400, "InvalidQueryParameterValue")
		}},
		{"408 then a page", 3, client.ListInput{}, 0, func(n int, w http.ResponseWriter, r *http.Request) {
			if n == 1 {
				azureError(w, r, 408, "")
				return
			}
			fmt.Fprint(w, page("", "a"))
		}},
		{"301 not followed", 3, client.ListInput{}, 0, redirect(301)},
		{"302 not followed", 3, client.ListInput{}, 0, redirect(302)},
		{"307 not followed", 3, client.ListInput{}, 0, redirect(307)},
		{"308 not followed", 3, client.ListInput{}, 0, redirect(308)},
		{"308 malformed Location", 3, client.ListInput{}, 0, func(n int, w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Location", "http://[::1%zz/?"+r.URL.RawQuery)
			w.WriteHeader(308)
		}},
		{"200 error envelope", 2, client.ListInput{}, 0, func(n int, w http.ResponseWriter, r *http.Request) {
			fmt.Fprint(w, "\xef\xbb\xbf<?xml version=\"1.0\"?><Error><Code>InternalError</Code><Message>x</Message></Error>")
		}},
		{"200 wrong root", 2, client.ListInput{}, 0, func(n int, w http.ResponseWriter, r *http.Request) {
			fmt.Fprint(w, "<ListBucketResult><Blobs/></ListBucketResult>")
		}},
		{"200 without Blobs", 2, client.ListInput{}, 0, func(n int, w http.ResponseWriter, r *http.Request) {
			fmt.Fprint(w, "<EnumerationResults><NextMarker/></EnumerationResults>")
		}},
		{"200 empty body", 2, client.ListInput{}, 0, func(n int, w http.ResponseWriter, r *http.Request) {}},
		{"200 BOM only", 2, client.ListInput{}, 0, func(n int, w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, "\xef\xbb\xbf") }},
		{"200 cut in an element", 2, client.ListInput{}, 0, func(n int, w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, listHead+"<Blob><Na") }},
		{"200 connection cut", 2, client.ListInput{}, 0, func(n int, w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Length", "100000")
			fmt.Fprint(w, listHead)
			w.(http.Flusher).Flush()
			conn, _, _ := w.(http.Hijacker).Hijack()
			conn.Close()
		}},
		{"200 out of order", 2, client.ListInput{}, 0, func(n int, w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, page("", "b", "a")) }},
		{"200 repeated marker", 2, client.ListInput{Marker: "m1"}, 0, func(n int, w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, page("m1", "a")) }},
		{"200 Encoded name not UTF-8", 2, client.ListInput{}, 0, func(n int, w http.ResponseWriter, r *http.Request) {
			io.WriteString(w, listHead+`<Blob><Name Encoded="true">a%FF</Name></Blob>`+listTail)
		}},
		{"200 bad Content-Length", 2, client.ListInput{}, 0, func(n int, w http.ResponseWriter, r *http.Request) {
			fmt.Fprint(w, listHead+`<Blob><Name>a</Name><Properties><Content-Length>x</Content-Length></Properties></Blob>`+listTail)
		}},
		{"200 bad Last-Modified", 2, client.ListInput{}, 0, func(n int, w http.ResponseWriter, r *http.Request) {
			fmt.Fprint(w, listHead+`<Blob><Name>a</Name><Properties><Last-Modified>yesterday</Last-Modified></Properties></Blob>`+listTail)
		}},
		{"200 hierarchy", 2, client.ListInput{Delimiter: "/"}, 0, func(n int, w http.ResponseWriter, r *http.Request) {
			fmt.Fprint(w, listHead+`<BlobPrefix><Name>a/</Name></BlobPrefix><Blob><Name>a0</Name></Blob><BlobPrefix><Name>b/</Name></BlobPrefix>`+
				`<Blob><Name>c</Name></Blob><BlobPrefix><Name>`+"\U00010000/"+`</Name></BlobPrefix><Blob><Name>`+"\ue000"+`</Name></Blob>`+
				`</Blobs><NextMarker>next</NextMarker></EnumerationResults>`)
		}},
		{"201", 3, client.ListInput{}, 0, func(n int, w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(201)
			fmt.Fprint(w, page(""))
		}},
		{"204", 3, client.ListInput{}, 0, func(n int, w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) }},
		{"at the cap", 1, client.ListInput{MaxResults: 4}, int64(len(big)), func(n int, w http.ResponseWriter, r *http.Request) {
			fmt.Fprint(w, sized(r))
		}},
		{"a byte past the cap, halved", 1, client.ListInput{MaxResults: 4}, int64(len(big)) - 1, func(n int, w http.ResponseWriter, r *http.Request) {
			fmt.Fprint(w, sized(r))
		}},
		{"one entry past the cap", 1, client.ListInput{MaxResults: 4}, 100, func(n int, w http.ResponseWriter, r *http.Request) {
			fmt.Fprint(w, sized(r))
		}},
	}
	var scrubbed atomic.Int64
	t.Cleanup(func() {
		if scrubbed.Load() == 0 {
			t.Error("no echoing answer's message reached a client")
		}
	})
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var got [2]outcome
			for i, sdk := range []bool{false, true} {
				var mu sync.Mutex
				n := 0
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					mu.Lock()
					n++
					k := n
					mu.Unlock()
					c.handler(k, w, r)
				}))
				cc := &closeCount{next: http.DefaultTransport}
				cfg := client.Config{Endpoint: srv.URL, Account: "acct", SAS: testSAS, MaxAttempts: c.attempts, RequestTimeout: 2 * time.Second,
					BaseBackoff: time.Millisecond, MaxBackoff: time.Millisecond, HTTPClient: &http.Client{Transport: cc}}
				var l interface {
					List(context.Context, *client.ListInput) (*client.ListOutput, error)
					Counters() client.Counters
					BodyCapShrinks() int64
					RetryAfterCapped() int64
				}
				if sdk {
					s, err := New(context.Background(), cfg)
					if err != nil {
						t.Fatal(err)
					}
					if c.cap > 0 {
						s.SetBodyCap(c.cap)
						for _, g := range s.guards {
							g.MaxBody = c.cap
						}
					}
					l = s
				} else {
					h, err := client.New(context.Background(), cfg)
					if err != nil {
						t.Fatal(err)
					}
					if c.cap > 0 {
						h.SetBodyCap(c.cap)
					}
					l = h
				}
				in := c.in
				in.Container = "c"
				p, err := l.List(context.Background(), &in)
				srv.Close()
				o := outcome{Page: p, Counts: l.Counters(), Shrinks: l.BodyCapShrinks(), Capped: l.RetryAfterCapped(), Requests: int64(n)}
				// latency is the clients' own (the SDK client's includes its
				// decoding); the rest of the account must agree
				o.Counts.Limiter, o.Counts.Rate = objhttp.LimiterCounts{}, objhttp.RateCounts{}
				o.Counts.InflightNs, o.Counts.P50Ns, o.Counts.P90Ns, o.Counts.P99Ns, o.Counts.LatencyOverflow = 0, 0, 0, 0, 0
				if err != nil {
					o.Class, o.Requeue = client.Classify(err), objhttp.Requeue(client.Classify)(err)
					if strings.Contains(err.Error(), secret) {
						t.Errorf("sdk %v: the signature in the error: %v", sdk, err)
					}
					if e, ok := errors.AsType[*client.Error](err); ok {
						o.Answer = e
						if strings.HasPrefix(e.Message, "m ") {
							// an echoing answer: its message kept, the
							// signature redacted. The echoed query is the
							// client's own wire form (the SDK sorts the SAS
							// in among the parameters, so only its sig is
							// redacted, where the hand-rolled client's SAS
							// suffix goes whole), so the texts are compared
							// for that, not equality.
							scrubbed.Add(1)
							if !strings.Contains(e.Message, "sig=REDACTED") || !strings.Contains(e.Message, "sp=rl") {
								t.Errorf("sdk %v: message %q", sdk, e.Message)
							}
							cp := *e
							cp.Message = "m (echo)"
							o.Answer = &cp
						}
					}
					for _, s := range []error{objhttp.ErrSyntax, client.ErrInvalidPage} {
						if errors.Is(err, s) {
							o.Err = s.Error()
						}
					}
				}
				if b, cl := cc.bodies.Load(), cc.closes.Load(); b != cl {
					t.Errorf("sdk %v: %d bodies, %d closed", sdk, b, cl)
				}
				got[i] = o
				t.Logf("sdk %v: %v; %+v", sdk, err, o.Counts)
			}
			if !reflect.DeepEqual(got[0], got[1]) {
				t.Errorf("hand-rolled %+v\nsdk         %+v", got[0], got[1])
				if got[0].Answer != nil && got[1].Answer != nil {
					t.Errorf("answers: %+v / %+v", *got[0].Answer, *got[1].Answer)
				}
			}
		})
	}
}

// redirect answers status with a Location that carries the request's
// query (the SAS).
func redirect(status int) func(int, http.ResponseWriter, *http.Request) {
	return func(n int, w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", "http://elsewhere.example/acct/c?"+r.URL.RawQuery)
		w.WriteHeader(status)
	}
}

// sized is a page of maxresults (4 when absent) blobs p/0..: a larger
// page is a longer body.
func sized(r *http.Request) string {
	n, err := strconv.Atoi(r.URL.Query().Get("maxresults"))
	if err != nil || n > 4 {
		n = 4
	}
	names := make([]string, n)
	for i := range names {
		names[i] = fmt.Sprintf("p/%d", i)
	}
	next := ""
	if n < 4 {
		next = fmt.Sprintf("m%d", n)
	}
	return page(next, names...)
}

// TestCancelAfterHeaders: a caller's cancellation after the answer's
// headers, mid-body, ends both clients' List as cancelled, not retried,
// with the body closed.
func TestCancelAfterHeaders(t *testing.T) {
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, listHead)
		w.(http.Flusher).Flush()
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(srv.Close)
	for _, sdk := range []bool{false, true} {
		cc := &closeCount{next: http.DefaultTransport}
		cfg := client.Config{Endpoint: srv.URL, Account: "acct", Anonymous: true, MaxAttempts: 3, HTTPClient: &http.Client{Transport: cc}}
		var l lister
		if sdk {
			s, err := New(context.Background(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			l = s
		} else {
			h, err := client.New(context.Background(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			l = h
		}
		ctx, cancel := context.WithCancel(context.Background())
		cc.next = headersThen(http.DefaultTransport, cancel)
		_, err := l.List(ctx, &client.ListInput{Container: "c"})
		if client.Classify(err) != objhttp.ClassCanceled || !errors.Is(err, context.Canceled) {
			t.Errorf("sdk %v: %v (class %q)", sdk, err, client.Classify(err))
		}
		if b, cl := cc.bodies.Load(), cc.closes.Load(); b != 1 || cl != 1 {
			t.Errorf("sdk %v: %d bodies, %d closed", sdk, b, cl)
		}
	}
}

// headersThen calls f once the response headers are in.
func headersThen(next http.RoundTripper, f func()) http.RoundTripper {
	return rtFunc(func(req *http.Request) (*http.Response, error) {
		resp, err := next.RoundTrip(req)
		f()
		return resp, err
	})
}

type rtFunc func(*http.Request) (*http.Response, error)

func (f rtFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }
