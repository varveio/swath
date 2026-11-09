// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package sdkclient

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/source/objhttp"
	"github.com/varveio/swath/go/internal/source/s3/client"
	"github.com/varveio/swath/go/internal/source/s3/s3test"
)

// closeCounter is a RoundTripper that counts each original response
// body's Close calls.
type closeCounter struct {
	next   http.RoundTripper
	mu     sync.Mutex
	bodies []*countedBody
}

type countedBody struct {
	io.ReadCloser
	closes atomic.Int32
}

func (b *countedBody) Close() error {
	b.closes.Add(1)
	return b.ReadCloser.Close()
}

func (c *closeCounter) RoundTrip(r *http.Request) (*http.Response, error) {
	resp, err := c.next.RoundTrip(r)
	if err != nil {
		return nil, err
	}
	b := &countedBody{ReadCloser: resp.Body}
	c.mu.Lock()
	c.bodies = append(c.bodies, b)
	c.mu.Unlock()
	resp.Body = b
	return resp, nil
}

// closedOnce fails unless every body was closed exactly once.
func (c *closeCounter) closedOnce(t *testing.T, who string) {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, b := range c.bodies {
		if n := b.closes.Load(); n != 1 {
			t.Errorf("%s: body %d closed %d times", who, i, n)
		}
	}
}

const maxBody = 16 << 20

// sized is a valid empty ListBucketResult of exactly n bytes (padded with
// a comment).
func sized(n int) string {
	head, tail := `<ListBucketResult><IsTruncated>false</IsTruncated><!--`, `--></ListBucketResult>`
	return head + strings.Repeat("x", n-len(head)-len(tail)) + tail
}

// TestAnswersMatchHandRolled: for each scripted answer, the hand-rolled
// client and the SDK-backed one (every decoder) fail with the same class,
// the same retry decision (exhausted or not) and the same request
// counters, and every original response body is closed once. Both are
// supplied an HTTP client whose policy follows redirects: neither follows
// a 307 or 308.
func TestAnswersMatchHandRolled(t *testing.T) {
	s3test.NoCredentials(t)
	xmlError := func(status int, code string, hdr ...string) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			for i := 0; i+1 < len(hdr); i += 2 {
				w.Header().Set(hdr[i], hdr[i+1])
			}
			w.WriteHeader(status)
			fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>%s</Code><Message>m</Message><RequestId>RID</RequestId></Error>`, code)
		}
	}
	body := func(status int, b string) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(status)
			io.WriteString(w, b)
		}
	}
	elsewhere := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("redirect followed to %s", r.URL)
		io.WriteString(w, okPage)
	}))
	defer elsewhere.Close()
	redirect := func(status int) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Location", elsewhere.URL+r.URL.RequestURI())
			w.WriteHeader(status)
		}
	}
	cut := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "1000")
		io.WriteString(w, okPage[:20])
	}
	stall := func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, okPage[:20])
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}
	const stalled = "200 past the deadline mid-body"
	gzipped := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Encoding", "gzip")
		io.WriteString(w, "not gzip at all")
	}
	for _, c := range []struct {
		name  string
		h     http.HandlerFunc
		class objhttp.Class // "" for success
	}{
		{"429 SlowDown", xmlError(429, "SlowDown"), objhttp.ClassThrottle},
		{"503 SlowDown", xmlError(503, "SlowDown"), objhttp.ClassThrottle},
		{"503 no body", body(503, ""), objhttp.ClassThrottle},
		{"500 InternalError", xmlError(500, "InternalError"), objhttp.ClassServer},
		{"500 html", body(500, "<html>oops"), objhttp.ClassServer},
		{"400 RequestTimeout", xmlError(400, "RequestTimeout"), objhttp.ClassTimeout},
		{"408", body(408, ""), objhttp.ClassTimeout}, // code RequestTimeout from the status text
		{"403 AccessDenied", xmlError(403, "AccessDenied"), objhttp.ClassAccessDenied},
		{"403 InvalidAccessKeyId", xmlError(403, "InvalidAccessKeyId"), objhttp.ClassAuth},
		{"404 NoSuchBucket", xmlError(404, "NoSuchBucket"), objhttp.ClassNoSuchBucket},
		{"404 no body", body(404, ""), objhttp.ClassNoSuchBucket},
		{"301 PermanentRedirect", xmlError(301, "PermanentRedirect", "X-Amz-Bucket-Region", "eu-west-2"), objhttp.ClassRedirect},
		{"307", redirect(307), objhttp.ClassOther},
		{"308", redirect(308), objhttp.ClassRedirect}, // code PermanentRedirect from the status text
		{"error body cut", body(500, `<Error><Code>InternalError</Code><Mess`), objhttp.ClassServer},
		{"error body over 64 KiB", body(403, `<Error><Code>AccessDenied</Code><Message>`+strings.Repeat("m", 70<<10)+`</Message></Error>`), objhttp.ClassAccessDenied},
		{"200 Error envelope", xmlError(200, "InternalError"), objhttp.ClassParse},
		{"200 wrong root", body(200, `<ListAllMyBucketsResult></ListAllMyBucketsResult>`), objhttp.ClassParse},
		{"200 empty", body(200, ""), objhttp.ClassParse},
		{"200 prolog only", body(200, `<?xml version="1.0" encoding="UTF-8"?>`), objhttp.ClassParse},
		{"200 cut mid-element", body(200, `<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>k</Ke`), objhttp.ClassParse},
		{"200 16 MiB", body(200, sized(maxBody)), ""},
		{"200 16 MiB + 1", body(200, sized(maxBody+1)), objhttp.ClassOther},
		{"200 cut mid-stream", cut, objhttp.ClassTransport},
		{stalled, stall, objhttp.ClassTimeout},
		{"200 malformed gzip", gzipped, objhttp.ClassOther},
		{"201", body(201, okPage), objhttp.ClassOther},
		{"201 SlowDown", xmlError(201, "SlowDown"), objhttp.ClassThrottle},
		{"202", body(202, ""), objhttp.ClassOther},
		{"204", body(204, ""), objhttp.ClassOther},
	} {
		for _, d := range decoders {
			t.Run(c.name+"/"+d.name, func(t *testing.T) {
				srv := httptest.NewServer(c.h)
				defer srv.Close()
				cfg := testConfig(srv.URL)
				cfg.MaxAttempts = 3
				if c.name == stalled {
					cfg.RequestTimeout = 200 * time.Millisecond // the others keep the default: 16 MiB under -race
				}
				hrt, srt := &closeCounter{next: http.DefaultTransport}, &closeCounter{next: http.DefaultTransport}
				hand, err := client.New(context.Background(), withHTTP(cfg, hrt))
				if err != nil {
					t.Fatal(err)
				}
				sdk, err := New(context.Background(), withHTTP(cfg, srt), WithDecoder(d.dec))
				if err != nil {
					t.Fatal(err)
				}
				in := &client.ListInput{Bucket: "b"}
				_, herr := hand.ListObjectsV2(context.Background(), in)
				_, serr := sdk.ListObjectsV2(context.Background(), in)
				if client.Classify(herr) != c.class || client.Classify(serr) != c.class {
					t.Fatalf("class: hand-rolled %q (%v), sdk %q (%v), want %q", client.Classify(herr), herr, client.Classify(serr), serr, c.class)
				}
				var hex, sex *objhttp.ExhaustedError
				hc, sc := requests(hand.Counters()), requests(sdk.Counters())
				if errors.As(herr, &hex) != errors.As(serr, &sex) || hc != sc {
					t.Fatalf("hand-rolled %+v (%v)\n        sdk %+v (%v)", hc, herr, sc, serr)
				}
				var he, se *client.Error
				if errors.As(herr, &he) {
					if !errors.As(serr, &se) || he.Status != se.Status || he.Code != se.Code || he.Region != se.Region || he.RequestID != se.RequestID {
						t.Fatalf("answers: hand-rolled %#v, sdk %#v", he, se)
					}
					if he.Message != se.Message {
						t.Logf("message differs: hand-rolled %q, sdk %q", he.Message, se.Message)
					}
				}
				hrt.closedOnce(t, "hand-rolled")
				srt.closedOnce(t, "sdk")
			})
		}
	}
}

// withHTTP supplies an HTTP client over rt whose policy follows redirects
// (http.Client's default): each client sets its own on a copy.
func withHTTP(cfg client.Config, rt http.RoundTripper) client.Config {
	cfg.HTTPClient = &http.Client{Transport: rt}
	return cfg
}

// TestCancelAfterHeaders: a caller's cancellation while the body is read
// is cancellation through both clients, not an attempt failure, and the
// body is closed.
func TestCancelAfterHeaders(t *testing.T) {
	s3test.NoCredentials(t)
	release := make(chan struct{})
	defer close(release)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `<ListBucketResult><IsTruncated>false</IsTruncated>`)
		w.(http.Flusher).Flush()
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()
	for _, d := range decoders {
		cfg := testConfig(srv.URL)
		hrt, srt := &closeCounter{next: http.DefaultTransport}, &closeCounter{next: http.DefaultTransport}
		hand, _ := client.New(context.Background(), withHTTP(cfg, hrt))
		sdk, _ := New(context.Background(), withHTTP(cfg, srt), WithDecoder(d.dec))
		for who, c := range map[string]lister{"hand-rolled": hand, "sdk": sdk} {
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			_, err := c.ListObjectsV2(ctx, &client.ListInput{Bucket: "b"})
			cancel()
			if client.Classify(err) != objhttp.ClassCanceled || c.Counters().Attempts != 1 {
				t.Fatalf("%s/%s: %v (%q) %+v", d.name, who, err, client.Classify(err), c.Counters())
			}
		}
		hrt.closedOnce(t, "hand-rolled")
		srt.closedOnce(t, "sdk")
	}
}

// TestBearerTokenFailureIsNotAnAttempt: a bearer token command that fails
// is a CredentialError (auth) before any request is sent or counted; one
// that works sends "Authorization: Bearer".
func TestBearerTokenFailureIsNotAnAttempt(t *testing.T) {
	s3test.NoCredentials(t)
	var auth atomic.Value
	var sent atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sent.Add(1)
		auth.Store(r.Header.Get("Authorization"))
		io.WriteString(w, okPage)
	}))
	defer srv.Close()
	cfg := testConfig(srv.URL)
	cfg.Anonymous = false
	cfg.Bearer = client.NewBearerToken("exit 3", 0)
	c, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.ListObjectsV2(context.Background(), &client.ListInput{Bucket: "b"})
	var ce *objhttp.CredentialError
	if !errors.As(err, &ce) || client.Classify(err) != objhttp.ClassAuth || c.Counters().Attempts != 0 || sent.Load() != 0 {
		t.Fatalf("%v (%q), %+v, %d sent", err, client.Classify(err), c.Counters(), sent.Load())
	}
	cfg.Bearer = client.NewBearerToken("echo tok", 0)
	if c, err = New(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	if _, err := c.ListObjectsV2(context.Background(), &client.ListInput{Bucket: "b"}); err != nil || auth.Load() != "Bearer tok" {
		t.Fatalf("%v, authorization %q", err, auth.Load())
	}
}
