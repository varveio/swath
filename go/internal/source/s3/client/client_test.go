// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const okPage = `<ListBucketResult><EncodingType>url</EncodingType><IsTruncated>false</IsTruncated><Contents><Key>k%20</Key><Size>1</Size></Contents></ListBucketResult>`

func testClient(t *testing.T, h http.HandlerFunc, cfg Config) (*Client, *httptest.Server) {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	cfg.Endpoint, cfg.PathStyle = srv.URL, true
	if cfg.Env == nil {
		cfg.Env = func(string) string { return "" }
	}
	cfg.BaseBackoff, cfg.MaxBackoff = time.Millisecond, 2*time.Millisecond
	c, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return c, srv
}

func TestRetriesAndThrottles(t *testing.T) {
	var n atomic.Int32
	c, _ := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		switch n.Add(1) {
		case 1:
			w.WriteHeader(503)
			w.Write([]byte(`<Error><Code>SlowDown</Code><Message>Please reduce your request rate.</Message></Error>`))
		case 2:
			w.WriteHeader(500)
		case 3:
			w.Header().Set("Content-Length", "1000") // body cut short: a connection error
			w.Write([]byte(okPage[:20]))
		default:
			if r.URL.Query().Get("list-type") != "2" || r.URL.Query().Get("encoding-type") != "url" || r.URL.Path != "/b" {
				t.Errorf("request %s", r.URL)
			}
			w.Write([]byte(okPage))
		}
	}, Config{Anonymous: true})
	pg, err := c.ListObjectsV2(context.Background(), &ListInput{Bucket: "b"})
	if err != nil {
		t.Fatal(err)
	}
	if pg.Contents[0].Key != "k " {
		t.Fatalf("%+v", pg)
	}
	if got := c.Counters(); got != (Counters{Attempts: 4, Retries: 3, Throttles: 1}) {
		t.Fatalf("%+v", got)
	}
}

func TestTypedErrorsAreNotRetried(t *testing.T) {
	var n atomic.Int32
	c, _ := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		n.Add(1)
		w.Header().Set("X-Amz-Bucket-Region", "eu-west-2")
		w.WriteHeader(301)
		w.Write([]byte(`<Error><Code>PermanentRedirect</Code><Message>wrong endpoint</Message><RequestId>RID</RequestId></Error>`))
	}, Config{Anonymous: true})
	_, err := c.ListObjectsV2(context.Background(), &ListInput{Bucket: "b"})
	var se *Error
	if !errors.As(err, &se) || se.Code != "PermanentRedirect" || se.RequestID != "RID" || se.Region != "eu-west-2" || n.Load() != 1 {
		t.Fatalf("%v (%d attempts)", err, n.Load())
	}
	if !strings.Contains(err.Error(), "-region eu-west-2") {
		t.Fatal(err)
	}
}

func TestTimeoutIsRetried(t *testing.T) {
	var n atomic.Int32
	c, _ := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		if n.Add(1) == 1 {
			<-r.Context().Done()
			return
		}
		w.Write([]byte(okPage))
	}, Config{Anonymous: true, RequestTimeout: 50 * time.Millisecond})
	if _, err := c.ListObjectsV2(context.Background(), &ListInput{Bucket: "b"}); err != nil {
		t.Fatal(err)
	}
	if got := c.Counters(); got.Timeouts != 1 || got.Retries != 1 {
		t.Fatalf("%+v", got)
	}
}

func TestAttemptsAreBounded(t *testing.T) {
	c, _ := testClient(t, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(500) }, Config{Anonymous: true, MaxAttempts: 3})
	_, err := c.ListObjectsV2(context.Background(), &ListInput{Bucket: "b"})
	if err == nil || !strings.Contains(err.Error(), "after 3 attempts") || c.Counters().Attempts != 3 {
		t.Fatalf("%v %+v", err, c.Counters())
	}
}

func TestCancelStopsBackoff(t *testing.T) {
	c, _ := testClient(t, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(503) }, Config{Anonymous: true})
	c.cfg.BaseBackoff, c.cfg.MaxBackoff = time.Hour, time.Hour
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := c.ListObjectsV2(ctx, &ListInput{Bucket: "b"}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
}

func TestSignedAndAnonymousRequests(t *testing.T) {
	var auth, token atomic.Value
	h := func(w http.ResponseWriter, r *http.Request) {
		auth.Store(r.Header.Get("Authorization"))
		token.Store(r.Header.Get("X-Amz-Security-Token"))
		w.Write([]byte(okPage))
	}
	env := map[string]string{"AWS_ACCESS_KEY_ID": "AK", "AWS_SECRET_ACCESS_KEY": "S", "AWS_SESSION_TOKEN": "T"}
	c, _ := testClient(t, h, Config{Env: func(k string) string { return env[k] }})
	c.ListObjectsV2(context.Background(), &ListInput{Bucket: "b"})
	if a := auth.Load().(string); !strings.HasPrefix(a, "AWS4-HMAC-SHA256 Credential=AK/") || token.Load() != "T" {
		t.Fatalf("auth %q token %q", a, token.Load())
	}
	c, _ = testClient(t, h, Config{Anonymous: true, Env: func(k string) string { return env[k] }})
	c.ListObjectsV2(context.Background(), &ListInput{Bucket: "b"})
	if auth.Load() != "" || token.Load() != "" || c.Note() == "" {
		t.Fatalf("anonymous request signed: %q, note %q", auth.Load(), c.Note())
	}
}

func TestURL(t *testing.T) {
	for _, c := range []struct {
		cfg  Config
		in   ListInput
		want string
	}{
		{Config{Region: "eu-west-1"}, ListInput{Bucket: "bkt", Prefix: "a b/", MaxKeys: 1000},
			"https://bkt.s3.eu-west-1.amazonaws.com/?encoding-type=url&list-type=2&max-keys=1000&prefix=a%20b%2F"},
		{Config{Region: "us-east-1", PathStyle: true}, ListInput{Bucket: "bkt", StartAfter: "x", FetchOwner: true, Delimiter: "/", ContinuationToken: "t+="},
			"https://s3.us-east-1.amazonaws.com/bkt?continuation-token=t%2B%3D&delimiter=%2F&encoding-type=url&fetch-owner=true&list-type=2&start-after=x"},
		{Config{Region: "us-east-1"}, ListInput{Bucket: "a.b"}, "https://s3.us-east-1.amazonaws.com/a.b?encoding-type=url&list-type=2"},
		{Config{Endpoint: "http://127.0.0.1:9000"}, ListInput{Bucket: "bkt"}, "http://127.0.0.1:9000/bkt?encoding-type=url&list-type=2"},
		{Config{Endpoint: "https://minio.example/s3/"}, ListInput{Bucket: "bkt"}, "https://bkt.minio.example/s3/?encoding-type=url&list-type=2"},
	} {
		c.cfg.Anonymous = true
		cl, err := New(c.cfg)
		if err != nil {
			t.Fatal(err)
		}
		if got := cl.url(&c.in).String(); got != c.want {
			t.Errorf("got  %s\nwant %s", got, c.want)
		}
	}
}

// TestParseFailuresAreRetried: a 200 whose body is cut or garbled is
// retried and counted, and the page is returned once a good body arrives.
func TestParseFailuresAreRetried(t *testing.T) {
	var n atomic.Int32
	c, _ := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		switch n.Add(1) {
		case 1:
			w.Write([]byte(okPage[:len(okPage)-30])) // complete response, truncated document
		case 2:
			w.Write([]byte(strings.Replace(okPage, "</Key>", "</Kye>", 1)))
		default:
			w.Write([]byte(okPage))
		}
	}, Config{Anonymous: true})
	pg, err := c.ListObjectsV2(context.Background(), &ListInput{Bucket: "b"})
	if err != nil || len(pg.Contents) != 1 {
		t.Fatalf("%v %+v", err, pg)
	}
	if got := c.Counters(); got.ParseFailures != 2 || got.Retries != 2 {
		t.Fatalf("%+v", got)
	}
}
