// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
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
	cfg.BaseBackoff, cfg.MaxBackoff = time.Millisecond, 2*time.Millisecond
	c, err := New(context.Background(), cfg)
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
	got := c.Counters()
	got.InflightMax, got.InflightNs, got.P50Ns, got.P90Ns, got.P99Ns = 0, 0, 0, 0, 0
	if got != (Counters{Attempts: 4, Retries: 3, Throttles: 1}) {
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
	if !strings.Contains(err.Error(), "the bucket is in eu-west-2") || strings.Contains(err.Error(), "-region") {
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
	c.retry.BaseBackoff, c.retry.MaxBackoff = time.Hour, time.Hour
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := c.ListObjectsV2(ctx, &ListInput{Bucket: "b"}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
}

// isolate clears the AWS environment and points the shared files at dir.
func isolate(t *testing.T, dir string) {
	for _, k := range []string{"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_PROFILE", "AWS_REGION", "AWS_DEFAULT_REGION", "AWS_WEB_IDENTITY_TOKEN_FILE", "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "AWS_CONTAINER_CREDENTIALS_FULL_URI", "AWS_ROLE_ARN", "AWS_EC2_METADATA_SERVICE_ENDPOINT"} {
		t.Setenv(k, "")
	}
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	old := dmiRoot
	dmiRoot = filepath.Join(dir, "sys") // not EC2, whatever the host is
	t.Cleanup(func() { dmiRoot = old })
	t.Setenv("AWS_CONFIG_FILE", filepath.Join(dir, "config"))
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", filepath.Join(dir, "credentials"))
}

// TestCredentialChain: the SDK's default chain signs with environment keys
// and profiles; -anonymous never signs; nothing found means anonymous; a
// named profile that does not exist is an error.
func TestCredentialChain(t *testing.T) {
	dir := t.TempDir()
	isolate(t, dir)
	os.WriteFile(filepath.Join(dir, "credentials"), []byte("[work]\naws_access_key_id = AKWORK\naws_secret_access_key = s\n"), 0o600)
	os.WriteFile(filepath.Join(dir, "config"), []byte("[profile work]\nregion = eu-west-2\n"), 0o600)
	var auth, token atomic.Value
	h := func(w http.ResponseWriter, r *http.Request) {
		auth.Store(r.Header.Get("Authorization"))
		token.Store(r.Header.Get("X-Amz-Security-Token"))
		w.Write([]byte(okPage))
	}
	list := func(cfg Config) *Client {
		t.Helper()
		c, _ := testClient(t, h, cfg)
		if _, err := c.ListObjectsV2(context.Background(), &ListInput{Bucket: "b"}); err != nil {
			t.Fatal(err)
		}
		return c
	}
	// Nothing configured: anonymous.
	if c := list(Config{}); auth.Load() != "" || !c.auth.Anonymous {
		t.Fatalf("unsigned expected, got %q", auth.Load())
	}
	// A named profile from the shared files, with its region.
	if c := list(Config{Profile: "work"}); !strings.Contains(auth.Load().(string), "Credential=AKWORK/") || c.Region() != "eu-west-2" ||
		!strings.Contains(auth.Load().(string), "/eu-west-2/s3/aws4_request") {
		t.Fatalf("profile: %q region %s", auth.Load(), c.Region())
	}
	// Environment keys with a session token.
	t.Setenv("AWS_ACCESS_KEY_ID", "AKENV")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "S")
	t.Setenv("AWS_SESSION_TOKEN", "T")
	if list(Config{}); !strings.HasPrefix(auth.Load().(string), "AWS4-HMAC-SHA256 Credential=AKENV/") || token.Load() != "T" {
		t.Fatalf("env: %q token %q", auth.Load(), token.Load())
	}
	// -anonymous is authoritative, with a warning.
	if c := list(Config{Anonymous: true}); auth.Load() != "" || token.Load() != "" || c.Note() == "" {
		t.Fatalf("anonymous request signed: %q, note %q", auth.Load(), c.Note())
	}
	if _, err := New(context.Background(), Config{Profile: "nope"}); err == nil {
		t.Fatal("missing profile accepted")
	}
}

// TestRequestPayer: --request-payer sends x-amz-request-payer: requester
// and signs it.
func TestRequestPayer(t *testing.T) {
	dir := t.TempDir()
	isolate(t, dir)
	t.Setenv("AWS_ACCESS_KEY_ID", "AKENV")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "S")
	var payer, auth atomic.Value
	h := func(w http.ResponseWriter, r *http.Request) {
		payer.Store(r.Header.Get("X-Amz-Request-Payer"))
		auth.Store(r.Header.Get("Authorization"))
		w.Write([]byte(okPage))
	}
	for _, on := range []bool{false, true} {
		c, _ := testClient(t, h, Config{RequestPayer: on})
		if _, err := c.ListObjectsV2(context.Background(), &ListInput{Bucket: "b"}); err != nil {
			t.Fatal(err)
		}
		signed := strings.Contains(auth.Load().(string), "x-amz-request-payer")
		if (payer.Load() == "requester") != on || signed != on {
			t.Fatalf("request payer %v: header %q, signed %v", on, payer.Load(), signed)
		}
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
		{Config{Region: "us-east-1"}, ListInput{Bucket: "a.b", Prefix: "~-_.é*\x01"},
			"https://s3.us-east-1.amazonaws.com/a.b?encoding-type=url&list-type=2&prefix=~-_.%C3%A9%2A%01"},
		{Config{Region: "us-east-1"}, ListInput{Bucket: "a.b"}, "https://s3.us-east-1.amazonaws.com/a.b?encoding-type=url&list-type=2"},
		{Config{Endpoint: "http://127.0.0.1:9000"}, ListInput{Bucket: "bkt"}, "http://127.0.0.1:9000/bkt?encoding-type=url&list-type=2"},
		{Config{Endpoint: "https://minio.example/s3/"}, ListInput{Bucket: "bkt"}, "https://bkt.minio.example/s3/?encoding-type=url&list-type=2"},
		{Config{Region: "us-gov-west-1", FIPS: true}, ListInput{Bucket: "bkt"}, "https://bkt.s3-fips.us-gov-west-1.amazonaws.com/?encoding-type=url&list-type=2"},
		{Config{Region: "eu-west-1", DualStack: true}, ListInput{Bucket: "bkt"}, "https://bkt.s3.dualstack.eu-west-1.amazonaws.com/?encoding-type=url&list-type=2"},
		{Config{Region: "cn-north-1"}, ListInput{Bucket: "bkt"}, "https://bkt.s3.cn-north-1.amazonaws.com.cn/?encoding-type=url&list-type=2"},
	} {
		c.cfg.Anonymous = true
		cl, err := New(context.Background(), c.cfg)
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

// TestSuppliedClientFollowsNoRedirect: a supplied HTTP client whose policy
// follows redirects is used without it: a 307 is an answer, and its
// Location receives no request.
func TestSuppliedClientFollowsNoRedirect(t *testing.T) {
	var followed atomic.Int32
	elsewhere := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		followed.Add(1)
		w.Write([]byte(okPage))
	}))
	defer elsewhere.Close()
	supplied := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return nil }}
	c, _ := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, elsewhere.URL+r.URL.RequestURI(), http.StatusTemporaryRedirect)
	}, Config{Anonymous: true, HTTPClient: supplied})
	_, err := c.ListObjectsV2(context.Background(), &ListInput{Bucket: "b"})
	var se *Error
	if !errors.As(err, &se) || se.Status != 307 || followed.Load() != 0 || supplied.CheckRedirect(nil, nil) != nil {
		t.Fatalf("%v, %d followed", err, followed.Load())
	}
}
