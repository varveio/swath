// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/source/s3/s3test"
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

// isolate clears the AWS environment and points the shared files at dir.
func isolate(t *testing.T, dir string) {
	s3test.NoCredentials(t)
	for _, k := range []string{"AWS_REGION", "AWS_DEFAULT_REGION", "AWS_EC2_METADATA_SERVICE_ENDPOINT"} {
		t.Setenv(k, "")
	}
	t.Setenv("AWS_CONFIG_FILE", filepath.Join(dir, "config"))
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", filepath.Join(dir, "credentials"))
}

// TestCredentialChain checks selected-profile and region wiring, the default
// SDK chain, and explicit anonymity taking precedence over configured keys.
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
	// A named profile from the shared files, with its region.
	if c := list(Config{Profile: "work"}); !strings.Contains(auth.Load().(string), "Credential=AKWORK/") || c.Region() != "eu-west-2" ||
		!strings.Contains(auth.Load().(string), "/eu-west-2/s3/aws4_request") {
		t.Fatalf("profile: %q region %s", auth.Load(), c.Region())
	}
	// The default chain is wired; its internal source precedence is SDK-owned.
	t.Setenv("AWS_ACCESS_KEY_ID", "AKENV")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "S")
	if list(Config{Region: "ap-south-1"}); !strings.Contains(auth.Load().(string), "Credential=AKENV/") || !strings.Contains(auth.Load().(string), "/ap-south-1/s3/") {
		t.Fatalf("default chain: %q", auth.Load())
	}
	// -anonymous is authoritative, with a warning.
	if c := list(Config{Anonymous: true}); auth.Load() != "" || token.Load() != "" || c.Note() == "" {
		t.Fatalf("anonymous request signed: %q, note %q", auth.Load(), c.Note())
	}
}

// TestRequestPayer: --s3-requester-pays sends x-amz-request-payer: requester
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
