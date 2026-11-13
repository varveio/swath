// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"golang.org/x/oauth2"

	"github.com/varveio/swath/go/internal/buildinfo"
	"github.com/varveio/swath/go/internal/source/objhttp/objhttptest"
	"github.com/varveio/swath/go/internal/source/s3/s3test"
	"github.com/varveio/swath/go/internal/source/tokensource"
)

// wireSnapshot is the checked-in rendering of every request shape the
// client sends (wire, objhttptest.Snapshot).
const wireSnapshot = "testdata/wire.txt"

// The fixed test credentials (AWS's documentation example).
const (
	testKeyID     = "AKIDEXAMPLE"
	testSecret    = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY"
	testSessionID = "session-token-example"
)

// TestWireSnapshot: each request the client sends, under each
// authentication form (anonymous, an access key from the environment, a
// session's credentials, a bearer token), virtual-host on AWS's endpoint
// and path-style (with requester pays) and on a custom endpoint:
// ListObjectsV2 with no options, with every option and continuing, and
// ListObjectVersions with no options and with every option, is method,
// intended authority, escaped path, every query parameter as sent, in
// order, every header value (duplicates kept) and the body's length,
// exactly as testdata/wire.txt has it, as the request reaches the
// transport below the Guard (recorder). A SigV4 signature is verified with the SDK's own signer
// before the rendering replaces it; its date, the X-Amz-Date, the
// invocation ID and the User-Agent's SDK, language and platform versions
// are replaced too. A difference is SDK drift for review: the semantic
// tests (TestRequestURLs, TestRequestOptions, TestUserAgent) say whether
// it is wrong.
func TestWireSnapshot(t *testing.T) {
	s3test.NoCredentials(t)
	ctx := context.Background()
	const key = "a b+c%/é&=?"
	full := ListInput{Bucket: "bkt", Prefix: "p/" + key, Delimiter: "/", StartAfter: "p/" + key + "x", MaxKeys: 7, FetchOwner: true}
	next := ListInput{Bucket: "bkt", Prefix: "p/", ContinuationToken: "1/tok+==", MaxKeys: 7}
	versions := VersionsInput{Bucket: "bkt", Prefix: "p/" + key, Delimiter: "/", KeyMarker: "p/" + key, VersionIDMarker: "null", MaxKeys: 7}
	var b strings.Builder
	for _, auth := range []struct {
		name  string
		cfg   Config
		creds aws.Credentials // set in the environment; zero: none
	}{
		{"anonymous, virtual-host", Config{Region: "us-east-1", Anonymous: true}, aws.Credentials{}},
		{"access key, virtual-host", Config{Region: "eu-west-1"}, aws.Credentials{AccessKeyID: testKeyID, SecretAccessKey: testSecret}},
		{"session, path-style, requester pays", Config{Region: "us-west-2", PathStyle: true, RequestPayer: true},
			aws.Credentials{AccessKeyID: testKeyID, SecretAccessKey: testSecret, SessionToken: testSessionID}},
		{"bearer, custom endpoint", Config{Region: "us-east-1", Endpoint: "https://s3.example/base/", PathStyle: true,
			Bearer: tokensource.NewShared(oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "tok"}))}, aws.Credentials{}},
	} {
		t.Setenv("AWS_ACCESS_KEY_ID", auth.creds.AccessKeyID)
		t.Setenv("AWS_SECRET_ACCESS_KEY", auth.creds.SecretAccessKey)
		t.Setenv("AWS_SESSION_TOKEN", auth.creds.SessionToken)
		rec := &recorder{}
		cfg := auth.cfg
		cfg.HTTPClient = &http.Client{Transport: rec}
		c := newTest(t, cfg)
		for _, call := range []struct {
			name string
			do   func() error
		}{
			{"objects", func() error { _, err := c.ListObjectsV2(ctx, &ListInput{Bucket: "bkt"}); return err }},
			{"objects, every option", func() error { _, err := c.ListObjectsV2(ctx, &full); return err }},
			{"objects, continuing", func() error { _, err := c.ListObjectsV2(ctx, &next); return err }},
			{"versions", func() error { _, err := c.ListObjectVersions(ctx, &VersionsInput{Bucket: "bkt"}); return err }},
			{"versions, every option", func() error { _, err := c.ListObjectVersions(ctx, &versions); return err }},
		} {
			n := len(rec.reqs)
			if err := call.do(); err != nil {
				t.Fatalf("%s, %s: %v", auth.name, call.name, err)
			}
			if len(rec.reqs) != n+1 {
				t.Fatalf("%s, %s: %d requests", auth.name, call.name, len(rec.reqs)-n)
			}
			req := rec.reqs[n]
			if auth.creds.HasKeys() {
				verifySigV4(t, req, auth.creds, cfg.Region)
			}
			fmt.Fprintf(&b, "== %s: %s\n%s", auth.name, call.name, wire(t, req, rec.bodies[n]))
		}
	}
	objhttptest.Snapshot(t, wireSnapshot, b.String())
}

var (
	amzDate    = regexp.MustCompile(`^\d{8}T\d{6}Z$`)
	sigV4      = regexp.MustCompile(`^(AWS4-HMAC-SHA256 Credential=[^/]+/)(\d{8})(/[^,]+, SignedHeaders=[^,]+, Signature=)[0-9a-f]{64}$`)
	uuid       = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
	uaVersions = regexp.MustCompile(`\b(aws-sdk-go-v2/|lang/go#|api/s3#)\S+`)
	uaPlatform = regexp.MustCompile(`\b(os/|md/GOOS#|md/GOARCH#)\S+`)
)

// verifySigV4 signs a copy of req with the SDK's own SigV4 signer, with
// creds, region and the X-Amz-Date and payload hash req carries, and
// requires req's Authorization: the rendering's <signature> stands for a
// valid one.
func verifySigV4(t *testing.T, req *http.Request, creds aws.Credentials, region string) {
	t.Helper()
	at, err := time.Parse("20060102T150405Z", req.Header.Get("X-Amz-Date"))
	if err != nil {
		t.Fatal(err)
	}
	sig := req.Clone(context.Background())
	sig.Header.Del("Authorization")
	if err := v4.NewSigner().SignHTTP(context.Background(), creds, sig, req.Header.Get("X-Amz-Content-Sha256"), "s3", region, at,
		func(o *v4.SignerOptions) { o.DisableURIPathEscaping = true }); err != nil {
		t.Fatal(err)
	}
	if got, want := req.Header.Get("Authorization"), sig.Header.Get("Authorization"); got != want {
		t.Errorf("%s %s: Authorization\n %s\nthe SDK's signer\n %s", req.Method, req.URL, got, want)
	}
}

// wire renders one request as the transport got it (objhttptest.Request).
// X-Amz-Date, the signature and the date in its scope (verified by the
// caller), the invocation ID and the User-Agent's swath token and SDK,
// language and platform versions are replaced by their form; anything
// else is as sent.
func wire(t *testing.T, req *http.Request, body int) string {
	t.Helper()
	return objhttptest.Request(req, req.URL.Scheme+"://"+req.URL.Host, body, func(k, v string) (string, bool) {
		switch k {
		case "X-Amz-Date":
			if !amzDate.MatchString(v) {
				t.Errorf("X-Amz-Date %q", v)
			}
			v = "<date>"
		case "Authorization":
			if strings.HasPrefix(v, "AWS4-HMAC-SHA256 ") {
				if !sigV4.MatchString(v) {
					t.Errorf("Authorization %q", v)
				}
				v = sigV4.ReplaceAllString(v, "${1}<date>${3}<signature>")
			}
		case "Amz-Sdk-Invocation-Id":
			if !uuid.MatchString(v) {
				t.Errorf("amz-sdk-invocation-id %q", v)
			}
			v = "<uuid>"
		case "User-Agent":
			v = strings.Replace(v, buildinfo.UserAgent(), "<swath>", 1)
			v = uaVersions.ReplaceAllString(v, "${1}<version>")
			v = uaPlatform.ReplaceAllString(v, "${1}<platform>")
		}
		return v, true
	})
}
