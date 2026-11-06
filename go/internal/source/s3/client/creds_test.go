// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/source/objhttp"
)

// fakeSTS answers AssumeRoleWithWebIdentity and AssumeRole, recording each
// call's action, external id and signing region.
type fakeSTS struct {
	mu    sync.Mutex
	calls []string
}

func (f *fakeSTS) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	b, _ := io.ReadAll(r.Body)
	q, _ := url.ParseQuery(string(b))
	region := ""
	if a := r.Header.Get("Authorization"); a != "" {
		if i := strings.Index(a, "Credential="); i >= 0 {
			if parts := strings.Split(a[i:], "/"); len(parts) > 2 {
				region = parts[2]
			}
		}
	}
	act := q.Get("Action")
	f.mu.Lock()
	f.calls = append(f.calls, fmt.Sprintf("%s ext=%s region=%s", act, q.Get("ExternalId"), region))
	f.mu.Unlock()
	exp := time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
	key := "AKWEB"
	if act == "AssumeRole" {
		key = "AKROLE"
	}
	fmt.Fprintf(w, `<%sResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/"><%sResult><Credentials>
<AccessKeyId>%s</AccessKeyId><SecretAccessKey>s</SecretAccessKey><SessionToken>tok-%s</SessionToken><Expiration>%s</Expiration>
</Credentials></%sResult></%sResponse>`, act, act, key, key, exp, act, act)
}

// TestCredentialSources: web identity, a role chained from it with an
// external id, the STS region independent of --region, and credentials
// refreshed when they expire mid-listing.
func TestCredentialSources(t *testing.T) {
	dir := t.TempDir()
	isolate(t, dir)
	sts := &fakeSTS{}
	stsSrv := httptest.NewServer(sts)
	t.Cleanup(stsSrv.Close)
	t.Setenv("AWS_ENDPOINT_URL_STS", stsSrv.URL)
	tokenFile := filepath.Join(dir, "token")
	os.WriteFile(tokenFile, []byte("jwt"), 0o600)
	os.WriteFile(filepath.Join(dir, "config"), []byte(fmt.Sprintf(`[profile web]
role_arn = arn:aws:iam::111111111111:role/web
web_identity_token_file = %s

[profile chained]
role_arn = arn:aws:iam::222222222222:role/target
source_profile = web
external_id = ext-42
`, tokenFile)), 0o600)
	var auth atomic.Value
	h := func(w http.ResponseWriter, r *http.Request) {
		auth.Store(r.Header.Get("Authorization"))
		w.Write([]byte(okPage))
	}
	list := func(cfg Config) {
		t.Helper()
		c, _ := testClient(t, h, cfg)
		if _, err := c.ListObjectsV2(context.Background(), &ListInput{Bucket: "b"}); err != nil {
			t.Fatal(err)
		}
	}
	// Web identity, with the bucket in another region: STS keeps us-east-1.
	list(Config{Profile: "web", Region: "eu-west-2"})
	if a := auth.Load().(string); !strings.Contains(a, "Credential=AKWEB/") || !strings.Contains(a, "/eu-west-2/s3/") {
		t.Fatalf("web identity: %q", a)
	}
	// A role chained from it, with its external id.
	list(Config{Profile: "chained", Region: "ap-southeast-2"})
	if a := auth.Load().(string); !strings.Contains(a, "Credential=AKROLE/") {
		t.Fatalf("chained role: %q", a)
	}
	sts.mu.Lock()
	calls := strings.Join(sts.calls, "; ")
	sts.mu.Unlock()
	if !strings.Contains(calls, "AssumeRoleWithWebIdentity ext= region=") || !strings.Contains(calls, "AssumeRole ext=ext-42 region=us-east-1") ||
		strings.Contains(calls, "eu-west-2") || strings.Contains(calls, "ap-southeast-2") {
		t.Fatalf("STS calls: %s", calls)
	}
}

// TestCredentialRefresh: credentials that expire during a listing are
// fetched again (a credential process whose keys last 2 s stands in for an
// hour-long session); requests are signed with the new keys, none fails.
func TestCredentialRefresh(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell credential process")
	}
	dir := t.TempDir()
	isolate(t, dir)
	counter := filepath.Join(dir, "n")
	script := filepath.Join(dir, "creds.sh")
	os.WriteFile(script, []byte(`#!/bin/sh
n=$(cat `+counter+` 2>/dev/null || echo 0); n=$((n+1)); echo $n > `+counter+`
exp=$(date -u -d "+2 seconds" +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u -v+2S +%Y-%m-%dT%H:%M:%SZ)
printf '{"Version":1,"AccessKeyId":"AKP%s","SecretAccessKey":"s","SessionToken":"t","Expiration":"%s"}' $n $exp
`), 0o700)
	os.WriteFile(filepath.Join(dir, "config"), []byte("[profile proc]\ncredential_process = "+script+"\n"), 0o600)
	var keys sync.Map
	h := func(w http.ResponseWriter, r *http.Request) {
		a := r.Header.Get("Authorization")
		if i := strings.Index(a, "Credential="); i >= 0 {
			keys.Store(strings.SplitN(a[i+len("Credential="):], "/", 2)[0], true)
		}
		w.Write([]byte(okPage))
	}
	c, _ := testClient(t, h, Config{Profile: "proc"})
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := c.ListObjectsV2(context.Background(), &ListInput{Bucket: "b"}); err != nil {
			t.Fatal(err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	n := 0
	keys.Range(func(any, any) bool { n++; return true })
	if n < 2 {
		t.Fatalf("signed with %d distinct keys over 4 s of 2-second credentials", n)
	}
}

// TestCredentialOutageRecovers: an STS outage at a refresh (503 for six
// calls: two of the SDK's three-attempt retrievals) is a transient
// credential failure, retried by the client until STS answers again; a
// refused identity token stays a fatal auth failure.
func TestCredentialOutageRecovers(t *testing.T) {
	dir := t.TempDir()
	isolate(t, dir)
	tokenFile := filepath.Join(dir, "token")
	os.WriteFile(tokenFile, []byte("jwt"), 0o600)
	os.WriteFile(filepath.Join(dir, "config"), []byte("[profile web]\nrole_arn = arn:aws:iam::111111111111:role/web\nweb_identity_token_file = "+tokenFile+"\n"), 0o600)
	var n atomic.Int32
	var refuse atomic.Bool
	sts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		i := n.Add(1)
		switch {
		case refuse.Load():
			w.WriteHeader(400)
			fmt.Fprint(w, `<ErrorResponse><Error><Type>Sender</Type><Code>InvalidIdentityToken</Code><Message>no</Message></Error></ErrorResponse>`)
		case i >= 2 && i <= 7:
			w.WriteHeader(503)
			fmt.Fprint(w, `<ErrorResponse><Error><Type>Receiver</Type><Code>ServiceUnavailable</Code><Message>down</Message></Error></ErrorResponse>`)
		default:
			exp := time.Now().Add(2 * time.Second).UTC().Format(time.RFC3339)
			fmt.Fprintf(w, `<AssumeRoleWithWebIdentityResponse><AssumeRoleWithWebIdentityResult><Credentials>
<AccessKeyId>AK%d</AccessKeyId><SecretAccessKey>s</SecretAccessKey><SessionToken>t</SessionToken><Expiration>%s</Expiration>
</Credentials></AssumeRoleWithWebIdentityResult></AssumeRoleWithWebIdentityResponse>`, i, exp)
		}
	}))
	t.Cleanup(sts.Close)
	t.Setenv("AWS_ENDPOINT_URL_STS", sts.URL)
	ok := func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(okPage)) }
	c, _ := testClient(t, ok, Config{Profile: "web", Region: "us-east-1"})
	if _, err := c.ListObjectsV2(context.Background(), &ListInput{Bucket: "b"}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2500 * time.Millisecond) // the first credentials expire; the refresh meets the outage
	if _, err := c.ListObjectsV2(context.Background(), &ListInput{Bucket: "b"}); err != nil {
		t.Fatalf("after the outage: %v (class %s, %d STS calls)", err, Classify(err), n.Load())
	}
	if c.Counters().Retries == 0 || n.Load() < 8 {
		t.Fatalf("retries %d, STS calls %d: the outage was not met", c.Counters().Retries, n.Load())
	}
	refuse.Store(true)
	time.Sleep(2500 * time.Millisecond)
	_, err := c.ListObjectsV2(context.Background(), &ListInput{Bucket: "b"})
	var ex *ExhaustedError
	if Classify(err) != objhttp.ClassAuth || errors.As(err, &ex) {
		t.Fatalf("refused token: %v (class %s), want auth, not retried", err, Classify(err))
	}
}
