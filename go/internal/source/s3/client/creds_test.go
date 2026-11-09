// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
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

	"github.com/aws/aws-sdk-go-v2/aws"

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

// TestCredentialSources: the bucket's region (--region) is the listing's
// signing region only; the credential service keeps the environment's or
// the profile's region (else us-east-1), so a bucket in another region
// never moves STS. A role assumed from a profile's keys: STS is signed in
// us-east-1, the listing in the bucket's region with the role's keys.
func TestCredentialSources(t *testing.T) {
	dir := t.TempDir()
	isolate(t, dir)
	sts := &fakeSTS{}
	stsSrv := httptest.NewServer(sts)
	t.Cleanup(stsSrv.Close)
	t.Setenv("AWS_ENDPOINT_URL_STS", stsSrv.URL)
	os.WriteFile(filepath.Join(dir, "credentials"), []byte("[base]\naws_access_key_id = AKBASE\naws_secret_access_key = s\n"), 0o600)
	os.WriteFile(filepath.Join(dir, "config"), []byte("[profile role]\nrole_arn = arn:aws:iam::222222222222:role/target\nsource_profile = base\n"), 0o600)
	var auth atomic.Value
	c, _ := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		auth.Store(r.Header.Get("Authorization"))
		w.Write([]byte(okPage))
	}, Config{Profile: "role", Region: "ap-southeast-2"})
	if _, err := c.ListObjectsV2(context.Background(), &ListInput{Bucket: "b"}); err != nil {
		t.Fatal(err)
	}
	if a := auth.Load().(string); !strings.Contains(a, "Credential=AKROLE/") || !strings.Contains(a, "/ap-southeast-2/s3/") {
		t.Fatalf("listing: %q", a)
	}
	sts.mu.Lock()
	calls := strings.Join(sts.calls, "; ")
	sts.mu.Unlock()
	if calls != "AssumeRole ext= region=us-east-1" {
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

// webIdentitySTS answers AssumeRoleWithWebIdentity after delay with key
// AKWEB, or with status and code when status is set.
func webIdentitySTS(t *testing.T, delay time.Duration, status int, code string) {
	sts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(delay)
		if status != 0 {
			w.WriteHeader(status)
			fmt.Fprintf(w, `<ErrorResponse><Error><Type>Sender</Type><Code>%s</Code><Message>no</Message></Error></ErrorResponse>`, code)
			return
		}
		exp := time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
		fmt.Fprintf(w, `<AssumeRoleWithWebIdentityResponse><AssumeRoleWithWebIdentityResult><Credentials>
<AccessKeyId>AKWEB</AccessKeyId><SecretAccessKey>s</SecretAccessKey><SessionToken>t</SessionToken><Expiration>%s</Expiration>
</Credentials></AssumeRoleWithWebIdentityResult></AssumeRoleWithWebIdentityResponse>`, exp)
	}))
	t.Cleanup(sts.Close)
	t.Setenv("AWS_ENDPOINT_URL_STS", sts.URL)
}

// TestCredentialsFailClosed: requests go unsigned only with --anonymous.
// A source the environment configures (here web identity, as on a hosted
// runner) that is slow or mounted late signs; one that is refused or whose
// token service is down at start refuses the client (auth: a later outage
// is retried per request, TestCredentialOutageRecovers); an empty chain
// refuses it too, and none of them sends a listing request. --anonymous
// never signing, even with keys in the environment, is TestCredentialChain's.
func TestCredentialsFailClosed(t *testing.T) {
	setup := func(t *testing.T) (dir, token string) {
		dir = t.TempDir()
		isolate(t, dir)
		token = filepath.Join(dir, "token")
		t.Setenv("AWS_WEB_IDENTITY_TOKEN_FILE", token)
		t.Setenv("AWS_ROLE_ARN", "arn:aws:iam::111111111111:role/runner")
		return dir, token
	}
	var auth atomic.Value
	h := func(w http.ResponseWriter, r *http.Request) {
		auth.Store(r.Header.Get("Authorization"))
		w.Write([]byte(okPage))
	}
	signedWith := func(t *testing.T, c *Client, key string) {
		t.Helper()
		if _, err := c.ListObjectsV2(context.Background(), &ListInput{Bucket: "b"}); err != nil {
			t.Fatal(err)
		}
		if a := auth.Load().(string); !strings.Contains(a, "Credential="+key+"/") {
			t.Fatalf("authorization %q (source %q), want key %s", a, c.auth.source, key)
		}
	}
	t.Run("slow STS", func(t *testing.T) {
		_, token := setup(t)
		os.WriteFile(token, []byte("jwt"), 0o600)
		webIdentitySTS(t, 5*time.Second, 0, "")
		c, _ := testClient(t, h, Config{Region: "us-east-1"})
		signedWith(t, c, "AKWEB")
	})
	t.Run("late token", func(t *testing.T) {
		_, token := setup(t)
		webIdentitySTS(t, 0, 0, "")
		go func() { time.Sleep(time.Second); os.WriteFile(token, []byte("jwt"), 0o600) }()
		c, _ := testClient(t, h, Config{Region: "us-east-1"})
		signedWith(t, c, "AKWEB")
	})
	// New's context bounds the first lookup: a caller that gives up is not
	// held for the configured source's 30 s wait, and its cancellation is
	// not read as a verdict on the credentials.
	t.Run("caller gives up", func(t *testing.T) {
		_, token := setup(t)
		os.WriteFile(token, []byte("jwt"), 0o600)
		webIdentitySTS(t, 2*time.Second, 0, "")
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		start := time.Now()
		_, err := New(ctx, Config{Region: "us-east-1", Endpoint: "http://127.0.0.1:1", PathStyle: true})
		if !errors.Is(err, context.DeadlineExceeded) || time.Since(start) > time.Second {
			t.Fatalf("after %v: %v, want the caller's deadline", time.Since(start), err)
		}
	})
	t.Run("refused", func(t *testing.T) {
		_, token := setup(t)
		os.WriteFile(token, []byte("jwt"), 0o600)
		webIdentitySTS(t, 0, 400, "InvalidIdentityToken")
		_, err := New(context.Background(), Config{Region: "us-east-1", Endpoint: "http://127.0.0.1:1", PathStyle: true})
		var ce *objhttp.CredentialError
		if !errors.As(err, &ce) || Classify(err) != objhttp.ClassAuth || !strings.Contains(err.Error(), "web identity") {
			t.Fatalf("refused token: %v (class %s), want an auth failure naming web identity", err, Classify(err))
		}
	})
	// refused is the outcome of a client the first lookup could not
	// serve: an auth failure, and no listing request sent.
	refused := func(t *testing.T) {
		t.Helper()
		var sent atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { sent.Add(1); h(w, r) }))
		t.Cleanup(srv.Close)
		_, err := New(context.Background(), Config{Region: "us-east-1", Endpoint: srv.URL, PathStyle: true})
		var ce *objhttp.CredentialError
		if !errors.As(err, &ce) || Classify(err) != objhttp.ClassAuth || sent.Load() != 0 {
			t.Fatalf("%v (class %s), %d requests: want an auth failure and none sent", err, Classify(err), sent.Load())
		}
	}
	t.Run("STS down", func(t *testing.T) {
		_, token := setup(t)
		os.WriteFile(token, []byte("jwt"), 0o600)
		webIdentitySTS(t, 0, 503, "ServiceUnavailable")
		refused(t)
	})
	t.Run("empty chain", func(t *testing.T) {
		isolate(t, t.TempDir())
		refused(t)
	})
}

// TestPartialCredentialsRefuse (durability review):
// credential intent the SDK drops (one access key without the other, a
// session token without keys, a role without its web-identity token or
// the reverse, a profile that does not exist) is configured but broken:
// the client refuses with an auth failure and sends nothing. Before, the
// SDK kept no environment credentials, the chain read as empty and the
// run listed a public bucket unsigned.
func TestPartialCredentialsRefuse(t *testing.T) {
	for name, env := range map[string]map[string]string{
		"access key only":      {"AWS_ACCESS_KEY_ID": "AKONLY"},
		"secret key only":      {"AWS_SECRET_ACCESS_KEY": "s"},
		"session token only":   {"AWS_SESSION_TOKEN": "tok"},
		"token file, no role":  {"AWS_WEB_IDENTITY_TOKEN_FILE": "/nonexistent/token"},
		"role, no token file":  {"AWS_ROLE_ARN": "arn:aws:iam::111111111111:role/runner"},
		"missing profile":      {"AWS_PROFILE": "nosuch"},
		"access key, old name": {"AWS_ACCESS_KEY": "AKONLY"},
	} {
		t.Run(name, func(t *testing.T) {
			isolate(t, t.TempDir())
			t.Setenv("AWS_ACCESS_KEY", "")
			t.Setenv("AWS_SECRET_KEY", "")
			for k, v := range env {
				t.Setenv(k, v)
			}
			var requests atomic.Int64
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				w.Write([]byte(okPage)) // a public bucket
			}))
			t.Cleanup(srv.Close)
			c, err := New(context.Background(), Config{Region: "us-east-1", Endpoint: srv.URL, PathStyle: true})
			if err == nil {
				_, lerr := c.ListObjectsV2(context.Background(), &ListInput{Bucket: "b"})
				t.Fatalf("not refused: source %q, list %v, %d requests", c.auth.source, lerr, requests.Load())
			}
			if Classify(err) != objhttp.ClassAuth {
				t.Fatalf("%v: class %s, want auth", err, Classify(err))
			}
			if n := requests.Load(); n != 0 {
				t.Fatalf("%d requests sent", n)
			}
		})
	}
}

// appearingToken is a credentials provider whose first lookup creates the
// token file and still fails with the file missing, as the SDK's web
// identity provider does when the file appears while it reads: the lookup
// before the fix returned that failure as final.
type appearingToken struct {
	token string
	calls int
}

func (p *appearingToken) Retrieve(context.Context) (aws.Credentials, error) {
	p.calls++
	if p.calls == 1 {
		os.WriteFile(p.token, []byte("jwt"), 0o600)
		return aws.Credentials{}, fmt.Errorf("unable to read file at %s: %w", p.token, fs.ErrNotExist)
	}
	return aws.Credentials{AccessKeyID: "AKWEB", SecretAccessKey: "s", Source: "web identity"}, nil
}

// TestRetrieveWaitsForTokenAppearingDuringLookup: a token file that appears
// during the lookup that read it as missing is looked up again, not
// reported missing.
func TestRetrieveWaitsForTokenAppearingDuringLookup(t *testing.T) {
	p := &appearingToken{token: filepath.Join(t.TempDir(), "token")}
	c, err := retrieve(context.Background(), p, p.token)
	if err != nil || c.AccessKeyID != "AKWEB" || p.calls != 2 {
		t.Fatalf("credentials %+v, err %v, calls %d; want AKWEB after 2 calls", c, err, p.calls)
	}
}
