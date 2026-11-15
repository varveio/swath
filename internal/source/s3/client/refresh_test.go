// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/varveio/swath/internal/source/objhttp"
	"github.com/varveio/swath/internal/source/s3/s3test"
)

// expiring is a credentials provider whose every lookup issues new
// credentials (AK1, AK2, ...) valid for life; expires records when each
// key expires, for a fake S3 to refuse it after.
type expiring struct {
	life    time.Duration
	n       atomic.Int64
	expires sync.Map // access key ID -> time.Time
}

func (p *expiring) Retrieve(context.Context) (aws.Credentials, error) {
	n := p.n.Add(1)
	c := aws.Credentials{AccessKeyID: fmt.Sprintf("AK%d", n), SecretAccessKey: "s", SessionToken: fmt.Sprintf("t%d", n),
		CanExpire: true, Expires: time.Now().Add(p.life), Source: "test"}
	p.expires.Store(c.AccessKeyID, c.Expires)
	return c, nil
}

// signedKey is the access key ID a request was signed with.
func signedKey(r *http.Request) string {
	_, s, _ := strings.Cut(r.Header.Get("Authorization"), "Credential=")
	k, _, _ := strings.Cut(s, "/")
	return k
}

const expiredAnswer = `<Error><Code>ExpiredToken</Code><Message>The provided token has expired.</Message></Error>`

// refreshingClient is a client signing with p's credentials through the
// SDK's cache with window, as loadAuth builds it.
func refreshingClient(t *testing.T, h http.HandlerFunc, p aws.CredentialsProvider, window time.Duration) *Client {
	t.Helper()
	s3test.NoCredentials(t)
	c := newTest(t, testConfig(s3test.Serve(t, h)))
	c.auth.credentials = aws.NewCredentialsCache(p, func(o *aws.CredentialsCacheOptions) { o.ExpiryWindow = window })
	c.auth.anonymous, c.auth.source = false, "test"
	return c
}

// TestCredentialsRefreshAcrossExpiries: workers list through several
// expiries of short-lived credentials against an S3 that refuses a key
// once it has expired, a request taking transit to arrive. With a window
// over transit, the credentials are refreshed before any request could
// arrive late, once per expiry however many workers wait on it, and no
// answer is an expired token. Without one, requests signed just before an
// expiry arrive after it: each such answer renews the credentials and its
// request is retried, so the run still completes, and the answers to
// requests signed together force one refresh.
func TestCredentialsRefreshAcrossExpiries(t *testing.T) {
	const (
		life, transit, run, workers = 1 * time.Second, 50 * time.Millisecond, 3 * time.Second, 8
	)
	for _, tc := range []struct {
		name   string
		window time.Duration
	}{{"window", 500 * time.Millisecond}, {"no window", 0}} {
		t.Run(tc.name, func(t *testing.T) {
			p := &expiring{life: life}
			var expired atomic.Int64
			c := refreshingClient(t, func(w http.ResponseWriter, r *http.Request) {
				e, ok := p.expires.Load(signedKey(r))
				if !ok || time.Now().Add(transit).After(e.(time.Time)) {
					expired.Add(1)
					w.WriteHeader(400)
					io.WriteString(w, expiredAnswer)
					return
				}
				io.WriteString(w, okPage)
			}, p, tc.window)
			ctx, cancel := context.WithTimeout(context.Background(), run)
			defer cancel()
			var wg sync.WaitGroup
			errs := make(chan error, workers)
			for range workers {
				wg.Go(func() {
					for ctx.Err() == nil {
						if _, err := c.ListObjectsV2(context.Background(), &ListInput{Bucket: "b"}); err != nil {
							errs <- err
							return
						}
					}
				})
			}
			wg.Wait()
			close(errs)
			for err := range errs {
				t.Fatalf("%v (class %s)", err, objhttp.Classify(err))
			}
			refreshes, forced := c.CredentialRefreshes()
			lookups := p.n.Load()
			// One lookup per lifetime the run spans (the first included), at
			// most one more than the run's length allows: never one per worker.
			most := int64(run/(life-tc.window)) + 2
			if lookups < 3 || lookups > most || refreshes != lookups-1 {
				t.Fatalf("%d lookups (want 3 to %d), %d refreshes", lookups, most, refreshes)
			}
			switch {
			case tc.window > 0 && (forced != 0 || expired.Load() != 0):
				t.Fatalf("with a window: %d forced refreshes, %d expired answers", forced, expired.Load())
			case tc.window == 0 && (forced == 0 || forced > lookups || expired.Load() == 0):
				t.Fatalf("without a window: %d forced refreshes for %d lookups, %d expired answers", forced, lookups, expired.Load())
			}
		})
	}
}

// TestExpiredAnswerRenewsOnce: S3 refuses the first credentials as expired
// (revoked, or by a clock ahead of this host's) though they are still valid
// here. Requests answered so together force one refresh, and each is
// retried once with the new credentials.
func TestExpiredAnswerRenewsOnce(t *testing.T) {
	const workers = 8
	p := &expiring{life: time.Hour}
	var first sync.WaitGroup
	first.Add(workers)
	c := refreshingClient(t, func(w http.ResponseWriter, r *http.Request) {
		if signedKey(r) == "AK1" {
			first.Done()
			first.Wait() // every worker has signed with AK1
			w.WriteHeader(400)
			io.WriteString(w, expiredAnswer)
			return
		}
		io.WriteString(w, okPage)
	}, p, expiryWindow)
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			if _, err := c.ListObjectsV2(context.Background(), &ListInput{Bucket: "b"}); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	refreshes, forced := c.CredentialRefreshes()
	n := c.Counters()
	if p.n.Load() != 2 || refreshes != 1 || forced != 1 || n.Attempts != 2*workers || n.Retries != workers {
		t.Fatalf("%d lookups, %d refreshes (%d forced), %+v", p.n.Load(), refreshes, forced, requests(n))
	}
}

// TestExpiredAnswerBounded: refreshable credentials still refused as
// expired after a refresh are an auth failure after one retry, not a loop;
// keys that do not refresh are one at once, saying so.
func TestExpiredAnswerBounded(t *testing.T) {
	refuse := func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(400)
		io.WriteString(w, expiredAnswer)
	}
	t.Run("refreshable", func(t *testing.T) {
		p := &expiring{life: time.Hour}
		c := refreshingClient(t, refuse, p, expiryWindow)
		_, err := c.ListObjectsV2(context.Background(), &ListInput{Bucket: "b"})
		_, forced := c.CredentialRefreshes()
		if objhttp.Classify(err) != objhttp.ClassAuth || !strings.Contains(fmt.Sprint(err), "refused again after the credentials from test were refreshed") ||
			c.Counters().Attempts != 2 || p.n.Load() != 2 || forced != 1 {
			t.Fatalf("%v (class %s): %d attempts, %d lookups, %d forced", err, objhttp.Classify(err), c.Counters().Attempts, p.n.Load(), forced)
		}
	})
	t.Run("static", func(t *testing.T) {
		dir := t.TempDir()
		isolate(t, dir)
		os.WriteFile(filepath.Join(dir, "credentials"), []byte("[default]\naws_access_key_id = AKSTATIC\naws_secret_access_key = s\naws_session_token = old\n"), 0o600)
		c, _ := testClient(t, refuse, Config{Region: "us-east-1"})
		_, err := c.ListObjectsV2(context.Background(), &ListInput{Bucket: "b"})
		var ex *objhttp.ExhaustedError
		refreshes, forced := c.CredentialRefreshes()
		if objhttp.Classify(err) != objhttp.ClassAuth || errors.As(err, &ex) || !strings.Contains(fmt.Sprint(err), "do not refresh: renew them") ||
			c.Counters().Attempts != 1 || refreshes != 0 || forced != 0 {
			t.Fatalf("%v (class %s): %d attempts, %d refreshes, %d forced", err, objhttp.Classify(err), c.Counters().Attempts, refreshes, forced)
		}
	})
}

// TestCredentialChainRefreshesAhead: credentials from the SDK's chain (web
// identity through STS) are refreshed expiryWindow before they expire: ones
// expiring in expiryWindow and a second are replaced after that second.
func TestCredentialChainRefreshesAhead(t *testing.T) {
	dir := t.TempDir()
	isolate(t, dir)
	tokenFile := filepath.Join(dir, "token")
	os.WriteFile(tokenFile, []byte("jwt"), 0o600)
	os.WriteFile(filepath.Join(dir, "config"), []byte("[profile web]\nrole_arn = arn:aws:iam::111111111111:role/web\nweb_identity_token_file = "+tokenFile+"\n"), 0o600)
	var n atomic.Int32
	sts := s3test.Serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		i := n.Add(1)
		exp := time.Now().Add(expiryWindow + time.Second).UTC().Format(time.RFC3339Nano)
		fmt.Fprintf(w, `<AssumeRoleWithWebIdentityResponse><AssumeRoleWithWebIdentityResult><Credentials>
<AccessKeyId>AK%d</AccessKeyId><SecretAccessKey>s</SecretAccessKey><SessionToken>t</SessionToken><Expiration>%s</Expiration>
</Credentials></AssumeRoleWithWebIdentityResult></AssumeRoleWithWebIdentityResponse>`, i, exp)
	}))
	t.Setenv("AWS_ENDPOINT_URL_STS", sts)
	var key atomic.Value
	c, _ := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		key.Store(signedKey(r))
		io.WriteString(w, okPage)
	}, Config{Profile: "web", Region: "us-east-1"})
	list := func() string {
		t.Helper()
		if _, err := c.ListObjectsV2(context.Background(), &ListInput{Bucket: "b"}); err != nil {
			t.Fatal(err)
		}
		return key.Load().(string)
	}
	if k := list(); k != "AK1" {
		t.Fatalf("first request signed with %s", k)
	}
	time.Sleep(1200 * time.Millisecond)
	if k := list(); k != "AK2" || n.Load() != 2 {
		t.Fatalf("after a second: signed with %s, %d STS calls", k, n.Load())
	}
	if refreshes, forced := c.CredentialRefreshes(); refreshes != 1 || forced != 0 {
		t.Fatalf("%d refreshes, %d forced", refreshes, forced)
	}
}
