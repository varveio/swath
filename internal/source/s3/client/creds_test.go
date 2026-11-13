// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/varveio/swath/go/internal/source"
	"github.com/varveio/swath/go/internal/source/objhttp"
)

// TestCredentialSources keeps the credential-service region independent of
// the listing region, including the default when neither file nor environment
// supplies a credential region.
func TestCredentialSources(t *testing.T) {
	for _, region := range []string{"eu-west-2", ""} {
		t.Run("credential region="+region, func(t *testing.T) {
			dir := t.TempDir()
			isolate(t, dir)
			os.WriteFile(filepath.Join(dir, "credentials"), []byte("[base]\naws_access_key_id = AKBASE\naws_secret_access_key = s\n"), 0o600)
			os.WriteFile(filepath.Join(dir, "config"), []byte("[profile role]\nrole_arn = arn:aws:iam::222222222222:role/target\nsource_profile = base\nregion = "+region+"\n"), 0o600)
			var stsRegion atomic.Value
			sts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				parts := strings.Split(r.Header.Get("Authorization"), "/")
				if len(parts) < 3 {
					t.Error("unsigned STS request")
					return
				}
				stsRegion.Store(parts[2])
				exp := time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
				fmt.Fprintf(w, `<AssumeRoleResponse><AssumeRoleResult><Credentials><AccessKeyId>AKROLE</AccessKeyId><SecretAccessKey>s</SecretAccessKey><SessionToken>tok</SessionToken><Expiration>%s</Expiration></Credentials></AssumeRoleResult></AssumeRoleResponse>`, exp)
			}))
			defer sts.Close()
			t.Setenv("AWS_ENDPOINT_URL_STS", sts.URL)
			var auth atomic.Value
			c, _ := testClient(t, func(w http.ResponseWriter, r *http.Request) {
				auth.Store(r.Header.Get("Authorization"))
				io.WriteString(w, okPage)
			}, Config{Profile: "role", Region: "ap-southeast-2"})
			if _, err := c.ListObjectsV2(context.Background(), &ListInput{Bucket: "b"}); err != nil {
				t.Fatal(err)
			}
			if a := auth.Load().(string); !strings.Contains(a, "Credential=AKROLE/") || !strings.Contains(a, "/ap-southeast-2/s3/") {
				t.Fatalf("listing: %q", a)
			}
			want := region
			if want == "" {
				want = "us-east-1"
			}
			if got := stsRegion.Load(); got != want {
				t.Fatalf("STS region %v, want %s", got, want)
			}
		})
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
		t.Fatalf("after the outage: %v (class %s, %d STS calls)", err, objhttp.Classify(err), n.Load())
	}
	if c.Counters().Retries == 0 || n.Load() < 8 {
		t.Fatalf("retries %d, STS calls %d: the outage was not met", c.Counters().Retries, n.Load())
	}
	refuse.Store(true)
	time.Sleep(2500 * time.Millisecond)
	_, err := c.ListObjectsV2(context.Background(), &ListInput{Bucket: "b"})
	var ex *objhttp.ExhaustedError
	if objhttp.Classify(err) != objhttp.ClassAuth || errors.As(err, &ex) {
		t.Fatalf("refused token: %v (class %s), want auth, not retried", err, objhttp.Classify(err))
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
		if !errors.As(err, &ce) || objhttp.Classify(err) != objhttp.ClassAuth || !strings.Contains(err.Error(), "web identity") {
			t.Fatalf("refused token: %v (class %s), want an auth failure naming web identity", err, objhttp.Classify(err))
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
		if !errors.As(err, &ce) || objhttp.Classify(err) != objhttp.ClassAuth || sent.Load() != 0 {
			t.Fatalf("%v (class %s), %d requests: want an auth failure and none sent", err, objhttp.Classify(err), sent.Load())
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
			if objhttp.Classify(err) != objhttp.ClassAuth {
				t.Fatalf("%v: class %s, want auth", err, objhttp.Classify(err))
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

// TestCredentialChainTrust: the listing's trust is the credential
// chain's too. Web identity against an STS with a self-signed certificate
// fails without it, and succeeds with --no-verify-ssl or with the
// certificate in --ca-bundle.
func TestCredentialChainTrust(t *testing.T) {
	dir := t.TempDir()
	isolate(t, dir)
	tokenFile := filepath.Join(dir, "token")
	os.WriteFile(tokenFile, []byte("jwt"), 0o600)
	os.WriteFile(filepath.Join(dir, "config"), []byte("[profile web]\nrole_arn = arn:aws:iam::111111111111:role/web\nweb_identity_token_file = "+tokenFile+"\n"), 0o600)
	sts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		exp := time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
		fmt.Fprintf(w, `<AssumeRoleWithWebIdentityResponse><AssumeRoleWithWebIdentityResult><Credentials>
<AccessKeyId>AKWEB</AccessKeyId><SecretAccessKey>s</SecretAccessKey><SessionToken>t</SessionToken><Expiration>%s</Expiration>
</Credentials></AssumeRoleWithWebIdentityResult></AssumeRoleWithWebIdentityResponse>`, exp)
	}))
	t.Cleanup(sts.Close)
	t.Setenv("AWS_ENDPOINT_URL_STS", sts.URL)
	t.Setenv("AWS_MAX_ATTEMPTS", "1")
	bundle := filepath.Join(dir, "ca.pem")
	os.WriteFile(bundle, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: sts.Certificate().Raw}), 0o600)
	for _, c := range []struct {
		name  string
		trust source.Trust
		ok    bool
	}{
		{"system roots", source.Trust{}, false},
		{"no-verify-ssl", source.Trust{NoVerify: true}, true},
		{"ca-bundle", source.Trust{CABundle: bundle}, true},
	} {
		a, err := loadAuth(context.Background(), Config{Profile: "web", Region: "us-east-1", Trust: c.trust})
		if c.ok != (err == nil) || c.ok && a.credentials == nil {
			t.Errorf("%s: %v", c.name, err)
		}
	}
}

// TestCredentialDiscovered: a credential failure is Discovered (the
// command's --anonymous advice) only where the default chain searched on
// its own: an empty chain, and a refresh of what it found. A profile (the
// flag or AWS_PROFILE), keys in the environment, a bearer token command,
// a configuration that does not load (an AWS_RETRY_MODE that does not
// parse) and a refresh of a profile's credentials are configured.
func TestCredentialDiscovered(t *testing.T) {
	discovered := func(t *testing.T, err error) bool {
		t.Helper()
		var ce *objhttp.CredentialError
		if !errors.As(err, &ce) {
			t.Fatalf("%v: not a credential failure", err)
		}
		return ce.Discovered
	}
	for _, c := range []struct {
		name string
		env  map[string]string
		cfg  Config
		want bool
	}{
		{"empty chain", nil, Config{}, true},
		{"AWS_PROFILE", map[string]string{"AWS_PROFILE": "nosuch"}, Config{}, false},
		{"--s3-profile", nil, Config{Profile: "nosuch"}, false},
		{"keys, bad retry mode", map[string]string{"AWS_ACCESS_KEY_ID": "AK", "AWS_SECRET_ACCESS_KEY": "s", "AWS_RETRY_MODE": "bogus"}, Config{}, false},
		{"bearer, bad retry mode", map[string]string{"AWS_RETRY_MODE": "bogus"}, Config{Bearer: NewBearerToken("echo tok", 0)}, false},
		{"bad retry mode alone", map[string]string{"AWS_RETRY_MODE": "bogus"}, Config{}, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			isolate(t, t.TempDir())
			for _, k := range []string{"AWS_DEFAULT_PROFILE", "AWS_ACCESS_KEY", "AWS_SECRET_KEY"} {
				t.Setenv(k, "")
			}
			for k, v := range c.env {
				t.Setenv(k, v)
			}
			c.cfg.Region = "us-east-1"
			if _, err := loadAuth(context.Background(), c.cfg); discovered(t, err) != c.want {
				t.Errorf("%v: discovered %v, want %v", err, !c.want, c.want)
			}
		})
	}
	// A refresh keeps the selected path's bit: the environment's web
	// identity is found by the chain, a profile's keys are configured.
	refusal := aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
		return aws.Credentials{}, errors.New("refused")
	})
	dir := t.TempDir()
	isolate(t, dir)
	token := filepath.Join(dir, "token")
	os.WriteFile(token, []byte("jwt"), 0o600)
	os.WriteFile(filepath.Join(dir, "credentials"), []byte("[work]\naws_access_key_id = AKWORK\naws_secret_access_key = s\n"), 0o600)
	webIdentitySTS(t, 0, 0, "")
	t.Setenv("AWS_WEB_IDENTITY_TOKEN_FILE", token)
	t.Setenv("AWS_ROLE_ARN", "arn:aws:iam::111111111111:role/runner")
	for _, c := range []struct {
		profile string
		want    bool
	}{{"", true}, {"work", false}} {
		a, err := loadAuth(context.Background(), Config{Region: "us-east-1", Profile: c.profile})
		if err != nil {
			t.Fatalf("profile %q: %v", c.profile, err)
		}
		a.credentials = refusal
		if _, _, err := a.ready(context.Background()); discovered(t, err) != c.want {
			t.Errorf("profile %q: a refused refresh is discovered %v, want %v", c.profile, !c.want, c.want)
		}
	}
}
