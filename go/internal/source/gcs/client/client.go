// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package client lists Cloud Storage buckets through Google's generated
// JSON API client (google.golang.org/api/storage/v1): objects.list only,
// one Objects.List call per attempt, with OAuth2 credentials from
// golang.org/x/oauth2/google (or none), counted retries with full-jitter
// backoff (objhttp.Do; the generated Do sends one request and never
// retries), and typed errors.
//
// Every response passes an objhttp.Guard first (16 MiB success bodies,
// 64 KiB error bodies, the original body closed); a 200 must decode to an
// objects envelope (kind storage#objects) without an error member, so an
// empty, null or error body is never read as an empty final page.
// Redirects are never followed.
//
// Credentials live as long as the client: the token source is built on a
// context that is never cancelled, whose HTTP client bounds each token
// exchange by the request timeout, and every attempt waits for its token
// under its own attempt deadline. Requests go unauthenticated only with
// Anonymous; Application Default Credentials that cannot be found or
// loaded refuse the client.
package client

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"cloud.google.com/go/compute/metadata"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
	storage "google.golang.org/api/storage/v1"

	"github.com/varveio/swath/go/internal/source/objhttp"
	"github.com/varveio/swath/go/internal/source/tokensource"
)

// DefaultEndpoint is the JSON API base.
const DefaultEndpoint = "https://storage.googleapis.com/storage/v1/"

// readScope is the OAuth2 scope a listing needs.
const readScope = "https://www.googleapis.com/auth/devstorage.read_only"

// fields asks only for what a listing reads (sent to DefaultEndpoint), and
// the envelope's kind, which a client can require of every 200;
// versionFields adds what a generations listing reads (timeDeleted).
const (
	fields        = "kind,nextPageToken,prefixes,items(name,size,updated,timeCreated,etag,generation,metageneration,storageClass,md5Hash,crc32c,contentType)"
	versionFields = "kind,nextPageToken,prefixes,items(name,size,updated,timeCreated,timeDeleted,etag,generation,metageneration,storageClass,md5Hash,crc32c,contentType)"
)

// Config configures a Client. Zero values take the defaults.
type Config struct {
	Endpoint string // JSON API base; default DefaultEndpoint
	// Anonymous sends no credentials, whatever the environment holds.
	Anonymous bool
	// CredentialsFile is a service-account or authorized-user JSON file;
	// else Application Default Credentials (GOOGLE_APPLICATION_CREDENTIALS,
	// gcloud's file, the metadata server).
	CredentialsFile string
	UserProject     string        // billed project for a requester-pays bucket
	RequestTimeout  time.Duration // per attempt; default 30 s
	MaxAttempts     int           // default 9
	BaseBackoff     time.Duration // default 100 ms
	MaxBackoff      time.Duration // default 5 s
	HTTPClient      *http.Client
	// Workers sizes the built transport pool (PoolSize); a supplied
	// HTTPClient is used alone.
	Workers int
	// Limiter, if set, admits every attempt (objhttp.Limiter).
	Limiter *objhttp.Limiter
	// Rate, if set, caps attempts per second (objhttp.Rate).
	Rate *objhttp.Rate
	// TokenSource overrides the credential lookup (tests).
	TokenSource oauth2.TokenSource
}

// ListInput is one objects.list request. StartOffset is inclusive and
// EndOffset exclusive, as the API defines them.
type ListInput struct {
	Bucket      string
	Prefix      string
	Delimiter   string
	StartOffset string
	EndOffset   string
	PageToken   string
	MaxResults  int // 0: the server default (1,000)
	// Versions lists every generation (versions=true): live and
	// noncurrent, by name ascending and, within a name, generation
	// ascending, as the API returns them.
	Versions bool
}

// Object is one listed object.
type Object struct {
	Name           string
	Size           int64
	Updated        int64 // Unix microseconds; 0 when absent
	TimeCreated    int64 // Unix microseconds; 0 when absent
	ETag           string
	Generation     int64
	Metageneration int64
	StorageClass   string
	MD5            string // base64, as sent
	CRC32C         string // base64, as sent
	ContentType    string
	// TimeDeleted is when the generation became noncurrent, Unix
	// microseconds; 0 for a live generation (and outside Versions).
	TimeDeleted int64
}

// Page is one objects.list response.
type Page struct {
	Items         []Object
	Prefixes      []string
	NextPageToken string
}

// Error is a non-200 answer.
type Error struct {
	Status  int
	Reason  string // the first error's reason (notFound, forbidden, rateLimitExceeded, ...)
	Message string
	Query   string // the request's query, on a 400
}

func (e *Error) Error() string {
	s := fmt.Sprintf("GCS %d", e.Status)
	if e.Reason != "" {
		s += " " + e.Reason
	}
	if e.Message != "" {
		s += ": " + e.Message
	}
	if e.Query != "" {
		s += " (request ?" + e.Query + ")"
	}
	return s
}

// Counters are what the client did.
type Counters = objhttp.Counts

// CredentialError is a credential failure (objhttp.CredentialError):
// configured credentials that do not load, or a token the token endpoint
// would not issue; transient ones are retried.
type CredentialError = objhttp.CredentialError

// ExhaustedError is a transient failure that outlived its attempts.
type ExhaustedError = objhttp.ExhaustedError

// ErrSyntax marks a 200 whose body did not parse (objhttp.ErrSyntax).
var ErrSyntax = objhttp.ErrSyntax

// Client lists Cloud Storage buckets; safe for concurrent use.
type Client struct {
	cfg      Config
	endpoint string             // the JSON API base, with a trailing slash
	http     []*http.Client     // the transport pool, requests round-robin (pick)
	svc      []*storage.Service // one per pooled transport, over its Guard
	next     atomic.Uint64
	ts       *tokensource.Shared // nil: anonymous
	source   string

	retry  objhttp.Retry
	counts objhttp.Counters
}

// New resolves credentials and builds the client. Requests go
// unauthenticated only with cfg.Anonymous (a public bucket); otherwise
// credentials that cannot be found or loaded (the client library's
// Application Default Credentials search) are a *CredentialError, and
// nothing is sent.
func New(ctx context.Context, cfg Config) (*Client, error) {
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = 30 * time.Second
	}
	ep, err := Endpoint(cfg.Endpoint)
	if err != nil {
		return nil, err
	}
	c := &Client{cfg: cfg, endpoint: ep, source: "anonymous",
		retry: objhttp.Retry{MaxAttempts: cfg.MaxAttempts, BaseBackoff: cfg.BaseBackoff, MaxBackoff: cfg.MaxBackoff, Limiter: cfg.Limiter, Rate: cfg.Rate}}
	c.retry.Defaults()
	// Redirects are answers, never followed, also through a supplied HTTP
	// client: the resume identity freezes the endpoint the requests go to
	// (as S3's client does).
	noRedirect := func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	if cfg.HTTPClient == nil {
		// One transport per connWorkers workers: Go's HTTP/2 transport opens a
		// second connection only at the server's stream limit (~100), so one
		// transport would carry every worker on one connection. The pool's
		// transports share one TLS session cache, so later connections resume.
		cache := tls.NewLRUClientSessionCache(0)
		for range PoolSize(cfg.Workers) {
			t := http.DefaultTransport.(*http.Transport).Clone()
			t.MaxIdleConns, t.MaxIdleConnsPerHost = 0, 1024
			t.TLSClientConfig = &tls.Config{ClientSessionCache: cache}
			c.http = append(c.http, &http.Client{Transport: t, CheckRedirect: noRedirect})
		}
	} else {
		hc := *cfg.HTTPClient
		hc.CheckRedirect = noRedirect
		c.http = []*http.Client{&hc}
	}
	// The token source keeps this context for every refresh: it must
	// outlive New's caller's deadline, and its HTTP client bounds each token
	// exchange (the attempt deadline bounds the wait, token below).
	life := context.WithValue(context.WithoutCancel(ctx), oauth2.HTTPClient,
		&http.Client{Transport: c.http[0].Transport, Timeout: cfg.RequestTimeout})
	switch {
	case cfg.TokenSource != nil:
		c.ts, c.source = tokensource.NewShared(cfg.TokenSource, 0), "token source"
	case cfg.Anonymous:
	case cfg.CredentialsFile != "":
		c.source = "file " + cfg.CredentialsFile
		b, err := os.ReadFile(cfg.CredentialsFile)
		if err != nil {
			return nil, &CredentialError{Source: c.source, Err: err}
		}
		creds, err := google.CredentialsFromJSON(life, b, readScope)
		if err != nil {
			return nil, &CredentialError{Source: c.source, Err: err}
		}
		c.ts = tokensource.NewShared(creds.TokenSource, 0)
	default:
		c.source = "application default credentials"
		creds, err := findCredentials(life, readScope)
		if err != nil {
			return nil, &CredentialError{Source: c.source, Err: err}
		}
		c.ts = tokensource.NewShared(creds.TokenSource, 0)
	}
	for _, hc := range c.http {
		g := *hc // keeps the pool's no-redirect policy
		g.Transport = record{&objhttp.Guard{Next: hc.Transport, Validate: validate}}
		svc, err := storage.NewService(ctx, option.WithHTTPClient(&g), option.WithEndpoint(c.endpoint), option.WithoutAuthentication(),
			option.WithLogger(quiet))
		if err != nil {
			return nil, err
		}
		c.svc = append(c.svc, svc)
	}
	return c, nil
}

// quiet is the generated client's logger: it emits nothing. The default
// one, switched on by GOOGLE_SDK_GO_LOGGING_LEVEL, writes every request's
// headers to stderr unredacted, the Authorization header this client sets
// included, and every response body.
var quiet = slog.New(slog.DiscardHandler)

// Endpoint is the JSON API base a client built with ep sends to, and the
// form the resume identity records: DefaultEndpoint for "", an http(s) URL
// with a host, its path's escapes kept as given and a literal trailing
// slash added when the escaped path has none (so "tenant%2F" is the base
// "tenant%2F/", not a base whose last segment the generated client's
// relative resolution would drop). One string is one destination: the
// client sends to exactly this base. A path holding a URI-template brace,
// literal or escaped (%7B, %7D), is refused: the generated client expands
// the request path as a template, which panics on an empty expression
// and drops a named one.
//
// The generated client resolves the request path against the base (dot
// segments removed: "/a/../storage/v1/" sends to "/storage/v1/...") and
// then re-escapes the decoded path, so an escaped separator is sent as a
// separator ("/proxy%2Ftenant/storage/v1/" sends to
// "/proxy/tenant/storage/v1/..."). Two such spellings record different
// identities for one destination, which refuses a resume that would have
// been safe; never the converse.
func Endpoint(ep string) (string, error) {
	if ep == "" {
		ep = DefaultEndpoint
	}
	u, err := url.Parse(ep)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return "", fmt.Errorf("endpoint %q: want http(s)://host[:port]/storage/v1/", ep)
	}
	if strings.ContainsAny(u.Path, "{}") {
		return "", fmt.Errorf("endpoint %q: a path with { or } (escaped or not) is a URI template to the generated client", ep)
	}
	if p := u.EscapedPath(); !strings.HasSuffix(p, "/") {
		u.Path, u.RawPath = u.Path+"/", p+"/"
	}
	return u.String(), nil
}

// connWorkers is the workers per pooled transport (one HTTP/2 connection
// each, below the server's stream limit); maxConns caps the pool.
const connWorkers, maxConns = 16, 8

// PoolSize is the number of transports a built client spreads its requests
// over for workers: one per connWorkers, at least 1, at most maxConns.
func PoolSize(workers int) int {
	return min(max((workers+connWorkers-1)/connWorkers, 1), maxConns)
}

// Transports is the size of the client's transport pool (1 with a supplied
// HTTPClient): the report's transport.pool_size.
func (c *Client) Transports() int { return len(c.http) }

// pick is the index of the transport (and service) the next request
// takes, round-robin.
func (c *Client) pick() int {
	return int((c.next.Add(1) - 1) % uint64(len(c.http)))
}

// findCredentials is the client library's ADC search (tests replace it: on
// a Google Cloud host the real one finds the metadata server, so what the
// client does when the search fails could not be tested there).
var findCredentials = google.FindDefaultCredentials

// token waits for the token source under the attempt's deadline (ctx);
// nil without credentials. A failure is a *CredentialError. Waiters
// share one outstanding call (tokensource.Shared: at most one goroutine
// calls the source at a time, however many attempts wait or give up); a
// call that outlives the waits finishes in the background (its own HTTP
// client is bounded by the request timeout) and later attempts reuse its
// token.
func (c *Client) token(ctx context.Context) (*oauth2.Token, error) {
	if c.ts == nil {
		return nil, nil
	}
	t, err := c.ts.Token(ctx)
	switch {
	case err == nil:
		return t, nil
	case ctx.Err() != nil:
		return nil, &CredentialError{Source: c.source, Transient: true, Timeout: true,
			Err: fmt.Errorf("no token within %s: %v", c.cfg.RequestTimeout, ctx.Err())}
	}
	return nil, &CredentialError{Source: c.source, Transient: transientToken(err),
		Timeout: errors.Is(err, context.DeadlineExceeded), Err: err}
}

// transientToken: the token endpoint's or the metadata server's 5xx, 408
// and 429, transport failures and a token exchange's own timeout may pass;
// a refused grant (400, 401) and unusable credentials will not.
func transientToken(err error) bool {
	status := func(s int) bool { return s >= 500 || s == 408 || s == 429 }
	var re *oauth2.RetrieveError
	var me *metadata.Error
	switch {
	case errors.As(err, &re):
		return re.Response != nil && status(re.Response.StatusCode)
	case errors.As(err, &me):
		return status(me.Code)
	}
	return objhttp.Transport(err) || errors.Is(err, context.DeadlineExceeded)
}

// Credentials names where the credentials came from (never the secret).
func (c *Client) Credentials() (source string, anonymous bool) { return c.source, c.ts == nil }

// Answered is the number of requests the service answered usably
// (objhttp.Counters.Answered).
func (c *Client) Answered() int64 { return c.counts.Answered() }

// Live is the request account now with the limiter's current limit, for
// the progress sampler (objhttp.Live).
func (c *Client) Live() objhttp.Live {
	l := c.counts.Live()
	l.Limit = c.retry.Limiter.Limit()
	return l
}

// Counters returns the request counters so far.
func (c *Client) Counters() Counters {
	n := c.counts.Load()
	n.Limiter = c.retry.Limiter.Counts()
	n.Rate = c.retry.Rate.Counts()
	return n
}
