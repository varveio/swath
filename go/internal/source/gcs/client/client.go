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
// context that is never cancelled. Every attempt bounds its wait for a
// token by its own deadline. Token exchanges using the supplied HTTP client
// have the request timeout; metadata refresh uses the metadata SDK's own
// HTTP timeout and retries, without a total refresh deadline. Requests go
// unauthenticated only with Anonymous; Application Default Credentials that
// cannot be found or loaded refuse the client.
package client

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"cloud.google.com/go/compute/metadata"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
	storage "google.golang.org/api/storage/v1"

	"github.com/varveio/swath/go/internal/source"
	"github.com/varveio/swath/go/internal/source/objhttp"
	"github.com/varveio/swath/go/internal/source/tokensource"
)

// DefaultEndpoint is the JSON API base.
const DefaultEndpoint = "https://storage.googleapis.com/storage/v1/"

// readScope is the OAuth2 scope a listing needs.
const readScope = "https://www.googleapis.com/auth/devstorage.read_only"

// fields asks only for what a listing reads, plus the envelope's kind,
// which a client can require of every 200;
// versionFields adds what a generations listing reads (timeDeleted).
const (
	fields        = "kind,nextPageToken,prefixes,items(name,size,updated,timeCreated,etag,generation,metageneration,storageClass,md5Hash,crc32c,contentType)"
	versionFields = "kind,nextPageToken,prefixes,items(name,size,updated,timeCreated,timeDeleted,etag,generation,metageneration,storageClass,md5Hash,crc32c,contentType)"
)

// Config configures a Client. Zero values take the defaults.
type Config struct {
	objhttp.Retry
	Endpoint string // JSON API base; default DefaultEndpoint
	// Anonymous sends no credentials, whatever the environment holds.
	Anonymous bool
	// CredentialsFile is a Google credential JSON file loaded by the
	// pinned OAuth library; else Application Default Credentials (GOOGLE_APPLICATION_CREDENTIALS,
	// gcloud's file, the metadata server).
	CredentialsFile string
	UserProject     string        // billed project for a requester-pays bucket
	RequestTimeout  time.Duration // per attempt; default 30 s
	Trust           source.Trust  // verifies the service's certificate; a supplied HTTPClient keeps its own
	HTTPClient      *http.Client
	// Workers sizes the built transport pool (objhttp.Pool); a supplied
	// HTTPClient is used alone.
	Workers int
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

// Client lists Cloud Storage buckets; safe for concurrent use.
type Client struct {
	cfg      Config
	endpoint string             // the JSON API base, with a trailing slash
	svc      []*storage.Service // one per pooled transport, over its Guard; requests round-robin (pick)
	next     atomic.Uint64
	ts       *tokensource.Shared // nil: anonymous
	source   string
	// discovered: the credentials are Application Default Credentials'
	// (objhttp.CredentialError.Discovered), not a configured file.
	discovered bool

	*objhttp.Meter
}

// New resolves credentials and builds the client. Requests go
// unauthenticated only with cfg.Anonymous (a public bucket); otherwise
// credentials that cannot be found or loaded (the client library's
// Application Default Credentials search) are a *objhttp.CredentialError, and
// nothing is sent.
func New(ctx context.Context, cfg Config) (*Client, error) {
	trust, err := cfg.Trust.Config()
	if err != nil {
		return nil, err
	}
	return newClient(ctx, cfg, google.FindDefaultCredentials, objhttp.Pool(cfg.Workers, cfg.HTTPClient, trust, nil))
}

// credentialFinder finds the credentials of a client given none: the
// client library's Application Default Credentials search, or in
// tests a fixed answer, so what the client does with each outcome does not
// depend on the host (on Google Cloud the real search finds the metadata
// server).
type credentialFinder func(ctx context.Context, scopes ...string) (*google.Credentials, error)

// newClient is New with its credential search and its transport pool
// given.
func newClient(ctx context.Context, cfg Config, finder credentialFinder, pool []*http.Client) (*Client, error) {
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = source.DefaultRequestTimeout
	}
	ep, err := Endpoint(cfg.Endpoint)
	if err != nil {
		return nil, err
	}
	c := &Client{cfg: cfg, endpoint: ep, source: "anonymous",
		Meter: objhttp.NewMeter(cfg.Retry)}
	// The token source keeps this context for every refresh: it must
	// outlive New's caller's deadline. The attempt deadline bounds the wait
	// for a token. This HTTP client bounds exchanges that use it; metadata
	// exchanges use the metadata SDK's background context, HTTP timeout
	// and retry policy.
	life := context.WithValue(context.WithoutCancel(ctx), oauth2.HTTPClient,
		&http.Client{Transport: pool[0].Transport, Timeout: cfg.RequestTimeout})
	switch {
	case cfg.Anonymous:
	case cfg.CredentialsFile != "":
		c.source = "file " + cfg.CredentialsFile
		b, err := os.ReadFile(cfg.CredentialsFile)
		if err != nil {
			return nil, &objhttp.CredentialError{Source: c.source, Err: err}
		}
		creds, err := google.CredentialsFromJSON(life, b, readScope)
		if err != nil {
			return nil, &objhttp.CredentialError{Source: c.source, Err: err}
		}
		c.ts = tokensource.NewShared(creds.TokenSource)
	default:
		c.source, c.discovered = "application default credentials", true
		creds, err := finder(life, readScope)
		if err != nil {
			return nil, &objhttp.CredentialError{Source: c.source, Discovered: true, Err: err}
		}
		c.ts = tokensource.NewShared(creds.TokenSource)
	}
	for _, hc := range pool {
		g := *hc // keeps the pool's no-redirect policy
		g.Transport = &objhttp.Guard{Next: hc.Transport, Validate: validate}
		svc, err := storage.NewService(ctx, option.WithHTTPClient(&g), option.WithEndpoint(c.endpoint),
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
// with a host and no user information (a password would be recorded), its
// path's escapes kept as given and a literal trailing slash added when the
// escaped path has none (so "tenant%2F" is the base "tenant%2F/", not a base
// whose last segment the generated client's relative resolution would
// drop). A path holding a URI-template brace, literal or escaped (%7B, %7D),
// is refused: the generated client expands the request path as a template,
// which panics on an empty expression and drops a named one.
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
	u, err := objhttp.ParseEndpoint(ep, "http(s)://host[:port]/storage/v1/")
	if err != nil {
		return "", err
	}
	if strings.ContainsAny(u.Path, "{}") {
		return "", source.Unsupported(fmt.Errorf("endpoint %q: a path with { or } (escaped or not) is a URI template to the generated client", ep))
	}
	if p := u.EscapedPath(); !strings.HasSuffix(p, "/") {
		u.Path, u.RawPath = u.Path+"/", p+"/"
	}
	return u.String(), nil
}

// Transports is the size of the client's transport pool (1 with a supplied
// HTTPClient): the report's transport.pool_size.
func (c *Client) Transports() int { return len(c.svc) }

// pick is the index of the transport (and service) the next request
// takes, round-robin.
func (c *Client) pick() int {
	return int((c.next.Add(1) - 1) % uint64(len(c.svc)))
}

// transientToken: the token endpoint's or the metadata server's 5xx, 408
// and 429, transport failures and a token exchange's own timeout may pass;
// a refused grant (400, 401) and unusable credentials will not.
func transientToken(err error) bool {
	var re *oauth2.RetrieveError
	var me *metadata.Error
	switch {
	case errors.As(err, &re):
		return re.Response != nil && objhttp.TransientStatus(re.Response.StatusCode)
	case errors.As(err, &me):
		return objhttp.TransientStatus(me.Code)
	}
	return objhttp.Transport(err) || errors.Is(err, context.DeadlineExceeded)
}

// Credentials names where the credentials came from (never the secret).
func (c *Client) Credentials() (source string, anonymous bool) { return c.source, c.ts == nil }
