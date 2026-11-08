// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package client is a small Cloud Storage JSON API client: objects.list
// only, with OAuth2 credentials from golang.org/x/oauth2/google (or none),
// counted retries with full-jitter backoff, and typed errors.
//
// Credentials live as long as the client: the token source is built on a
// context that is never cancelled, whose HTTP client bounds each token
// exchange by the request timeout, and every attempt waits for its token
// under its own attempt deadline. Absent Application Default Credentials
// mean anonymous access; configured but unusable ones refuse the client.
package client

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"cloud.google.com/go/compute/metadata"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"

	"github.com/varveio/swath/go/internal/buildinfo"
	"github.com/varveio/swath/go/internal/faults"
	"github.com/varveio/swath/go/internal/source/objhttp"
	"github.com/varveio/swath/go/internal/source/tokensource"
)

// DefaultEndpoint is the JSON API base.
const DefaultEndpoint = "https://storage.googleapis.com/storage/v1/"

// readScope is the OAuth2 scope a listing needs.
const readScope = "https://www.googleapis.com/auth/devstorage.read_only"

// fields asks only for what a listing reads (sent to DefaultEndpoint).
const fields = "nextPageToken,prefixes,items(name,size,updated,etag,generation,metageneration,storageClass,md5Hash,crc32c,contentType)"

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
}

// Object is one listed object.
type Object struct {
	Name           string
	Size           int64
	Updated        int64 // Unix microseconds; 0 when absent
	ETag           string
	Generation     int64
	Metageneration int64
	StorageClass   string
	MD5            string // base64, as sent
	CRC32C         string // base64, as sent
	ContentType    string
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

// Throttle reports a rate-limit answer.
func (e *Error) Throttle() bool {
	return e.Status == 429 || e.Reason == "rateLimitExceeded" || e.Reason == "userRateLimitExceeded"
}

// Retryable reports an answer worth repeating.
func (e *Error) Retryable() bool { return e.Throttle() || e.Status >= 500 || e.Status == 408 }

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
	cfg    Config
	base   *url.URL
	http   []*http.Client // the transport pool, requests round-robin (pick)
	next   atomic.Uint64
	ts     *tokensource.Shared // nil: anonymous
	source string

	retry  objhttp.Retry
	counts objhttp.Counters
}

// New resolves credentials and builds the client. Without Application
// Default Credentials (no GOOGLE_APPLICATION_CREDENTIALS, no gcloud file, not
// on Google Cloud), requests go unauthenticated, which public buckets allow.
// Credentials that are configured but do not load are a *CredentialError.
func New(ctx context.Context, cfg Config) (*Client, error) {
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = 30 * time.Second
	}
	ep := cfg.Endpoint
	if ep == "" {
		ep = DefaultEndpoint
	}
	u, err := url.Parse(ep)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return nil, fmt.Errorf("endpoint %q: want http(s)://host[:port]/storage/v1/", ep)
	}
	if !strings.HasSuffix(u.Path, "/") {
		u.Path += "/"
	}
	c := &Client{cfg: cfg, base: u, source: "anonymous",
		retry: objhttp.Retry{MaxAttempts: cfg.MaxAttempts, BaseBackoff: cfg.BaseBackoff, MaxBackoff: cfg.MaxBackoff, Limiter: cfg.Limiter, Rate: cfg.Rate, Answer: answer}}
	c.retry.Defaults()
	// Redirects are answers, never followed, also through a supplied HTTP
	// client: the resume identity freezes the route the requests go to (as
	// S3's client does).
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
		creds, err := findCredentials(life, readScope)
		switch {
		case err == nil:
			c.ts, c.source = tokensource.NewShared(creds.TokenSource, 0), "application default credentials"
		case adcConfigured():
			return nil, &CredentialError{Source: "application default credentials", Err: err}
		case onGCEMachine():
			// A Google Cloud machine whose metadata server did not answer the
			// client library's detection in time: its service account is the
			// configured source, never anonymity; token failures are retried
			// per attempt like any refresh.
			c.ts = tokensource.NewShared(google.ComputeTokenSource("", readScope), 0)
			c.source = "compute metadata (detection failed: " + err.Error() + ")"
		default:
			c.source = "anonymous (no application default credentials)"
		}
	}
	return c, nil
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

// pick is the next transport, round-robin.
func (c *Client) pick() *http.Client {
	return c.http[(c.next.Add(1)-1)%uint64(len(c.http))]
}

// adcConfigured reports whether Application Default Credentials are
// configured in a place golang.org/x/oauth2/google reads: then a failure
// to load them is an error, not anonymity. Only a well-known file that
// does not exist means "not configured": one that cannot be examined (an
// unreadable parent directory, an I/O error) fails closed, since it may
// hold the credentials the operator meant.
func adcConfigured() bool {
	if os.Getenv("GOOGLE_APPLICATION_CREDENTIALS") != "" {
		return true
	}
	dir := os.Getenv("APPDATA")
	if runtime.GOOS != "windows" {
		home := os.Getenv("HOME")
		if home == "" {
			home, _ = os.UserHomeDir()
		}
		dir = filepath.Join(home, ".config")
	}
	_, err := os.Stat(filepath.Join(dir, "gcloud", "application_default_credentials.json"))
	return !errors.Is(err, fs.ErrNotExist)
}

// findCredentials is the client library's ADC search (tests replace it: on
// a Google Cloud host the real one finds the metadata server, so what the
// client does when the search fails could not be tested there).
var findCredentials = google.FindDefaultCredentials

// gceProductFile names the machine's product (tests move it).
var gceProductFile = "/sys/class/dmi/id/product_name"

// onGCEMachine reports a Google Cloud machine by its DMI product name (the
// sign the metadata library itself trusts) or a configured metadata host.
func onGCEMachine() bool {
	if os.Getenv("GCE_METADATA_HOST") != "" {
		return true
	}
	b, _ := os.ReadFile(gceProductFile)
	n := strings.TrimSpace(string(b))
	return n == "Google" || n == "Google Compute Engine"
}

// token waits for the token source under the attempt's deadline. Waiters
// share one outstanding call (tokensource.Shared: at most one goroutine
// calls the source at a time, however many attempts wait or give up); a
// call that outlives the waits finishes in the background (its own HTTP
// client is bounded by the request timeout) and later attempts reuse its
// token.
func (c *Client) token(ctx context.Context) (*oauth2.Token, error) {
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

// route is the URL of a bucket's objects collection.
func (c *Client) route(bucket string) url.URL {
	u := *c.base
	u.Path += "b/" + bucket + "/o"
	u.RawPath = strings.TrimSuffix(c.base.EscapedPath(), "/") + "/b/" + url.PathEscape(bucket) + "/o"
	return u
}

// Route is the URL a bucket's listing requests go to, before the query.
func (c *Client) Route(bucket string) string {
	u := c.route(bucket)
	return u.String()
}

// UserProject is the billed project every request names (userProject).
func (c *Client) UserProject() string { return c.cfg.UserProject }

// requestURL is the request URL for in.
func (c *Client) requestURL(in *ListInput) string {
	u := c.route(in.Bucket)
	q := url.Values{}
	if c.base.String() == DefaultEndpoint {
		// Partial responses on Google's endpoint only: emulators and the
		// replay server refuse or ignore a fields selector.
		q.Set("fields", fields)
	}
	q.Set("projection", "noAcl")
	if in.Prefix != "" {
		q.Set("prefix", in.Prefix)
	}
	if in.Delimiter != "" {
		q.Set("delimiter", in.Delimiter)
	}
	if in.StartOffset != "" {
		q.Set("startOffset", in.StartOffset)
	}
	if in.EndOffset != "" {
		q.Set("endOffset", in.EndOffset)
	}
	if in.PageToken != "" {
		q.Set("pageToken", in.PageToken)
	}
	if in.MaxResults > 0 {
		q.Set("maxResults", strconv.Itoa(in.MaxResults))
	}
	if c.cfg.UserProject != "" {
		q.Set("userProject", c.cfg.UserProject)
	}
	u.RawQuery = q.Encode()
	return u.String()
}

// List sends one objects.list request, retrying retryable failures
// (objhttp.Do).
func (c *Client) List(ctx context.Context, in *ListInput) (*Page, error) {
	u := c.requestURL(in)
	return objhttp.Do(ctx, c.retry, &c.counts, func() (*Page, error) { return c.attempt(ctx, u) })
}

// maxResponse bounds a page body: 1,000 items of 1,024-byte names with
// every field requested stay well under it.
const maxResponse = 16 << 20

func (c *Client) attempt(ctx context.Context, u string) (*Page, error) {
	actx, cancel := context.WithTimeout(ctx, c.cfg.RequestTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(actx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", buildinfo.Tool)
	if c.ts != nil {
		tok, err := c.token(actx)
		if err != nil {
			return nil, err
		}
		tok.SetAuthHeader(req)
	}
	defer c.counts.Attempt()()
	resp, err := c.pick().Do(req)
	if err != nil {
		return nil, objhttp.Timeout(actx, ctx, c.cfg.RequestTimeout, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponse+1))
	if err != nil {
		return nil, objhttp.Timeout(actx, ctx, c.cfg.RequestTimeout, err)
	}
	if resp.StatusCode != http.StatusOK {
		e := responseError(resp.StatusCode, body)
		if loc := resp.Header.Get("Location"); e.Status/100 == 3 && e.Message == "" {
			e.Message = "redirect to " + loc + " (not followed)"
		}
		if e.Status == http.StatusBadRequest {
			e.Query = req.URL.RawQuery // a malformed request names itself
		}
		return nil, e
	}
	if len(body) > maxResponse {
		return nil, fmt.Errorf("objects.list response larger than %d bytes", maxResponse)
	}
	if err := faults.At("response-read"); err != nil {
		return nil, err
	}
	p, err := parse(body)
	if err != nil {
		c.counts.ParseFailure()
		return nil, err
	}
	return p, nil
}

func responseError(status int, body []byte) *Error {
	e := &Error{Status: status}
	var w struct {
		Error struct {
			Message string `json:"message"`
			Errors  []struct {
				Reason string `json:"reason"`
			} `json:"errors"`
		} `json:"error"`
	}
	if json.Unmarshal(body, &w) == nil {
		e.Message = w.Error.Message
		if len(w.Error.Errors) > 0 {
			e.Reason = w.Error.Errors[0].Reason
		}
	}
	return e
}

type wireObject struct {
	Name           string `json:"name"`
	Size           string `json:"size"`
	Updated        string `json:"updated"`
	ETag           string `json:"etag"`
	Generation     string `json:"generation"`
	Metageneration string `json:"metageneration"`
	StorageClass   string `json:"storageClass"`
	MD5            string `json:"md5Hash"`
	CRC32C         string `json:"crc32c"`
	ContentType    string `json:"contentType"`
}

func parse(body []byte) (*Page, error) {
	var w struct {
		Items         []wireObject    `json:"items"`
		Prefixes      []string        `json:"prefixes"`
		NextPageToken string          `json:"nextPageToken"`
		Error         json.RawMessage `json:"error"`
	}
	if err := json.Unmarshal(body, &w); err != nil {
		return nil, fmt.Errorf("%w (%d bytes): %v", ErrSyntax, len(body), err)
	}
	// A 200 must carry an objects.list envelope: a JSON object without an
	// error member. `null`, another JSON value or an error envelope would
	// otherwise read as an empty final page and end the range early; they
	// are retried like a garbled body. `{}` (an empty listing under a fields
	// selector) is a legitimate empty page.
	if t := bytes.TrimLeft(body, " \t\r\n"); len(t) == 0 || t[0] != '{' {
		return nil, fmt.Errorf("%w: a 200 whose body is not a JSON object (%d bytes)", ErrSyntax, len(body))
	}
	if w.Error != nil {
		return nil, fmt.Errorf("%w: a 200 carrying an error envelope: %.200s", ErrSyntax, w.Error)
	}
	p := &Page{Prefixes: w.Prefixes, NextPageToken: w.NextPageToken, Items: make([]Object, len(w.Items))}
	for i, o := range w.Items {
		x := &p.Items[i]
		x.Name, x.ETag, x.StorageClass, x.MD5, x.CRC32C, x.ContentType = o.Name, o.ETag, o.StorageClass, o.MD5, o.CRC32C, o.ContentType
		var err error
		if x.Size, err = optInt(o.Size); err != nil {
			return nil, fmt.Errorf("%w: size %q", ErrSyntax, o.Size)
		}
		if x.Generation, err = optInt(o.Generation); err != nil {
			return nil, fmt.Errorf("%w: generation %q", ErrSyntax, o.Generation)
		}
		if x.Metageneration, err = optInt(o.Metageneration); err != nil {
			return nil, fmt.Errorf("%w: metageneration %q", ErrSyntax, o.Metageneration)
		}
		if o.Updated != "" {
			t, err := time.Parse(time.RFC3339Nano, o.Updated)
			if err != nil {
				return nil, fmt.Errorf("%w: updated %q", ErrSyntax, o.Updated)
			}
			x.Updated = t.UnixMicro()
		}
	}
	return p, nil
}

func optInt(s string) (int64, error) {
	if s == "" {
		return 0, nil
	}
	return strconv.ParseInt(s, 10, 64)
}
