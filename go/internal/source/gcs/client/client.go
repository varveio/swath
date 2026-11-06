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
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"

	"github.com/varveio/swath/go/internal/faults"
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

// ExhaustedError is a retryable failure that outlived its attempts.
type ExhaustedError struct {
	Attempts int
	Err      error
}

func (e *ExhaustedError) Error() string {
	return fmt.Sprintf("after %d attempts: %v", e.Attempts, e.Err)
}
func (e *ExhaustedError) Unwrap() error { return e.Err }

// ErrSyntax marks a 200 whose body did not parse.
var ErrSyntax = errors.New("malformed objects.list response")

// CredentialError is a credential failure: configured credentials that do
// not load, or a token the token endpoint would not issue. Transient
// failures (the endpoint's 5xx, 408 or 429, a cut connection, an attempt
// past the request timeout) are retried; the others stop the request.
type CredentialError struct {
	Source    string // where the credentials came from (never the secret)
	Transient bool
	Timeout   bool // the token did not arrive within the request timeout
	Err       error
}

func (e *CredentialError) Error() string {
	return "GCS credentials (" + e.Source + "): " + e.Err.Error()
}
func (e *CredentialError) Unwrap() error { return e.Err }

var errTimeout = errors.New("request timed out")

// Counters are what the client did.
type Counters struct {
	Attempts, Retries, Throttles, Timeouts, ParseFailures int64
}

// Client lists Cloud Storage buckets; safe for concurrent use.
type Client struct {
	cfg    Config
	base   *url.URL
	http   *http.Client
	ts     oauth2.TokenSource // nil: anonymous
	source string

	attempts, retries, throttles, timeouts, parseFailures atomic.Int64
}

// New resolves credentials and builds the client. Without Application
// Default Credentials (no GOOGLE_APPLICATION_CREDENTIALS, no gcloud file, not
// on Google Cloud), requests go unauthenticated, which public buckets allow.
// Credentials that are configured but do not load are a *CredentialError.
func New(ctx context.Context, cfg Config) (*Client, error) {
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = 30 * time.Second
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 9
	}
	if cfg.BaseBackoff <= 0 {
		cfg.BaseBackoff = 100 * time.Millisecond
	}
	if cfg.MaxBackoff <= 0 {
		cfg.MaxBackoff = 5 * time.Second
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
	c := &Client{cfg: cfg, base: u, http: cfg.HTTPClient, source: "anonymous"}
	if c.http == nil {
		t := http.DefaultTransport.(*http.Transport).Clone()
		t.MaxIdleConns, t.MaxIdleConnsPerHost = 0, 1024
		c.http = &http.Client{Transport: t}
	}
	// The token source keeps this context for every refresh: it must
	// outlive New's caller's deadline, and its HTTP client bounds each token
	// exchange (the attempt deadline bounds the wait, token below).
	life := context.WithValue(context.WithoutCancel(ctx), oauth2.HTTPClient,
		&http.Client{Transport: c.http.Transport, Timeout: cfg.RequestTimeout})
	switch {
	case cfg.TokenSource != nil:
		c.ts, c.source = cfg.TokenSource, "token source"
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
		c.ts = creds.TokenSource
	default:
		creds, err := google.FindDefaultCredentials(life, readScope)
		switch {
		case err == nil:
			c.ts, c.source = creds.TokenSource, "application default credentials"
		case adcConfigured():
			return nil, &CredentialError{Source: "application default credentials", Err: err}
		default:
			c.source = "anonymous (no application default credentials)"
		}
	}
	return c, nil
}

// adcConfigured reports whether Application Default Credentials are
// configured in a place golang.org/x/oauth2/google reads: then a failure
// to load them is an error, not anonymity.
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
	return err == nil
}

// token waits for the token source under the attempt's deadline. A refresh
// that outlives the wait finishes in the background (its own HTTP client is
// bounded by the request timeout) and later attempts reuse its token.
func (c *Client) token(ctx context.Context) (*oauth2.Token, error) {
	type result struct {
		t   *oauth2.Token
		err error
	}
	ch := make(chan result, 1)
	go func() {
		t, err := c.ts.Token()
		ch <- result{t, err}
	}()
	select {
	case r := <-ch:
		if r.err != nil {
			return nil, &CredentialError{Source: c.source, Transient: transientToken(r.err), Err: r.err}
		}
		return r.t, nil
	case <-ctx.Done():
		return nil, &CredentialError{Source: c.source, Transient: true, Timeout: true,
			Err: fmt.Errorf("no token within %s: %w", c.cfg.RequestTimeout, ctx.Err())}
	}
}

// transientToken: the token endpoint's 5xx, 408 and 429, and transport
// failures, may pass; a refused grant (400, 401) will not.
func transientToken(err error) bool {
	var re *oauth2.RetrieveError
	if errors.As(err, &re) {
		if re.Response == nil {
			return false
		}
		s := re.Response.StatusCode
		return s >= 500 || s == 408 || s == 429
	}
	return transport(err)
}

// Credentials names where the credentials came from (never the secret).
func (c *Client) Credentials() (source string, anonymous bool) { return c.source, c.ts == nil }

// Counters returns the request counters so far.
func (c *Client) Counters() Counters {
	return Counters{c.attempts.Load(), c.retries.Load(), c.throttles.Load(), c.timeouts.Load(), c.parseFailures.Load()}
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

// URL is the request URL for in (exported for the oracle test).
func (c *Client) URL(in *ListInput) string {
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

// List sends one objects.list request, retrying retryable failures.
func (c *Client) List(ctx context.Context, in *ListInput) (*Page, error) {
	u := c.URL(in)
	for attempt := 1; ; attempt++ {
		page, err := c.attempt(ctx, u)
		if err == nil {
			return page, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		retry, throttle, timeout := retryable(err)
		if !retry || attempt >= c.cfg.MaxAttempts {
			if retry {
				err = &ExhaustedError{Attempts: attempt, Err: err}
			}
			return nil, err
		}
		c.retries.Add(1)
		if throttle {
			c.throttles.Add(1)
		}
		if timeout {
			c.timeouts.Add(1)
		}
		d := min(c.cfg.MaxBackoff, c.cfg.BaseBackoff<<min(attempt-1, 30))
		t := time.NewTimer(rand.N(d) + 1)
		select {
		case <-ctx.Done():
			t.Stop()
			return nil, ctx.Err()
		case <-t.C:
		}
	}
}

func retryable(err error) (retry, throttle, timeout bool) {
	var se *Error
	switch {
	case errors.As(err, &se):
		return se.Retryable(), se.Throttle(), false
	case errors.Is(err, errTimeout):
		return true, false, true
	case errors.Is(err, ErrSyntax):
		return true, false, false
	}
	var ce *CredentialError
	if errors.As(err, &ce) {
		return ce.Transient, false, ce.Timeout
	}
	return transport(err), false, false
}

// transport reports a connection failure worth repeating (not a
// certificate refusal).
func transport(err error) bool {
	var cert *tls.CertificateVerificationError
	if errors.As(err, &cert) {
		return false
	}
	var ne net.Error
	return errors.As(err, &ne) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) ||
		errors.Is(err, net.ErrClosed) || strings.Contains(err.Error(), "connection reset")
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
	req.Header.Set("User-Agent", "swath-go")
	if c.ts != nil {
		tok, err := c.token(actx)
		if err != nil {
			return nil, err
		}
		tok.SetAuthHeader(req)
	}
	c.attempts.Add(1)
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, c.wrap(actx, ctx, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponse+1))
	if err != nil {
		return nil, c.wrap(actx, ctx, err)
	}
	if resp.StatusCode != http.StatusOK {
		e := responseError(resp.StatusCode, body)
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
		c.parseFailures.Add(1)
		return nil, err
	}
	return p, nil
}

func (c *Client) wrap(actx, ctx context.Context, err error) error {
	if ctx.Err() == nil && errors.Is(actx.Err(), context.DeadlineExceeded) {
		return fmt.Errorf("%w after %s: %v", errTimeout, c.cfg.RequestTimeout, err)
	}
	return err
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
		Items         []wireObject `json:"items"`
		Prefixes      []string     `json:"prefixes"`
		NextPageToken string       `json:"nextPageToken"`
	}
	if err := json.Unmarshal(body, &w); err != nil {
		return nil, fmt.Errorf("%w (%d bytes): %v", ErrSyntax, len(body), err)
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
