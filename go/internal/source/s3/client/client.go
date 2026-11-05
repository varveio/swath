// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Config configures a Client. Zero values take the defaults.
type Config struct {
	Region         string        // else AWS_REGION, AWS_DEFAULT_REGION, the profile's region, us-east-1
	Endpoint       string        // custom endpoint URL (S3-compatible stores, tests)
	PathStyle      bool          // https://host/bucket instead of https://bucket.host/
	Profile        string        // shared-file profile; overrides the environment keys
	Anonymous      bool          // never sign
	RequestTimeout time.Duration // per attempt, headers and body; default 30 s
	MaxAttempts    int           // default 9
	BaseBackoff    time.Duration // default 100 ms
	MaxBackoff     time.Duration // default 5 s
	HTTPClient     *http.Client
	Env            Env              // default os.Getenv
	Now            func() time.Time // signing clock; default time.Now
}

// Counters are what the client did, for the run summary.
type Counters struct {
	Attempts  int64 `json:"attempts"`  // HTTP requests sent
	Retries   int64 `json:"retries"`   // attempts repeated after a retryable failure
	Throttles int64 `json:"throttles"` // of those, 429/503/SlowDown answers
	Timeouts  int64 `json:"timeouts"`  // of those, attempts past the per-request timeout
}

// Client lists S3 buckets. It is safe for concurrent use.
type Client struct {
	cfg    Config
	creds  Credentials
	region string
	base   *url.URL
	signer *signer
	http   *http.Client
	note   string

	attempts, retries, throttles, timeouts atomic.Int64

	parsers sync.Pool // *parser
	buffers sync.Pool // *bytes.Buffer
}

// New resolves the region and credentials and builds the client.
func New(cfg Config) (*Client, error) {
	if cfg.Env == nil {
		cfg.Env = os.Getenv
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
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
	creds, profileRegion, err := resolveCredentials(cfg.Profile, cfg.Anonymous, cfg.Env)
	if err != nil {
		return nil, err
	}
	c := &Client{cfg: cfg, creds: creds, http: cfg.HTTPClient}
	c.region = firstNonEmpty(cfg.Region, cfg.Env("AWS_REGION"), cfg.Env("AWS_DEFAULT_REGION"), profileRegion, "us-east-1")
	if cfg.Endpoint != "" {
		u, err := url.Parse(cfg.Endpoint)
		if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
			return nil, fmt.Errorf("endpoint %q: want http(s)://host[:port][/path]", cfg.Endpoint)
		}
		c.base = &url.URL{Scheme: u.Scheme, Host: u.Host, Path: strings.TrimSuffix(u.Path, "/")}
	} else {
		c.base = &url.URL{Scheme: "https", Host: "s3." + c.region + ".amazonaws.com"}
	}
	if cfg.Anonymous && (cfg.Env("AWS_ACCESS_KEY_ID") != "" || cfg.Env("AWS_PROFILE") != "" || cfg.Env("AWS_SESSION_TOKEN") != "") {
		c.note = "-anonymous: ignoring the AWS_* credentials in the environment"
	}
	if !creds.Anonymous() {
		c.signer = &signer{creds: creds, region: c.region}
	}
	if c.http == nil {
		t := http.DefaultTransport.(*http.Transport).Clone()
		t.MaxIdleConns, t.MaxIdleConnsPerHost = 0, 1024 // one keep-alive connection per worker
		c.http = &http.Client{Transport: t, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	}
	return c, nil
}

// Credentials returns the resolved credentials' source (never the secret).
func (c *Client) Credentials() (source string, anonymous bool) {
	return c.creds.Source, c.creds.Anonymous()
}

// Note is a one-time warning for the user, or "".
func (c *Client) Note() string { return c.note }

// Region is the signing region.
func (c *Client) Region() string { return c.region }

// Counters returns the request counters so far.
func (c *Client) Counters() Counters {
	return Counters{c.attempts.Load(), c.retries.Load(), c.throttles.Load(), c.timeouts.Load()}
}

// ListObjectsV2 sends one ListObjectsV2 request, retrying retryable
// failures with full-jitter exponential backoff.
func (c *Client) ListObjectsV2(ctx context.Context, in *ListInput) (*Page, error) {
	u := c.url(in)
	for attempt := 1; ; attempt++ {
		page, err := c.attempt(ctx, u)
		if err == nil {
			return page, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		var timeout bool
		retry, throttle := retryable(err, &timeout)
		if !retry || attempt >= c.cfg.MaxAttempts {
			if retry {
				err = fmt.Errorf("after %d attempts: %w", attempt, err)
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

var errTimeout = errors.New("request timed out")

// retryable classifies a failed attempt.
func retryable(err error, timeout *bool) (retry, throttle bool) {
	var se *Error
	switch {
	case errors.As(err, &se):
		return se.Retryable(), se.Throttle()
	case errors.Is(err, errTimeout):
		*timeout = true
		return true, false
	case errors.Is(err, ErrSyntax):
		return false, false
	}
	var cert *tls.CertificateVerificationError
	if errors.As(err, &cert) {
		return false, false
	}
	var ne net.Error
	return errors.As(err, &ne) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) ||
		errors.Is(err, net.ErrClosed) || strings.Contains(err.Error(), "connection reset"), false
}

// url builds the request URL with the canonical query string (sorted keys,
// SigV4 encoding), so the signature covers exactly what is sent.
func (c *Client) url(in *ListInput) *url.URL {
	u := *c.base
	if c.cfg.PathStyle || !virtualHostable(in.Bucket) || net.ParseIP(hostname(u.Host)) != nil {
		u.Path += "/" + in.Bucket
	} else {
		u.Host = in.Bucket + "." + u.Host
		u.Path += "/"
	}
	var q strings.Builder
	add := func(k, v string) {
		if q.Len() > 0 {
			q.WriteByte('&')
		}
		q.WriteString(k)
		q.WriteByte('=')
		q.WriteString(queryEscape(v))
	}
	if in.ContinuationToken != "" {
		add("continuation-token", in.ContinuationToken)
	}
	if in.Delimiter != "" {
		add("delimiter", in.Delimiter)
	}
	add("encoding-type", "url")
	if in.FetchOwner {
		add("fetch-owner", "true")
	}
	add("list-type", "2")
	if in.MaxKeys > 0 {
		add("max-keys", strconv.Itoa(in.MaxKeys))
	}
	if in.Prefix != "" {
		add("prefix", in.Prefix)
	}
	if in.StartAfter != "" {
		add("start-after", in.StartAfter)
	}
	u.RawQuery = q.String()
	return &u
}

func hostname(hostport string) string {
	if h, _, err := net.SplitHostPort(hostport); err == nil {
		return h
	}
	return hostport
}

// virtualHostable: a DNS label that also works under the wildcard TLS
// certificate (no dots).
func virtualHostable(b string) bool {
	if len(b) < 3 || len(b) > 63 {
		return false
	}
	for i := 0; i < len(b); i++ {
		c := b[i]
		if !('a' <= c && c <= 'z' || '0' <= c && c <= '9' || c == '-' && i > 0 && i < len(b)-1) {
			return false
		}
	}
	return true
}

func (c *Client) attempt(ctx context.Context, u *url.URL) (*Page, error) {
	actx, cancel := context.WithTimeout(ctx, c.cfg.RequestTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(actx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Host = u.Host
	req.Header.Set("User-Agent", "swath-go")
	if c.signer != nil {
		c.signer.sign(req, c.cfg.Now())
	}
	c.attempts.Add(1)
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, c.wrap(actx, ctx, err)
	}
	defer resp.Body.Close()
	buf, _ := c.buffers.Get().(*bytes.Buffer)
	if buf == nil {
		buf = new(bytes.Buffer)
	}
	defer func() { buf.Reset(); c.buffers.Put(buf) }()
	buf.Reset()
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(buf, io.LimitReader(resp.Body, 64<<10))
		return nil, responseError(resp, buf.Bytes())
	}
	if _, err := buf.ReadFrom(resp.Body); err != nil {
		return nil, c.wrap(actx, ctx, err)
	}
	p, _ := c.parsers.Get().(*parser)
	if p == nil {
		p = new(parser)
	}
	defer c.parsers.Put(p)
	return p.parseList(buf.Bytes())
}

// wrap marks an attempt that ran out of its own time (not the caller's).
func (c *Client) wrap(actx, ctx context.Context, err error) error {
	if ctx.Err() == nil && errors.Is(actx.Err(), context.DeadlineExceeded) {
		return fmt.Errorf("%w after %s: %v", errTimeout, c.cfg.RequestTimeout, err)
	}
	return err
}

func responseError(resp *http.Response, body []byte) *Error {
	e := &Error{Status: resp.StatusCode, RequestID: resp.Header.Get("X-Amz-Request-Id")}
	if code, msg, id, ok := parseError(body); ok {
		e.Code, e.Message = code, msg
		if id != "" {
			e.RequestID = id
		}
	}
	if e.Code == "" {
		e.Code = strings.ReplaceAll(http.StatusText(resp.StatusCode), " ", "")
	}
	if resp.StatusCode == http.StatusMovedPermanently || e.Code == "AuthorizationHeaderMalformed" || e.Code == "PermanentRedirect" {
		e.Region = resp.Header.Get("X-Amz-Bucket-Region")
	}
	return e
}
