// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/varveio/swath/go/internal/faults"
	"github.com/varveio/swath/go/internal/source/objhttp"
)

// Config configures a Client. Zero values take the defaults.
type Config struct {
	Region         string        // else the SDK's resolution (AWS_REGION, profile), else us-east-1
	Endpoint       string        // custom endpoint URL (S3-compatible stores, tests)
	PathStyle      bool          // https://host/bucket instead of https://bucket.host/
	FIPS           bool          // s3-fips.REGION.amazonaws.com
	DualStack      bool          // s3.dualstack.REGION.amazonaws.com (IPv6)
	Profile        string        // shared config profile (else AWS_PROFILE, else default)
	Anonymous      bool          // never sign
	RequestPayer   bool          // send x-amz-request-payer: requester (signed): the caller pays for a requester-pays bucket
	RequestTimeout time.Duration // per attempt, headers and body; default 30 s
	MaxAttempts    int           // default 9
	BaseBackoff    time.Duration // default 100 ms
	MaxBackoff     time.Duration // default 5 s
	HTTPClient     *http.Client
	Now            func() time.Time // signing clock; default time.Now
	// Limiter, if set, admits every attempt (objhttp.Limiter).
	Limiter *objhttp.Limiter
}

// Counters are what the client did, for the run summary.
type Counters = objhttp.Counts

// Client lists S3 buckets. It is safe for concurrent use.
type Client struct {
	cfg    Config
	auth   *auth
	region string
	base   *url.URL
	http   *http.Client
	note   string

	retry  objhttp.Retry
	counts objhttp.Counters

	parsers sync.Pool // *parser
	buffers sync.Pool // *bytes.Buffer
}

// New resolves the region and credentials and builds the client.
func New(cfg Config) (*Client, error) {
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = 30 * time.Second
	}
	a, err := loadAuth(cfg)
	if err != nil {
		return nil, err
	}
	c := &Client{cfg: cfg, auth: a, region: a.region, http: cfg.HTTPClient, note: anonymousNote(cfg),
		retry: objhttp.Retry{MaxAttempts: cfg.MaxAttempts, BaseBackoff: cfg.BaseBackoff, MaxBackoff: cfg.MaxBackoff, Limiter: cfg.Limiter}}
	c.retry.Defaults()
	if cfg.Endpoint != "" {
		u, err := url.Parse(cfg.Endpoint)
		if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
			return nil, fmt.Errorf("endpoint %q: want http(s)://host[:port][/path]", cfg.Endpoint)
		}
		c.base = &url.URL{Scheme: u.Scheme, Host: u.Host, Path: strings.TrimSuffix(u.Path, "/")}
	} else {
		c.base = &url.URL{Scheme: "https", Host: awsHost(c.region, cfg.FIPS, cfg.DualStack)}
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
	return c.auth.source, c.auth.anonymous
}

// Note is a one-time warning for the user, or "".
func (c *Client) Note() string { return c.note }

// Region is the signing region.
func (c *Client) Region() string { return c.region }

// Answered is the number of requests the service answered usably
// (objhttp.Counters.Answered).
func (c *Client) Answered() int64 { return c.counts.Answered() }

// Counters returns the request counters so far.
func (c *Client) Counters() Counters {
	n := c.counts.Load()
	n.Limiter = c.retry.Limiter.Counts()
	return n
}

// ListObjectsV2 sends one ListObjectsV2 request, retrying retryable
// failures with full-jitter exponential backoff (objhttp.Do).
func (c *Client) ListObjectsV2(ctx context.Context, in *ListInput) (*Page, error) {
	u := c.url(in)
	return objhttp.Do(ctx, c.retry, &c.counts, func() (*Page, error) { return c.attempt(ctx, u) })
}

// A ListObjectsV2 page of 1,000 keys of 1,024 bytes with owners and
// checksums stays under 4 MiB; a larger body is refused, not buffered.
const (
	maxResponse = 16 << 20
	maxPooled   = 4 << 20
)

// route is the URL a bucket's requests go to, before the query: the bucket
// in the path (path-style, a bucket that is not virtual-hostable, an IP
// host) or in the host (virtual-host style).
func (c *Client) route(bucket string) url.URL {
	u := *c.base
	if c.cfg.PathStyle || !virtualHostable(bucket) || net.ParseIP(hostname(u.Host)) != nil {
		u.Path += "/" + bucket
	} else {
		u.Host = bucket + "." + u.Host
		u.Path += "/"
	}
	return u
}

// Route is the URL a bucket's listing requests go to, before the query:
// it fixes the endpoint, its path bytes and the addressing style.
func (c *Client) Route(bucket string) string {
	u := c.route(bucket)
	return u.String()
}

// url builds the request URL with the canonical query string (sorted keys,
// SigV4 encoding), so the signature covers exactly what is sent.
func (c *Client) url(in *ListInput) *url.URL {
	u := c.route(in.Bucket)
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

// awsHost is the regional S3 endpoint host: FIPS and dual-stack variants,
// and the amazonaws.com.cn partition for China regions. Access points,
// S3 Express directory buckets and other partitions are not supported yet.
func awsHost(region string, fips, dualStack bool) string {
	h := "s3"
	if fips {
		h += "-fips"
	}
	if dualStack {
		h += ".dualstack"
	}
	domain := "amazonaws.com"
	if strings.HasPrefix(region, "cn-") {
		domain = "amazonaws.com.cn"
	}
	return h + "." + region + "." + domain
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
	if c.cfg.RequestPayer {
		req.Header.Set("X-Amz-Request-Payer", "requester") // before signing: it is a signed header
	}
	if err := c.auth.sign(actx, req, c.cfg.Now()); err != nil {
		return nil, err
	}
	c.counts.Attempt()
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, objhttp.Timeout(actx, ctx, c.cfg.RequestTimeout, err)
	}
	defer resp.Body.Close()
	buf, _ := c.buffers.Get().(*bytes.Buffer)
	if buf == nil {
		buf = new(bytes.Buffer)
	}
	defer func() {
		if buf.Cap() <= maxPooled { // do not keep an outsized buffer
			buf.Reset()
			c.buffers.Put(buf)
		}
	}()
	buf.Reset()
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(buf, io.LimitReader(resp.Body, 64<<10))
		return nil, responseError(resp, buf.Bytes())
	}
	if _, err := buf.ReadFrom(io.LimitReader(resp.Body, maxResponse+1)); err != nil {
		return nil, objhttp.Timeout(actx, ctx, c.cfg.RequestTimeout, err)
	}
	if buf.Len() > maxResponse {
		return nil, fmt.Errorf("ListObjectsV2 response larger than %d bytes", maxResponse)
	}
	if err := faults.At("response-read"); err != nil {
		return nil, err
	}
	p, _ := c.parsers.Get().(*parser)
	if p == nil {
		p = new(parser)
	}
	defer c.parsers.Put(p)
	page, err := p.parseList(buf.Bytes())
	if err != nil {
		c.counts.ParseFailure()
		return nil, fmt.Errorf("ListObjectsV2 response (%d bytes): %w", buf.Len(), err)
	}
	return page, nil
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

// queryEscape is SigV4's URI encoding: everything but A-Z a-z 0-9 - _ . ~
// is percent-encoded with upper-case hex. The query is sent exactly as the
// signer canonicalises it.
func queryEscape(s string) string {
	n := 0
	for i := 0; i < len(s); i++ {
		if !unreserved(s[i]) {
			n++
		}
	}
	if n == 0 {
		return s
	}
	b := make([]byte, 0, len(s)+2*n)
	for i := 0; i < len(s); i++ {
		c := s[i]
		if unreserved(c) {
			b = append(b, c)
		} else {
			b = append(b, '%', "0123456789ABCDEF"[c>>4], "0123456789ABCDEF"[c&15])
		}
	}
	return string(b)
}

func unreserved(c byte) bool {
	return 'A' <= c && c <= 'Z' || 'a' <= c && c <= 'z' || '0' <= c && c <= '9' || c == '-' || c == '_' || c == '.' || c == '~'
}
