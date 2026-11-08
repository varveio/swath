// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package client is a small Azure Blob Storage client: List Blobs (flat or
// with a delimiter) and Get Account Information only, authenticated by a
// SAS, an account key signed here (Shared Key), Entra ID tokens from
// azidentity's DefaultAzureCredential, or nothing, with counted retries and
// typed errors from objhttp.
//
// Entra ID tokens live as long as the client: they are fetched by a token
// source whose HTTP client bounds each exchange by the request timeout, and
// every attempt waits for its token under its own attempt deadline, all
// waiters sharing one outstanding call (tokensource.Shared). Without a
// configured credential and with none found, requests go unauthenticated
// (public containers allow it); a credential that is configured but will
// not load is a *CredentialError, never anonymity.
package client

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"golang.org/x/oauth2"

	"github.com/varveio/swath/go/internal/buildinfo"
	"github.com/varveio/swath/go/internal/faults"
	"github.com/varveio/swath/go/internal/source/objhttp"
	"github.com/varveio/swath/go/internal/source/tokensource"
)

// Version is the service version every request names (x-ms-version):
// the newer of the two the live account and the replay answer, which
// behave the same for listing.
const Version = "2026-10-06"

// DefaultEndpoint is the blob service of an account, ACCOUNT standing for
// the account name.
const DefaultEndpoint = "https://ACCOUNT.blob.core.windows.net"

// storageScope is the Entra ID scope of Azure Storage.
const storageScope = "https://storage.azure.com/.default"

// accountName is a storage account name: 3 to 24 lower-case letters and
// digits.
var accountName = regexp.MustCompile(`^[a-z0-9]{3,24}$`)

// Config configures a Client. Zero values take the defaults.
type Config struct {
	// Endpoint is the blob service base URL; default DefaultEndpoint for
	// Account. Any other endpoint (an emulator, the replay server) takes
	// the account as its first path segment: ENDPOINT/ACCOUNT/CONTAINER.
	Endpoint string
	Account  string
	// Anonymous sends no credentials, whatever is configured.
	Anonymous bool
	// SAS is a shared access signature, a query string with or without a
	// leading '?', appended to every request and kept out of every error.
	SAS string
	// SharedKey is the account key (base64): requests are signed here.
	SharedKey string
	// Credential overrides the Entra ID lookup (tests).
	Credential     azcore.TokenCredential
	RequestTimeout time.Duration // per attempt; default 30 s
	MaxAttempts    int           // default 9
	BaseBackoff    time.Duration // default 100 ms
	MaxBackoff     time.Duration // default 5 s
	HTTPClient     *http.Client
	// Workers sizes the built transport pool (PoolSize); a supplied
	// HTTPClient is used alone.
	Workers int
	// Limiter, if set, admits every attempt (objhttp.Limiter).
	Limiter *objhttp.Limiter
	// Rate, if set, caps attempts per second (objhttp.Rate).
	Rate *objhttp.Rate
}

// ListInput is one List Blobs request. StartFrom is inclusive; Marker is
// the previous page's NextMarker. An empty field is not sent.
type ListInput struct {
	Container  string
	Prefix     string
	Delimiter  string
	StartFrom  string
	Marker     string
	MaxResults int // 0: the server default (5,000, also the cap)
}

// Item is one listed entry: a blob, or with Prefix a BlobPrefix (a
// delimiter roll-up, of which only Name is set).
type Item struct {
	Name         string
	Prefix       bool
	Size         int64
	LastModified int64 // Unix microseconds; 0 when absent
	CreationTime int64 // Unix microseconds; 0 when absent
	ETag         string
	ContentMD5   string // base64, as listed
	ContentType  string
	BlobType     string
	AccessTier   string
}

// ListOutput is one page: blobs and prefixes in wire order, which ascends
// in UTF-16 code-unit order; an empty NextMarker ends the listing.
type ListOutput struct {
	Items      []Item
	NextMarker string
}

// Client lists Azure Blob containers; safe for concurrent use.
type Client struct {
	cfg  Config
	base *url.URL // ends in '/': the account's root
	http []*http.Client
	next atomic.Uint64

	sas    string              // without '?'; "" when none
	key    *sharedKey          // nil: no shared key
	ts     *tokensource.Shared // nil: no bearer token
	source string
	note   string

	retry  objhttp.Retry
	counts objhttp.Counters
}

// New resolves credentials and builds the client.
func New(ctx context.Context, cfg Config) (*Client, error) {
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = 30 * time.Second
	}
	if !accountName.MatchString(cfg.Account) {
		return nil, fmt.Errorf("account %q: want 3 to 24 lower-case letters and digits", cfg.Account)
	}
	base, err := baseURL(cfg.Endpoint, cfg.Account)
	if err != nil {
		return nil, err
	}
	c := &Client{cfg: cfg, base: base,
		retry: objhttp.Retry{MaxAttempts: cfg.MaxAttempts, BaseBackoff: cfg.BaseBackoff, MaxBackoff: cfg.MaxBackoff, Limiter: cfg.Limiter, Rate: cfg.Rate, Answer: answer}}
	c.retry.Defaults()
	// Redirects are answers, never followed, also through a supplied HTTP
	// client: the resume identity freezes the route (as the S3 and GCS
	// clients do).
	noRedirect := func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	if cfg.HTTPClient == nil {
		// One transport per connWorkers workers, sharing one TLS session
		// cache (the GCS client's pool, for the same HTTP/2 reason).
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
	if err := c.credentials(ctx); err != nil {
		return nil, err
	}
	return c, nil
}

// baseURL is the account's root URL, ending in '/'.
func baseURL(endpoint, account string) (*url.URL, error) {
	if endpoint == "" {
		return &url.URL{Scheme: "https", Host: account + ".blob.core.windows.net", Path: "/"}, nil
	}
	u, err := url.Parse(endpoint)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.RawQuery != "" || u.Fragment != "" {
		return nil, fmt.Errorf("endpoint %q: want http(s)://host[:port][/path]", endpoint)
	}
	u.Path = strings.TrimSuffix(u.Path, "/") + "/" + account + "/"
	u.RawPath = ""
	return u, nil
}

// credentials resolves the credential in precedence order: anonymous, a
// SAS, a shared key, a supplied token credential, then Entra ID through
// DefaultAzureCredential.
func (c *Client) credentials(ctx context.Context) error {
	cfg := c.cfg
	switch {
	case cfg.Anonymous:
		c.source, c.note = "anonymous", anonymousNote(cfg)
	case cfg.SAS != "":
		c.sas = strings.TrimPrefix(cfg.SAS, "?")
		c.source = "shared access signature"
		if _, err := url.ParseQuery(c.sas); err != nil || c.sas == "" {
			// the parse error would quote the signature
			return &CredentialError{Source: c.source, Err: errors.New("not a query string")}
		}
	case cfg.SharedKey != "":
		c.source = "shared key"
		k, err := newSharedKey(cfg.Account, cfg.SharedKey)
		if err != nil {
			return &CredentialError{Source: c.source, Err: err}
		}
		c.key = k
	case cfg.Credential != nil:
		c.ts, c.source = c.shared(cfg.Credential), "token credential"
	default:
		return c.entra(ctx)
	}
	return nil
}

// newDefaultCredential is azidentity's DefaultAzureCredential (tests
// replace it to fake an empty environment on any host).
var newDefaultCredential = func(o *azidentity.DefaultAzureCredentialOptions) (azcore.TokenCredential, error) {
	return azidentity.NewDefaultAzureCredential(o)
}

// entraVars are the environment variables by which DefaultAzureCredential
// is configured (environment, workload identity and managed identity
// credentials, and its own selector): when one is set, a failure is an
// error, not anonymity.
var entraVars = []string{"AZURE_TENANT_ID", "AZURE_CLIENT_ID", "AZURE_CLIENT_SECRET", "AZURE_CLIENT_CERTIFICATE_PATH",
	"AZURE_USERNAME", "AZURE_FEDERATED_TOKEN_FILE", "IDENTITY_ENDPOINT", "MSI_ENDPOINT", "AZURE_TOKEN_CREDENTIALS"}

func entraConfigured() bool {
	for _, v := range entraVars {
		if os.Getenv(v) != "" {
			return true
		}
	}
	return false
}

// entra sets up Entra ID through DefaultAzureCredential. Configured by the
// environment, its token failures are attempts' credential errors. With
// nothing configured it may still find a managed identity or an Azure CLI
// login: one token is asked for under the request timeout, and when none
// comes the requests go unauthenticated (the note says so); a refused
// grant is a credential error.
func (c *Client) entra(ctx context.Context) error {
	const source = "Entra ID (DefaultAzureCredential)"
	cred, err := newDefaultCredential(&azidentity.DefaultAzureCredentialOptions{ClientOptions: azcore.ClientOptions{
		// Each token exchange is one attempt: the listing's retry loop
		// repeats it, not azcore's.
		Transport: c.tokenHTTP(), Retry: policy.RetryOptions{MaxRetries: -1}}})
	configured := entraConfigured()
	switch {
	case err != nil && configured:
		return &CredentialError{Source: source, Err: err}
	case err != nil:
		c.source, c.note = "anonymous", "no Azure credentials found ("+err.Error()+"): listing unauthenticated"
		return nil
	case configured:
		c.ts, c.source = c.shared(cred), source
		return nil
	}
	pctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.cfg.RequestTimeout)
	defer cancel()
	_, err = cred.GetToken(pctx, policy.TokenRequestOptions{Scopes: []string{storageScope}})
	switch {
	case err == nil:
		c.ts, c.source = c.shared(cred), source
	case refused(err):
		return &CredentialError{Source: source, Err: err}
	default:
		c.source, c.note = "anonymous", "no Azure credentials found ("+err.Error()+"): listing unauthenticated"
	}
	return nil
}

// tokenHTTP is the HTTP client of token exchanges: the first pooled
// transport, each exchange bounded by the request timeout.
func (c *Client) tokenHTTP() *http.Client {
	return &http.Client{Transport: c.http[0].Transport, Timeout: c.cfg.RequestTimeout}
}

// shared wraps cred as the client's coalesced token source.
func (c *Client) shared(cred azcore.TokenCredential) *tokensource.Shared {
	return tokensource.NewShared(credentialSource{cred: cred, timeout: c.cfg.RequestTimeout}, 0)
}

// credentialSource is an azcore credential as an oauth2 token source. It
// runs under its own context, bounded by the request timeout: a token call
// that outlives the attempts waiting on it finishes in the background
// (tokensource.Shared).
type credentialSource struct {
	cred    azcore.TokenCredential
	timeout time.Duration
}

func (s credentialSource) Token() (*oauth2.Token, error) {
	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	defer cancel()
	t, err := s.cred.GetToken(ctx, policy.TokenRequestOptions{Scopes: []string{storageScope}})
	if err != nil {
		return nil, err
	}
	return &oauth2.Token{AccessToken: t.Token, TokenType: "Bearer", Expiry: t.ExpiresOn}, nil
}

// anonymousNote warns when Anonymous hides credentials the environment
// holds.
func anonymousNote(cfg Config) string {
	if cfg.SAS != "" || cfg.SharedKey != "" || os.Getenv("AZURE_STORAGE_KEY") != "" || os.Getenv("AZURE_STORAGE_SAS_TOKEN") != "" || entraConfigured() {
		return "anonymous: ignoring the configured Azure credentials"
	}
	return ""
}

// token waits for the token source under the attempt's deadline (as the
// GCS client does).
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

// transientToken: a token endpoint's 5xx, 408 and 429, an exchange that got
// no answer (a transport failure, which azidentity reports without a
// response) and an exchange's own timeout may pass; a refused grant and a
// credential that is unavailable will not.
func transientToken(err error) bool {
	var af *azidentity.AuthenticationFailedError
	if errors.As(err, &af) {
		if af.RawResponse == nil {
			return true
		}
		s := af.RawResponse.StatusCode
		return s >= 500 || s == 408 || s == 429
	}
	return objhttp.Transport(err) || errors.Is(err, context.DeadlineExceeded)
}

// refused reports a token endpoint's refusal (an answer that is not
// transient).
func refused(err error) bool {
	var af *azidentity.AuthenticationFailedError
	return errors.As(err, &af) && af.RawResponse != nil && !transientToken(err)
}

// Credentials names where the credentials came from (never the secret).
func (c *Client) Credentials() (source string, anonymous bool) {
	return c.source, c.sas == "" && c.key == nil && c.ts == nil
}

// Note is a warning about the credentials for the run's log ("" when
// none).
func (c *Client) Note() string { return c.note }

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

func (c *Client) pick() *http.Client {
	return c.http[(c.next.Add(1)-1)%uint64(len(c.http))]
}

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

// route is the URL of a container, without a query.
func (c *Client) route(container string) url.URL {
	u := *c.base
	u.Path += container
	u.RawPath = ""
	return u
}

// Route is the URL a container's requests go to, before the query (never
// the SAS).
func (c *Client) Route(container string) string {
	u := c.route(container)
	return u.String()
}

// encodeQuery is q's encoding with spaces as %20, as the Azure SDK sends
// it ('+' itself is %2B either way).
func encodeQuery(q url.Values) string { return strings.ReplaceAll(q.Encode(), "+", "%20") }

// listURL is the List Blobs URL for in, without the SAS.
func (c *Client) listURL(in *ListInput) url.URL {
	u := c.route(in.Container)
	q := url.Values{"restype": {"container"}, "comp": {"list"}}
	for name, v := range map[string]string{"prefix": in.Prefix, "delimiter": in.Delimiter, "startFrom": in.StartFrom, "marker": in.Marker} {
		if v != "" {
			q.Set(name, v)
		}
	}
	if in.MaxResults > 0 {
		q.Set("maxresults", strconv.Itoa(in.MaxResults))
	}
	u.RawQuery = encodeQuery(q)
	return u
}

// List sends one List Blobs request, retrying retryable failures
// (objhttp.Do), and validates the page (ErrInvalidPage).
func (c *Client) List(ctx context.Context, in *ListInput) (*ListOutput, error) {
	u := c.listURL(in)
	limit := bodyCap(in.MaxResults)
	return objhttp.Do(ctx, c.retry, &c.counts, func() (*ListOutput, error) {
		_, body, err := c.attempt(ctx, &u, limit)
		if err != nil {
			return nil, err
		}
		if err := faults.At("response-read"); err != nil {
			return nil, err
		}
		p, err := parse(body)
		if err != nil {
			if errors.Is(err, ErrSyntax) {
				c.counts.ParseFailure()
			}
			return nil, err
		}
		if err := p.validate(in.Marker); err != nil {
			return nil, err
		}
		return p, nil
	})
}

// errHNSUnknown is an account-information answer without
// x-ms-is-hns-enabled: the namespace is unknown, never taken for flat.
var errHNSUnknown = errors.New("account information without " + headerHNSEnabled)

// AccountInfo sends Get Account Information on the container's path
// (restype=account&comp=properties, which a SAS scoped to the container
// may call) and reports whether the account has a hierarchical namespace.
// A refused call or a missing header is an error the caller takes for
// "unknown", never for flat.
func (c *Client) AccountInfo(ctx context.Context, container string) (bool, error) {
	u := c.route(container)
	u.RawQuery = encodeQuery(url.Values{"restype": {"account"}, "comp": {"properties"}})
	return objhttp.Do(ctx, c.retry, &c.counts, func() (bool, error) {
		hdr, _, err := c.attempt(ctx, &u, maxErrorBody)
		if err != nil {
			return false, err
		}
		switch strings.ToLower(hdr.Get(headerHNSEnabled)) {
		case "true":
			return true, nil
		case "false":
			return false, nil
		}
		return false, errHNSUnknown
	})
}

// attempt sends one GET of u (the SAS appended on the wire only) and
// returns a 200's header and body, read up to limit bytes; any other
// answer is an *Error. Errors never carry the SAS.
func (c *Client) attempt(ctx context.Context, u *url.URL, limit int64) (http.Header, []byte, error) {
	actx, cancel := context.WithTimeout(ctx, c.cfg.RequestTimeout)
	defer cancel()
	wire := *u
	if c.sas != "" {
		wire.RawQuery += "&" + c.sas
	}
	req, err := http.NewRequestWithContext(actx, http.MethodGet, wire.String(), nil)
	if err != nil {
		return nil, nil, c.redact(err, u)
	}
	h := req.Header
	h.Set(headerVersion, Version)
	h.Set(headerClientRequestID, requestID())
	h.Set(headerAccept, acceptXML)
	h.Set(headerUserAgent, buildinfo.Tool)
	switch {
	case c.key != nil:
		h.Set(headerDate, time.Now().UTC().Format(http.TimeFormat))
		if err := c.key.sign(req); err != nil {
			return nil, nil, &CredentialError{Source: c.source, Err: err}
		}
	case c.ts != nil:
		tok, err := c.token(actx)
		if err != nil {
			return nil, nil, err
		}
		h.Set(headerAuthorization, "Bearer "+tok.AccessToken)
	}
	defer c.counts.Attempt()()
	resp, err := c.pick().Do(req)
	if err != nil {
		return nil, nil, objhttp.Timeout(actx, ctx, c.cfg.RequestTimeout, c.redact(err, u))
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		limit = maxErrorBody
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	if err != nil {
		return nil, nil, objhttp.Timeout(actx, ctx, c.cfg.RequestTimeout, c.redact(err, u))
	}
	if resp.StatusCode != http.StatusOK {
		return nil, nil, c.responseError(resp, body, u)
	}
	if int64(len(body)) > limit {
		return nil, nil, fmt.Errorf("Azure response larger than %d bytes", limit)
	}
	return resp.Header, body, nil
}

// responseError is a non-200 answer as an *Error: the code from
// x-ms-error-code or else the body, a redirect's target, a 400's query
// (without the SAS), the Retry-After wait.
func (c *Client) responseError(resp *http.Response, body []byte, u *url.URL) *Error {
	code, msg := errorBody(body)
	e := &Error{Status: resp.StatusCode, Code: resp.Header.Get(headerErrorCode), Message: msg,
		Wait: retryAfter(resp.Header.Get(headerRetryAfter), time.Now())}
	if e.Code == "" {
		e.Code = code
	}
	if loc := resp.Header.Get(headerLocation); e.Status/100 == 3 {
		e.Message = "redirect to " + loc + " (not followed)"
	}
	if e.Status == http.StatusBadRequest {
		e.Query = u.RawQuery
	}
	e.Message = c.scrub(e.Message)
	return e
}

// redact keeps the SAS out of a request error: the URL net/http names
// is the one without it. It runs before any wrapping that flattens the
// error into text (objhttp.Timeout).
func (c *Client) redact(err error, u *url.URL) error {
	if c.sas == "" {
		return err
	}
	var ue *url.Error
	if errors.As(err, &ue) {
		ue.URL = u.String()
	}
	return err
}

// scrub removes the SAS, and its signature as sent or decoded, from text
// the service or a redirect returned.
func (c *Client) scrub(s string) string {
	if c.sas == "" || s == "" {
		return s
	}
	secrets := []string{c.sas}
	if q, err := url.ParseQuery(c.sas); err == nil {
		for _, sig := range q["sig"] {
			secrets = append(secrets, sig, url.QueryEscape(sig))
		}
	}
	for _, x := range secrets {
		if x != "" {
			s = strings.ReplaceAll(s, x, "REDACTED")
		}
	}
	return s
}

// requestID is a fresh x-ms-client-request-id: a random UUID.
func requestID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6], b[8] = b[6]&0x0f|0x40, b[8]&0x3f|0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[:4], b[4:6], b[6:8], b[8:10], b[10:])
}
