// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package client is a small Azure Blob Storage client: List Blobs (flat or
// with a delimiter), Get Account Information and, on the account's Data
// Lake host, Filesystem Get Properties (NamespaceInfo, which exists for
// anonymous admission: only it tells an anonymous caller whether the
// namespace is flat) only, authenticated by a
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
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"regexp"
	"slices"
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
type Client struct{ *Core }

// New resolves credentials and builds the client (NewCore).
func New(ctx context.Context, cfg Config) (*Client, error) {
	c, err := NewCore(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &Client{c}, nil
}

// Core is what an Azure Blob listing client is built on, whatever sends
// its List Blobs: the account's root, the transport pool, credentials, the
// retry policy and the counters, and the calls both clients share (Filesystem
// Get Properties, the body-cap halving, answers as *Error, SAS redaction).
// Client embeds it; so does the SDK-backed client (azblob/sdkclient).
type Core struct {
	cfg  Config
	base *url.URL // ends in '/': the account's root
	http []*http.Client
	next atomic.Uint64

	sas    string              // without '?'; "" when none
	key    *sharedKey          // nil: no shared key
	ts     *tokensource.Shared // nil: no bearer token
	source string
	note   string

	maxBody          int64 // MaxBody; tests lower it
	retry            objhttp.Retry
	counts           objhttp.Counters
	retryAfterCapped atomic.Int64
	bodyCapShrinks   atomic.Int64
}

// MaxBody is the body cap: the memory one in-flight response may hold.
// Responses are read whole and sit outside the engine's memory budget, so
// the cap is fixed, not derived from the page size; a page past it is
// asked again with fewer entries (List).
const MaxBody = 64 << 20

// ErrBodyCap is a 200 whose body passed the cap (List halves maxresults).
var ErrBodyCap = errors.New("response larger than the body cap")

// NewCore resolves credentials and builds the transport pool.
func NewCore(ctx context.Context, cfg Config) (*Core, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
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
	c := &Core{cfg: cfg, base: base, maxBody: MaxBody,
		retry: objhttp.Retry{MaxAttempts: cfg.MaxAttempts, BaseBackoff: cfg.BaseBackoff, MaxBackoff: cfg.MaxBackoff, Limiter: cfg.Limiter, Rate: cfg.Rate, Answer: answer}}
	c.retry.Defaults()
	// Redirects are answers, never followed, also through a supplied HTTP
	// client: the resume identity freezes the route (as the S3 and GCS
	// clients do).
	// The Location header is removed before net/http sees it (noLocation):
	// a malformed one would otherwise end the request in an error that
	// quotes it, SAS included, before CheckRedirect runs.
	noRedirect := func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	if cfg.HTTPClient == nil {
		// One transport per connWorkers workers, sharing one TLS session
		// cache (the GCS client's pool, for the same HTTP/2 reason).
		cache := tls.NewLRUClientSessionCache(0)
		for range PoolSize(cfg.Workers) {
			t := http.DefaultTransport.(*http.Transport).Clone()
			t.MaxIdleConns, t.MaxIdleConnsPerHost = 0, 1024
			t.TLSClientConfig = &tls.Config{ClientSessionCache: cache}
			c.http = append(c.http, &http.Client{Transport: noLocation{t}, CheckRedirect: noRedirect})
		}
	} else {
		hc := *cfg.HTTPClient
		hc.CheckRedirect = noRedirect
		next := hc.Transport
		if next == nil {
			next = http.DefaultTransport
		}
		hc.Transport = noLocation{next}
		c.http = []*http.Client{&hc}
	}
	if err := c.credentials(ctx); err != nil {
		return nil, err
	}
	return c, nil
}

// noLocation drops the Location header of every 3xx answer, so net/http
// returns the answer (a redirect class failure) instead of parsing a
// target the client never follows; no Location value reaches an error.
type noLocation struct{ next http.RoundTripper }

func (n noLocation) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := n.next.RoundTrip(req)
	if err == nil && resp.StatusCode/100 == 3 && resp.Header.Get(headerLocation) != "" {
		resp.Header = resp.Header.Clone()
		resp.Header.Del(headerLocation)
	}
	return resp, err
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
func (c *Core) credentials(ctx context.Context) error {
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

// newLoginCredential is the unconfigured discovery's chain off an Azure
// VM: the developer logins only (the Azure CLI, the Azure Developer CLI),
// which run a local command and never ask the instance metadata service.
// A login whose command is not installed (exec.LookPath: az, azd; az.cmd
// on Windows through PATHEXT) is absent and left out, so with neither the
// chain is an exec.ErrNotFound and nothing is started: azidentity would
// run each through a shell (cmd.exe on Windows, /bin/sh elsewhere) and
// report a shell it cannot start as an authentication failure without a
// response, which reads as transient (tests replace it).
var newLoginCredential = func() (azcore.TokenCredential, error) {
	var logins []azcore.TokenCredential
	if _, err := exec.LookPath("az"); err == nil {
		cli, err := azidentity.NewAzureCLICredential(nil)
		if err != nil {
			return nil, err
		}
		logins = append(logins, cli)
	}
	if _, err := exec.LookPath("azd"); err == nil {
		azd, err := azidentity.NewAzureDeveloperCLICredential(nil)
		if err != nil {
			return nil, err
		}
		logins = append(logins, azd)
	}
	if len(logins) == 0 {
		return nil, fmt.Errorf("neither the Azure CLI (az) nor the Azure Developer CLI (azd) is installed: %w", exec.ErrNotFound)
	}
	return azidentity.NewChainedTokenCredential(logins, nil)
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

// entra sets up Entra ID. Configured by the environment, it is
// DefaultAzureCredential, whose token failures are attempts' credential
// errors. With nothing configured: on an Azure VM (its chassis asset tag)
// the same chain, its managed identity kept even when it does not answer
// now; elsewhere only the developer logins (newLoginCredential), never the
// instance metadata service, which off Azure is a proxy's refusal or a
// probe that hangs. There one token is asked for: found, it is used; when
// every login is absent the requests go unauthenticated (the note says
// so), as GCS goes without Application Default Credentials; a refused
// grant is a credential error; a transient failure keeps the chain.
func (c *Core) entra(ctx context.Context) error {
	const source = "Entra ID (DefaultAzureCredential)"
	configured, vm := entraConfigured(), onAzureVM()
	if !configured && !vm {
		return c.login(ctx)
	}
	cred, err := newDefaultCredential(&azidentity.DefaultAzureCredentialOptions{ClientOptions: azcore.ClientOptions{
		// Each token exchange is one attempt: the listing's retry loop
		// repeats it, not azcore's.
		Transport: c.tokenHTTP(), Retry: policy.RetryOptions{MaxRetries: -1}}})
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
	// An Azure VM's managed identity is the configured source even when
	// the metadata service does not answer now (DefaultAzureCredential
	// reports a probe timeout as unavailable): never anonymity.
	c.ts, c.source = c.shared(cred), source+" on an Azure VM"
	return nil
}

// login is the unconfigured discovery off an Azure VM: the developer
// logins (newLoginCredential), one token asked for under the caller's
// context and the request timeout. No login installed (exec.ErrNotFound)
// or none logged in (azidentity's unavailable signal: "not found on
// path", "az login") is no credential: anonymous with the note, as a
// missing gcloud login is for GCS. Only a login that ran and had its
// token refused refuses the run.
func (c *Core) login(ctx context.Context) error {
	const source = "Entra ID (Azure CLI login)"
	cred, err := newLoginCredential()
	if err != nil {
		c.source, c.note = "anonymous", "no Azure credentials found ("+err.Error()+"): listing unauthenticated"
		return nil
	}
	pctx, cancel := context.WithTimeout(ctx, c.cfg.RequestTimeout)
	defer cancel()
	_, err = cred.GetToken(pctx, policy.TokenRequestOptions{Scopes: []string{storageScope}})
	switch {
	case err == nil:
		c.ts, c.source = c.shared(cred), source
	case ctx.Err() != nil:
		return ctx.Err()
	case refused(err):
		return &CredentialError{Source: source, Err: err}
	case unavailable(err), errors.Is(err, exec.ErrNotFound):
		c.source, c.note = "anonymous", "no Azure credentials found ("+err.Error()+"): listing unauthenticated"
	default:
		// Found but not reachable now (a timeout, a transport failure, a
		// token endpoint's 5xx): the chain stays and each attempt asks for
		// its token again, a failure classified transport or timeout.
		c.ts, c.source = c.shared(cred), source
		c.note = "Entra ID token not obtained at start (" + err.Error() + "): retried per request"
	}
	return nil
}

// unavailable reports azidentity's explicit absence signal: every source
// of the chain said it has no credential (credentialUnavailableError, the
// non-retriable error that is not an authentication failure).
func unavailable(err error) bool {
	var af *azidentity.AuthenticationFailedError
	var nr interface{ NonRetriable() }
	return !errors.As(err, &af) && errors.As(err, &nr)
}

// azureAssetTagFile holds the chassis asset tag Azure sets on its VMs
// (tests move it); IMDS answers only there.
var azureAssetTagFile = "/sys/class/dmi/id/chassis_asset_tag"

// azureAssetTag is the chassis asset tag of every Azure VM.
const azureAssetTag = "7783-7084-3265-9085-8269-3286-77"

// onAzureVM reports an Azure VM by its chassis asset tag.
func onAzureVM() bool {
	b, _ := os.ReadFile(azureAssetTagFile)
	return strings.TrimSpace(string(b)) == azureAssetTag
}

// tokenHTTP is the HTTP client of token exchanges: the first pooled
// transport. Each exchange is bounded by the token call's context
// (credentialSource), the one deadline whose expiry is classified.
func (c *Core) tokenHTTP() *http.Client {
	return &http.Client{Transport: c.http[0].Transport}
}

// shared wraps cred as the client's coalesced token source.
func (c *Core) shared(cred azcore.TokenCredential) *tokensource.Shared {
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
		if ctx.Err() != nil {
			// azidentity flattens the deadline into its message: the call's
			// own context says it ran out of time (a timeout, retried).
			return nil, fmt.Errorf("token call past %s: %w: %w", s.timeout, ctx.Err(), err)
		}
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

// Token waits for the token source under the attempt's deadline (as the
// GCS client does).
func (c *Core) Token(ctx context.Context) (*oauth2.Token, error) {
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
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
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
func (c *Core) Credentials() (source string, anonymous bool) {
	return c.source, c.sas == "" && c.key == nil && c.ts == nil
}

// Note is a warning about the credentials for the run's log ("" when
// none).
func (c *Core) Note() string { return c.note }

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
func (c *Core) Transports() int { return len(c.http) }

func (c *Core) pick() *http.Client { return c.http[c.Next()] }

// Answered is the number of requests the service answered usably
// (objhttp.Counters.Answered).
func (c *Core) Answered() int64 { return c.counts.Answered() }

// Live is the request account now with the limiter's current limit, for
// the progress sampler (objhttp.Live).
func (c *Core) Live() objhttp.Live {
	l := c.counts.Live()
	l.Limit = c.retry.Limiter.Limit()
	return l
}

// BodyCapShrinks is the number of times a response past the body cap made
// List halve maxresults (body_cap_shrinks).
func (c *Core) BodyCapShrinks() int64 { return c.bodyCapShrinks.Load() }

// RetryAfterCapped is the number of answers whose Retry-After was cut to
// the request timeout (retry_after_capped).
func (c *Core) RetryAfterCapped() int64 { return c.retryAfterCapped.Load() }

// Counters returns the request counters so far.
func (c *Core) Counters() Counters {
	n := c.counts.Load()
	n.Limiter = c.retry.Limiter.Counts()
	n.Rate = c.retry.Rate.Counts()
	return n
}

// Retry is the client's retry policy, for objhttp.Do.
func (c *Core) Retry() objhttp.Retry { return c.retry }

// Counts is the client's request account, for objhttp.Do and its own
// attempts.
func (c *Core) Counts() *objhttp.Counters { return &c.counts }

// Timeout is the per-attempt deadline (Config.RequestTimeout).
func (c *Core) Timeout() time.Duration { return c.cfg.RequestTimeout }

// BodyCap is the success body cap (MaxBody unless a test lowered it).
func (c *Core) BodyCap() int64 { return c.maxBody }

// SetBodyCap lowers the success body cap (tests).
func (c *Core) SetBodyCap(n int64) { c.maxBody = n }

// Pool is a copy of the transport pool: redirects never followed, a 3xx's
// Location dropped (noLocation).
func (c *Core) Pool() []*http.Client { return slices.Clone(c.http) }

// Next is the index of the pooled transport the next request takes
// (round-robin, as the client's own requests).
func (c *Core) Next() int { return int((c.next.Add(1) - 1) % uint64(len(c.http))) }

// Account is the storage account's name.
func (c *Core) Account() string { return c.cfg.Account }

// Base is the account's root URL (ending in '/'), without the SAS.
func (c *Core) Base() string { return c.base.String() }

// SAS is the shared access signature sent with every request, without
// '?'; "" when none.
func (c *Core) SAS() string { return c.sas }

// SharedKey is the account key requests are signed with; "" when the
// credential is another.
func (c *Core) SharedKey() string {
	if c.key == nil {
		return ""
	}
	return c.cfg.SharedKey
}

// Bearer reports Entra ID: every attempt carries a token (Token).
func (c *Core) Bearer() bool { return c.ts != nil }

// route is the URL of a container, without a query.
func (c *Core) route(container string) url.URL {
	u := *c.base
	u.Path += container
	u.RawPath = ""
	return u
}

// Route is the URL a container's requests go to, before the query (never
// the SAS).
func (c *Core) Route(container string) string {
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
// (objhttp.Do), and validates the page (ErrInvalidPage), its size bounded
// as Shrinking says.
func (c *Client) List(ctx context.Context, in *ListInput) (*ListOutput, error) {
	return c.Shrinking(ctx, in, c.list)
}

// Shrinking runs list (one logical List Blobs request: retries included)
// for in. A response past the body cap (list returns ErrBodyCap) is asked
// again with maxresults halved, the marker and query unchanged (the next
// page continues from the shorter one's NextMarker), down to 1; only a
// single entry past the cap is a parse failure. Each call starts again at
// the caller's page size.
func (c *Core) Shrinking(ctx context.Context, in *ListInput, list func(context.Context, *ListInput) (*ListOutput, error)) (*ListOutput, error) {
	req := *in // sent as the caller set it until a response passes the cap
	for {
		p, err := list(ctx, &req)
		if !errors.Is(err, ErrBodyCap) {
			return p, err
		}
		if req.MaxResults == 1 {
			c.counts.ParseFailure()
			return nil, fmt.Errorf("%w: one entry's page is larger than %d bytes", ErrSyntax, c.maxBody)
		}
		if req.MaxResults <= 0 || req.MaxResults > MaxPageSize {
			req.MaxResults = MaxPageSize // what the server answered with
		}
		req.MaxResults = max(req.MaxResults/2, 1)
		c.bodyCapShrinks.Add(1)
	}
}

// list is one logical List Blobs request at in's page size.
func (c *Client) list(ctx context.Context, in *ListInput) (*ListOutput, error) {
	u := c.listURL(in)
	return objhttp.Do(ctx, c.retry, &c.counts, func() (*ListOutput, error) {
		_, body, err := c.attempt(ctx, http.MethodGet, &u, c.maxBody)
		if err != nil {
			return nil, err
		}
		if err := faults.At("response-read"); err != nil {
			return nil, err
		}
		p, err := parse(ctx, body)
		if err != nil {
			if errors.Is(err, ErrSyntax) {
				c.counts.ParseFailure()
			}
			return nil, err
		}
		if err := p.Validate(in.Marker); err != nil {
			return nil, err
		}
		return p, nil
	})
}

// ErrHNSUnknown is an account-information answer without
// x-ms-is-hns-enabled: the namespace is unknown, never taken for flat.
var ErrHNSUnknown = errors.New("account information without " + headerHNSEnabled)

// AccountInfo sends Get Account Information on the container's path
// (restype=account&comp=properties, which a SAS scoped to the container
// may call) and reports whether the account has a hierarchical namespace.
// A refused call or a missing header is an error the caller takes for
// "unknown", never for flat.
func (c *Client) AccountInfo(ctx context.Context, container string) (bool, error) {
	u := c.route(container)
	u.RawQuery = encodeQuery(url.Values{"restype": {"account"}, "comp": {"properties"}})
	return objhttp.Do(ctx, c.retry, &c.counts, func() (bool, error) {
		hdr, _, err := c.attempt(ctx, http.MethodGet, &u, maxErrorBody)
		if err != nil {
			return false, err
		}
		switch strings.ToLower(hdr.Get(headerHNSEnabled)) {
		case "true":
			return true, nil
		case "false":
			return false, nil
		}
		return false, ErrHNSUnknown
	})
}

// ErrNamespaceUnknown is a Filesystem Get Properties answer without
// x-ms-namespace-enabled: unknown, never taken for flat.
var ErrNamespaceUnknown = errors.New("filesystem properties without " + headerNamespaceEnabled)

// ErrNoDFSEndpoint: the client's endpoint is not an account's blob host
// (ACCOUNT.blob.core.windows.net), so it has no Data Lake (DFS) host to
// ask.
var ErrNoDFSEndpoint = errors.New("no Data Lake endpoint: the blob endpoint is not ACCOUNT.blob.core.windows.net")

// dfsSuffix and blobSuffix are the public cloud's Data Lake and blob host
// suffixes.
const blobSuffix, dfsSuffix = ".blob.core.windows.net", ".dfs.core.windows.net"

// NamespaceInfo sends Filesystem Get Properties (HEAD
// https://ACCOUNT.dfs.core.windows.net/CONTAINER?resource=filesystem) and
// reports whether the account has a hierarchical namespace, from
// x-ms-namespace-enabled. It exists for anonymous admission: Azure answers
// an anonymous Get Account Information with 401, but tells an anonymous
// caller of a public container this header (checked live on 2026-11-08).
// The DFS host is the blob host with its blob label replaced, so only the
// exact default endpoint, https://ACCOUNT.blob.core.windows.net without a
// port or path, has one; any other endpoint answers ErrNoDFSEndpoint. Credentials, retries and classification are
// AccountInfo's; a refused call or a missing header is an error, never
// "flat".
func (c *Core) NamespaceInfo(ctx context.Context, container string) (bool, error) {
	if !c.defaultEndpoint() {
		return false, ErrNoDFSEndpoint
	}
	u := url.URL{Scheme: "https", Host: c.cfg.Account + dfsSuffix, Path: "/" + container,
		RawQuery: encodeQuery(url.Values{"resource": {"filesystem"}})}
	return objhttp.Do(ctx, c.retry, &c.counts, func() (bool, error) {
		hdr, _, err := c.attempt(ctx, http.MethodHead, &u, maxErrorBody)
		if err != nil {
			return false, err
		}
		switch strings.ToLower(hdr.Get(headerNamespaceEnabled)) {
		case "true":
			return true, nil
		case "false":
			return false, nil
		}
		return false, ErrNamespaceUnknown
	})
}

// defaultEndpoint: the client's endpoint is exactly the account's blob
// host, https://ACCOUNT.blob.core.windows.net with no port or path (the
// default, or that URL given explicitly), the only form with a Data Lake
// host beside it.
func (c *Core) defaultEndpoint() bool {
	if c.cfg.Endpoint == "" {
		return true
	}
	u, err := url.Parse(c.cfg.Endpoint)
	return err == nil && u.Scheme == "https" && strings.EqualFold(u.Host, c.cfg.Account+blobSuffix) &&
		(u.Path == "" || u.Path == "/") && u.User == nil
}

// attempt sends one request of u with method (GET or HEAD; the SAS
// appended on the wire only) and returns a 200's header and body, read up
// to limit bytes; any other answer is an *Error. Errors never carry the
// SAS.
func (c *Core) attempt(ctx context.Context, method string, u *url.URL, limit int64) (http.Header, []byte, error) {
	actx, cancel := context.WithTimeout(ctx, c.cfg.RequestTimeout)
	defer cancel()
	wire := *u
	if c.sas != "" {
		wire.RawQuery += "&" + c.sas
	}
	req, err := http.NewRequestWithContext(actx, method, wire.String(), nil)
	if err != nil {
		return nil, nil, c.Redact(err, u)
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
		tok, err := c.Token(actx)
		if err != nil {
			return nil, nil, err
		}
		h.Set(headerAuthorization, "Bearer "+tok.AccessToken)
	}
	defer c.counts.Attempt()()
	resp, err := c.pick().Do(req)
	if err != nil {
		return nil, nil, objhttp.Timeout(actx, ctx, c.cfg.RequestTimeout, c.Redact(err, u))
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		limit = maxErrorBody
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	if err != nil {
		return nil, nil, objhttp.Timeout(actx, ctx, c.cfg.RequestTimeout, c.Redact(err, u))
	}
	if resp.StatusCode != http.StatusOK {
		return nil, nil, c.ResponseError(resp, body, u)
	}
	if int64(len(body)) > limit {
		return nil, nil, ErrBodyCap
	}
	return resp.Header, body, nil
}

// ResponseError is a non-200 answer as an *Error: the code from
// x-ms-error-code or else the body, a redirect's target, a 400's query
// (without the SAS), the Retry-After wait.
func (c *Core) ResponseError(resp *http.Response, body []byte, u *url.URL) *Error {
	code, msg := errorBody(body)
	e := &Error{Status: resp.StatusCode, Code: resp.Header.Get(headerErrorCode), Message: msg,
		Wait: retryAfter(resp.Header.Get(headerRetryAfter), time.Now())}
	if e.Wait > c.cfg.RequestTimeout {
		// A wait past the attempt deadline the caller set holds a worker
		// while the engine sees no progress: it is cut to that deadline and
		// counted; the attempt budget still bounds the total.
		e.Wait = c.cfg.RequestTimeout
		c.retryAfterCapped.Add(1)
	}
	if e.Code == "" {
		e.Code = code
	}
	if e.Status/100 == 3 {
		e.Message = "redirect not followed" // its target is never quoted: it may carry the SAS
	}
	if e.Status == http.StatusBadRequest {
		e.Query = u.RawQuery
	}
	e.Message = c.scrub(e.Message)
	return e
}

// Redact keeps the SAS out of a request error: the URL net/http names
// is the one without it. It runs before any wrapping that flattens the
// error into text (objhttp.Timeout).
func (c *Core) Redact(err error, u *url.URL) error {
	if c.sas == "" || err == nil {
		return err
	}
	for e := err; e != nil; e = errors.Unwrap(e) {
		if ue, ok := e.(*url.Error); ok {
			ue.URL = u.String()
		}
	}
	msg := err.Error()
	if s := c.scrub(msg); s != msg {
		// Some layer still quotes the SAS: the error becomes its scrubbed
		// text, keeping only whether it is a transport failure (retried).
		if objhttp.Transport(err) {
			var ne net.Error
			timeout := errors.As(err, &ne) && ne.Timeout()
			return &scrubbedNetError{msg: s, timeout: timeout}
		}
		return errors.New(s)
	}
	return err
}

// scrubbedNetError is a transport failure whose text was scrubbed of the
// SAS.
type scrubbedNetError struct {
	msg     string
	timeout bool
}

func (e *scrubbedNetError) Error() string   { return e.msg }
func (e *scrubbedNetError) Timeout() bool   { return e.timeout }
func (e *scrubbedNetError) Temporary() bool { return false }

// scrub removes the SAS, and its signature as sent or decoded, from text
// the service or a redirect returned.
func (c *Core) scrub(s string) string {
	if s == "" {
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
	return sigParam.ReplaceAllString(s, "${1}REDACTED")
}

// sigParam is a SAS signature parameter in any text, as sent or escaped.
var sigParam = regexp.MustCompile(`(?i)((?:^|[?&;\s"'(])sig(?:=|%3D))[^&\s"'<>)]*`)

// requestID is a fresh x-ms-client-request-id: a random UUID.
func requestID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6], b[8] = b[6]&0x0f|0x40, b[8]&0x3f|0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[:4], b[4:6], b[6:8], b[8:10], b[10:])
}
