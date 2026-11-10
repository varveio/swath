// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package client is swath's Azure Blob Storage client: List Blobs (flat or
// with a delimiter) and Get Account Information through the Azure SDK's
// service and container clients (list.go), and, on the account's Data
// Lake host, Filesystem Get Properties (NamespaceInfo, which exists for
// anonymous admission: only it tells an anonymous caller whether the
// namespace is flat), authenticated by a SAS, an account key (Shared Key,
// signed by the SDK), Entra ID tokens from azidentity's
// DefaultAzureCredential, or nothing, with counted retries and typed
// errors from objhttp.
//
// Entra ID tokens live as long as the client: they are fetched by a token
// source whose HTTP client bounds each exchange by the request timeout, and
// every attempt waits for its token under its own attempt deadline, all
// waiters sharing one outstanding call (tokensource.Shared). Requests go
// unauthenticated only with Anonymous; a credential that is configured but
// will not load, or a DefaultAzureCredential chain that finds none, is a
// *objhttp.CredentialError, never anonymity.
package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"golang.org/x/oauth2"

	"github.com/varveio/swath/go/internal/buildinfo"
	"github.com/varveio/swath/go/internal/source"
	"github.com/varveio/swath/go/internal/source/objhttp"
	"github.com/varveio/swath/go/internal/source/tokensource"
)

// Version is the service version every request names (x-ms-version):
// the newer of the two the live account and the replay answer, which
// behave the same for listing.
const Version = "2026-10-06"

// storageScope is the Entra ID scope of Azure Storage.
const storageScope = "https://storage.azure.com/.default"

// accountName is a storage account name: 3 to 24 lower-case letters and
// digits.
var accountName = regexp.MustCompile(`^[a-z0-9]{3,24}$`)

// Config configures a Client. Zero values take the defaults.
type Config struct {
	objhttp.Retry
	// Endpoint is the blob service base URL; default
	// https://ACCOUNT.blob.core.windows.net. An explicit endpoint (an
	// emulator, the replay server) takes the account as its first path
	// segment: ENDPOINT/ACCOUNT/CONTAINER.
	Endpoint string
	Account  string
	// Anonymous sends no credentials, whatever is configured.
	Anonymous bool
	// SAS is a shared access signature, a query string with or without a
	// leading '?', appended to every request and kept out of every error.
	SAS string
	// SharedKey is the account key (base64): the SDK signs requests.
	SharedKey      string
	RequestTimeout time.Duration // per attempt; default 30 s
	HTTPClient     *http.Client
	// Workers sizes the built transport pool (objhttp.Pool); a supplied
	// HTTPClient is used alone.
	Workers int
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

// Client lists Azure Blob containers; safe for concurrent use. It holds
// the account's root, the transport pool with one SDK service client per
// transport, the credentials, the retry policy and the counters.
type Client struct {
	cfg  Config
	base *url.URL          // ends in '/': the account's root
	http []*http.Client    // the pool: redirects never followed, Location dropped
	svc  []*service.Client // one per pooled transport, the same order
	next atomic.Uint64

	sas    string              // without '?'; "" when none
	sasKey *regexp.Regexp      // any parameter of the SAS in text (sasParams); nil when none
	key    string              // the account key; "" when the credential is another
	ts     *tokensource.Shared // nil: no bearer token
	source string
	note   string

	*objhttp.Meter
	retryAfterCapped atomic.Int64
}

// MaxBody is the body cap: the memory one in-flight response may hold.
// Responses are read whole and sit outside the engine's memory budget, so
// the cap is fixed, not derived from the page size. A listing page past it
// fails the request (ErrBodyCap): Azure answers at most 5,000 entries,
// which stay far below it.
const MaxBody = 64 << 20

// ErrBodyCap is a 200 whose body passed the cap: a terminal failure, not
// retried, not asked again with fewer entries.
var ErrBodyCap = errors.New("response larger than the body cap")

// New resolves credentials and builds the transport pool and the SDK
// clients over it.
func New(ctx context.Context, cfg Config) (*Client, error) {
	return newClient(ctx, cfg, build{bodyCap: MaxBody, entra: defaultChain})
}

// build is what New fixes and tests vary: the success body cap and the
// source of the Entra ID credential.
type build struct {
	bodyCap int64
	entra   entraSource
}

// entraSource builds the Entra ID credential: azidentity's
// DefaultAzureCredential (defaultChain), or in tests a fixed one, so what
// the client does with each outcome does not depend on the host.
type entraSource func(*azidentity.DefaultAzureCredentialOptions) (azcore.TokenCredential, error)

func defaultChain(o *azidentity.DefaultAzureCredentialOptions) (azcore.TokenCredential, error) {
	return azidentity.NewDefaultAzureCredential(o)
}

// newClient is New with its body cap and Entra ID source given.
func newClient(ctx context.Context, cfg Config, b build) (*Client, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = source.DefaultRequestTimeout
	}
	if !accountName.MatchString(cfg.Account) {
		shown := cfg.Account
		if strings.ContainsAny(shown, "@?#:") {
			shown = "[redacted]" // a mistyped root can put a credential here
		}
		return nil, fmt.Errorf("account %q: want 3 to 24 lower-case letters and digits", shown)
	}
	base, err := baseURL(cfg.Endpoint, cfg.Account)
	if err != nil {
		return nil, err
	}
	c := &Client{cfg: cfg, base: base,
		Meter: objhttp.NewMeter(cfg.Retry)}
	// Redirects are answers, never followed (objhttp.Pool). The Location
	// header is removed before net/http sees it (noLocation): a malformed
	// one would otherwise end the request in an error that quotes it, SAS
	// included, before CheckRedirect runs.
	c.http = objhttp.Pool(cfg.Workers, cfg.HTTPClient, func(next http.RoundTripper) http.RoundTripper { return noLocation{next} })
	if err := c.credentials(b.entra); err != nil {
		return nil, err
	}
	if err := c.services(b.bodyCap); err != nil {
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
		return &url.URL{Scheme: "https", Host: account + blobSuffix, Path: "/"}, nil
	}
	u, err := objhttp.ParseEndpoint(endpoint, "http(s)://host[:port][/path]")
	if err != nil {
		return nil, err
	}
	u.Path = strings.TrimSuffix(u.Path, "/") + "/" + account + "/"
	u.RawPath = ""
	return u, nil
}

// credentials resolves the credential in precedence order: anonymous, a
// SAS, a shared key, then Entra ID through entra.
func (c *Client) credentials(entra entraSource) error {
	cfg := c.cfg
	switch {
	case cfg.Anonymous:
		c.source, c.note = "anonymous", anonymousNote(cfg)
	case cfg.SAS != "":
		c.sas = strings.TrimPrefix(cfg.SAS, "?")
		c.source = "shared access signature"
		if _, err := url.ParseQuery(c.sas); err != nil || c.sas == "" {
			// the parse error would quote the signature
			return &objhttp.CredentialError{Source: c.source, Err: errors.New("not a query string")}
		}
		c.sasKey = sasParams(c.sas)
	case cfg.SharedKey != "":
		c.source = "shared key"
		// The SDK signs; services refuses a key that is not base64 (its
		// error does not quote the key).
		c.key = cfg.SharedKey
	default:
		return c.entra(entra)
	}
	return nil
}

// entraVars are the environment variables by which DefaultAzureCredential
// is configured (environment, workload identity and managed identity
// credentials, and its own selector): Anonymous ignores them (anonymousNote).
var entraVars = []string{"AZURE_TENANT_ID", "AZURE_CLIENT_ID", "AZURE_CLIENT_SECRET", "AZURE_CLIENT_CERTIFICATE_PATH",
	"AZURE_USERNAME", "AZURE_FEDERATED_TOKEN_FILE", "IDENTITY_ENDPOINT", "MSI_ENDPOINT", "AZURE_TOKEN_CREDENTIALS"}

// entra sets up Entra ID through DefaultAzureCredential, its chain as the
// SDK builds it (environment, workload identity, managed identity, the
// Azure CLI and Developer CLI logins). Its tokens are fetched per attempt,
// before the attempt is sent: a chain that finds no credential fails the
// first request with azidentity's own error (class auth), and nothing is
// listed; a token endpoint's transient failure is retried.
func (c *Client) entra(entra entraSource) error {
	const source = "Entra ID (DefaultAzureCredential)"
	cred, err := entra(&azidentity.DefaultAzureCredentialOptions{ClientOptions: azcore.ClientOptions{
		// Each token exchange is one attempt: the listing's retry loop
		// repeats it, not azcore's.
		Transport: c.tokenHTTP(), Retry: policy.RetryOptions{MaxRetries: -1}}})
	if err != nil {
		return &objhttp.CredentialError{Source: source, Err: err}
	}
	c.ts, c.source = c.shared(cred), source
	return nil
}

// tokenHTTP is the HTTP client of token exchanges: the first pooled
// transport. Each exchange is bounded by the token call's context
// (credentialSource), the one deadline whose expiry is classified.
func (c *Client) tokenHTTP() *http.Client {
	return &http.Client{Transport: c.http[0].Transport}
}

// shared wraps cred as the client's coalesced token source.
func (c *Client) shared(cred azcore.TokenCredential) *tokensource.Shared {
	return tokensource.NewShared(credentialSource{cred: cred, timeout: c.cfg.RequestTimeout})
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
	configured := cfg.SAS != "" || cfg.SharedKey != "" || os.Getenv("AZURE_STORAGE_KEY") != "" || os.Getenv("AZURE_STORAGE_SAS_TOKEN") != ""
	for _, v := range entraVars {
		configured = configured || os.Getenv(v) != ""
	}
	if configured {
		return "anonymous: ignoring the configured Azure credentials"
	}
	return ""
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
		return objhttp.TransientStatus(af.RawResponse.StatusCode)
	}
	return objhttp.Transport(err)
}

// Credentials names where the credentials came from (never the secret).
func (c *Client) Credentials() (source string, anonymous bool) {
	return c.source, c.sas == "" && c.key == "" && c.ts == nil
}

// Note is a warning about the credentials for the run's log ("" when
// none).
func (c *Client) Note() string { return c.note }

// Transports is the size of the client's transport pool (1 with a supplied
// HTTPClient): the report's transport.pool_size.
func (c *Client) Transports() int { return len(c.http) }

// pick is the index of the pooled transport the next request takes
// (round-robin).
func (c *Client) pick() int { return int((c.next.Add(1) - 1) % uint64(len(c.http))) }

// RetryAfterCapped is the number of answers whose Retry-After was cut to
// the request timeout (retry_after_capped).
func (c *Client) RetryAfterCapped() int64 { return c.retryAfterCapped.Load() }

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

// ErrHNSUnknown is an account-information answer without
// x-ms-is-hns-enabled: the namespace is unknown, never taken for flat.
var ErrHNSUnknown = errors.New("account information without " + headerHNSEnabled)

// hns reads an account-information answer's x-ms-is-hns-enabled exactly as
// Azure sends it: "true" is a hierarchical namespace, "false" a flat one,
// anything else (absent, another spelling) ErrHNSUnknown, never flat.
func hns(h http.Header) (bool, error) {
	switch h.Get(headerHNSEnabled) {
	case "true":
		return true, nil
	case "false":
		return false, nil
	}
	return false, ErrHNSUnknown
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
// There is no azblob call for it: it is one plain HEAD, with the SAS or the
// Entra ID token. The DFS host is the blob host with its blob label
// replaced, so only the exact default endpoint,
// https://ACCOUNT.blob.core.windows.net without a port or path, has one;
// any other endpoint answers ErrNoDFSEndpoint, and so does Shared Key,
// which this request is not signed with (a key Get Account Information
// refused would be refused here too). Retries and classification are
// AccountInfo's; a refused call or a missing header is an error, never
// "flat".
func (c *Client) NamespaceInfo(ctx context.Context, container string) (bool, error) {
	if !c.defaultEndpoint() {
		return false, ErrNoDFSEndpoint
	}
	if c.key != "" {
		return false, fmt.Errorf("%w (Shared Key: not asked)", ErrNoDFSEndpoint)
	}
	u := url.URL{Scheme: "https", Host: c.cfg.Account + dfsSuffix, Path: "/" + container,
		RawQuery: encodeQuery(url.Values{"resource": {"filesystem"}})}
	return objhttp.Do(ctx, c.Retry, &c.Tally, func() (bool, error) {
		hdr, err := c.head(ctx, &u)
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
func (c *Client) defaultEndpoint() bool {
	if c.cfg.Endpoint == "" {
		return true
	}
	u, err := url.Parse(c.cfg.Endpoint)
	return err == nil && u.Scheme == "https" && strings.EqualFold(u.Host, c.cfg.Account+blobSuffix) &&
		(u.Path == "" || u.Path == "/") && u.User == nil
}

// head sends one HEAD of u (the SAS appended on the wire only) and returns
// a 200's header; any other answer is an *Error. Errors never carry the
// SAS.
func (c *Client) head(ctx context.Context, u *url.URL) (http.Header, error) {
	actx, cancel := context.WithTimeout(ctx, c.cfg.RequestTimeout)
	defer cancel()
	wire := *u
	if c.sas != "" {
		wire.RawQuery += "&" + c.sas
	}
	req, err := http.NewRequestWithContext(actx, http.MethodHead, wire.String(), nil)
	if err != nil {
		return nil, c.redact(err, u)
	}
	req.Header.Set(headerVersion, Version)
	req.Header.Set(headerUserAgent, buildinfo.UserAgent())
	if c.ts != nil {
		tok, err := objhttp.WaitToken(actx, c.ts, c.source, c.cfg.RequestTimeout, transientToken)
		if err != nil {
			return nil, err
		}
		req.Header.Set(headerAuthorization, "Bearer "+tok.AccessToken)
	}
	defer c.Tally.Attempt()()
	resp, err := c.http[c.pick()].Do(req)
	if err != nil {
		return nil, objhttp.Timeout(actx, ctx, c.cfg.RequestTimeout, c.redact(err, u))
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxErrorBody))
	if err != nil {
		return nil, objhttp.Timeout(actx, ctx, c.cfg.RequestTimeout, c.redact(err, u))
	}
	if resp.StatusCode != http.StatusOK {
		// a HEAD answer has no body: its code is the header's
		return nil, c.responseError(resp, resp.Header.Get(headerErrorCode), body, u)
	}
	return resp.Header, nil
}

// responseError is a non-200 answer as an *Error: the header code, else
// decoded XML code, else the SDK code (which also reads JSON); the message
// from the bounded XML body, both scrubbed of the SAS; a
// redirect's target withheld, a 400's query (without the SAS), the
// Retry-After wait.
func (c *Client) responseError(resp *http.Response, code string, body []byte, u *url.URL) *Error {
	decoded, message := errorDocument(body)
	if header := resp.Header.Get(headerErrorCode); header != "" {
		code = header
	} else if decoded != "" {
		code = decoded
	}
	e := &Error{Status: resp.StatusCode, Code: c.scrub(code), Message: message,
		Wait: retryAfter(resp.Header.Get(headerRetryAfter), time.Now())}
	if len(e.Code) > maxErrorBody {
		e.Code = e.Code[:maxErrorBody]
	}
	if e.Wait > c.cfg.RequestTimeout {
		// A wait past the attempt deadline the caller set holds a worker
		// while the engine sees no progress: it is cut to that deadline and
		// counted; the attempt budget still bounds the total.
		e.Wait = c.cfg.RequestTimeout
		c.retryAfterCapped.Add(1)
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

// redact keeps the SAS out of a request error: the URL net/http names
// is the one without it. It runs before any wrapping that flattens the
// error into text (objhttp.Timeout).
func (c *Client) redact(err error, u *url.URL) error {
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

// scrub removes the SAS from text the service, a redirect or the SDK's
// decoder returned: the SAS as given, its signature as sent or decoded,
// and the value of every parameter the SAS names, wherever it stands in
// the text. The SDK sorts the SAS's parameters in among the request's and
// re-encodes them, so the SAS is not found as one string: each parameter
// is found by name (sasParams), its value replaced by REDACTED, and the
// rest of the text kept.
func (c *Client) scrub(s string) string {
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
	re := c.sasKey
	if re == nil {
		re = sigParam
	}
	return re.ReplaceAllString(s, "${1}REDACTED")
}

// sasParams matches, in any text, a parameter the SAS names (and sig,
// always) with its value: the name as given or escaped, in any case, after
// the start of the text, a query or text separator, or an escaped '&' or
// '?'; then '=' or its escape %3D; then the value (sasValue). That covers
// the query as sent (in any order), decoded, and escaped once more (a
// query quoted inside another URL).
func sasParams(sas string) *regexp.Regexp {
	names := map[string]bool{"sig": true}
	if q, err := url.ParseQuery(sas); err == nil {
		for k := range q {
			if k != "" {
				names[k] = true
				names[url.QueryEscape(k)] = true
			}
		}
	}
	alts := make([]string, 0, len(names))
	for k := range names {
		alts = append(alts, regexp.QuoteMeta(k))
	}
	slices.Sort(alts) // a stable pattern
	return regexp.MustCompile(`(?i)((?:^|[?&;\s"'(]|%26|%3F)(?:` + strings.Join(alts, "|") + `)(?:=|%3D))` + sasValue)
}

// sasValue is a parameter's value in text: anything up to '&', an escaped
// '&' (%26), a space, a quote or a bracket.
const sasValue = `(?:[^&%\s"'<>)]|%(?:[013-9A-Fa-f][0-9A-Fa-f]|2[0-57-9A-Fa-f]))*`

// sigParam is the SAS signature parameter in any text (sasParams for a
// client without a SAS).
var sigParam = sasParams("")
