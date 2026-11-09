// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package client is the S3 client for the listing calls swath makes,
// ListObjectsV2 and ListObjectVersions, over the AWS SDK's service client
// (service/s3), with the provider's wire types (ListInput, Page,
// VersionPage).
//
// The SDK owns the request: serialization, endpoint resolution, signing,
// transport and the decoding of error answers. Its retryer is
// aws.NopRetryer: objhttp.Do is the one retry owner, and an attempt is an
// application attempt (net/http may still replay an idempotent GET on a
// reused connection below it). swath owns only what a 2xx answer means:
// objhttp.Guard, under an HTTP client that follows no redirect, bounds the
// body, reads a 2xx other than 200 as an answer, and captures a 200's body;
// the SDK's deserializer is handed a minimal envelope in its place, and
// this package's scanner decodes the captured body (parseList,
// parseVersions: the operation's envelope, whole, or ErrSyntax), as Java
// swath's StreamingListObjectsV2Interceptor does. Measured through the same
// stack over a 1,000-entry page, the SDK's deserializer cost about 14 µs
// per object and 100,000 allocations per page against the scanner's 1.8 µs
// per object and 410 per page; an encoding/xml token walk cost about 12 µs
// per object. The scanner also keeps <Version> and <DeleteMarker> entries
// in wire order, which the SDK's two lists cannot give back
// (TestStockSDKLosesVersionsOrder), and accepts XML forms the SDK's
// deserializer refuses (CDATA within text, spaces around numbers:
// xml_test.go); TestScannerConformsToSDKDecoder holds it to the SDK's
// decoder. Credentials are this package's resolution (loadAuth, auth.go),
// fetched before each attempt is counted.
package client

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsmiddleware "github.com/aws/aws-sdk-go-v2/aws/middleware"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/aws/smithy-go/logging"
	smithymiddleware "github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"

	"github.com/varveio/swath/go/internal/buildinfo"
	"github.com/varveio/swath/go/internal/faults"
	"github.com/varveio/swath/go/internal/source/objhttp"
	"github.com/varveio/swath/go/internal/source/tokensource"
)

// Config configures a Client. Zero values take the defaults.
type Config struct {
	Region         string        // else the SDK's resolution (AWS_REGION, profile), else us-east-1
	Endpoint       string        // custom endpoint URL (S3-compatible stores, tests)
	PathStyle      bool          // https://host/bucket instead of https://bucket.host/
	FIPS           bool          // s3-fips.REGION.amazonaws.com (AWS endpoints only)
	DualStack      bool          // s3.dualstack.REGION.amazonaws.com (IPv6; AWS endpoints only)
	Profile        string        // shared config profile (else AWS_PROFILE, else default)
	Anonymous      bool          // never sign
	RequestPayer   bool          // send x-amz-request-payer: requester (signed): the caller pays for a requester-pays bucket
	RequestTimeout time.Duration // per attempt, headers and body; default 30 s
	MaxAttempts    int           // default 9
	BaseBackoff    time.Duration // default 100 ms
	MaxBackoff     time.Duration // default 5 s
	HTTPClient     *http.Client
	// Limiter, if set, admits every attempt (objhttp.Limiter).
	Limiter *objhttp.Limiter
	// Rate, if set, caps attempts per second (objhttp.Rate).
	Rate *objhttp.Rate
	// Bearer, if set, authenticates every request with its token
	// ("Authorization: Bearer") instead of SigV4; no credential is looked up.
	Bearer *tokensource.Shared
}

// Counters are what the client did, for the run summary.
type Counters = objhttp.Counts

// Client lists S3 buckets. It is safe for concurrent use.
type Client struct {
	cfg    Config
	auth   *auth
	sdk    *awss3.Client
	retry  objhttp.Retry
	counts objhttp.Counters
}

// New resolves the region and credentials (loadAuth) and builds the SDK
// client; ctx bounds the configuration load and the first credential
// lookup, not later refreshes (each request's own context bounds those).
// FIPS and dual-stack select among AWS endpoints: with a custom endpoint
// they are not used.
func New(ctx context.Context, cfg Config) (*Client, error) {
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = 30 * time.Second
	}
	a, err := loadAuth(ctx, cfg)
	if err != nil {
		return nil, err
	}
	fips, dualStack := cfg.FIPS, cfg.DualStack
	var base *string
	if cfg.Endpoint != "" {
		u, err := url.Parse(cfg.Endpoint)
		if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
			return nil, fmt.Errorf("endpoint %q: want http(s)://host[:port][/path]", cfg.Endpoint)
		}
		base, fips, dualStack = aws.String(cfg.Endpoint), false, false
	}
	c := &Client{cfg: cfg, auth: a,
		retry: objhttp.Retry{MaxAttempts: cfg.MaxAttempts, BaseBackoff: cfg.BaseBackoff, MaxBackoff: cfg.MaxBackoff,
			Limiter: cfg.Limiter, Rate: cfg.Rate, Answer: answer},
	}
	c.retry.Defaults()

	guard := &objhttp.Guard{Validate: validate}
	if a.bearer != nil {
		guard.Header = bearerHeader
	}
	// No redirect is followed, a supplied client's included (a copy): a 3xx
	// is an answer. A plain *http.Client is used as given: the SDK wraps
	// only its own BuildableClient (service/s3 api_client.go
	// resolveHTTPClient).
	var hc http.Client
	if cfg.HTTPClient != nil {
		hc = *cfg.HTTPClient
		guard.Next = hc.Transport
	} else {
		t := http.DefaultTransport.(*http.Transport).Clone()
		t.MaxIdleConns, t.MaxIdleConnsPerHost = 0, 1024 // one keep-alive connection per worker
		guard.Next = t
	}
	hc.Transport = guard
	hc.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }

	c.sdk = awss3.New(awss3.Options{
		Region:       a.region,
		Credentials:  aws.AnonymousCredentials{}, // a signed attempt supplies its own (attempt)
		HTTPClient:   &hc,
		Retryer:      aws.NopRetryer{},
		BaseEndpoint: base,
		UsePathStyle: cfg.PathStyle,
		EndpointOptions: awss3.EndpointResolverOptions{
			UseFIPSEndpoint:      endpointState[aws.FIPSEndpointState](fips, aws.FIPSEndpointStateEnabled, aws.FIPSEndpointStateDisabled),
			UseDualStackEndpoint: endpointState[aws.DualStackEndpointState](dualStack, aws.DualStackEndpointStateEnabled, aws.DualStackEndpointStateDisabled),
		},
		Logger:                      logging.Nop{},
		DisableS3ExpressSessionAuth: aws.Bool(true), // directory buckets are refused before any request
		RequestChecksumCalculation:  aws.RequestChecksumCalculationWhenRequired,
		ResponseChecksumValidation:  aws.ResponseChecksumValidationWhenRequired,
		APIOptions:                  []func(*smithymiddleware.Stack) error{prependUserAgent, allowGzip},
	})
	return c, nil
}

// prependUserAgent runs after the SDK builds its header: its AddUserAgent
// helpers append products, and AddUserAgentKey also replaces the slash.
func prependUserAgent(stack *smithymiddleware.Stack) error {
	token := buildinfo.UserAgent()
	return stack.Build.Insert(smithymiddleware.BuildMiddlewareFunc("SwathUserAgent", func(ctx context.Context, in smithymiddleware.BuildInput, next smithymiddleware.BuildHandler) (
		smithymiddleware.BuildOutput, smithymiddleware.Metadata, error,
	) {
		req, ok := in.Request.(*smithyhttp.Request)
		if !ok {
			return smithymiddleware.BuildOutput{}, smithymiddleware.Metadata{}, fmt.Errorf("User-Agent: unexpected request %T", in.Request)
		}
		req.Header.Set("User-Agent", token+" "+req.Header.Get("User-Agent"))
		return next.HandleBuild(ctx, in)
	}), (&awsmiddleware.RequestUserAgent{}).ID(), smithymiddleware.After)
}

func endpointState[S any](on bool, enabled, disabled S) S {
	if on {
		return enabled
	}
	return disabled
}

// allowGzip removes the SDK's "Accept-Encoding: identity", so net/http
// asks for gzip and decompresses: a body that does not decompress is a
// read failure (class other), not a garbled document.
func allowGzip(stack *smithymiddleware.Stack) error {
	_, err := stack.Finalize.Remove("DisableAcceptEncodingGzip")
	return err
}

// Credentials returns the resolved credentials' source (never the secret).
func (c *Client) Credentials() (source string, anonymous bool) {
	return c.auth.source, c.auth.anonymous
}

// Note is a one-time warning for the user, or "".
func (c *Client) Note() string { return c.auth.note }

// Region is the signing region: as given, else as the environment or the
// profile resolves it, else us-east-1.
func (c *Client) Region() string { return c.auth.region }

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

// The minimal envelopes the SDK decodes in place of a captured body.
var (
	emptyList     = []byte(`<?xml version="1.0" encoding="UTF-8"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></ListBucketResult>`)
	emptyVersions = []byte(`<?xml version="1.0" encoding="UTF-8"?><ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></ListVersionsResult>`)
)

// ListObjectsV2 sends one ListObjectsV2 request, retrying retryable
// failures with full-jitter exponential backoff (objhttp.Do).
func (c *Client) ListObjectsV2(ctx context.Context, in *ListInput) (*Page, error) {
	req := &awss3.ListObjectsV2Input{Bucket: aws.String(in.Bucket), EncodingType: types.EncodingTypeUrl,
		Prefix: str(in.Prefix), StartAfter: str(in.StartAfter), ContinuationToken: str(in.ContinuationToken), Delimiter: str(in.Delimiter)}
	if in.MaxKeys > 0 {
		req.MaxKeys = aws.Int32(int32(in.MaxKeys))
	}
	if in.FetchOwner {
		req.FetchOwner = aws.Bool(true)
	}
	if c.cfg.RequestPayer {
		req.RequestPayer = types.RequestPayerRequester
	}
	return objhttp.Do(ctx, c.retry, &c.counts, func() (*Page, error) {
		return attempt(c, ctx, emptyList, parseList, func(actx context.Context, opts ...func(*awss3.Options)) error {
			_, err := c.sdk.ListObjectsV2(actx, req, opts...)
			return err
		})
	})
}

// ListObjectVersions sends one ListObjectVersions request on
// ListObjectsV2's path: the same signing, admission, retries and
// classification. A 200 that is not a ListVersionsResult is a parse
// failure (retried); a truncated page without a NextKeyMarker is an
// invalid page (ErrInvalidPage, not retried).
func (c *Client) ListObjectVersions(ctx context.Context, in *VersionsInput) (*VersionPage, error) {
	if in.VersionIDMarker != "" && in.KeyMarker == "" {
		return nil, errors.New("ListObjectVersions: a version-id marker without a key marker")
	}
	req := &awss3.ListObjectVersionsInput{Bucket: aws.String(in.Bucket), EncodingType: types.EncodingTypeUrl,
		Prefix: str(in.Prefix), Delimiter: str(in.Delimiter), KeyMarker: str(in.KeyMarker)}
	if in.KeyMarker != "" {
		req.VersionIdMarker = str(in.VersionIDMarker)
	}
	if in.MaxKeys > 0 {
		req.MaxKeys = aws.Int32(int32(in.MaxKeys))
	}
	if c.cfg.RequestPayer {
		req.RequestPayer = types.RequestPayerRequester
	}
	return objhttp.Do(ctx, c.retry, &c.counts, func() (*VersionPage, error) {
		return attempt(c, ctx, emptyVersions, parseVersions, func(actx context.Context, opts ...func(*awss3.Options)) error {
			_, err := c.sdk.ListObjectVersions(actx, req, opts...)
			return err
		})
	})
}

func str(s string) *string {
	if s == "" {
		return nil
	}
	return aws.String(s)
}

type tokenKey struct{}

// bearerHeader is the Guard's Header in bearer mode: the token attempt
// fetched before the request was counted, carried by its context.
func bearerHeader(req *http.Request) error {
	tok, ok := req.Context().Value(tokenKey{}).(string)
	if !ok {
		return &objhttp.CredentialError{Source: "--bearer-token-command", Err: errors.New("no token for the request")}
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	return nil
}

// attempt runs one SDK call under the request timeout. What the request
// needs (the credentials, a bearer token) is fetched first, once: a failure
// there is a *objhttp.CredentialError and not an attempt, and the call
// signs with what was fetched (no second lookup inside the counted call).
// The attempt is counted from just before the call until it returns (the
// Guard has read and checked the body by then). The Guard captures a 200's
// body and hands the SDK replace in its place; parse decodes the captured
// body.
func attempt[T any](c *Client, ctx context.Context, replace []byte, parse func([]byte) (*T, error), call func(context.Context, ...func(*awss3.Options)) error) (*T, error) {
	actx, cancel := context.WithTimeout(ctx, c.cfg.RequestTimeout)
	defer cancel()
	tok, creds, err := c.auth.ready(actx)
	if err != nil {
		return nil, err
	}
	var opts []func(*awss3.Options)
	if c.auth.credentials != nil {
		opts = append(opts, func(o *awss3.Options) {
			o.Credentials = aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) { return creds, nil })
		})
	}
	cctx := actx
	if c.auth.bearer != nil {
		cctx = context.WithValue(cctx, tokenKey{}, tok)
	}
	capture := &objhttp.Capture{Replace: replace}
	cctx = objhttp.WithCapture(cctx, capture)
	done := c.counts.Attempt()
	err = call(cctx, opts...)
	done()
	if err == nil {
		err = faults.At("response-read")
	}
	var v *T
	if err == nil {
		v, err = parse(capture.Body)
	}
	if err != nil {
		err = c.failure(actx, ctx, err)
		if errors.Is(err, objhttp.ErrSyntax) {
			c.counts.ParseFailure()
		}
		return nil, err
	}
	return v, nil
}

// failure maps an attempt's error to the provider's vocabulary: a Guard
// failure is its cause (ErrSyntax, a 2xx answer other than 200, the size
// cap, a credential error, a failed 2xx body read: a cut body is
// transport, a read past the attempt's deadline ErrTimeout); an HTTP
// answer other than 2xx is a *Error, its body cut or not (the Guard hands
// up the bytes read, and the status classifies it); a 2xx body the SDK
// could not decode is ErrSyntax; the
// scanner's ErrSyntax and ErrInvalidPage stand; a transport failure stays
// one (an attempt timeout is ErrTimeout).
func (c *Client) failure(actx, ctx context.Context, err error) error {
	var ge *objhttp.GuardError
	if errors.As(err, &ge) {
		return objhttp.Timeout(actx, ctx, c.cfg.RequestTimeout, ge.Err)
	}
	var re *awshttp.ResponseError
	if errors.As(err, &re) && re.HTTPStatusCode() != 0 { // 0: no answer (the SDK wraps a send failure too)
		if s := re.HTTPStatusCode(); s < 200 || s > 299 {
			return responseError(re, err)
		}
		var de *smithy.DeserializationError
		if errors.As(err, &de) {
			return fmt.Errorf("%w: %v", objhttp.ErrSyntax, de)
		}
		return err
	}
	if errors.Is(err, objhttp.ErrSyntax) || errors.Is(err, objhttp.ErrInvalidPage) {
		return err
	}
	return objhttp.Timeout(actx, ctx, c.cfg.RequestTimeout, err)
}

// responseError is an HTTP answer's *Error: the status, the SDK's decoded
// code and message (the status text without spaces when the body named no
// code), the request ID and, for a redirect, the bucket's region
// (X-Amz-Bucket-Region).
func responseError(re *awshttp.ResponseError, err error) *Error {
	status := re.HTTPStatusCode()
	e := &Error{Status: status, RequestID: re.RequestID}
	if e.RequestID == "" {
		e.RequestID = re.Response.Header.Get("X-Amz-Request-Id")
	}
	var ae smithy.APIError
	if errors.As(err, &ae) && ae.ErrorCode() != "UnknownError" { // the SDK's name for a body without a code
		e.Code, e.Message = ae.ErrorCode(), ae.ErrorMessage()
	}
	if e.Code == "" {
		e.Code = statusCode(status)
	}
	if status == http.StatusMovedPermanently || e.Code == "AuthorizationHeaderMalformed" || e.Code == "PermanentRedirect" {
		e.Region = re.Response.Header.Get("X-Amz-Bucket-Region")
	}
	return e
}

// statusCode is the code of an answer whose body names none: the status
// text without spaces.
func statusCode(status int) string { return strings.ReplaceAll(http.StatusText(status), " ", "") }

// validate is the Guard's check of a 2xx answer. One other than 200 is an
// answer: the code, message and request ID an <Error> body names, else the
// status text as the code (the response headers are not seen here, so no
// request ID or region from them). A 200's body is the scanner's to check:
// it requires the operation's envelope and refuses an empty or cut
// document (ErrSyntax, retried, never an empty final page).
func validate(_ *http.Request, status int, body []byte) error {
	if status == http.StatusOK {
		return nil
	}
	e := &Error{Status: status}
	if code, msg, id, ok := parseError(body); ok {
		e.Code, e.Message, e.RequestID = code, msg, id
	}
	if e.Code == "" {
		e.Code = statusCode(status)
	}
	return e
}
