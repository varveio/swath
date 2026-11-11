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
// parseVersions: the operation's envelope, whole, or objhttp.ErrSyntax), as Java
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
	"os"
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
	"github.com/varveio/swath/go/internal/source"
	"github.com/varveio/swath/go/internal/source/objhttp"
	"github.com/varveio/swath/go/internal/source/tokensource"
)

// Config configures a Client. Zero values take the defaults.
type Config struct {
	objhttp.Retry
	Region         string        // else the SDK's resolution (AWS_REGION, profile), else us-east-1
	Endpoint       string        // custom endpoint URL (S3-compatible stores, tests)
	PathStyle      bool          // https://host/bucket instead of https://bucket.host/
	FIPS           bool          // s3-fips.REGION.amazonaws.com (AWS endpoints only)
	DualStack      bool          // s3.dualstack.REGION.amazonaws.com (IPv6; AWS endpoints only)
	Profile        string        // shared config profile (else AWS_PROFILE, else default)
	Anonymous      bool          // never sign
	RequestPayer   bool          // send x-amz-request-payer: requester (signed): the caller pays for a requester-pays bucket
	RequestTimeout time.Duration // per attempt, headers and body; default 30 s
	// Trust verifies the service's certificate; its CA bundle defaults to
	// AWS_CA_BUNDLE's, as the AWS CLI's. A supplied HTTPClient keeps its own.
	Trust      source.Trust
	HTTPClient *http.Client
	// Bearer, if set, authenticates every request with its token
	// ("Authorization: Bearer") instead of SigV4; no credential is looked up.
	Bearer *tokensource.Shared
}

// Client lists S3 buckets. It is safe for concurrent use.
type Client struct {
	cfg  Config
	auth *auth
	sdk  *awss3.Client
	*objhttp.Meter
}

// New resolves the region and credentials (loadAuth) and builds the SDK
// client; ctx bounds the configuration load and the first credential
// lookup, not later refreshes (each request's own context bounds those).
// FIPS and dual-stack select among AWS endpoints: with a custom endpoint
// they are not used.
func New(ctx context.Context, cfg Config) (*Client, error) {
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = source.DefaultRequestTimeout
	}
	if cfg.Trust.CABundle == "" {
		cfg.Trust.CABundle = os.Getenv("AWS_CA_BUNDLE")
	}
	trust, err := cfg.Trust.Config()
	if err != nil {
		return nil, err
	}
	a, err := loadAuth(ctx, cfg)
	if err != nil {
		return nil, err
	}
	fips, dualStack := cfg.FIPS, cfg.DualStack
	var base *string
	if cfg.Endpoint != "" {
		if _, err := objhttp.ParseEndpoint(cfg.Endpoint, "http(s)://host[:port][/path]"); err != nil {
			return nil, err
		}
		base, fips, dualStack = aws.String(cfg.Endpoint), false, false
	}
	c := &Client{cfg: cfg, auth: a,
		Meter: objhttp.NewMeter(cfg.Retry),
	}

	hc := objhttp.Pool(1, cfg.HTTPClient, trust, func(next http.RoundTripper) http.RoundTripper {
		guard := &objhttp.Guard{Next: next, Validate: validate}
		if a.bearer != nil {
			guard.Header = bearerHeader
		}
		return guard
	})[0]

	c.sdk = awss3.New(awss3.Options{
		Region:       a.region,
		Credentials:  aws.AnonymousCredentials{}, // a signed attempt supplies its own (attempt)
		HTTPClient:   hc,
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
		APIOptions:                  []func(*smithymiddleware.Stack) error{prependUserAgent(buildinfo.UserAgent()), allowGzip},
	})
	return c, nil
}

// prependUserAgent puts swath's token first, after the SDK builds its
// header: its AddUserAgent helpers append products, and AddUserAgentKey
// also replaces the slash. The token is read once per client: API options
// run on every call, and reading the build info parses it (about 11 µs a
// request, BenchmarkDecode).
func prependUserAgent(token string) func(*smithymiddleware.Stack) error {
	return func(stack *smithymiddleware.Stack) error { return insertUserAgent(stack, token) }
}

func insertUserAgent(stack *smithymiddleware.Stack, token string) error {
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
	return objhttp.Do(ctx, c.Retry, &c.Tally, func(ctx context.Context) (*Page, error) {
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
// invalid page (objhttp.ErrInvalidPage, not retried).
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
	return objhttp.Do(ctx, c.Retry, &c.Tally, func(ctx context.Context) (*VersionPage, error) {
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
		return &objhttp.CredentialError{Source: "--s3-bearer-token-command", Err: errors.New("no token for the request")}
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
	done := c.Tally.Attempt()
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
		return nil, err
	}
	return v, nil
}

// failure maps an attempt's error to safe provider diagnostics. A Guard
// failure keeps its marker for objhttp.Normalize to recover the cause:
// syntax, a non-200 2xx answer, the cap, credentials or a failed body read.
// An HTTP answer outside 2xx becomes an *Error even when its body is cut;
// the status classifies it. The scanner's syntax and invalid-page errors
// stand, and a transport failure stays one. An attempt timeout is ErrTimeout.
func (c *Client) failure(actx, ctx context.Context, err error) error {
	var ge *objhttp.GuardError
	if errors.As(err, &ge) {
		return objhttp.Timeout(actx, ctx, c.cfg.RequestTimeout, ge)
	}
	var re *awshttp.ResponseError
	if errors.As(err, &re) && re.HTTPStatusCode() != 0 { // 0: no answer (the SDK wraps a send failure too)
		if s := re.HTTPStatusCode(); s < 200 || s > 299 {
			return responseError(re, err)
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
// status text as the code (no request ID or region from the response
// headers). A 200's body is the scanner's to check:
// it requires the operation's envelope and refuses an empty or cut
// document (objhttp.ErrSyntax, retried, never an empty final page).
func validate(_ *http.Request, status int, _ http.Header, body []byte) error {
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
