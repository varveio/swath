// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package sdkclient lists S3 through the AWS SDK's service client
// (service/s3), behind the provider's API (s3.API, s3.VersionsAPI) and wire
// types (package client).
//
// The SDK owns the request: serialization, endpoint resolution, signing,
// transport and the decoding of error answers. Its retryer is
// aws.NopRetryer: objhttp.Do is the one retry owner, and an attempt is an
// application attempt (net/http may still replay an idempotent GET on a
// reused connection below it, as for the hand-rolled client). swath owns
// only what a 2xx answer means: objhttp.Guard, under an HTTP client that
// follows no redirect, bounds the body, reads a 2xx other than 200 as an
// answer, and captures a 200's body; the SDK's deserializer is handed a
// minimal envelope in its place, and package client's scanner decodes the
// captured body (client.ParseList, client.ParseVersions: the operation's
// envelope, whole, or ErrSyntax), as Java
// swath's StreamingListObjectsV2Interceptor does. Measured through the same
// stack over a 1,000-entry page (BenchmarkDecode), the SDK's deserializer
// costs about 14 µs per object and 100,000 allocations per page against the
// scanner's 1.8 µs per object and 410 per page; an encoding/xml token walk
// cost about 12 µs per object. The
// scanner also keeps <Version> and <DeleteMarker> entries in wire order,
// which the SDK's two lists cannot give back (TestStockSDKLosesVersionsOrder),
// and accepts XML forms the SDK's deserializer refuses (CDATA within text,
// spaces around numbers: client/xml_test.go). Credentials are package
// client's resolution (client.LoadAuth), fetched before each attempt is
// counted.
package sdkclient

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
	"github.com/varveio/swath/go/internal/source/s3/client"
)

// Client lists S3 buckets through the SDK. It is safe for concurrent use.
type Client struct {
	cfg       client.Config
	auth      *client.Auth
	region    string // the signing region, a FIPS alias resolved
	sdk       *awss3.Client
	params    awss3.EndpointParameters // Route's, without the bucket
	retry     objhttp.Retry
	counts    objhttp.Counters
	sdkDecode bool // measurement plumbing (measure.go)
}

// New resolves the region and credentials (client.LoadAuth) and builds the
// SDK client; ctx bounds the configuration load and the first credential
// lookup. cfg.Now is not used: the SDK signs with its own clock. As for the
// hand-rolled client, FIPS and dual-stack select among AWS endpoints: with
// a custom endpoint they are not used.
func New(ctx context.Context, cfg client.Config) (*Client, error) {
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = 30 * time.Second
	}
	a, err := client.LoadAuth(ctx, cfg)
	if err != nil {
		return nil, err
	}
	region, fips := fipsRegion(a.Region, cfg.FIPS)
	dualStack := cfg.DualStack
	var base *string
	if cfg.Endpoint != "" {
		u, err := url.Parse(cfg.Endpoint)
		if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
			return nil, fmt.Errorf("endpoint %q: want http(s)://host[:port][/path]", cfg.Endpoint)
		}
		base, fips, dualStack = aws.String(cfg.Endpoint), false, false
	}
	c := &Client{cfg: cfg, auth: a, region: region, sdkDecode: sdkDecode(),
		retry: objhttp.Retry{MaxAttempts: cfg.MaxAttempts, BaseBackoff: cfg.BaseBackoff, MaxBackoff: cfg.MaxBackoff,
			Limiter: cfg.Limiter, Rate: cfg.Rate, Answer: answer},
		params: awss3.EndpointParameters{Region: aws.String(region), UseFIPS: aws.Bool(fips), UseDualStack: aws.Bool(dualStack),
			Endpoint: base, ForcePathStyle: aws.Bool(cfg.PathStyle), Accelerate: aws.Bool(false),
			DisableMultiRegionAccessPoints: aws.Bool(false), UseArnRegion: aws.Bool(false), DisableS3ExpressSessionAuth: aws.Bool(true)},
	}
	c.retry.Defaults()

	guard := &objhttp.Guard{Validate: validate}
	if a.Bearer != nil {
		guard.Header = bearerHeader
	}
	// A plain *http.Client is used as given: the SDK wraps only its own
	// BuildableClient (service/s3 api_client.go resolveHTTPClient).
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
		Region:       region,
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

// fipsRegion resolves a FIPS region alias (fips-us-east-1, us-east-1-fips,
// a -fips- infix) to its region with FIPS on, as the SDK does inside each
// operation (service/s3 endpoints.go finalizeClientEndpointResolverOptions,
// and internal/endpoints.MapFIPSRegion for the endpoint's region), so Route
// and the requests resolve the same endpoint.
func fipsRegion(region string, fips bool) (string, bool) {
	r := strings.ReplaceAll(strings.ReplaceAll(strings.ReplaceAll(region, "-fips-", "-"), "fips-", ""), "-fips", "")
	return r, fips || r != region
}

func endpointState[S any](on bool, enabled, disabled S) S {
	if on {
		return enabled
	}
	return disabled
}

// allowGzip removes the SDK's "Accept-Encoding: identity", so net/http
// asks for gzip and decompresses as it does for the hand-rolled client:
// the same request on the wire, and a body that does not decompress is a
// read failure (class other), not a garbled document.
func allowGzip(stack *smithymiddleware.Stack) error {
	_, err := stack.Finalize.Remove("DisableAcceptEncodingGzip")
	return err
}

// Credentials returns the resolved credentials' source (never the secret).
func (c *Client) Credentials() (source string, anonymous bool) {
	return c.auth.Source, c.auth.Anonymous
}

// Note is a one-time warning for the user, or "".
func (c *Client) Note() string { return c.auth.Note }

// Region is the signing region (a FIPS alias resolved to its region).
func (c *Client) Region() string { return c.region }

// Answered is the number of requests the service answered usably
// (objhttp.Counters.Answered).
func (c *Client) Answered() int64 { return c.counts.Answered() }

// Live is the request account now with the limiter's current limit
// (objhttp.Live).
func (c *Client) Live() objhttp.Live {
	l := c.counts.Live()
	l.Limit = c.retry.Limiter.Limit()
	return l
}

// Counters returns the request counters so far.
func (c *Client) Counters() client.Counters {
	n := c.counts.Load()
	n.Limiter = c.retry.Limiter.Counts()
	n.Rate = c.retry.Rate.Counts()
	return n
}

// Route is the URL a bucket's listing requests go to, before the query, as
// the SDK's endpoint resolver names it: scheme, host and path. It is "" when
// the resolver refuses the configuration (every request would fail too).
func (c *Client) Route(bucket string) string {
	p := c.params
	p.Bucket = aws.String(bucket)
	ep, err := awss3.NewDefaultEndpointResolverV2().ResolveEndpoint(context.Background(), p)
	if err != nil {
		return ""
	}
	u := ep.URI
	if u.Path == "" {
		u.Path = "/" // the path a virtual-hosted request is sent to
	}
	return (&url.URL{Scheme: u.Scheme, Host: u.Host, Path: u.Path, RawPath: u.RawPath}).String()
}

// The minimal envelopes the SDK decodes in place of a captured body.
var (
	emptyList     = []byte(`<?xml version="1.0" encoding="UTF-8"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></ListBucketResult>`)
	emptyVersions = []byte(`<?xml version="1.0" encoding="UTF-8"?><ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></ListVersionsResult>`)
)

// ListObjectsV2 sends one ListObjectsV2 request through the SDK, retrying
// retryable failures (objhttp.Do).
func (c *Client) ListObjectsV2(ctx context.Context, in *client.ListInput) (*client.Page, error) {
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
	op := operation[client.Page]{replace: emptyList, parse: client.ParseList}
	if c.sdkDecode {
		op = operation[client.Page]{}
	}
	return objhttp.Do(ctx, c.retry, &c.counts, func() (*client.Page, error) {
		return attempt(c, ctx, op, func(actx context.Context, opts ...func(*awss3.Options)) (*client.Page, error) {
			out, err := c.sdk.ListObjectsV2(actx, req, opts...)
			if err != nil || op.parse != nil {
				return nil, err
			}
			return page(out)
		})
	})
}

// ListObjectVersions sends one ListObjectVersions request on
// ListObjectsV2's path; the scanner decodes the body in wire order and
// checks the page (VersionPage.Validate).
func (c *Client) ListObjectVersions(ctx context.Context, in *client.VersionsInput) (*client.VersionPage, error) {
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
	op := operation[client.VersionPage]{replace: emptyVersions, parse: client.ParseVersions}
	return objhttp.Do(ctx, c.retry, &c.counts, func() (*client.VersionPage, error) {
		return attempt(c, ctx, op, func(actx context.Context, opts ...func(*awss3.Options)) (*client.VersionPage, error) {
			_, err := c.sdk.ListObjectVersions(actx, req, opts...)
			return nil, err
		})
	})
}

func str(s string) *string {
	if s == "" {
		return nil
	}
	return aws.String(s)
}

// operation is how an attempt's 200 body is decoded: with parse, the Guard
// captures the body, the SDK decodes replace in its place and parse decodes
// the body; without (measure.go), the SDK's call result is the page.
type operation[T any] struct {
	replace []byte
	parse   func([]byte) (*T, error)
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
// Guard has read and checked the body by then); op says how the body is
// decoded.
func attempt[T any](c *Client, ctx context.Context, op operation[T], call func(context.Context, ...func(*awss3.Options)) (*T, error)) (*T, error) {
	actx, cancel := context.WithTimeout(ctx, c.cfg.RequestTimeout)
	defer cancel()
	tok, creds, err := c.auth.Ready(actx)
	if err != nil {
		return nil, err
	}
	var opts []func(*awss3.Options)
	if c.auth.Credentials != nil {
		opts = append(opts, func(o *awss3.Options) {
			o.Credentials = aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) { return creds, nil })
		})
	}
	cctx := actx
	if c.auth.Bearer != nil {
		cctx = context.WithValue(cctx, tokenKey{}, tok)
	}
	capture := &objhttp.Capture{Replace: op.replace}
	if op.parse != nil {
		cctx = objhttp.WithCapture(cctx, capture)
	}
	done := c.counts.Attempt()
	v, err := call(cctx, opts...)
	done()
	if err == nil {
		err = faults.At("response-read")
	}
	if err == nil && op.parse != nil {
		v, err = op.parse(capture.Body)
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
// cap, a credential error, a failed body read: a cut body is transport, a
// read past the attempt's deadline ErrTimeout); an HTTP answer other than
// 2xx is a *client.Error; a 2xx body the SDK could not decode is ErrSyntax;
// the scanner's ErrSyntax and ErrInvalidPage stand; a transport failure
// stays one (an attempt timeout is ErrTimeout).
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

// responseError is an HTTP answer's *client.Error: the status, the SDK's
// decoded code and message (the status text without spaces when the body
// named no code), the request ID and, for a redirect, the bucket's region.
func responseError(re *awshttp.ResponseError, err error) *client.Error {
	status := re.HTTPStatusCode()
	e := &client.Error{Status: status, RequestID: re.RequestID}
	if e.RequestID == "" {
		e.RequestID = re.Response.Header.Get("X-Amz-Request-Id")
	}
	var ae smithy.APIError
	if errors.As(err, &ae) && ae.ErrorCode() != "UnknownError" { // the SDK's name for a body without a code
		e.Code, e.Message = ae.ErrorCode(), ae.ErrorMessage()
	}
	if e.Code == "" {
		e.Code = strings.ReplaceAll(http.StatusText(status), " ", "")
	}
	if status == http.StatusMovedPermanently || e.Code == "AuthorizationHeaderMalformed" || e.Code == "PermanentRedirect" {
		e.Region = re.Response.Header.Get("X-Amz-Bucket-Region")
	}
	return e
}

// answer is client.Classify's mapping of S3 answers.
func answer(err error) (objhttp.Class, bool) {
	var se *client.Error
	if !errors.As(err, &se) {
		return "", false
	}
	return client.Classify(se), true
}

// validate is the Guard's check of a 2xx answer: one other than 200 is an
// answer, read as the hand-rolled client reads it (client.ResponseError: an
// <Error> body's code, else the status text; the response headers are not
// seen here, so no request ID or region from them). A 200's body is the
// scanner's to check: it requires the operation's envelope and refuses an
// empty or cut document (ErrSyntax, retried, never an empty final page).
func validate(_ *http.Request, status int, body []byte) error {
	if status != http.StatusOK {
		return client.ResponseError(status, nil, body)
	}
	return nil
}
