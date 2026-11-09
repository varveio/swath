// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package sdkclient lists S3 through the AWS SDK's service client
// (service/s3), behind the provider's API (s3.API, s3.VersionsAPI) and wire
// types (package client). The SDK builds, routes and signs the requests and
// decodes current-object pages and error answers; this package keeps what
// the SDK does not provide: one retry owner (objhttp.Do; the SDK's retryer
// is aws.NopRetryer), bounded and validated bodies (objhttp.Guard under the
// SDK's HTTP client, which follows no redirect), the provider's failure
// vocabulary, and versions pages decoded by package client's scanner,
// which keeps <Version> and <DeleteMarker> entries in wire order (the SDK
// splits them into two lists that cannot be merged back:
// TestStockSDKLosesVersionsOrder). Credentials are package client's
// resolution (client.LoadAuth).
package sdkclient

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
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

	"github.com/varveio/swath/go/internal/buildinfo"
	"github.com/varveio/swath/go/internal/faults"
	"github.com/varveio/swath/go/internal/source/objhttp"
	"github.com/varveio/swath/go/internal/source/s3/client"
)

// Client lists S3 buckets through the SDK. It is safe for concurrent use.
type Client struct {
	cfg    client.Config
	auth   *client.Auth
	sdk    *awss3.Client
	dec    Decoder
	params awss3.EndpointParameters // Route's, without the bucket
	retry  objhttp.Retry
	counts objhttp.Counters
}

// New resolves the region and credentials (client.LoadAuth) and builds the
// SDK client; ctx bounds the configuration load and the first credential
// lookup. cfg.Now is not used: the SDK signs with its own clock.
func New(ctx context.Context, cfg client.Config, opts ...Option) (*Client, error) {
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = 30 * time.Second
	}
	a, err := client.LoadAuth(ctx, cfg)
	if err != nil {
		return nil, err
	}
	var base *string
	if cfg.Endpoint != "" {
		u, err := url.Parse(cfg.Endpoint)
		if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
			return nil, fmt.Errorf("endpoint %q: want http(s)://host[:port][/path]", cfg.Endpoint)
		}
		base = aws.String(cfg.Endpoint)
	}
	c := &Client{cfg: cfg, auth: a,
		retry: objhttp.Retry{MaxAttempts: cfg.MaxAttempts, BaseBackoff: cfg.BaseBackoff, MaxBackoff: cfg.MaxBackoff,
			Limiter: cfg.Limiter, Rate: cfg.Rate, Answer: answer},
		params: awss3.EndpointParameters{Region: aws.String(a.Region), UseFIPS: aws.Bool(cfg.FIPS), UseDualStack: aws.Bool(cfg.DualStack),
			Endpoint: base, ForcePathStyle: aws.Bool(cfg.PathStyle), Accelerate: aws.Bool(false),
			DisableMultiRegionAccessPoints: aws.Bool(false), UseArnRegion: aws.Bool(false), DisableS3ExpressSessionAuth: aws.Bool(true)},
	}
	c.retry.Defaults()
	for _, o := range opts {
		o(c)
	}

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

	var creds aws.CredentialsProvider = aws.AnonymousCredentials{}
	if a.Credentials != nil {
		creds = a.Credentials
	}
	c.sdk = awss3.New(awss3.Options{
		Region:       a.Region,
		Credentials:  creds,
		HTTPClient:   &hc,
		Retryer:      aws.NopRetryer{},
		BaseEndpoint: base,
		UsePathStyle: cfg.PathStyle,
		EndpointOptions: awss3.EndpointResolverOptions{
			UseFIPSEndpoint:      fipsState(cfg.FIPS),
			UseDualStackEndpoint: dualStackState(cfg.DualStack),
		},
		Logger:                      logging.Nop{},
		DisableS3ExpressSessionAuth: aws.Bool(true), // directory buckets are refused before any request
		RequestChecksumCalculation:  aws.RequestChecksumCalculationWhenRequired,
		ResponseChecksumValidation:  aws.ResponseChecksumValidationWhenRequired,
		APIOptions:                  []func(*smithymiddleware.Stack) error{awsmiddleware.AddUserAgentKey(buildinfo.Tool)},
	})
	return c, nil
}

func fipsState(on bool) aws.FIPSEndpointState {
	if on {
		return aws.FIPSEndpointStateEnabled
	}
	return aws.FIPSEndpointStateDisabled
}

func dualStackState(on bool) aws.DualStackEndpointState {
	if on {
		return aws.DualStackEndpointStateEnabled
	}
	return aws.DualStackEndpointStateDisabled
}

// Credentials returns the resolved credentials' source (never the secret).
func (c *Client) Credentials() (source string, anonymous bool) {
	return c.auth.Source, c.auth.Anonymous
}

// Note is a one-time warning for the user, or "".
func (c *Client) Note() string { return c.auth.Note }

// Region is the signing region.
func (c *Client) Region() string { return c.auth.Region }

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
	return objhttp.Do(ctx, c.retry, &c.counts, func() (*client.Page, error) {
		if c.dec == DecoderSDK {
			return attempt(c, ctx, nil, func(actx context.Context) (*client.Page, error) {
				out, err := c.sdk.ListObjectsV2(actx, req)
				if err != nil {
					return nil, err
				}
				return page(out)
			})
		}
		capture := &objhttp.Capture{Replace: emptyList}
		return attempt(c, ctx, capture, func(actx context.Context) (*client.Page, error) {
			if _, err := c.sdk.ListObjectsV2(actx, req); err != nil {
				return nil, err
			}
			return client.ParseList(capture.Body)
		})
	})
}

// ListObjectVersions sends one ListObjectVersions request on
// ListObjectsV2's path. The SDK decodes a minimal envelope in place of the
// body (objhttp.Capture); package client's scanner decodes the captured
// body in wire order and checks the page (VersionPage.Validate), whatever
// the Decoder.
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
	return objhttp.Do(ctx, c.retry, &c.counts, func() (*client.VersionPage, error) {
		capture := &objhttp.Capture{Replace: emptyVersions}
		return attempt(c, ctx, capture, func(actx context.Context) (*client.VersionPage, error) {
			if _, err := c.sdk.ListObjectVersions(actx, req); err != nil {
				return nil, err
			}
			return client.ParseVersions(capture.Body)
		})
	})
}

// emptyList and emptyVersions are what the SDK decodes in place of a
// captured body.
var (
	emptyList     = []byte(`<?xml version="1.0" encoding="UTF-8"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></ListBucketResult>`)
	emptyVersions = []byte(`<?xml version="1.0" encoding="UTF-8"?><ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></ListVersionsResult>`)
)

// Decoder selects how current-object (ListObjectsV2) pages are decoded;
// versions pages are always the scanner's.
type Decoder int

const (
	// DecoderSDK: the SDK's generated deserializer.
	DecoderSDK Decoder = iota
	// DecoderScanner: package client's scanner (client.ParseList) on the
	// captured body; the SDK decodes an empty envelope.
	DecoderScanner
)

// Option configures a Client beyond client.Config.
type Option func(*Client)

// WithDecoder selects the body decoder (default DecoderSDK).
func WithDecoder(d Decoder) Option { return func(c *Client) { c.dec = d } }

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

// attempt runs one SDK call (call) under the request timeout. What the
// request needs (credentials, a bearer token) is fetched first: a failure
// there is a *objhttp.CredentialError and not an attempt. The attempt is
// counted from just before the call until it returns (the Guard has read
// the body by then). capture, if set, is filled by the Guard.
func attempt[T any](c *Client, ctx context.Context, capture *objhttp.Capture, call func(context.Context) (*T, error)) (*T, error) {
	actx, cancel := context.WithTimeout(ctx, c.cfg.RequestTimeout)
	defer cancel()
	tok, _, err := c.auth.Ready(actx)
	if err != nil {
		return nil, err
	}
	cctx := actx
	if c.auth.Bearer != nil {
		cctx = context.WithValue(cctx, tokenKey{}, tok)
	}
	if capture != nil {
		cctx = objhttp.WithCapture(cctx, capture)
	}
	done := c.counts.Attempt()
	v, err := call(cctx)
	done()
	if err == nil {
		err = faults.At("response-read")
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
// failure is its cause (ErrSyntax, the size cap, a credential error); an
// HTTP answer other than 2xx is a *client.Error; a 2xx body the SDK could
// not decode, or a body the scanner refused, is ErrSyntax; a
// transport failure stays one (an attempt timeout is ErrTimeout).
func (c *Client) failure(actx, ctx context.Context, err error) error {
	var ge *objhttp.GuardError
	if errors.As(err, &ge) {
		return ge.Err
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

// validate is the Guard's check of a 2xx body: a ListBucketResult or a
// ListVersionsResult root element. An empty body, another root (an <Error>
// under 200) or another 2xx status is ErrSyntax: retried, never read as an
// empty final page.
func validate(_ *http.Request, status int, body []byte) error {
	if status != http.StatusOK {
		return fmt.Errorf("%w: HTTP %d answer to a listing", objhttp.ErrSyntax, status)
	}
	d := xml.NewDecoder(bytes.NewReader(body))
	for {
		tok, err := d.Token()
		if err == io.EOF {
			return fmt.Errorf("%w: no root element (%d bytes)", objhttp.ErrSyntax, len(body))
		}
		if err != nil {
			return fmt.Errorf("%w: %v", objhttp.ErrSyntax, err)
		}
		if s, ok := tok.(xml.StartElement); ok {
			switch s.Name.Local {
			case "ListBucketResult", "ListVersionsResult":
				return nil
			}
			return fmt.Errorf("%w: root element <%s>, want <ListBucketResult> or <ListVersionsResult>", objhttp.ErrSyntax, s.Name.Local)
		}
	}
}

// page converts the SDK's ListObjectsV2 output, URL-decoding keys and
// common prefixes once when the answer says encoding-type=url (the SDK
// keeps them as sent).
func page(out *awss3.ListObjectsV2Output) (*client.Page, error) {
	enc := string(out.EncodingType)
	pg := &client.Page{
		IsTruncated: aws.ToBool(out.IsTruncated), NextContinuationToken: aws.ToString(out.NextContinuationToken),
		HasContinuation: out.NextContinuationToken != nil, KeyCount: int(aws.ToInt32(out.KeyCount)), EncodingType: enc,
		Contents: make([]client.Object, len(out.Contents)),
	}
	var err error
	for i, o := range out.Contents {
		c := &pg.Contents[i]
		if c.Key, err = unescape(enc, aws.ToString(o.Key)); err != nil {
			return nil, err
		}
		c.ETag, c.Size = aws.ToString(o.ETag), aws.ToInt64(o.Size)
		if o.LastModified != nil {
			c.Mtime = o.LastModified.UnixMicro()
		}
		c.StorageClass = string(o.StorageClass)
		if o.Owner != nil {
			c.OwnerID, c.OwnerDisplayName = aws.ToString(o.Owner.ID), aws.ToString(o.Owner.DisplayName)
		}
		if len(o.ChecksumAlgorithm) > 0 {
			c.ChecksumAlgorithm = string(o.ChecksumAlgorithm[0])
		}
		c.ChecksumType = string(o.ChecksumType)
	}
	if len(out.CommonPrefixes) > 0 {
		pg.CommonPrefixes = make([]string, len(out.CommonPrefixes))
		for i, p := range out.CommonPrefixes {
			if pg.CommonPrefixes[i], err = unescape(enc, aws.ToString(p.Prefix)); err != nil {
				return nil, err
			}
		}
	}
	return pg, nil
}

// unescape decodes a key field once when enc is "url", with form semantics
// ('+' is a space) as S3's encoding-type=url answers need and as package
// client decodes them; a bad escape is ErrSyntax.
func unescape(enc, s string) (string, error) {
	if enc != "url" {
		return s, nil
	}
	v, err := url.QueryUnescape(s)
	if err != nil {
		return "", fmt.Errorf("%w: invalid URL escape in %q", objhttp.ErrSyntax, s)
	}
	return v, nil
}
