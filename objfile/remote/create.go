// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package remote

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	azsdk "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	s3sdk "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go/logging"

	"github.com/varveio/swath/objfile"
	"github.com/varveio/swath/objfile/azblob"
	"github.com/varveio/swath/objfile/gcs"
	"github.com/varveio/swath/objfile/internal/redact"
	"github.com/varveio/swath/objfile/s3"
)

// Options configure the clients OpenWith and OpenDestination build for a
// location. The zero Options is the provider's environment and SDK
// defaults (Open). They do not change any other client's credentials,
// endpoint or connection pool; the fields of another provider are ignored.
type Options struct {
	// CredentialsFile (gs://) is a Google credentials JSON file (a service
	// account's key, or authorized-user credentials); "" uses Application
	// Default Credentials. Either way OpenWith requests the read-only scope
	// and OpenDestination the read-write scope.
	CredentialsFile string
	// Endpoint is, for gs://, the JSON API base the client sends to
	// (https://storage.googleapis.com/storage/v1/ by default), as the SDK
	// takes it; "" is the default, or the emulator STORAGE_EMULATOR_HOST
	// names (then without credentials: ResolveEndpoint). For s3://, it is
	// an S3-compatible endpoint URL, addressed path-style (/BUCKET/KEY).
	// With "", OpenWith sends where the AWS SDK's configuration says
	// (AWS_ENDPOINT_URL_S3, AWS_ENDPOINT_URL, else AWS's regional
	// endpoint), while OpenDestination applies none of the environment's
	// endpoint settings (endpoint URLs, FIPS, dual-stack) and sends to AWS's
	// regional endpoint (virtual-hosted): the destination is the one named.
	// For az://, it is a blob service base URL that takes the account as its
	// first path segment, as an emulator's does (ENDPOINT/ACCOUNT/CONTAINER);
	// "" is https://ACCOUNT.blob.core.windows.net. It may not carry user
	// information, a query or a fragment.
	Endpoint string
	// Emulator says Endpoint is an emulator's host or URL, reached without
	// credentials (what ResolveEndpoint returns for STORAGE_EMULATOR_HOST).
	// A caller that freezes the route resolves it once and passes both, so
	// the environment is not read again. gs:// only.
	Emulator bool
	// RequestTimeout bounds the wait for a storage request's response
	// headers after it is sent; zero or negative is DefaultRequestTimeout,
	// and there is no unlimited value. Once the headers arrive, the body
	// may take longer: bound a read's or an upload's whole transfer, and
	// all its retries, with the operation's context. S3 multipart
	// initiation and abort cleanup use S3Write.AbortTimeout independently.
	// The S3 credential chain's requests (STS, SSO, container and instance
	// roles) go through the location's client, so their headers are bounded
	// the same way. It also bounds each whole token request (headers and
	// body) of a GCS CredentialsFile, and of Azure's DefaultAzureCredential
	// where it requests tokens over HTTP (not the developer tools it may
	// run). It does not bound GCS Application Default Credentials, which
	// keep the auth library's clients: 30 seconds per token request, the
	// metadata server's own on Compute Engine, an mTLS client for X.509
	// workload identity.
	RequestTimeout time.Duration
	// Region (s3://) is the bucket's region, which requests are signed for;
	// "" is AWS_REGION, AWS_DEFAULT_REGION or the profile's, else (for
	// OpenDestination) us-east-1. Destination.Region is the one resolved.
	Region string
	// Profile (s3://) is the shared config profile the credentials (and,
	// without Region, the region) come from; "" is the AWS SDK's default
	// chain (environment, AWS_PROFILE or default, SSO, web identity,
	// container and instance roles). OpenDestination always signs, and
	// honors AWS_CA_BUNDLE and the profile's ca_bundle for TLS trust;
	// OpenWith fails on a CA bundle, which its observed client cannot take.
	Profile string
	// SAS (az://) is a shared access signature (a query string, with or
	// without a leading '?') appended to every request; "" uses Entra ID
	// through azidentity's DefaultAzureCredential, which needs an https
	// endpoint. No error or Scope holds it.
	SAS string
	// GCSWrite configures OpenDestination's gs:// upload buffering, retries
	// and chunk timeouts.
	GCSWrite gcs.WriteOptions
	// S3Write configures OpenDestination's s3:// upload buffering, retries
	// and abort cleanup.
	S3Write s3.WriteOptions
	// AzureWrite configures OpenDestination's az:// upload buffering and
	// retries.
	AzureWrite azblob.WriteOptions
}

// timeout is o's RequestTimeout, or DefaultRequestTimeout when unset.
func (o Options) timeout() time.Duration {
	if o.RequestTimeout > 0 {
		return o.RequestTimeout
	}
	return DefaultRequestTimeout
}

// Destination provides reads and conditional writes under a location's
// prefix, through clients it owns. It is safe for concurrent use.
type Destination struct {
	o      objfile.Opener
	w      objfile.Writer
	scope  string
	region string
	close  func() error
}

// OpenDestination opens location for reads and conditional writes. The
// location must be a URI objfile.ParseLocation accepts. It creates no
// bucket, container or object and makes no object-store request; credential
// and client setup can fail here, while permissions are checked on use.
// The Destination owns its clients and outlives ctx: subsequent operations
// use their own contexts. Close it after finishing its reads and uploads.
//
// GCS requests the read-write scope; S3 signs every request; Azure uses SAS
// or Entra ID. Options control their credentials and endpoints. GCS and S3
// SDK logging is disabled to keep credentials and upload handles out of
// their logs, the S3 credential chain's logger included. The Azure SDK's
// log is process-wide and not changed here: the swath command turns it off
// before any SDK use.
func OpenDestination(ctx context.Context, location string, o Options) (*Destination, error) {
	l, err := objfile.ParseLocation(location)
	if err != nil {
		return nil, err
	}
	switch l.Scheme {
	case "gs":
	case "s3":
		return createS3(ctx, l, o)
	case "az":
		return createAzure(l, o)
	}
	pool := http.DefaultTransport.(*http.Transport).Clone()
	pool.ResponseHeaderTimeout = o.timeout()
	d := &Destination{scope: l.String()}
	if o.Endpoint == "" && !o.Emulator {
		o.Endpoint, o.Emulator = ResolveEndpoint("")
	}
	s := &Store{scope: d.scope}
	if err := s.at(o.Endpoint); err != nil {
		return nil, err
	}
	d.scope = s.scope
	client, err := gcsClient(ctx, pool, o, storage.ScopeReadWrite)
	if err != nil {
		return nil, err
	}
	// Reads (metadata, ranges) are bounded as uploads are: the SDK's
	// default retries them until the context ends.
	client.SetRetry(storage.WithMaxAttempts(gcs.DefaultMaxAttempts))
	d.o = objfile.Root(gcs.New(client, l.Container), l.Prefix)
	d.w = objfile.RootWriter(gcs.NewWriter(client, l.Container, o.GCSWrite), l.Prefix)
	d.close = func() error { err := client.Close(); pool.CloseIdleConnections(); return err }
	return d, nil
}

// DefaultRequestTimeout is the default for Options.RequestTimeout.
const DefaultRequestTimeout = 2 * time.Minute

// ResolveEndpoint resolves a GCS endpoint: endpoint when set, else
// STORAGE_EMULATOR_HOST (with emulator true), else "" for Google's
// default. It does not resolve S3 or Azure endpoints. A caller recording a
// location's route passes both results to OpenWith or OpenDestination as
// Options.Endpoint and Options.Emulator.
func ResolveEndpoint(endpoint string) (string, bool) {
	if endpoint != "" {
		return endpoint, false
	}
	if host := os.Getenv("STORAGE_EMULATOR_HOST"); host != "" {
		return host, true
	}
	return "", false
}

// createS3 is OpenDestination of an s3:// location: a client of its own over its
// own connection pool, from the AWS SDK's configuration with o's profile
// and region, signing every request.
func createS3(ctx context.Context, l objfile.Location, o Options) (*Destination, error) {
	d := &Destination{scope: l.String()}
	s := &Store{scope: d.scope}
	if err := s.at(o.Endpoint); err != nil {
		return nil, err
	}
	d.scope = s.scope
	timeout := o.timeout()
	// The SDK's buildable client (which answers a redirect with itself, and
	// has a pool of its own), so the configuration can add a CA bundle to it
	// (AWS_CA_BUNDLE, a profile's ca_bundle), which a plain *http.Client
	// refuses.
	hc := awshttp.NewBuildableClient().WithTransportOptions(func(t *http.Transport) { t.ResponseHeaderTimeout = timeout })
	opts := []func(*config.LoadOptions) error{config.WithHTTPClient(hc), config.WithLogger(logging.Nop{})}
	if o.Profile != "" {
		opts = append(opts, config.WithSharedConfigProfile(o.Profile))
	}
	if o.Region != "" {
		opts = append(opts, config.WithRegion(o.Region))
	}
	cfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("remote: destination configuration: %w", err)
	}
	d.region = cfg.Region
	if d.region == "" {
		d.region = "us-east-1"
	}
	var frozen *http.Client // the client requests go through, its pool closed by Close
	client := s3sdk.NewFromConfig(cfg, func(so *s3sdk.Options) {
		// The buildable client as configured (the CA bundle, the defaults'
		// timeouts) is frozen into the *http.Client it builds, whose pool
		// Close can then close.
		if bc, ok := so.HTTPClient.(*awshttp.BuildableClient); ok {
			if hc, ok := bc.Freeze().(*http.Client); ok {
				so.HTTPClient, frozen = hc, hc
			}
		}
		so.Region = d.region
		// The endpoint named, or AWS's for the region: never one the
		// environment sets, so the destination is what its identity says.
		so.BaseEndpoint, so.UsePathStyle = nil, false
		if o.Endpoint != "" {
			so.BaseEndpoint, so.UsePathStyle = aws.String(o.Endpoint), true
		}
		// Nothing is logged: no request, response or credential.
		so.Logger, so.ClientLogMode = logging.Nop{}, 0
		so.EndpointOptions.UseFIPSEndpoint = aws.FIPSEndpointStateDisabled
		so.EndpointOptions.UseDualStackEndpoint = aws.DualStackEndpointStateDisabled
	})
	w, err := s3.NewWriter(client, l.Container, o.S3Write)
	if err != nil {
		return nil, err
	}
	d.o = objfile.Root(s3.New(client, l.Container), l.Prefix)
	d.w = objfile.RootWriter(w, l.Prefix)
	d.close = func() error {
		if frozen != nil {
			frozen.CloseIdleConnections()
		}
		return nil
	}
	return d, nil
}

// createAzure is OpenDestination of an az:// location: a client of its own over its
// own connection pool, authorized by o's SAS or by DefaultAzureCredential.
func createAzure(l objfile.Location, o Options) (*Destination, error) {
	d := &Destination{scope: l.String()}
	s := &Store{scope: d.scope}
	if err := s.at(o.Endpoint); err != nil {
		return nil, err
	}
	d.scope = s.scope
	pool := http.DefaultTransport.(*http.Transport).Clone()
	pool.ResponseHeaderTimeout = o.timeout()
	client, err := azureClient(l.Account, o, pool)
	if err != nil {
		return nil, err
	}
	w, err := azblob.NewWriter(client, l.Container, o.AzureWrite)
	if err != nil {
		return nil, err
	}
	d.o = objfile.Root(azblob.New(client, l.Container), l.Prefix)
	d.w = objfile.RootWriter(w, l.Prefix)
	d.close = func() error { pool.CloseIdleConnections(); return nil }
	return d, nil
}

// azureClient is a client of account's blob service on o's endpoint
// (azureService) over tr, authorized by o's SAS, which error bodies are
// scrubbed of (scrubbed), or else by DefaultAzureCredential, whose token
// exchanges go through a client of their own bounded by the request
// timeout.
func azureClient(account string, o Options, tr http.RoundTripper) (*azsdk.Client, error) {
	service := azureService(account, o.Endpoint)
	sas := strings.TrimPrefix(o.SAS, "?")
	q, err := url.ParseQuery(sas)
	if err != nil {
		return nil, errors.New("remote: the shared access signature is not a query string") // the parse error would quote it
	}
	if q.Has("sig") {
		tr = scrubbed{next: tr, scrub: redact.SAS(sas)}
	}
	opts := &azsdk.ClientOptions{ClientOptions: azcore.ClientOptions{Transport: &http.Client{Transport: tr}}}
	if sas != "" {
		client, err := azsdk.NewClientWithNoCredential(service+"?"+sas, opts)
		if err != nil {
			return nil, objfile.RedactError(err)
		}
		return client, nil
	}
	cred, err := azidentity.NewDefaultAzureCredential(&azidentity.DefaultAzureCredentialOptions{
		ClientOptions: azcore.ClientOptions{Transport: &http.Client{Timeout: o.timeout(), Transport: http.DefaultTransport.(*http.Transport).Clone()}}})
	if err != nil {
		return nil, fmt.Errorf("remote: credentials: %w", err)
	}
	return azsdk.NewClient(service, cred, opts)
}

// azureService is the blob service URL of account: on endpoint, the
// account its first path segment, as an emulator's; else the account's
// own host.
func azureService(account, endpoint string) string {
	if endpoint == "" {
		return "https://" + account + ".blob.core.windows.net/"
	}
	return strings.TrimSuffix(endpoint, "/") + "/" + account + "/"
}

// scrubbed scrubs the body of every error response, which an SDK error
// quotes: a service that echoes a request's query back would otherwise put
// a SAS's signature in an error's text, however it spelled it.
type scrubbed struct {
	next  http.RoundTripper
	scrub func(string) string
}

// maxErrorBody bounds the error body scrubbed and kept.
const maxErrorBody = 64 << 10

func (s scrubbed) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := s.next.RoundTrip(req)
	if err != nil || resp.StatusCode < 400 {
		return resp, err
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, maxErrorBody))
	_ = resp.Body.Close()
	clean := s.scrub(string(b))
	resp.Body, resp.ContentLength = io.NopCloser(strings.NewReader(clean)), int64(len(clean))
	resp.Header.Del("Content-Length")
	return resp, err
}

// Region is an s3:// destination's region, as resolved ("" for another).
func (d *Destination) Region() string { return d.region }

// Writer is the Destination's writer of names under the location's
// prefix.
func (d *Destination) Writer() objfile.Writer { return d.w }

// Open opens name under the location's prefix (reading back a write).
func (d *Destination) Open(ctx context.Context, name string) (objfile.Object, error) {
	return d.o.Open(ctx, name)
}

// OpenSized opens name, of size bytes, under the location's prefix.
func (d *Destination) OpenSized(ctx context.Context, name string, size int64) (objfile.Object, error) {
	return d.o.OpenSized(ctx, name, size)
}

// ReadObject reads name under the location's prefix whole in one request
// (objfile.ObjectReader).
func (d *Destination) ReadObject(ctx context.Context, name string, limit int64) ([]byte, string, error) {
	return objfile.ReadObject(ctx, d.o, name, limit)
}

// Scope names the destination's namespace: the location's canonical URI,
// and " at " the endpoint when one is set. It holds no credentials.
func (d *Destination) Scope() string { return d.scope }

// Close releases the client and its idle connections. Finish the
// Destination's reads and uploads before calling Close.
func (d *Destination) Close() error { return d.close() }

var _ interface {
	objfile.Opener
	objfile.ObjectReader
	io.Closer
} = (*Destination)(nil)
