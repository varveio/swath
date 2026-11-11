// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package remote

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/auth"
	"cloud.google.com/go/auth/credentials"
	"cloud.google.com/go/storage"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	azsdk "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	s3sdk "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go/logging"
	"google.golang.org/api/option"
	htransport "google.golang.org/api/transport/http"

	"github.com/varveio/swath/go/objfile"
	"github.com/varveio/swath/go/objfile/azblob"
	"github.com/varveio/swath/go/objfile/gcs"
	"github.com/varveio/swath/go/objfile/internal/ranged"
	"github.com/varveio/swath/go/objfile/internal/redact"
	"github.com/varveio/swath/go/objfile/s3"
)

// DestinationOptions configure the clients OpenDestination owns. They do
// not change any other client's credentials, endpoint or connection pool.
type DestinationOptions struct {
	// CredentialsFile (gs://) is a Google credentials JSON file (a service
	// account's key, or authorized-user credentials) read for the
	// destination; "" uses Application Default Credentials. Either way the
	// read-write scope is requested.
	CredentialsFile string
	// Endpoint is, for gs://, the JSON API base the client sends to
	// (https://storage.googleapis.com/storage/v1/ by default), as the SDK
	// takes it; "" is the default, or the emulator STORAGE_EMULATOR_HOST
	// names (then without credentials: ResolveEndpoint). For s3://, it is
	// an S3-compatible endpoint URL, addressed path-style (/BUCKET/KEY); ""
	// is AWS's regional endpoint (virtual-hosted), and the environment's
	// endpoint variables (AWS_ENDPOINT_URL_S3, AWS_ENDPOINT_URL, FIPS,
	// dual-stack) are not applied: the destination is the one named. For
	// az://, it is a blob service base URL that takes the account as its
	// first path segment, as an emulator's does (ENDPOINT/ACCOUNT/CONTAINER);
	// "" is https://ACCOUNT.blob.core.windows.net. It may not carry user
	// information, a query or a fragment.
	Endpoint string
	// Emulator says Endpoint is an emulator's host or URL, reached without
	// credentials (what ResolveEndpoint returns for STORAGE_EMULATOR_HOST).
	// A caller that freezes the route resolves it once and passes both, so
	// the environment is not read again. gs:// only.
	Emulator bool
	// RequestTimeout bounds the wait for response headers after sending a
	// request (0: DefaultRequestTimeout). It also bounds GCS and Azure
	// token exchanges. It does not bound an upload's body transfer or all
	// its retries. Bound those with the operation's context; S3 multipart
	// initiation and abort cleanup use S3Write.AbortTimeout independently.
	RequestTimeout time.Duration
	// Region (s3://) is the bucket's region, which requests are signed for;
	// "" is AWS_REGION, AWS_DEFAULT_REGION or the profile's, else
	// us-east-1. Destination.Region is the one resolved.
	Region string
	// Profile (s3://) is the shared config profile the credentials (and,
	// without Region, the region) come from; "" is the AWS SDK's default
	// chain (environment, AWS_PROFILE or default, SSO, web identity,
	// container and instance roles). Requests are always signed. The SDK
	// also honors AWS_CA_BUNDLE and the profile's ca_bundle for TLS trust.
	Profile string
	// SAS (az://) is a shared access signature (a query string, with or
	// without a leading '?') appended to every request; "" uses Entra ID
	// through azidentity's DefaultAzureCredential, which needs an https
	// endpoint. No error or Scope holds it.
	SAS string
	// GCSWrite configures gs:// upload buffering, retries and chunk timeouts.
	GCSWrite gcs.WriteOptions
	// S3Write configures s3:// upload buffering, retries and abort cleanup.
	S3Write s3.WriteOptions
	// AzureWrite configures az:// upload buffering and retries.
	AzureWrite azblob.WriteOptions
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
// or Entra ID. DestinationOptions controls their credentials and endpoints.
// GCS and S3 SDK logging is disabled to keep credentials and upload handles
// out of their logs.
func OpenDestination(ctx context.Context, location string, o DestinationOptions) (*Destination, error) {
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
	pool.ResponseHeaderTimeout = o.RequestTimeout
	if pool.ResponseHeaderTimeout <= 0 {
		pool.ResponseHeaderTimeout = DefaultRequestTimeout
	}
	d := &Destination{scope: l.String()}
	if o.Endpoint == "" && !o.Emulator {
		o.Endpoint, o.Emulator = ResolveEndpoint("")
	}
	s := &Store{scope: d.scope}
	if err := s.at(o.Endpoint); err != nil {
		return nil, errors.New("remote: the destination endpoint is not a URL without user information, query or fragment")
	}
	d.scope = s.scope
	client, err := writeClient(ctx, pool, o)
	if err != nil {
		return nil, err
	}
	d.o = objfile.Root(gcs.New(client, l.Container), l.Prefix)
	d.w = objfile.RootWriter(gcs.NewWriter(client, l.Container, o.GCSWrite), l.Prefix)
	d.close = func() error { err := client.Close(); pool.CloseIdleConnections(); return err }
	return d, nil
}

// writeClient is a read-write GCS client over pool for o (emulator: the
// SDK's emulator client, without credentials). Reads go through
// ranged.Transport, as Open's do. The SDK's logger is silenced: its debug
// output, switched on by the environment, writes request headers
// unredacted, the bearer token included.
func writeClient(ctx context.Context, pool http.RoundTripper, o DestinationOptions) (*storage.Client, error) {
	tr := ranged.Transport(pool)
	quiet := slog.New(slog.DiscardHandler)
	opts := []option.ClientOption{option.WithLogger(quiet)}
	if !o.Emulator {
		creds, err := writeCredentials(o, quiet)
		if err != nil {
			return nil, err
		}
		if tr, err = htransport.NewTransport(ctx, tr, option.WithAuthCredentials(creds), option.WithLogger(quiet)); err != nil {
			return nil, err
		}
	}
	opts = append(opts, option.WithHTTPClient(&http.Client{Transport: tr}))
	if ep := o.Endpoint; ep != "" {
		if o.Emulator {
			// The emulator's JSON API base, as the SDK makes it from
			// STORAGE_EMULATOR_HOST, given explicitly: the environment
			// is not read again.
			if !strings.Contains(ep, "://") {
				ep = "http://" + ep
			}
			ep = strings.TrimSuffix(ep, "/") + "/storage/v1/"
		} else if ep[len(ep)-1] != '/' {
			ep += "/"
		}
		opts = append(opts, option.WithEndpoint(ep))
	}
	c, err := storage.NewClient(ctx, opts...)
	if err != nil {
		return nil, err
	}
	// Reads (metadata, ranges) are bounded as uploads are: the SDK's
	// default retries them until the context ends.
	c.SetRetry(storage.WithMaxAttempts(gcs.DefaultMaxAttempts))
	return c, nil
}

// writeCredentials loads the destination's credentials with the read-write
// scope: the file o names (a service account key or authorized-user
// credentials, refused otherwise), else Application Default Credentials.
// Their token exchanges go through a client of their own, bounded by the
// request timeout (headers and body), and are fetched with the context of
// the request that needs one; their debug logging is off whatever the
// environment says (it would print tokens and signed assertions).
func writeCredentials(o DestinationOptions, quiet *slog.Logger) (*auth.Credentials, error) {
	timeout := o.RequestTimeout
	if timeout <= 0 {
		timeout = DefaultRequestTimeout
	}
	dopts := &credentials.DetectOptions{Scopes: []string{storage.ScopeReadWrite}, Logger: quiet,
		Client: &http.Client{Timeout: timeout, Transport: http.DefaultTransport.(*http.Transport).Clone()}}
	if o.CredentialsFile == "" {
		creds, err := credentials.DetectDefault(dopts)
		if err != nil {
			return nil, fmt.Errorf("remote: destination credentials: %w", err)
		}
		return creds, nil
	}
	b, err := os.ReadFile(o.CredentialsFile)
	if err != nil {
		return nil, fmt.Errorf("remote: destination credentials: %w", err)
	}
	var kind struct {
		Type credentials.CredType `json:"type"`
	}
	if err := json.Unmarshal(b, &kind); err != nil || kind.Type != credentials.ServiceAccount && kind.Type != credentials.AuthorizedUser {
		return nil, fmt.Errorf("remote: destination credentials %s: want a service account key or authorized-user credentials", o.CredentialsFile)
	}
	creds, err := credentials.NewCredentialsFromJSON(kind.Type, b, dopts)
	if err != nil {
		return nil, fmt.Errorf("remote: destination credentials %s: %w", o.CredentialsFile, err)
	}
	return creds, nil
}

// DefaultRequestTimeout is the default for DestinationOptions.RequestTimeout.
const DefaultRequestTimeout = 2 * time.Minute

// ResolveEndpoint resolves a GCS destination endpoint: endpoint when set,
// else STORAGE_EMULATOR_HOST (with emulator true), else "" for Google's
// default. It does not resolve S3 or Azure endpoints. A caller recording a
// destination's route passes both results to OpenDestination as Endpoint
// and Emulator.
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
func createS3(ctx context.Context, l objfile.Location, o DestinationOptions) (*Destination, error) {
	d := &Destination{scope: l.String()}
	s := &Store{scope: d.scope}
	if err := s.at(o.Endpoint); err != nil {
		return nil, errors.New("remote: the destination endpoint is not a URL without user information, query or fragment")
	}
	d.scope = s.scope
	timeout := o.RequestTimeout
	if timeout <= 0 {
		timeout = DefaultRequestTimeout
	}
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
func createAzure(l objfile.Location, o DestinationOptions) (*Destination, error) {
	d := &Destination{scope: l.String()}
	s := &Store{scope: d.scope}
	if err := s.at(o.Endpoint); err != nil {
		return nil, errors.New("remote: the destination endpoint is not a URL without user information, query or fragment")
	}
	d.scope = s.scope
	timeout := o.RequestTimeout
	if timeout <= 0 {
		timeout = DefaultRequestTimeout
	}
	pool := http.DefaultTransport.(*http.Transport).Clone()
	pool.ResponseHeaderTimeout = timeout
	service := azureService(l.Account, o.Endpoint)
	sas := strings.TrimPrefix(o.SAS, "?")
	q, err := url.ParseQuery(sas)
	if err != nil {
		return nil, errors.New("remote: the destination's shared access signature is not a query string") // the parse error would quote it
	}
	var tr http.RoundTripper = pool
	if q.Has("sig") {
		tr = scrubbed{next: pool, scrub: redact.SAS(sas)}
	}
	opts := &azsdk.ClientOptions{ClientOptions: azcore.ClientOptions{Transport: &http.Client{Transport: tr}}}
	var client *azsdk.Client
	if sas != "" {
		if client, err = azsdk.NewClientWithNoCredential(service+"?"+sas, opts); err != nil {
			return nil, objfile.RedactError(err)
		}
	} else {
		// Token exchanges go through a client of their own, bounded by the
		// request timeout.
		cred, err := azidentity.NewDefaultAzureCredential(&azidentity.DefaultAzureCredentialOptions{
			ClientOptions: azcore.ClientOptions{Transport: &http.Client{Timeout: timeout, Transport: http.DefaultTransport.(*http.Transport).Clone()}}})
		if err != nil {
			return nil, fmt.Errorf("remote: destination credentials: %w", err)
		}
		if client, err = azsdk.NewClient(service, cred, opts); err != nil {
			return nil, err
		}
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

// Scope names the destination's namespace: the location's canonical URI,
// and " at " the endpoint when one is set. It holds no credentials.
func (d *Destination) Scope() string { return d.scope }

// Close releases the client and its idle connections. Finish the
// Destination's reads and uploads before calling Close.
func (d *Destination) Close() error { return d.close() }

var _ interface {
	objfile.Opener
	io.Closer
} = (*Destination)(nil)
