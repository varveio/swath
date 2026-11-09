// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package remote opens a dataset root in S3, GCS or Azure Blob Storage by
// its URI, with each provider's default credentials. A program that needs a
// region, an endpoint, anonymous access or its own transport builds the SDK
// client itself and uses objfile/s3, objfile/gcs or objfile/azblob with
// objfile.Root instead.
package remote

import (
	"context"
	"io"
	"net/http"
	"os"

	"cloud.google.com/go/storage"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	azsdk "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	s3sdk "github.com/aws/aws-sdk-go-v2/service/s3"
	"google.golang.org/api/option"
	htransport "google.golang.org/api/transport/http"

	"github.com/varveio/swath/go/objfile"
	"github.com/varveio/swath/go/objfile/azblob"
	"github.com/varveio/swath/go/objfile/gcs"
	"github.com/varveio/swath/go/objfile/internal/ranged"
	"github.com/varveio/swath/go/objfile/s3"
)

// Store is a dataset root opened by Open: an objfile.Opener of the names
// under the location's prefix, through clients it owns. It is safe for
// concurrent use. It does not bound the requests in flight; wrap it with
// objfile.Limit for that.
type Store struct {
	o     objfile.Opener
	scope string
	close func() error
}

// Open returns the Store of location, a URI ParseLocation accepts, with the
// provider's default credentials:
//
//   - s3: the AWS SDK's default configuration (environment, shared files,
//     container and instance roles), which also gives the region and an
//     endpoint set by AWS_ENDPOINT_URL_S3 or AWS_ENDPOINT_URL;
//   - gs: Application Default Credentials with the read-only scope, or none
//     when STORAGE_EMULATOR_HOST names an emulator;
//   - az: azidentity's DefaultAzureCredential, on the account's
//     blob.core.windows.net endpoint.
//
// Open makes no request; a missing bucket or a refused credential fails the
// first Open of the Store.
func Open(ctx context.Context, location string) (*Store, error) {
	l, err := objfile.ParseLocation(location)
	if err != nil {
		return nil, err
	}
	s := &Store{scope: l.String(), close: func() error { return nil }}
	var o objfile.Opener
	switch l.Scheme {
	case "s3":
		cfg, err := config.LoadDefaultConfig(ctx)
		if err != nil {
			return nil, err
		}
		client := s3sdk.NewFromConfig(cfg)
		if ep := aws.ToString(client.Options().BaseEndpoint); ep != "" {
			s.scope += " at " + ep
		}
		o = s3.New(client, l.Container)
	case "gs":
		client, endpoint, err := gcsClient(ctx)
		if err != nil {
			return nil, err
		}
		if endpoint != "" {
			s.scope += " at " + endpoint
		}
		o, s.close = gcs.New(client, l.Container), client.Close
	case "az":
		client, err := azureClient(l.Account)
		if err != nil {
			return nil, err
		}
		o = azblob.New(client, l.Container)
	}
	s.o = objfile.Root(o, l.Prefix)
	return s, nil
}

// gcsClient returns a read-only GCS client whose transport checks every
// ranged response, including those to the requests the SDK's reader sends
// on its own (ranged.Transport), and the emulator endpoint when there is one.
func gcsClient(ctx context.Context) (*storage.Client, string, error) {
	base := ranged.Transport(http.DefaultTransport)
	if host := os.Getenv("STORAGE_EMULATOR_HOST"); host != "" {
		client, err := storage.NewClient(ctx, option.WithHTTPClient(&http.Client{Transport: base}))
		return client, host, err
	}
	tr, err := htransport.NewTransport(ctx, base, option.WithScopes(storage.ScopeReadOnly))
	if err != nil {
		return nil, "", err
	}
	client, err := storage.NewClient(ctx, option.WithHTTPClient(&http.Client{Transport: tr}))
	return client, "", err
}

// azureClient returns the client of account; a test replaces it.
var azureClient = func(account string) (*azsdk.Client, error) {
	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return nil, err
	}
	return azsdk.NewClient("https://"+account+".blob.core.windows.net/", cred, nil)
}

// Open opens name, a path objfile.CheckPath accepts, under the location's
// prefix.
func (s *Store) Open(ctx context.Context, name string) (objfile.Object, error) {
	return s.o.Open(ctx, name)
}

// OpenSized opens name, of size bytes, under the location's prefix.
func (s *Store) OpenSized(ctx context.Context, name string, size int64) (objfile.Object, error) {
	return s.o.OpenSized(ctx, name, size)
}

// Scope names the Store's storage namespace for objfile.Pin and
// objfile.Restore: the location's canonical URI, followed by " at " and the
// endpoint when one is set by the environment. It holds no credentials.
func (s *Store) Scope() string { return s.scope }

var _ interface {
	objfile.Opener
	io.Closer
} = (*Store)(nil)

// Close releases the clients the Store owns. Read the Objects it opened
// before Close.
func (s *Store) Close() error { return s.close() }
