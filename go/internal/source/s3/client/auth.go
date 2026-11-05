// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/smithy-go/logging"
)

// Credentials and signing come from the AWS SDK's auth modules, not from
// this package: config's default chain (environment, shared config and
// credentials files and profiles, SSO, web identity, credential processes,
// assume-role through stscreds, container credentials, IMDSv2) and the
// SigV4 signer. Only the S3 service client is left out of the binary.

const unsignedPayload = "UNSIGNED-PAYLOAD"

// credentialProbe bounds the first credential lookup, so a host without
// credentials (no instance metadata service) falls back to anonymous
// quickly.
const credentialProbe = 3 * time.Second

type auth struct {
	creds     aws.CredentialsProvider // nil: anonymous
	source    string
	region    string
	signer    *v4.Signer
	anonymous bool
}

// loadAuth resolves the region and credentials. With -anonymous no
// credentials are looked up. Otherwise the default chain runs once; if it
// finds nothing (and no profile was named) requests go unsigned.
func loadAuth(cfg Config) (*auth, error) {
	// The SDK's own log lines (an IMDS fallback warning on every host
	// without instance metadata) are not for swath's users.
	opts := []func(*config.LoadOptions) error{config.WithLogger(logging.Nop{})}
	if cfg.Profile != "" {
		opts = append(opts, config.WithSharedConfigProfile(cfg.Profile))
	}
	// The bucket's region (--region) is the signing region only. The
	// credential chain (STS for web identity and assumed roles) keeps the
	// environment's or the profile's region, else us-east-1, so a bucket in
	// another region never moves the STS endpoint.
	opts = append(opts, config.WithDefaultRegion("us-east-1"))
	if cfg.Anonymous {
		opts = append(opts, config.WithCredentialsProvider(aws.AnonymousCredentials{}))
	}
	ac, err := config.LoadDefaultConfig(context.Background(), opts...)
	if err != nil {
		return nil, fmt.Errorf("AWS configuration: %w", err)
	}
	a := &auth{region: cfg.Region, signer: v4.NewSigner(func(o *v4.SignerOptions) { o.DisableURIPathEscaping = true })}
	if a.region == "" {
		a.region = ac.Region // the environment's or the profile's, else us-east-1
	}
	if cfg.Anonymous || ac.Credentials == nil {
		a.anonymous, a.source = true, "anonymous"
		return a, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), credentialProbe)
	defer cancel()
	c, err := ac.Credentials.Retrieve(ctx)
	switch {
	case err != nil && cfg.Profile != "":
		return nil, fmt.Errorf("credentials for profile %q: %w", cfg.Profile, err)
	case err != nil:
		a.anonymous, a.source = true, "anonymous (no credentials found)"
		return a, nil
	}
	a.creds, a.source = ac.Credentials, c.Source
	return a, nil
}

// anonymousNote warns once when -anonymous hides credentials the
// environment holds.
func anonymousNote(cfg Config) string {
	if cfg.Anonymous && (os.Getenv("AWS_ACCESS_KEY_ID") != "" || os.Getenv("AWS_PROFILE") != "" || os.Getenv("AWS_SESSION_TOKEN") != "") {
		return "-anonymous: ignoring the AWS_* credentials in the environment"
	}
	return ""
}

// errCredentials marks a failed credential refresh (not retried).
var errCredentials = errors.New("credentials")

// sign signs a GET with SigV4 and an unsigned payload.
func (a *auth) sign(ctx context.Context, req *http.Request, now time.Time) error {
	if a.anonymous {
		return nil
	}
	c, err := a.creds.Retrieve(ctx) // cached by the SDK until expiry
	if err != nil {
		return fmt.Errorf("%w: %w", errCredentials, err)
	}
	req.Header.Set("X-Amz-Content-Sha256", unsignedPayload)
	return a.signer.SignHTTP(ctx, c, req, unsignedPayload, "s3", a.region, now)
}
