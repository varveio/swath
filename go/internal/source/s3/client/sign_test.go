// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
)

// TestSignMatchesSDK signs the same requests with this client and with the
// AWS SDK's SigV4 signer (the oracle) and compares the Authorization header.
func TestSignMatchesSDK(t *testing.T) {
	now := time.Date(2026, 11, 5, 12, 34, 56, 0, time.UTC)
	inputs := []*ListInput{
		{Bucket: "bucket", MaxKeys: 1000},
		{Bucket: "bucket", Prefix: "a b/ü+%~", StartAfter: "a b/ü+%~/x", MaxKeys: 1000, FetchOwner: true},
		{Bucket: "bucket", ContinuationToken: "1ueGcxLPRx1Tr/XYExHnhbYLgveDs2J/wm36Hy4vbOwM=", Delimiter: "/"},
		{Bucket: "my.dotted.bucket", Prefix: "x"},
	}
	for _, token := range []string{"", "FwoGZXIvYXdzEJr//////////session"} {
		for _, endpoint := range []string{"", "http://127.0.0.1:9000/base"} {
			for _, path := range []bool{false, true} {
				for _, in := range inputs {
					creds := Credentials{AccessKeyID: "AKIDEXAMPLE", SecretAccessKey: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY", SessionToken: token}
					c := &Client{cfg: Config{Endpoint: endpoint, PathStyle: path}, region: "eu-west-1"}
					c.base = mustBase(t, endpoint, "eu-west-1")
					u := c.url(in)

					ours, _ := http.NewRequest(http.MethodGet, u.String(), nil)
					ours.Host = u.Host
					(&signer{creds: creds, region: "eu-west-1"}).sign(ours, now)

					sdk, _ := http.NewRequest(http.MethodGet, u.String(), nil)
					sdk.Host = u.Host
					sdk.Header.Set("X-Amz-Content-Sha256", unsignedPayload)
					err := v4.NewSigner().SignHTTP(context.Background(),
						aws.Credentials{AccessKeyID: creds.AccessKeyID, SecretAccessKey: creds.SecretAccessKey, SessionToken: token},
						sdk, unsignedPayload, "s3", "eu-west-1", now, func(o *v4.SignerOptions) { o.DisableURIPathEscaping = true })
					if err != nil {
						t.Fatal(err)
					}
					if g, w := ours.Header.Get("Authorization"), sdk.Header.Get("Authorization"); g != w {
						t.Fatalf("%s\n got %s\nwant %s", u, g, w)
					}
				}
			}
		}
	}
}

func mustBase(t *testing.T, endpoint, region string) *url.URL {
	t.Helper()
	c, err := New(Config{Endpoint: endpoint, Region: region, Anonymous: true})
	if err != nil {
		t.Fatal(err)
	}
	return c.base
}
