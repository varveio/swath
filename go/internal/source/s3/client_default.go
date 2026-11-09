// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build !swath_sdkclient

package s3

import (
	"context"

	"github.com/varveio/swath/go/internal/source/s3/client"
)

// clientKind is Stats.Client for the built client.
const clientKind = "hand-rolled"

// newClient builds the hand-rolled client (package client); the AWS SDK's
// S3 service client is not linked in.
func newClient(ctx context.Context, cc client.Config) (built, error) {
	c, err := client.New(ctx, cc)
	if err != nil {
		return nil, err
	}
	return c, nil
}
