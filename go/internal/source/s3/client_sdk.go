// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build swath_sdkclient

package s3

import (
	"context"

	"github.com/varveio/swath/go/internal/source/s3/client"
	"github.com/varveio/swath/go/internal/source/s3/sdkclient"
)

// clientKind is Stats.Client for the built client.
const clientKind = "sdk"

// newClient builds the SDK-backed client (package sdkclient). Measurement
// plumbing, removed after the comparison: SWATH_S3_DECODER=sdk has the
// SDK's deserializer decode current-object pages instead of the scanner
// (sdkclient/measure.go).
func newClient(ctx context.Context, cc client.Config) (built, error) {
	c, err := sdkclient.New(ctx, cc)
	if err != nil {
		return nil, err
	}
	return c, nil
}
