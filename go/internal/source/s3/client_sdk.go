// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build swath_sdkclient

package s3

import (
	"context"
	"fmt"
	"os"

	"github.com/varveio/swath/go/internal/source/s3/client"
	"github.com/varveio/swath/go/internal/source/s3/sdkclient"
)

// clientKind is Stats.Client for the built client.
const clientKind = "sdk"

// newClient builds the SDK-backed client (package sdkclient).
//
// SWATH_S3_DECODER (sdk, the default, or scanner) selects the body decoder
// (sdkclient.Decoder), so one tagged binary can measure both.
// It is temporary experiment plumbing, read here only, and goes when the
// decoder is chosen.
func newClient(ctx context.Context, cc client.Config) (built, error) {
	dec := sdkclient.DecoderSDK
	switch v := os.Getenv("SWATH_S3_DECODER"); v {
	case "", "sdk":
	case "scanner":
		dec = sdkclient.DecoderScanner
	default:
		return nil, fmt.Errorf("SWATH_S3_DECODER=%q: want sdk or scanner", v)
	}
	c, err := sdkclient.New(ctx, cc, sdkclient.WithDecoder(dec))
	if err != nil {
		return nil, err
	}
	return c, nil
}
