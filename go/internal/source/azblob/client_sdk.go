// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build swath_sdkclient

package azblob

import (
	"context"

	"github.com/varveio/swath/go/internal/source/azblob/client"
	"github.com/varveio/swath/go/internal/source/azblob/sdkclient"
)

// clientKind names the built client in Stats.
const clientKind = "sdk"

// newClient builds the client over the Azure SDK's container client.
func newClient(ctx context.Context, cfg client.Config) (built, error) {
	c, err := sdkclient.New(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return c, nil
}
