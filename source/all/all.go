// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package all links every provider source.Open opens by address: S3, GCS
// and Azure Blob Storage.
package all

import (
	_ "github.com/varveio/swath/source/azblob"
	_ "github.com/varveio/swath/source/gcs"
	_ "github.com/varveio/swath/source/s3"
)
