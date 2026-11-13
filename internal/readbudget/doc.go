// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package readbudget holds the request budgets of swath's reader over the
// object stores: tests that count HTTP requests on arrival and separately
// check admission concurrency with channel barriers against the objfile
// fakes of S3, GCS and Azure Blob Storage, through the SeekParquet Source
// objfile/spsource provides. It has no code of its own.
package readbudget
