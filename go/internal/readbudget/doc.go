// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package readbudget holds the request budgets of swath's reader over the
// object stores: tests that count the HTTP requests, and the rounds of
// requests one after another, that reader operations make against the
// objfile fakes of S3, GCS and Azure Blob Storage, through the SeekParquet
// Source objfile/spsource provides. It has no code of its own.
package readbudget
