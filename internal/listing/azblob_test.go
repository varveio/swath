// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"strings"
	"testing"

	"github.com/varveio/swath/internal/dataset/datasettest"
	"github.com/varveio/swath/internal/source/azblob"
	"github.com/varveio/swath/internal/source/azblob/azblobtest"
	"github.com/varveio/swath/internal/source/gcs/gcstest"
)

// azblobBucket serves n names through the replay's Azure route.
func azblobBucket(t *testing.T, n int) *azblobtest.Replay {
	t.Helper()
	return azblobtest.Serve(t, gcstest.Keys(n))
}

func azblobConfig(ep *azblobtest.Replay, out string, resume bool) Config {
	return Config{Source: SourceConfig{URI: "az://" + ep.Account + "/" + ep.Container,
		Options: &azblob.Options{Endpoint: strings.TrimSuffix(ep.Endpoint, "/"+ep.Account), Anonymous: true}},
		Output: OutputConfig{Dir: out, Resume: resume, NoJournal: datasettest.NoJournal()}, Limits: Limits{PartSize: 6 << 10, Workers: 8, RowBudget: 128 << 10, Spill: SpillLimits{Off: true}, PartInterval: -1}}
}
