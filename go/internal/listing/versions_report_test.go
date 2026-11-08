// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/varveio/swath/go/internal/filter"
	"github.com/varveio/swath/go/internal/source/gcs"
	gcsclient "github.com/varveio/swath/go/internal/source/gcs/client"
	"github.com/varveio/swath/go/internal/source/keyrange"
	s3client "github.com/varveio/swath/go/internal/source/s3/client"
)

// TestVersionReportIncludesAllS3Counters: Successful S3 version reports serialize
// every version-specific counter.
func TestVersionReportIncludesAllS3Counters(t *testing.T) {
	api := versionFixture(0)
	api.rows = api.rows[:8]
	rep, err := Run(context.Background(), versionListingConfig("", true, false, api))
	if err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal(rep)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"versions", "delete_markers", "distinct_keys", "live_objects"} {
		if !strings.Contains(string(b), `"`+name+`"`) {
			t.Errorf("successful versions report omits %s", name)
		}
	}
}

func checkVersionReport(t *testing.T, r Report, want map[string]int64, absent ...string) {
	t.Helper()
	b, err := json.Marshal(r.Output)
	if err != nil {
		t.Fatal(err)
	}
	var out map[string]any
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	for k, v := range want {
		if got, ok := out[k]; !ok || got != float64(v) {
			t.Errorf("%s=%v want=%d; %s", k, got, v, b)
		}
	}
	for _, k := range absent {
		if _, ok := out[k]; ok {
			t.Errorf("unexpected %s: %s", k, b)
		}
	}
}
func TestVersionReportS3CountsAndFilters(t *testing.T) {
	for _, output := range []bool{false, true} {
		for _, filtered := range []bool{false, true} {
			t.Run(fmt.Sprintf("dataset%v/filter%v", output, filtered), func(t *testing.T) {
				api := versionFixture(0)
				api.rows = []s3client.Version{
					{Object: s3client.Object{Key: "a", Size: 0}, VersionID: "new", IsLatest: true},
					{Object: s3client.Object{Key: "a"}, VersionID: "marker", DeleteMarker: true},
					{Object: s3client.Object{Key: "a", Size: 4}, VersionID: "old"},
					{Object: s3client.Object{Key: "b"}, VersionID: "null", DeleteMarker: true, IsLatest: true},
					{Object: s3client.Object{Key: "c", Size: 4}, VersionID: "old"},
				}
				// The fake's marker indexes are fixture positions.
				for i := range api.rows {
					api.rows[i].VersionID = fmt.Sprint(i)
				}
				dir := ""
				if output {
					dir = filepath.Join(t.TempDir(), "out")
				}
				cfg := versionListingConfig(dir, true, false, api)
				if filtered {
					cfg.Filter = &filter.Spec{MaxSize: "0"}
				}
				rep, err := Run(context.Background(), cfg)
				if err != nil {
					t.Fatal(err)
				}
				want := map[string]int64{"versions": 3, "delete_markers": 2, "distinct_keys": 3, "live_objects": 1, "rows_emitted": 5}
				if filtered {
					want = map[string]int64{"versions": 1, "delete_markers": 0, "distinct_keys": 1, "live_objects": 1, "rows_emitted": 1, "rows_filtered": 4}
				}
				checkVersionReport(t, rep, want, "noncurrent")
			})
		}
	}
}

type reportGenerations struct{ calls int }

func (p *reportGenerations) List(_ context.Context, in *gcsclient.ListInput) (*gcsclient.Page, error) {
	p.calls++
	if in.PageToken == "" {
		return &gcsclient.Page{Items: []gcsclient.Object{{Name: "a", Generation: 1, TimeDeleted: 5}, {Name: "a", Generation: 2}}, NextPageToken: "next"}, nil
	}
	return &gcsclient.Page{Items: []gcsclient.Object{{Name: "b", Generation: 1, TimeDeleted: 9}}}, nil
}
func TestVersionReportGCS(t *testing.T) {
	api := &reportGenerations{}
	cfg := Config{Source: SourceConfig{URI: "gs://b", Options: &gcs.Options{AllVersions: true, Client: api, Policy: keyrange.Policy{PageSize: 2}}}, Limits: Limits{Workers: 1}}
	rep, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	checkVersionReport(t, rep, map[string]int64{"versions": 3, "noncurrent": 2, "distinct_keys": 2, "live_objects": 1, "rows_emitted": 3}, "delete_markers")
	if api.calls != 2 {
		t.Fatal(api.calls)
	}
}
func TestVersionReportCurrentAbsent(t *testing.T) {
	rep, err := Run(context.Background(), versionListingConfig("", false, false, versionFixture(0)))
	if err != nil {
		t.Fatal(err)
	}
	checkVersionReport(t, rep, map[string]int64{"rows_emitted": 4}, "versions", "delete_markers", "noncurrent", "distinct_keys", "live_objects")
}

func TestVersionReportEmptyCountsPresent(t *testing.T) {
	api := versionFixture(0)
	api.rows = nil
	rep, err := Run(context.Background(), versionListingConfig("", true, false, api))
	if err != nil {
		t.Fatal(err)
	}
	checkVersionReport(t, rep, map[string]int64{"versions": 0, "delete_markers": 0, "distinct_keys": 0, "live_objects": 0, "rows_emitted": 0}, "noncurrent")
}
