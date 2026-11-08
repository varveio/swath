// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/varveio/swath/go/internal/faults"
	"github.com/varveio/swath/go/internal/journal"
	"github.com/varveio/swath/go/internal/source/keyrange"
	"github.com/varveio/swath/go/internal/source/s3"
	"github.com/varveio/swath/go/internal/source/s3/client"
)

type reviewVersionAPI struct {
	rows    []client.Version
	crashAt int
}

func (p *reviewVersionAPI) Route(string) string { return "https://review.invalid/b" }
func (p *reviewVersionAPI) ListObjectsV2(context.Context, *client.ListInput) (*client.Page, error) {
	return &client.Page{Contents: []client.Object{{Key: "a"}, {Key: "b"}, {Key: "c"}, {Key: "d"}}}, nil
}
func (p *reviewVersionAPI) ListObjectVersions(_ context.Context, in *client.VersionsInput) (*client.VersionPage, error) {
	i := sort.Search(len(p.rows), func(i int) bool { return p.rows[i].Key > in.KeyMarker })
	if in.VersionIDMarker != "" {
		n, _ := strconv.Atoi(in.VersionIDMarker)
		i = n + 1
	}
	if p.crashAt > 0 && i >= p.crashAt {
		return nil, faults.ErrCrash
	}
	end := min(len(p.rows), i+2)
	out := &client.VersionPage{Versions: append([]client.Version(nil), p.rows[i:end]...)}
	if end < len(p.rows) {
		out.IsTruncated = true
		out.NextKeyMarker = p.rows[end-1].Key
		out.NextVersionIDMarker = p.rows[end-1].VersionID
	}
	return out, nil
}
func reviewVersionFixture(crashAt int) *reviewVersionAPI {
	p := &reviewVersionAPI{crashAt: crashAt}
	for i, n := range []int{3, 5000, 2, 3} {
		for j := range n {
			p.rows = append(p.rows, client.Version{Object: client.Object{Key: string(rune('a' + i)), Size: int64(j), Mtime: 1}, VersionID: strconv.Itoa(len(p.rows)), IsLatest: j == 0, DeleteMarker: j%7 == 6})
		}
	}
	return p
}
func reviewListingConfig(out string, versions, resume bool, api *reviewVersionAPI) Config {
	return Config{Source: SourceConfig{URI: "s3://b", Options: &s3.Options{Client: api, AllVersions: versions, Policy: keyrange.Policy{PageSize: 2}}},
		Output: OutputConfig{Dir: out, Resume: resume}, Limits: Limits{Workers: 1, Encoders: 1, PartSize: 64 << 20, PartMaxRows: 2}}
}
func TestReviewOKCrashDuringHotGroup(t *testing.T) {
	ref, out := filepath.Join(t.TempDir(), "ref"), filepath.Join(t.TempDir(), "out")
	if _, err := Run(context.Background(), reviewListingConfig(ref, true, false, reviewVersionFixture(0))); err != nil {
		t.Fatal(err)
	}
	_, err := Run(context.Background(), reviewListingConfig(out, true, false, reviewVersionFixture(4200)))
	if !errors.Is(err, faults.ErrCrash) {
		t.Fatalf("did not stop mid-group: %v", err)
	}
	j, err := journal.Read(out)
	if err != nil {
		t.Fatal(err)
	}
	if len(j.Records) != 1 || string(j.Records[0].Part.MaxKey) != "a" {
		t.Fatalf("durable prefix after mid-b crash: %+v", j.Records)
	}
	rep, err := Run(context.Background(), reviewListingConfig(out, true, true, reviewVersionFixture(0)))
	if err != nil {
		t.Fatal(err)
	}
	checkVersionReport(t, rep, map[string]int64{"rows_retained": 3, "rows_emitted": 5005, "versions": 4291, "delete_markers": 714, "distinct_keys": 3, "live_objects": 3}, "noncurrent")
	m1, m2 := manifestOf(t, ref), manifestOf(t, out)
	if len(m1.Parts) != len(m2.Parts) {
		t.Fatalf("parts %d vs %d", len(m1.Parts), len(m2.Parts))
	}
	for i := range m1.Parts {
		if m1.Parts[i].PartInfo != m2.Parts[i].PartInfo {
			t.Fatalf("part %d differs", i)
		}
	}
}
func TestReviewOKModeSwitchRefused(t *testing.T) {
	for _, versions := range []bool{false, true} {
		t.Run(fmt.Sprint(versions), func(t *testing.T) {
			out := filepath.Join(t.TempDir(), "out")
			var appends atomic.Int64
			restore := faults.Set(func(point string) error {
				if point == "journal-append" && appends.Add(1) == 2 {
					return faults.ErrCrash
				}
				return nil
			})
			_, err := Run(context.Background(), reviewListingConfig(out, versions, false, reviewVersionFixture(0)))
			restore()
			if !errors.Is(err, faults.ErrCrash) {
				t.Fatalf("did not leave interrupted journal: %v", err)
			}
			_, err = Run(context.Background(), reviewListingConfig(out, !versions, true, reviewVersionFixture(0)))
			if err == nil || !strings.Contains(err.Error(), "schema") {
				t.Fatalf("mode switch not refused by schema identity: %v", err)
			}
		})
	}
}
