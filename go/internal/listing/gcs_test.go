// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/faults"
	"github.com/varveio/swath/go/internal/journal"
)

// gcsBucket serves objects.list over 8,000 names with 1 ms per request: a
// minimal JSON API (prefix, startOffset inclusive, endOffset exclusive,
// maxResults, a token naming the last name; no delimiter: the default
// policy sends none) for the run-level GCS tests.
func gcsBucket(t *testing.T) string {
	t.Helper()
	keys := make([]string, 8000)
	for i := range keys {
		keys[i] = fmt.Sprintf("d%02d/sub%d/obj+%06d", i%9, i%4, i)
	}
	sort.Strings(keys)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(time.Millisecond)
		q := r.URL.Query()
		if q.Get("delimiter") != "" || r.URL.Path != "/storage/v1/b/bkt/o" {
			http.Error(w, `{"error":{"code":400,"message":"unsupported"}}`, 400)
			return
		}
		after := ""
		if tok := q.Get("pageToken"); tok != "" {
			b, err := base64.RawURLEncoding.DecodeString(tok)
			if err != nil {
				http.Error(w, `{"error":{"code":400,"message":"token"}}`, 400)
				return
			}
			after = string(b)
		}
		limit := 1000
		if n, err := strconv.Atoi(q.Get("maxResults")); err == nil && n > 0 && n < limit {
			limit = n
		}
		var resp struct {
			Items []map[string]string `json:"items,omitempty"`
			Next  string              `json:"nextPageToken,omitempty"`
		}
		start, end, prefix := q.Get("startOffset"), q.Get("endOffset"), q.Get("prefix")
		for i := sort.SearchStrings(keys, max(start, prefix)); i < len(keys); i++ {
			k := keys[i]
			if !strings.HasPrefix(k, prefix) || (end != "" && k >= end) {
				break
			}
			if k <= after {
				continue
			}
			if len(resp.Items) == limit {
				resp.Next = base64.RawURLEncoding.EncodeToString([]byte(resp.Items[limit-1]["name"]))
				break
			}
			resp.Items = append(resp.Items, map[string]string{"name": k, "size": strconv.Itoa(len(k)),
				"updated": "2026-01-01T00:00:00Z", "generation": strconv.Itoa(1000 + i), "etag": "e" + strconv.Itoa(i)})
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	t.Cleanup(srv.Close)
	return srv.URL + "/storage/v1/"
}

func gcsConfig(ep, out string, resume bool) Config {
	return Config{Source: SourceConfig{URI: "gs://bkt", Options: &GCSConfig{Endpoint: ep, Anonymous: true}},
		Output: OutputConfig{Dir: out, Resume: resume}, Limits: Limits{PartSize: 6 << 10, Workers: 8, Memory: 256 << 10, Spill: SpillLimits{Off: true}}}
}

// TestGCSInterruptedResume: a GCS run killed (an injected crash that skips
// all cleanup) at a response, at a split, between a part's record and its
// rename, and after a part's rename, then resumed, yields the dataset of an
// uninterrupted run byte for byte; a resume through another route or
// another billed project is refused.
func TestGCSInterruptedResume(t *testing.T) {
	ep := gcsBucket(t)
	ref := filepath.Join(t.TempDir(), "ref")
	if _, err := Run(context.Background(), gcsConfig(ep, ref, false)); err != nil {
		t.Fatal(err)
	}
	for _, point := range []string{"response-read", "split", "recorded", "renamed"} {
		for _, nth := range []int64{1, 4} {
			t.Run(fmt.Sprintf("%s#%d", point, nth), func(t *testing.T) {
				out := filepath.Join(t.TempDir(), "ds")
				var seen atomic.Int64
				restore := faults.Set(func(p string) error {
					if p == point && seen.Add(1) == nth {
						return faults.ErrCrash
					}
					return nil
				})
				_, err := Run(context.Background(), gcsConfig(ep, out, false))
				restore()
				if seen.Load() < nth {
					t.Fatalf("%s reached %d times only (err %v)", point, seen.Load(), err)
				}
				if !errors.Is(err, faults.ErrCrash) {
					t.Fatalf("crash not injected: %v", err)
				}
				sum, err := Run(context.Background(), gcsConfig(ep, out, true))
				if err != nil {
					t.Fatalf("resume: %v", err)
				}
				sameDataset(t, point, ref, out)
				t.Logf("resumed %d parts (%d rows), listed %d more rows", sum.Output.PartsRetained, sum.Output.RowsRetained, sum.Output.RowsListed)
			})
		}
	}
	for name, change := range map[string]func(*GCSConfig){
		"route":   func(o *GCSConfig) { o.Endpoint = strings.Replace(ep, "127.0.0.1", "localhost", 1) },
		"project": func(o *GCSConfig) { o.UserProject = "other" },
	} {
		t.Run("refused/"+name, func(t *testing.T) {
			out := filepath.Join(t.TempDir(), "ds")
			var seen atomic.Int64
			restore := faults.Set(func(p string) error {
				if p == "renamed" && seen.Add(1) == 2 {
					return faults.ErrCrash
				}
				return nil
			})
			_, err := Run(context.Background(), gcsConfig(ep, out, false))
			restore()
			if !errors.Is(err, faults.ErrCrash) {
				t.Fatalf("crash not injected: %v", err)
			}
			cfg := gcsConfig(ep, out, true)
			change(cfg.Source.Options.(*GCSConfig))
			var mm *journal.MismatchError
			if _, err := Run(context.Background(), cfg); !errors.As(err, &mm) {
				t.Fatalf("resume with another %s: %v, want a mismatch", name, err)
			}
		})
	}
}
