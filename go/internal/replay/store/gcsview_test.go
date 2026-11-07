// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/parquet-go/parquet-go"
)

// TestGCSETag pins gcsETag, the S3 ETag of a GCS object as Cloud Storage's
// XML API forms it (server TestLiveGCSXMLAPIContents has the probes).
func TestGCSETag(t *testing.T) {
	for _, c := range []struct{ md5, etag, want string }{
		{"Hn9gS4ZBWt6U4V/vhidgmw==", "CMDE1s/L3JADEAE=", "1e7f604b86415ade94e15fef8627609b"},
		{"1B2M2Y8AsgTpgAmY7PhCfg==", "", "d41d8cd98f00b204e9800998ecf8427e"},
		{"", "CM6Qk6/M3JADEAE=", "-CM6Qk6/M3JADEAE="},
		{"", "", syntheticGCSETag},
		// Not the standard base64 of 16 bytes: the etag answers.
		{"bm90IGFuIE1ENQ==", "CAEQAQ==", "-CAEQAQ=="},
		{"Hn9gS4ZBWt6U4V_vhidgmw==", "CAEQAQ==", "-CAEQAQ=="},
		{"Hn9gS4ZBWt6U4V/vhidgmx==", "CAEQAQ==", "-CAEQAQ=="},
		{"Hn9gS4ZBWt6U4V/vhidgmw", "CAEQAQ==", "-CAEQAQ=="},
		{"Hn9gS4ZBWt6U4V/vhidg\nmw=", "", syntheticGCSETag},
	} {
		if got := string(gcsETag([]byte("x"), c.md5, c.etag)); got != "x"+c.want {
			t.Errorf("gcsETag(%q, %q) = %q, want x%q", c.md5, c.etag, got, c.want)
		}
	}
}

// gcsRow is a swath/gcs-objects/v1 row with the columns the S3 view reads.
type gcsRow struct {
	Key          string  `parquet:"key"`
	Size         int64   `parquet:"size"`
	ETag         *string `parquet:"etag,optional"`
	StorageClass *string `parquet:"storage_class,optional"`
	MD5          *string `parquet:"md5,optional"`
}

// gcsRows returns n rows cycling through the cases the S3 ETag has: an
// MD5 (the key's), an etag and no MD5 (a composite object), an MD5 that is
// not one, neither, and empty strings; each with the ETag the S3 view
// must give it.
func gcsRows(n int) ([]gcsRow, []string) {
	str := func(s string) *string { return &s }
	classes := []string{"STANDARD", "NEARLINE", "COLDLINE", "ARCHIVE", "MULTI_REGIONAL"}
	rows, want := make([]gcsRow, n), make([]string, n)
	for i := range rows {
		key := fmt.Sprintf("k/%05d", i)
		sum := md5.Sum([]byte(key))
		etag := base64.StdEncoding.EncodeToString(fmt.Appendf(nil, "\x08%c\x10\x01", byte(i%128)))
		r := gcsRow{Key: key, Size: int64(i), StorageClass: str(classes[i%len(classes)])}
		switch i % 5 {
		case 0:
			r.MD5, r.ETag, want[i] = str(base64.StdEncoding.EncodeToString(sum[:])), str(etag), hex.EncodeToString(sum[:])
		case 1:
			r.ETag, want[i] = str(etag), "-"+etag
		case 2:
			r.MD5, r.ETag, want[i] = str("not an md5"), str(etag), "-"+etag
		case 3:
			want[i] = syntheticGCSETag
		case 4:
			r.MD5, r.ETag, want[i] = str(""), str(""), syntheticGCSETag
		}
		rows[i] = r
	}
	return rows, want
}

// writeGCSRows writes rows as the one part of a swath/gcs-objects/v1
// dataset: writeTypedFormat2's, its manifest entry given the part's row
// count and key bounds.
func writeGCSRows(t *testing.T, rows []gcsRow) string {
	t.Helper()
	dir := writeTypedFormat2(t, "swath/gcs-objects/v1", rows)
	path := filepath.Join(dir, "manifest.json")
	raw, _ := os.ReadFile(path)
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	part := m["parts"].([]any)[0].(map[string]any)
	part["rows"] = len(rows)
	part["min_key_base64"] = base64.StdEncoding.EncodeToString([]byte(rows[0].Key))
	part["max_key_base64"] = base64.StdEncoding.EncodeToString([]byte(rows[len(rows)-1].Key))
	raw, _ = json.Marshal(m)
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	return dir
}

// TestGCSRecordingS3View reads a swath/gcs-objects/v1 dataset through the
// window cache (packed, ETags derived per window), through the bare
// reader (each row's Object), through a cache whose arena limit sends
// every fill to the unpacked path, and as a rollup, and requires every
// row's ETag the XML API's form of its MD5 and etag, no StorageClass, and
// the answers equal. Before, the ETag was the GCS etag (nil without one)
// and StorageClass the GCS class.
func TestGCSRecordingS3View(t *testing.T) {
	ctx := context.Background()
	rows, want := gcsRows(1000)
	dir := writeGCSRows(t, rows)
	open := func(opts Options) *Sorted {
		s, _, err := Open(ctx, dir, opts)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = s.Close() })
		return s.(*Sorted)
	}
	bare := open(Options{Prefetch: Prefetch{Off: true}})
	cached := open(Options{Prefetch: Prefetch{WindowRows: 37, MaxWindows: 5}})
	// A 100-row read's keys (700 bytes), etags (480) and MD5s (680) fit
	// in 900 bytes, its derived ETags (1,840) do not.
	overflow := open(Options{Prefetch: Prefetch{WindowRows: 37, MaxWindows: 5}})
	overflow.arenaLimit = 900
	check := func(how string, got []Object, from int) {
		t.Helper()
		for i, o := range got {
			if o.ETag == nil || *o.ETag != want[from+i] || o.StorageClass != nil || o.ChecksumAlgorithm != nil || o.OwnerID != nil {
				t.Fatalf("%s: %s: %+v, want ETag %q and no StorageClass", how, o.Key, o, want[from+i])
			}
		}
	}
	for name, s := range map[string]*Sorted{"bare": bare, "cached": cached, "overflow": overflow} {
		for page, from := 0, []byte(nil); ; page++ {
			got, err := s.Rows(ctx, from, false, nil, 100, Projection{Owner: page%2 == 0})
			if err != nil {
				t.Fatal(err)
			}
			if len(got) == 0 {
				break
			}
			check(fmt.Sprintf("%s page %d", name, page), got, page*100)
			from = got[len(got)-1].Key
		}
		entries, ok, err := s.DelimitedRollup(ctx, nil, true, nil, []byte("k/"), []byte("/"), 1000, Projection{}, true)
		if err != nil || !ok || len(entries) != len(rows) {
			t.Fatalf("%s rollup: %d entries, %v %v", name, len(entries), ok, err)
		}
		objects := make([]Object, len(entries))
		for i, e := range entries {
			objects[i] = *e.Object
		}
		check(name+" rollup", objects, 0)
	}
}

// TestPackerDerivedETagsStopAtTheArenaLimit packs a GCS recording's MD5s
// within the arena limit and requires the ETags derived from them (32
// bytes for every 24) refused past it: errPackedOverflow, so the read is
// served unpacked, never with an offset past the limit.
func TestPackerDerivedETagsStopAtTheArenaLimit(t *testing.T) {
	sum := base64.StdEncoding.EncodeToString(make([]byte, 16))
	values := []parquet.Value{parquet.ByteArrayValue([]byte(sum)), parquet.ByteArrayValue([]byte(sum))}
	columns := gcsObjects.columns(Projection{})
	md5Column := len(columns) - 1
	if columns[md5Column] != "md5" {
		t.Fatalf("columns %v", columns)
	}
	for limit, ok := range map[int]bool{64: true, 63: false, 48: false} {
		k := gcsObjects.packer(columns, 2, limit)
		k.Keys([][]byte{[]byte("a"), []byte("b")})
		k.Values(md5Column, values)
		for c := range columns[:md5Column] {
			k.Missing(c)
		}
		p, err := k.packed()
		if ok != (err == nil) || !ok && !errors.Is(err, errPackedOverflow) {
			t.Fatalf("limit %d: %v", limit, err)
		}
		if ok {
			got := p.objects(nil, 0, 2)
			if *got[1].ETag != "00000000000000000000000000000000" || got[1].StorageClass != nil {
				t.Fatalf("limit %d: %+v", limit, got[1])
			}
		}
	}
}
