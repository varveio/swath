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
	"reflect"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"

	"github.com/varveio/swath/go/sortedparquet/sortedparquettest"
)

// TestGCSETag pins gcsETag, the S3 ETag of a GCS object as Cloud Storage's
// XML API forms it (server TestLiveGCSXMLAPIContents has the probes).
func TestGCSETag(t *testing.T) {
	for _, c := range []struct{ md5, etag, want string }{
		{"Hn9gS4ZBWt6U4V/vhidgmw==", "CMDE1s/L3JADEAE=", "1e7f604b86415ade94e15fef8627609b"},
		{"1B2M2Y8AsgTpgAmY7PhCfg==", "", "d41d8cd98f00b204e9800998ecf8427e"},
		{"", "CM6Qk6/M3JADEAE=", "-CM6Qk6/M3JADEAE="},
		{"", "", syntheticETag},
		// Not the standard base64 of 16 bytes: the etag answers.
		{"bm90IGFuIE1ENQ==", "CAEQAQ==", "-CAEQAQ=="},
		{"Hn9gS4ZBWt6U4V_vhidgmw==", "CAEQAQ==", "-CAEQAQ=="},
		{"Hn9gS4ZBWt6U4V/vhidgmx==", "CAEQAQ==", "-CAEQAQ=="},
		{"Hn9gS4ZBWt6U4V/vhidgmw", "CAEQAQ==", "-CAEQAQ=="},
		{"Hn9gS4ZBWt6U4V/vhidg\nmw=", "", syntheticETag},
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
	ContentType  *string `parquet:"content_type,optional"`
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
		if i%3 != 0 {
			r.ContentType = str(fmt.Sprintf("type/%d", i%7))
		}
		switch i % 5 {
		case 0:
			r.MD5, r.ETag, want[i] = str(base64.StdEncoding.EncodeToString(sum[:])), str(etag), hex.EncodeToString(sum[:])
		case 1:
			r.ETag, want[i] = str(etag), "-"+etag
		case 2:
			r.MD5, r.ETag, want[i] = str("not an md5"), str(etag), "-"+etag
		case 3:
			want[i] = syntheticETag
		case 4:
			r.MD5, r.ETag, want[i] = str(""), str(""), syntheticETag
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
		k := gcsObjects.packer(columns, Projection{}, 2, limit)
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

// TestGCSRecordingAzureView reads a swath/gcs-objects/v1 dataset with the
// Content projection through the window cache (the MD5 column packed once
// for the ETags and the MD5 field), the bare reader, the arena overflow
// path and a rollup, and requires every row's ContentType and MD5 as
// stored (nil for a null), its ETag still the S3 view's, and both nil
// without the projection, which reads no content_type column. A schema
// without the columns drops the projection, so its reads share windows
// with reads without it. Before, no read carried a content type or MD5.
func TestGCSRecordingAzureView(t *testing.T) {
	ctx := context.Background()
	rows, etags := gcsRows(1000)
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
	overflow := open(Options{Prefetch: Prefetch{WindowRows: 37, MaxWindows: 5}})
	overflow.arenaLimit = 900
	same := func(a, b *string) bool { return a == nil && b == nil || a != nil && b != nil && *a == *b }
	check := func(how string, got []Object, from int, content bool) {
		t.Helper()
		for i, o := range got {
			r := rows[from+i]
			wantType, wantMD5 := r.ContentType, r.MD5
			if !content {
				wantType, wantMD5 = nil, nil
			}
			if string(o.Key) != r.Key || !same(o.ContentType, wantType) || !same(o.MD5, wantMD5) || o.ETag == nil || *o.ETag != etags[from+i] {
				t.Fatalf("%s: %s: %+v, want ContentType %v MD5 %v", how, o.Key, o, wantType, wantMD5)
			}
		}
	}
	for name, s := range map[string]*Sorted{"bare": bare, "cached": cached, "overflow": overflow} {
		for page, from := 0, []byte(nil); ; page++ {
			content := page%3 != 2
			got, err := s.Rows(ctx, from, false, nil, 100, Projection{Owner: page%2 == 0, Content: content})
			if err != nil {
				t.Fatal(err)
			}
			if len(got) == 0 {
				break
			}
			check(fmt.Sprintf("%s page %d", name, page), got, page*100, content)
			from = got[len(got)-1].Key
		}
		entries, ok, err := s.DelimitedRollup(ctx, nil, true, nil, []byte("k/"), []byte("/"), 1000, Projection{Content: true}, true)
		if err != nil || !ok || len(entries) != len(rows) {
			t.Fatalf("%s rollup: %d entries, %v %v", name, len(entries), ok, err)
		}
		objects := make([]Object, len(entries))
		for i, e := range entries {
			objects[i] = *e.Object
		}
		check(name+" rollup", objects, 0, true)
	}
	for _, p := range []objectProjection{s3Objects, localObjects} {
		if got := p.projection(Projection{Owner: true, Content: true}); got != (Projection{Owner: true}) {
			t.Errorf("%+v: projection %+v, want Content dropped", p, got)
		}
	}
	if got := gcsObjects.projection(Projection{Content: true}); got != (Projection{Content: true}) {
		t.Errorf("GCS projection %+v", got)
	}
}

// gcsTimedRow is a swath/gcs-objects/v1 row with both time columns, as
// the lister writes since it records time_created; gcsUntimedRow is one
// written before, without the column.
type gcsTimedRow struct {
	Key          string  `parquet:"key"`
	Size         int64   `parquet:"size"`
	LastModified *int64  `parquet:"last_modified,optional,timestamp(microsecond)"`
	ETag         *string `parquet:"etag,optional"`
	TimeCreated  *int64  `parquet:"time_created,optional,timestamp(microsecond)"`
}

type gcsUntimedRow struct {
	Key          string  `parquet:"key"`
	Size         int64   `parquet:"size"`
	LastModified *int64  `parquet:"last_modified,optional,timestamp(microsecond)"`
	ETag         *string `parquet:"etag,optional"`
}

// writeGCSParts writes a swath/gcs-objects/v1 dataset of one part per
// rows slice, in order: parts of gcsUntimedRow where untimed is true,
// else of gcsTimedRow (the column set of a run resumed by a build that
// records time_created).
func writeGCSParts(t *testing.T, parts [][]gcsTimedRow, untimed []bool) string {
	t.Helper()
	const schemaID = "swath/gcs-objects/v1"
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "data"), 0o755); err != nil {
		t.Fatal(err)
	}
	options := []parquet.WriterOption{parquet.SortingWriterConfig(parquet.SortingColumns(parquet.Ascending("key"))), parquet.PageBufferSize(512)}
	for k, v := range sortedparquettest.Format2Stamps(schemaID, 1024) {
		options = append(options, parquet.KeyValueMetadata(k, v))
	}
	var entries []map[string]any
	for i, rows := range parts {
		name := fmt.Sprintf("data/part-%05d.parquet", i)
		path := filepath.Join(dir, filepath.FromSlash(name))
		var err error
		if untimed[i] {
			old := make([]gcsUntimedRow, len(rows))
			for j, r := range rows {
				old[j] = gcsUntimedRow{Key: r.Key, Size: r.Size, LastModified: r.LastModified, ETag: r.ETag}
			}
			err = parquet.WriteFile(path, old, options...)
		} else {
			err = parquet.WriteFile(path, rows, options...)
		}
		if err != nil {
			t.Fatal(err)
		}
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		entries = append(entries, map[string]any{"path": name, "rows": len(rows), "bytes": info.Size(),
			"min_key_base64": base64.StdEncoding.EncodeToString([]byte(rows[0].Key)),
			"max_key_base64": base64.StdEncoding.EncodeToString([]byte(rows[len(rows)-1].Key))})
	}
	raw, _ := json.Marshal(map[string]any{"format": "swath-dataset", "format_version": 2, "schema_id": schemaID,
		"coverage": map[string]any{"complete": true}, "parts": entries})
	if err := os.WriteFile(filepath.Join(dir, "manifest.json"), raw, 0o644); err != nil {
		t.Fatal(err)
	}
	return dir
}

// TestGCSRecordingLastModifiedFromTimeCreated reads a swath/gcs-objects/v1
// dataset of three parts, the first written without time_created and the
// others with it (some rows null), through the window cache (packed),
// the bare reader, the arena overflow path and a rollup. With Created
// (the S3 route) every row's LastModified is its time_created, else its
// last_modified; without it (the Azure route, the GCS route's
// non-native reads) always its last_modified; the answers of every path
// are equal. A schema without the column drops Created, so its reads
// share windows with reads without it. Before, LastModified was
// last_modified on every route.
func TestGCSRecordingLastModifiedFromTimeCreated(t *testing.T) {
	ctx := context.Background()
	var parts [][]gcsTimedRow
	want := map[string][2]int64{} // key: LastModified with and without Created
	for p := range 3 {
		var rows []gcsTimedRow
		for i := range 400 {
			n := p*400 + i
			key := fmt.Sprintf("k/%05d", n)
			etag := fmt.Sprint(n)
			updated, created := int64(1_700_000_000_000_000+n*1_000_003), int64(1_600_000_000_000_000+n*999_983)
			r := gcsTimedRow{Key: key, Size: int64(n), LastModified: &updated, ETag: &etag}
			stored := p > 0 && n%7 != 0
			if stored {
				r.TimeCreated = &created
				want[key] = [2]int64{created, updated}
			} else {
				want[key] = [2]int64{updated, updated}
			}
			rows = append(rows, r)
		}
		parts = append(parts, rows)
	}
	dir := writeGCSParts(t, parts, []bool{true, false, false})
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
	overflow := open(Options{Prefetch: Prefetch{WindowRows: 37, MaxWindows: 5}})
	overflow.arenaLimit = 300
	check := func(how string, got []Object, created bool) {
		t.Helper()
		for _, o := range got {
			w, ok := want[string(o.Key)]
			at := w[1]
			if created {
				at = w[0]
			}
			if !ok || o.LastModified == nil || o.LastModified.UnixMicro() != at || o.LastModified.Location() != time.UTC {
				t.Fatalf("%s: %s: LastModified %v, want %d", how, o.Key, o.LastModified, at)
			}
		}
	}
	var walks [2]map[string][]Object // by Created, per path
	for c, created := range []bool{true, false} {
		walks[c] = map[string][]Object{}
		for name, s := range map[string]*Sorted{"bare": bare, "cached": cached, "overflow": overflow} {
			var all []Object
			for page, from := 0, []byte(nil); ; page++ {
				got, err := s.Rows(ctx, from, false, nil, 100, Projection{Owner: page%2 == 0, Content: page%3 == 0, Created: created})
				if err != nil {
					t.Fatal(err)
				}
				if len(got) == 0 {
					break
				}
				for i := range got {
					got[i].ContentType, got[i].MD5 = nil, nil // read on some pages only
				}
				all = append(all, got...)
				from = got[len(got)-1].Key
			}
			if len(all) != len(want) {
				t.Fatalf("%s created %v: %d rows", name, created, len(all))
			}
			check(fmt.Sprintf("%s created %v", name, created), all, created)
			walks[c][name] = all
			entries, ok, err := s.DelimitedRollup(ctx, nil, true, nil, []byte("k/"), []byte("/"), 2000, Projection{Created: created}, true)
			if err != nil || !ok || len(entries) != len(want) {
				t.Fatalf("%s rollup: %d entries, %v %v", name, len(entries), ok, err)
			}
			objects := make([]Object, len(entries))
			for i, e := range entries {
				objects[i] = *e.Object
			}
			check(name+" rollup", objects, created)
		}
		for _, name := range []string{"cached", "overflow"} {
			if !reflect.DeepEqual(walks[c][name], walks[c]["bare"]) {
				t.Errorf("created %v: the %s walk differs from the bare reader's", created, name)
			}
		}
	}
	for _, p := range []objectProjection{s3Objects, localObjects} {
		if got := p.projection(Projection{Created: true}); got != (Projection{}) {
			t.Errorf("%+v: projection %+v, want Created dropped", p, got)
		}
	}
	if got := gcsObjects.projection(Projection{Created: true}); got != (Projection{Created: true}) {
		t.Errorf("GCS projection %+v", got)
	}
}
