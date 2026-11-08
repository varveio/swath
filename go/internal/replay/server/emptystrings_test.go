// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/varveio/swath/go/internal/replay/legacy/legacytest"
	"github.com/varveio/swath/go/internal/replay/sortfixture"
	"github.com/varveio/swath/go/internal/replay/store"
)

// emptyAndNull returns two objects: "empty", whose six string columns are
// non-null empty strings, and "null", whose string columns are null.
func emptyAndNull() []legacytest.Row {
	e := ""
	empty := legacytest.Object("empty", 1)
	empty.ETag, empty.StorageClass, empty.OwnerID, empty.OwnerDisplayName, empty.ChecksumAlgorithm, empty.ChecksumType = &e, &e, &e, &e, &e, &e
	null := legacytest.Object("null", 2)
	null.ETag, null.StorageClass, null.OwnerID, null.OwnerDisplayName, null.ChecksumAlgorithm, null.ChecksumType = nil, nil, nil, nil, nil, nil
	return []legacytest.Row{empty, null}
}

// TestEmptyAndNullStringsEndToEnd serves a stamped Java-era part and the
// SeekParquet dataset sort-fixture makes of the same rows (captured unsorted)
// on the S3, GCS and Azure routes. Every answer is the Java server's for
// the Java-era part, byte for byte (recorded from it), but for Owner,
// which the S3 route writes before StorageClass as live S3 does
// (s3.TestLiveOwnerBeforeStorageClass): on S3 an empty
// string renders as an empty element (<ETag>&quot;&quot;</ETag>, an empty
// StorageClass, owner and checksum) and a null one not at all; GCS and
// Azure render their synthetic values for both. Before, sort-fixture wrote
// the empty strings as nulls and the converted dataset lost the elements.
func TestEmptyAndNullStringsEndToEnd(t *testing.T) {
	dir := t.TempDir()
	javaEra := filepath.Join(dir, "java")
	capture := filepath.Join(dir, "capture")
	for _, d := range []string{javaEra, capture} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	rows := emptyAndNull()
	if err := legacytest.Write(filepath.Join(javaEra, "part-00001.parquet"), legacytest.Part{Rows: rows, Stamps: legacytest.Stamps(1, true)}); err != nil {
		t.Fatal(err)
	}
	if err := legacytest.Write(filepath.Join(capture, "part-00000.parquet"), legacytest.Part{Rows: []legacytest.Row{rows[1], rows[0]}}); err != nil {
		t.Fatal(err)
	}
	converted := filepath.Join(dir, "converted")
	if _, err := sortfixture.Run(context.Background(), sortfixture.Options{Capture: capture, Output: converted}, io.Discard); err != nil {
		t.Fatal(err)
	}
	const (
		s3Want = `<?xml version="1.0" encoding="UTF-8"?>` + "\n" + `<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>bucket</Name><Prefix></Prefix><KeyCount>2</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>` +
			`<Contents><Key>empty</Key><LastModified>2026-01-01T00:00:01.000Z</LastModified><ETag>&quot;&quot;</ETag><ChecksumAlgorithm></ChecksumAlgorithm><ChecksumType></ChecksumType><Size>1</Size><Owner><ID></ID><DisplayName></DisplayName></Owner><StorageClass></StorageClass></Contents>` +
			`<Contents><Key>null</Key><LastModified>2026-01-01T00:00:02.000Z</LastModified><Size>2</Size></Contents></ListBucketResult>`
		gcsWant = `{"kind":"storage#objects","items":[` +
			`{"kind":"storage#object","bucket":"bucket","name":"empty","size":"1","updated":"2026-01-01T00:00:01.000000Z","etag":"swath-gcs-synthetic-v1","generation":"1","metageneration":"1","storageClass":"STANDARD","contentType":"application/octet-stream"},` +
			`{"kind":"storage#object","bucket":"bucket","name":"null","size":"2","updated":"2026-01-01T00:00:02.000000Z","etag":"swath-gcs-synthetic-v1","generation":"1","metageneration":"1","storageClass":"STANDARD","contentType":"application/octet-stream"}]}`
		azureBlobs = `<Blobs><Blob><Name>empty</Name><Properties><Last-Modified>Thu, 01 Jan 2026 00:00:01 GMT</Last-Modified><Etag>0x000000000000001</Etag><Content-Length>1</Content-Length><Content-Type>application/octet-stream</Content-Type><BlobType>BlockBlob</BlobType><AccessTier>Hot</AccessTier><AccessTierInferred>true</AccessTierInferred><LeaseStatus>unlocked</LeaseStatus><LeaseState>available</LeaseState></Properties></Blob>` +
			`<Blob><Name>null</Name><Properties><Last-Modified>Thu, 01 Jan 2026 00:00:02 GMT</Last-Modified><Etag>0x000000000000001</Etag><Content-Length>2</Content-Length><Content-Type>application/octet-stream</Content-Type><BlobType>BlockBlob</BlobType><AccessTier>Hot</AccessTier><AccessTierInferred>true</AccessTierInferred><LeaseStatus>unlocked</LeaseStatus><LeaseState>available</LeaseState></Properties></Blob></Blobs><NextMarker></NextMarker></EnumerationResults>`
	)
	for _, fixture := range []string{javaEra, converted} {
		t.Run(filepath.Base(fixture), func(t *testing.T) {
			st, _, err := store.Open(context.Background(), fixture, store.Options{})
			if err != nil {
				t.Fatal(err)
			}
			cfg := config(st)
			cfg.Protocols, cfg.AzureAccount = []string{"s3", "gcs", "azure"}, DefaultAzureAccount
			s := start(t, cfg)
			if r := get(t, s, "/bucket?list-type=2&fetch-owner=true"); r.status != 200 || r.body != s3Want {
				t.Errorf("S3: %d\n%s\nwant\n%s", r.status, r.body, s3Want)
			}
			if r := get(t, s, "/storage/v1/b/bucket/o"); r.status != 200 || r.body != gcsWant {
				t.Errorf("GCS: %d\n%s\nwant\n%s", r.status, r.body, gcsWant)
			}
			r := azureDo(t, s, "GET", "/"+DefaultAzureAccount+"/bucket?restype=container&comp=list", "x-ms-version", "2026-06-06")
			wantAzure := fmt.Sprintf(bom+"<?xml version=\"1.0\" encoding=\"utf-8\"?>\n<EnumerationResults ServiceEndpoint=\"http://127.0.0.1:%d/%s/\" ContainerName=\"bucket\">", s.Port(), DefaultAzureAccount) + azureBlobs
			if r.status != 200 || r.body != wantAzure {
				t.Errorf("Azure: %d\n%q\nwant\n%q", r.status, r.body, wantAzure)
			}
			if !strings.Contains(r.body, "<Name>empty</Name>") {
				t.Error("Azure: no empty")
			}
		})
	}
}
