// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"

	"github.com/varveio/swath/internal/source/objhttp"
)

// hnsPage is a delimiter listing of a hierarchical namespace's directory
// in the shape the live service answered on 2026-11-11
// (overturemapswestus2/release, bingmlbuildings/footprints): BlobPrefix
// items carry properties with ResourceType directory, blobs ResourceType
// file, and each kind is listed by name, '/' lowest (a/ before a-b/).
const hnsPage = "\xef\xbb\xbf" + `<?xml version="1.0" encoding="utf-8"?><EnumerationResults ServiceEndpoint="https://acct.blob.core.windows.net/" ContainerName="c"><Prefix>d/</Prefix><MaxResults>5</MaxResults><Delimiter>/</Delimiter><Blobs>` +
	`<Blob><Name>d/a.c</Name><Properties><Creation-Time>Wed, 21 Oct 2026 17:39:49 GMT</Creation-Time><Last-Modified>Wed, 21 Oct 2026 17:49:00 GMT</Last-Modified><Etag>0x8DF2F9B96C89168</Etag><ResourceType>file</ResourceType><Content-Length>7</Content-Length><Content-Type>application/octet-stream</Content-Type><BlobType>BlockBlob</BlobType><AccessTier>Hot</AccessTier></Properties></Blob>` +
	`<Blob><Name>d/a0</Name><Properties><Last-Modified>Wed, 21 Oct 2026 17:49:00 GMT</Last-Modified><Etag>0x1</Etag><ResourceType>file</ResourceType><Content-Length>3</Content-Length><BlobType>BlockBlob</BlobType></Properties></Blob>` +
	`<BlobPrefix><Name>d/a/</Name><Properties><Creation-Time>Wed, 23 Sep 2026 17:41:32 GMT</Creation-Time><Last-Modified>Wed, 23 Sep 2026 17:41:32 GMT</Last-Modified><Etag>0x8DF1999E82EEA58</Etag><ResourceType>directory</ResourceType><Content-Length>0</Content-Length><Content-Type /><BlobType>BlockBlob</BlobType><AccessTier>Hot</AccessTier></Properties></BlobPrefix>` +
	`<BlobPrefix><Name>d/a-b/</Name><Properties><Last-Modified>Wed, 23 Sep 2026 17:41:33 GMT</Last-Modified><Etag>0x8DF1999E82EEA59</Etag><ResourceType>directory</ResourceType><Content-Length>0</Content-Length><BlobType>BlockBlob</BlobType></Properties></BlobPrefix>` +
	`</Blobs><NextMarker /></EnumerationResults>`

// TestTreeListing: with ListInput.Tree a hierarchical namespace's
// directory listing merges blobs and directories by name, the directories
// named without '/' and carrying their properties (Dir); without it the
// same page is the flat reading, an invalid page.
func TestTreeListing(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		io.WriteString(w, hnsPage)
	}))
	defer srv.Close()
	cl := newTestClient(t, Config{Endpoint: srv.URL, Anonymous: true, Retry: objhttp.Retry{MaxAttempts: 1}})
	list := func(tree bool) *ListOutput {
		out, err := cl.List(context.Background(), &ListInput{Container: "c", Prefix: "d/", Delimiter: "/", Tree: tree})
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
	names := func(out *ListOutput) (n []string) {
		for _, it := range out.Items {
			s := it.Name
			if it.Dir {
				s += " (dir)"
			}
			if it.Prefix {
				s += " (prefix)"
			}
			n = append(n, s)
		}
		return n
	}
	tree := list(true)
	if got, want := names(tree), []string{"d/a (dir)", "d/a-b (dir)", "d/a.c", "d/a0"}; !slices.Equal(got, want) {
		t.Fatalf("tree: %q, want %q", got, want)
	}
	if d := tree.Items[0]; d.ETag != "0x8DF1999E82EEA58" || d.LastModified == 0 || d.CreationTime == 0 || d.BlobType != "BlockBlob" || d.AccessTier != "Hot" {
		t.Fatalf("directory properties: %+v", d)
	}
	if f := tree.Items[2]; f.Dir || f.Size != 7 || f.ContentType != "application/octet-stream" {
		t.Fatalf("file: %+v", f)
	}
	// The flat reading of the same page fails its order check: the
	// directories a/ and a-b/ are in name order, not full-name order.
	if _, err := cl.List(context.Background(), &ListInput{Container: "c", Prefix: "d/", Delimiter: "/"}); !errors.Is(err, objhttp.ErrInvalidPage) {
		t.Fatalf("flat reading: %v, want an invalid page", err)
	}
}
