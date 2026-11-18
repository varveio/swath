// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package azblobtest

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/varveio/swath/internal/source/azblob/client"
)

// Properties answers Get Blob Properties from the store's current blobs
// (client.Client.Properties' contract): a name that is not one is an
// *client.Error of status 404, code BlobNotFound. The container is not
// checked; Handler checks it. It is not logged.
func (s *Store) Properties(ctx context.Context, container, name string) (*client.Head, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, it := range s.view(client.Include{Metadata: true}) {
		if it.Name == name {
			return &client.Head{Size: it.Size, LastModified: it.LastModified, AccessTier: it.AccessTier, Directory: directory(it.Metadata)}, nil
		}
	}
	return nil, &client.Error{Status: http.StatusNotFound, Code: "BlobNotFound"}
}

// directory reads metadata, a client.Props.Metadata JSON object, as
// client.Directory does.
func directory(metadata string) bool {
	var m map[string]*string
	return metadata != "" && json.Unmarshal([]byte(metadata), &m) == nil && client.Directory(m)
}

// Handler serves s over HTTP as Azure serves the container of account:
// List Blobs (GET /ACCOUNT/CONTAINER?restype=container&comp=list, its
// prefix, delimiter, startFrom, marker, maxresults and include=metadata) as
// XML, and Get Blob Properties (HEAD /ACCOUNT/CONTAINER/NAME) as headers. A
// request of another container is 404 ContainerNotFound; a marker the
// store refuses is 400 OutOfRangeInput; a failure s injects is 500
// InternalError, which a client retries. A client reaches it with the
// server's URL as its endpoint and account as its account.
func Handler(s *Store, account, container string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rest, ok := strings.CutPrefix(r.URL.Path, "/"+account+"/")
		if !ok {
			fail(w, http.StatusNotFound, "ResourceNotFound")
			return
		}
		c, name, blob := strings.Cut(rest, "/")
		switch {
		case c != container:
			fail(w, http.StatusNotFound, "ContainerNotFound")
		case blob && r.Method == http.MethodHead:
			serveProperties(w, r, s, container, name)
		case !blob && r.Method == http.MethodGet && r.URL.Query().Get("comp") == "list":
			serveList(w, r, s, container)
		default:
			fail(w, http.StatusBadRequest, "UnsupportedHttpVerb")
		}
	})
}

// fail writes an Azure error answer.
func fail(w http.ResponseWriter, status int, code string) {
	w.Header().Set("x-ms-error-code", code)
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	fmt.Fprintf(w, `<?xml version="1.0" encoding="utf-8"?><Error><Code>%s</Code><Message>%s</Message></Error>`, code, code)
}

func serveProperties(w http.ResponseWriter, r *http.Request, s *Store, container, name string) {
	h, err := s.Properties(r.Context(), container, name)
	if e, ok := errors.AsType[*client.Error](err); ok {
		w.Header().Set("x-ms-error-code", e.Code)
		w.WriteHeader(e.Status)
		return
	}
	if err != nil {
		fail(w, http.StatusInternalServerError, "InternalError")
		return
	}

	w.Header().Set("Content-Length", strconv.FormatInt(h.Size, 10))
	w.Header().Set("x-ms-blob-type", "BlockBlob")
	if h.LastModified != 0 {
		w.Header().Set("Last-Modified", time.UnixMicro(h.LastModified).UTC().Format(http.TimeFormat))
	}
	if h.AccessTier != "" {
		w.Header().Set("x-ms-access-tier", h.AccessTier)
	}
	if h.Directory {
		w.Header().Set("x-ms-meta-hdi_isfolder", "true")
	}
}

func serveList(w http.ResponseWriter, r *http.Request, s *Store, container string) {
	q := r.URL.Query()
	in := &client.ListInput{Container: container, Prefix: q.Get("prefix"), Delimiter: q.Get("delimiter"), StartFrom: q.Get("startFrom"),
		Marker: q.Get("marker"), Include: client.Include{Metadata: strings.Contains(q.Get("include"), "metadata")}}
	if m := q.Get("maxresults"); m != "" {
		in.MaxResults, _ = strconv.Atoi(m)
	}
	out, err := s.List(r.Context(), in)
	switch {
	case errors.Is(err, ErrMarker):
		fail(w, http.StatusBadRequest, "OutOfRangeInput")
		return
	case err != nil:
		fail(w, http.StatusInternalServerError, "InternalError")
		return
	}
	w.Header().Set("Content-Type", "application/xml")
	fmt.Fprint(w, ListXML(out))
}

// ListXML is out as Azure's XML List Blobs answer: its blobs, with their
// size, blob type, access tier, last-modified time and user metadata where
// set, and its prefixes, in out's order, then the next marker.
func ListXML(out *client.ListOutput) string {
	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="utf-8"?><EnumerationResults ServiceEndpoint="http://127.0.0.1/" ContainerName="c"><Blobs>`)
	for _, it := range out.Items {
		if it.Prefix {
			fmt.Fprintf(&b, "<BlobPrefix><Name>%s</Name></BlobPrefix>", escape(it.Name))
			continue
		}
		fmt.Fprintf(&b, "<Blob><Name>%s</Name><Properties><Content-Length>%d</Content-Length>", escape(it.Name), it.Size)
		if it.BlobType != "" {
			fmt.Fprintf(&b, "<BlobType>%s</BlobType>", it.BlobType)
		}
		if it.AccessTier != "" {
			fmt.Fprintf(&b, "<AccessTier>%s</AccessTier>", it.AccessTier)
		}
		if it.LastModified != 0 {
			fmt.Fprintf(&b, "<Last-Modified>%s</Last-Modified>", time.UnixMicro(it.LastModified).UTC().Format(http.TimeFormat))
		}
		b.WriteString("</Properties>")
		writeMetadata(&b, it.Metadata)
		b.WriteString("</Blob>")
	}
	fmt.Fprintf(&b, "</Blobs><NextMarker>%s</NextMarker></EnumerationResults>", escape(out.NextMarker))
	return b.String()
}

// writeMetadata writes metadata, a JSON object of names and values, as a
// listing's <Metadata> element, names sorted; nothing when it is empty.
func writeMetadata(b *strings.Builder, metadata string) {
	var m map[string]string
	if metadata == "" || json.Unmarshal([]byte(metadata), &m) != nil || len(m) == 0 {
		return
	}
	names := make([]string, 0, len(m))
	for k := range m {
		names = append(names, k)
	}
	sort.Strings(names)
	b.WriteString("<Metadata>")
	for _, k := range names {
		fmt.Fprintf(b, "<%s>%s</%s>", k, escape(m[k]), k)
	}
	b.WriteString("</Metadata>")
}

func escape(s string) string {
	var b strings.Builder
	xml.EscapeText(&b, []byte(s))
	return b.String()
}
