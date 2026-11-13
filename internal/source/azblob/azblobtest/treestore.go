// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package azblobtest

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"

	"github.com/varveio/swath/internal/source/azblob/client"
)

// TreeStore is List Blobs over a hierarchical namespace (HNS), for the
// provider as a supplied client of an HNS account: Get Account Information
// reports HNS, and a delimiter listing with ListInput.Tree lists one
// directory's entries by name in Azure's order (UTF-16 code units, sorted
// with CompareUTF16; across a directory's subtrees this is tree order, '/'
// lowest), directories as Items with Dir and properties, as the live
// service does (2026-11-11). startFrom is inclusive, markers are bound to
// the query that issued them, and a prefix may end inside a name (the
// entries of its directory that start with the rest). Every directory on a
// path exists; a path ending in '/' is a directory, empty unless others
// lie below it. A file's metadata, tags and immutability (SetProps) are
// listed only with the include that asks for them; a directory, a
// BlobPrefix, carries none. It records every request. Fail, when set
// before the first request, fails request n (from 0) with the error it
// returns.
type TreeStore struct {
	Fail func(req int) error

	dirs map[string][]client.Item // directory prefix -> its entries, by name in Azure's order

	mu      sync.Mutex
	markers map[string]treeMarker
	log     []Record
}

type treeMarker struct {
	in  client.ListInput // the query, marker cleared
	pos int
}

// NewTreeStore is a namespace holding paths. A file's size is its path's
// length in bytes; every entry has an ETag naming it and a fixed
// Last-Modified.
func NewTreeStore(paths []string) *TreeStore {
	s := &TreeStore{dirs: map[string][]client.Item{"": nil}, markers: map[string]treeMarker{}}
	seen := map[string]bool{} // path -> is a directory
	add := func(dir, name string, isDir bool) {
		if d, ok := seen[dir+name]; ok {
			if d != isDir {
				panic("azblobtest: a file and a directory named " + dir + name)
			}
			return
		}
		seen[dir+name] = isDir
		it := client.Item{Name: dir + name, Dir: isDir, Hierarchical: true, ETag: "0x" + fmt.Sprintf("%x", dir+name), LastModified: 1_790_000_000_000_000, BlobType: "BlockBlob"}
		if !isDir {
			it.Size = int64(len(dir + name))
		}
		s.dirs[dir] = append(s.dirs[dir], it)
	}
	for _, p := range paths {
		parts := strings.Split(strings.TrimSuffix(p, "/"), "/")
		dir := ""
		for i, part := range parts {
			if i < len(parts)-1 || strings.HasSuffix(p, "/") {
				add(dir, part, true)
				dir += part + "/"
				if _, ok := s.dirs[dir]; !ok {
					s.dirs[dir] = nil
				}
				continue
			}
			add(dir, part, false)
		}
	}
	for d, items := range s.dirs {
		slices.SortFunc(items, func(a, b client.Item) int { return CompareUTF16(a.Name[len(d):], b.Name[len(d):]) })
	}
	return s
}

// Keys is every path's key as swath lists it, a directory's ending in '/',
// in unsigned byte order: the tree walk's expected output.
func (s *TreeStore) Keys() []string {
	var out []string
	for _, items := range s.dirs {
		for _, it := range items {
			k := it.Name
			if it.Dir {
				k += "/"
			}
			out = append(out, k)
		}
	}
	slices.Sort(out)
	return out
}

// SetProps gives the file at path the properties p, before the first
// request.
func (s *TreeStore) SetProps(path string, p client.Props) {
	dir := path[:strings.LastIndexByte(path, '/')+1]
	for i := range s.dirs[dir] {
		if it := &s.dirs[dir][i]; it.Name == path {
			if it.Dir {
				panic("azblobtest: a directory carries no properties: " + path)
			}
			it.Props = p
			return
		}
	}
	panic("azblobtest: no file " + path)
}

// Log is every request so far.
func (s *TreeStore) Log() []Record {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.log)
}

// AccountInfo reports a hierarchical namespace.
func (s *TreeStore) AccountInfo(context.Context, string) (bool, error) { return true, nil }

// List serves one page.
func (s *TreeStore) List(ctx context.Context, in *client.ListInput) (*client.ListOutput, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := len(s.log)
	out, err := s.list(ctx, n, in)
	rec := Record{In: *in, Err: err}
	if out != nil {
		rec.Marker = out.NextMarker
		for _, it := range out.Items {
			rec.Names = append(rec.Names, it.Name)
		}
	}
	s.log = append(s.log, rec)
	return out, err
}

func (s *TreeStore) list(ctx context.Context, n int, in *client.ListInput) (*client.ListOutput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.Fail != nil {
		if err := s.Fail(n); err != nil {
			return nil, err
		}
	}
	if !in.Tree || in.Delimiter != "/" {
		return nil, fmt.Errorf("azblobtest: the tree store answers directory listings (Tree, delimiter /), not %+v", *in)
	}
	if in.MaxResults < 0 || in.MaxResults > client.MaxPageSize {
		return nil, fmt.Errorf("azblobtest: maxresults %d", in.MaxResults)
	}
	size := in.MaxResults
	if size == 0 {
		size = client.MaxPageSize
	}
	dir := in.Prefix[:strings.LastIndexByte(in.Prefix, '/')+1]
	partial := in.Prefix[len(dir):]
	items := s.dirs[dir]
	var pos int
	if in.Marker != "" {
		m, ok := s.markers[in.Marker]
		q := *in
		q.Marker = ""
		if !ok || m.in != q {
			return nil, ErrMarker
		}
		pos = m.pos
	} else {
		from := partial
		if strings.HasPrefix(in.StartFrom, dir) && CompareUTF16(in.StartFrom[len(dir):], from) > 0 {
			from = in.StartFrom[len(dir):]
		} else if in.StartFrom != "" && !strings.HasPrefix(in.StartFrom, dir) && CompareUTF16(in.StartFrom, dir) > 0 {
			pos = len(items) // a start past the directory
		}
		if pos == 0 {
			pos = sort.Search(len(items), func(i int) bool { return CompareUTF16(items[i].Name[len(dir):], from) >= 0 })
		}
	}
	out := &client.ListOutput{}
	i := pos
	for ; i < len(items) && len(out.Items) < size; i++ {
		if !strings.HasPrefix(items[i].Name[len(dir):], partial) {
			i = len(items)
			break
		}
		out.Items = append(out.Items, strip(items[i], in.Include))
	}
	if i < len(items) && strings.HasPrefix(items[i].Name[len(dir):], partial) {
		out.NextMarker = fmt.Sprintf("t%d", len(s.markers)+1)
		q := *in
		q.Marker = ""
		s.markers[out.NextMarker] = treeMarker{in: q, pos: i}
	}
	return out, nil
}
