// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package keyrange

import (
	"context"
	"fmt"
	"sync"

	"github.com/varveio/swath/go/internal/schema"
	"github.com/varveio/swath/go/internal/source/keyrange/keyrangetest"
)

// The engine's tests list through the fixtures' fake API (start-after
// exclusive, continuation tokens) with a payload of the object size.

type sorted = keyrangetest.Sorted

var (
	newSorted        = keyrangetest.NewSorted
	keyspaces        = keyrangetest.Keyspaces
	hostile          = keyrangetest.Hostile
	errBadStartAfter = keyrangetest.ErrBadStartAfter
)

// Object is the test payload.
type Object struct{ Size int64 }

// Row is a test row.
type Row = schema.Row[Object]

// Schema is the test schema: the key and the size.
var Schema = schema.Schema[Object]{
	Info:    schema.Info{Name: "keyrange_test", ID: "swath/keyrange-test/v1", Key: schema.String, MaxKeyBytes: 1024},
	Columns: []schema.Column[Object]{{Spec: schema.Spec{Name: "size", Type: schema.Int64}, Int: func(o *Object) (int64, bool) { return o.Size, true }}},
	Heap:    func(*Object) int64 { return 0 },
	Bytes:   func(*Object) int64 { return 8 },
}

// lister is the fake API's shape.
type lister interface {
	List(ctx context.Context, in *keyrangetest.ListInput) (*keyrangetest.Page, error)
}

// fetchFrom adapts a fake API to the engine's Fetch.
func fetchFrom(api lister) Fetch[Object] {
	return func(ctx context.Context, q *Query) (*Page[Object], error) {
		out, err := api.List(ctx, &keyrangetest.ListInput{Prefix: q.Prefix, Delimiter: q.Delimiter, StartAfter: q.After,
			ContinuationToken: q.Token, MaxKeys: q.Max})
		if err != nil {
			return nil, err
		}
		pg := &Page[Object]{Prefixes: out.CommonPrefixes, Truncated: out.IsTruncated, Token: out.NextContinuationToken}
		for _, o := range out.Contents {
			pg.Rows = append(pg.Rows, Row{Key: o.Key, Data: Object{Size: o.Size}})
		}
		return pg, nil
	}
}

// newTest is an engine over a fake API.
func newTest(api lister, opt Options) (*Engine[Object], error) {
	return New(opt, Description[Object]{Target: "test", Size: func(o *Object) int64 { return o.Size }}, fetchFrom(api))
}

type collect struct {
	mu   sync.Mutex
	rows []Row
}

func (c *collect) Write(_ context.Context, segs [][]Row) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, s := range segs {
		c.rows = append(c.rows, s...)
	}
	return nil
}

func keysOf(rows []Row) []string {
	out := make([]string, len(rows))
	for i := range rows {
		out[i] = rows[i].Key
	}
	return out
}

// stub answers fixed pages in turn.
type stub struct{ pages []*keyrangetest.Page }

func (s *stub) List(_ context.Context, in *keyrangetest.ListInput) (*keyrangetest.Page, error) {
	i := 0
	if in.ContinuationToken != "" {
		fmt.Sscan(in.ContinuationToken, &i)
	}
	out := *s.pages[i]
	if i+1 < len(s.pages) {
		out.IsTruncated, out.NextContinuationToken = true, fmt.Sprint(i+1)
	}
	return &out, nil
}
