// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package gcs

import (
	"context"
	"errors"
	"fmt"
	"github.com/varveio/swath/go/internal/order"
	"github.com/varveio/swath/go/internal/source/gcs/client"
	"github.com/varveio/swath/go/internal/source/keyrange"
	"strings"
	"testing"
)

type reviewDescendingGenerations struct{}

func (reviewDescendingGenerations) List(_ context.Context, in *client.ListInput) (*client.Page, error) {
	if in.PageToken == "" {
		return &client.Page{Items: []client.Object{{Name: "k", Generation: 2}}, NextPageToken: "next"}, nil
	}
	return &client.Page{Items: []client.Object{{Name: "k", Generation: 1}}}, nil
}
func TestReviewGenerationSeam(t *testing.T) {
	p, err := NewGenerations(context.Background(), Options{Bucket: "b", Workers: 1, Client: reviewDescendingGenerations{}})
	if err != nil {
		t.Fatal(err)
	}
	st, err := order.Count(context.Background(), p, nil, order.Options{Workers: 1, Groups: true})
	if !errors.Is(err, client.ErrInvalidPage) {
		t.Fatalf("descending generation seam accepted: rows=%d err=%v", st.Rows, err)
	}
}

type generationAPI func(context.Context, *client.ListInput) (*client.Page, error)

func (f generationAPI) List(ctx context.Context, in *client.ListInput) (*client.Page, error) {
	return f(ctx, in)
}

func TestGenerationsSeamsAndEmptyPages(t *testing.T) {
	for _, generation := range []int64{1, 2, 3} {
		for _, sequential := range []bool{false, true} {
			t.Run(fmt.Sprintf("g%d/seq%v", generation, sequential), func(t *testing.T) {
				calls := 0
				api := generationAPI(func(_ context.Context, in *client.ListInput) (*client.Page, error) {
					calls++
					if !in.Versions || in.MaxResults != 1 {
						t.Fatalf("input=%+v", in)
					}
					switch in.PageToken {
					case "":
						return &client.Page{Items: []client.Object{{Name: "k", Generation: 2}}, NextPageToken: "empty"}, nil
					case "empty":
						return &client.Page{NextPageToken: "tail"}, nil
					case "tail":
						return &client.Page{Items: []client.Object{{Name: "k", Generation: generation}}}, nil
					default:
						t.Fatal(in.PageToken)
						return nil, nil
					}
				})
				p, err := NewGenerations(context.Background(), Options{Bucket: "b", Workers: 1, Client: api, Policy: keyrange.Policy{PageSize: 1, Sequential: sequential}})
				if err != nil {
					t.Fatal(err)
				}
				st, err := order.Count(context.Background(), p, nil, order.Options{Workers: 1, Groups: true})
				if generation <= 2 {
					if !errors.Is(err, client.ErrInvalidPage) {
						t.Fatalf("rows=%d err=%v", st.Rows, err)
					}
				} else if err != nil || st.Rows != 2 || st.DistinctKeys != 1 {
					t.Fatalf("stats=%+v err=%v", st, err)
				}
				if calls != 3 {
					t.Fatal(calls)
				}
			})
		}
	}
}

func TestFetchGenerationsResumeStart(t *testing.T) {
	for _, n := range []int{10, 1024} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			name := strings.Repeat("a", n)
			start := name + "\x00"
			if n == 1024 {
				start = name
			}
			calls := 0
			api := generationAPI(func(_ context.Context, in *client.ListInput) (*client.Page, error) {
				calls++
				if in.StartOffset != start || !in.Versions || in.Bucket != "b" || in.MaxResults != 1 {
					t.Fatalf("input=%+v", in)
				}
				if n == 1024 {
					switch in.PageToken {
					case "":
						return &client.Page{Items: []client.Object{{Name: name, Generation: 1}}, NextPageToken: "second"}, nil
					case "second":
						return &client.Page{Items: []client.Object{{Name: name, Generation: 2}}, NextPageToken: "next"}, nil
					case "next":
					default:
						t.Fatal(in.PageToken)
					}
				} else if in.PageToken != "" {
					t.Fatal(in.PageToken)
				}
				return &client.Page{Items: []client.Object{{Name: "b", Generation: 3}}}, nil
			})
			p, err := NewGenerations(context.Background(), Options{Bucket: "b", Workers: 1, Client: api, Resume: &Resume{After: name}, Policy: keyrange.Policy{PageSize: 1}})
			if err != nil {
				t.Fatal(err)
			}
			st, err := order.Count(context.Background(), p, nil, order.Options{Workers: 1, Groups: true, After: &name})
			if err != nil || st.Rows != 1 || st.DistinctKeys != 1 {
				t.Fatalf("stats=%+v err=%v", st, err)
			}
			want := 1
			var drops int64
			if n == 1024 {
				want, drops = 3, 2
			}
			if calls != want || p.Stats().Counters.Value("bound_drops") != drops {
				t.Fatalf("calls=%d want=%d drops=%d want=%d", calls, want, p.Stats().Counters.Value("bound_drops"), drops)
			}
		})
	}
}

func TestFetchGenerationsWithinPageOrder(t *testing.T) {
	for _, g := range []int64{1, 2} {
		api := generationAPI(func(context.Context, *client.ListInput) (*client.Page, error) {
			return &client.Page{Items: []client.Object{{Name: "k", Generation: 2}, {Name: "k", Generation: g}}}, nil
		})
		p, err := NewGenerations(context.Background(), Options{Bucket: "b", Workers: 1, Client: api})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := p.fetchGenerations(context.Background(), &keyrange.Query{}); !errors.Is(err, client.ErrInvalidPage) {
			t.Fatal(err)
		}
	}
}
