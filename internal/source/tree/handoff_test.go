// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package tree

import (
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/varveio/swath/internal/schema"
)

type handoffEmitter struct {
	trace   []string
	failAt  string
	failure error
}

func (e *handoffEmitter) Rows(rows []schema.Row[string]) error {
	for _, row := range rows {
		e.trace = append(e.trace, "row "+row.Key)
	}
	// The emitter owns the slice, including the capacity it received.
	// Reuse that storage immediately, including when returning an error.
	clear(rows[:cap(rows)])
	for i := range rows {
		rows[i].Key = "overwritten"
	}
	if len(rows) == 0 {
		return nil
	}
	if e.trace[len(e.trace)-1] == e.failAt {
		return e.failure
	}
	return nil
}

func (e *handoffEmitter) Child(child Range[string, struct{}]) error {
	event := "child " + child.Prefix + " (" + child.Dir + ")"
	e.trace = append(e.trace, event)
	if event == e.failAt {
		return e.failure
	}
	return nil
}

func TestOutRowsOwnership(t *testing.T) {
	walk := func(row *schema.Row[string]) (string, Walk) {
		if row.Data != "" {
			return row.Data, Descend
		}
		return "", Leaf
	}
	for _, noDirs := range []bool{false, true} {
		first := []string{"row a"}
		second := []string{"row c"}
		if !noDirs {
			first = append(first, "row b/")
			second = append(second, "row d/")
		}
		throughFirstChild := append(slices.Clone(first), "child b/ (first)")
		throughSecondRows := append(slices.Clone(throughFirstChild), second...)
		fullTrace := append(slices.Clone(throughSecondRows), "child d/ (second)", "row e")
		for _, tc := range []struct {
			name   string
			failAt string
			want   []string
		}{
			{"success", "", fullTrace},
			{"rows-error", second[len(second)-1], throughSecondRows},
			{"child-error", "child b/ (first)", throughFirstChild},
			{"tail-error", "row e", fullTrace},
		} {
			t.Run(fmt.Sprintf("noDirs=%t/%s", noDirs, tc.name), func(t *testing.T) {
				e := &handoffEmitter{failAt: tc.failAt, failure: errors.New("handoff failed")}
				out := Out[string, struct{}, string]{e: e, noDirs: noDirs}
				rows := []schema.Row[string]{
					{Key: "a"}, {Key: "b/", Data: "first"},
					{Key: "c"}, {Key: "d/", Data: "second"},
					{Key: "e"},
				}
				err := out.Rows(rows, walk)
				if !slices.Equal(e.trace, tc.want) {
					t.Errorf("handoffs = %q, want %q", e.trace, tc.want)
				}
				var wantErr error
				if tc.failAt != "" {
					wantErr = e.failure
				}
				if !errors.Is(err, wantErr) {
					t.Errorf("Rows returned %v, want %v", err, wantErr)
				}
			})
		}
	}
	t.Run("noDirs=true/leading-directory", func(t *testing.T) {
		e := &handoffEmitter{}
		out := Out[string, struct{}, string]{e: e, noDirs: true}
		rows := []schema.Row[string]{{Key: "a/", Data: "first"}, {Key: "b"}}
		err := out.Rows(rows, walk)
		want := []string{"child a/ (first)", "row b"}
		if err != nil || !slices.Equal(e.trace, want) {
			t.Errorf("handoffs = %q, err = %v, want %q, nil", e.trace, err, want)
		}
	})
}
