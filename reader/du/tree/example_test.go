// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package tree_test

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/varveio/swath/reader/du/tree"
)

// Draw the view of a bucket's totals with a column of the program's own:
// a monthly cost for each row, a fold's for what it holds.
func Example() {
	at := func(day int) *time.Time {
		t := time.Date(2026, 3, day, 0, 0, 0, 0, time.UTC)
		return &t
	}
	t := &tree.Tree{
		// The total's newest children date its fold: tmp/ has no node.
		Total: tree.Node{Key: "", Bytes: 10 << 30, Objects: 1200, Children: 3, Newest: at(20),
			Recent: []tree.Child{{Key: "logs/", Newest: *at(20)}, {Key: "tmp/", Newest: *at(19)}}},
		Nodes: []tree.Node{
			{Key: "models/", Bytes: 7 << 30, Objects: 40, Children: 2, Newest: at(18)},
			{Key: "models/v3.bin", Bytes: 5 << 30, Objects: 1, Newest: at(18)},
			{Key: "models/v2.bin", Bytes: 2 << 30, Objects: 1, Newest: at(2)},
			{Key: "logs/", Bytes: 2 << 30, Objects: 1100, Children: 31, Newest: at(20)},
		},
	}
	rows := tree.Select(t, 10)

	// The program's figure per row: here 2 cents a GiB, for a fold too.
	cost := tree.Column{Heading: "cost/mo", Align: tree.Right}
	for _, r := range rows {
		cost.Cells = append(cost.Cells, fmt.Sprintf("$%.2f", float64(r.Bytes)/(1<<30)*0.02))
	}
	err := tree.Render(os.Stdout, tree.Layout{Label: "s3://bucket", Width: 80, Loc: time.UTC}, rows, cost)
	if err != nil {
		log.Fatal(err)
	}
	// Output:
	//     size  objects  share              newest          cost/mo  path
	// 10.0 GiB    1.20k   100%  ██████████  2026-03-20 UTC    $0.20  s3://bucket
	// 7.00 GiB       40  70.0%  ███████     2026-03-18        $0.14    models/
	// 5.00 GiB        1  50.0%  █████       2026-03-18        $0.10      v3.bin
	// 2.00 GiB        1  20.0%  ██          2026-03-02        $0.04      v2.bin
	// 2.00 GiB    1.10k  20.0%  ██          2026-03-20        $0.04    logs/
	// 1.00 GiB       60  10.0%  █           2026-03-19        $0.02    (1 more)
}
