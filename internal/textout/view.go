// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package textout

import (
	"io"
	"time"

	"github.com/varveio/swath/internal/totals"
	"github.com/varveio/swath/reader/du/tree"
)

// ViewLayout is how the adaptive view (swath du on a terminal) is drawn.
type ViewLayout struct {
	// Label names the total's line: SOURCE as given, written as it is.
	Label string
	// Local: the source is a local tree, whose objects are files.
	Local bool
	// Width is the terminal's columns (0: unknown, 80 assumed).
	Width int
	// Color styles the heading, the bar and the fold lines (ANSI).
	Color bool
	// Loc is the dates' zone (nil: time.Local), named once, on the total's
	// line.
	Loc *time.Location
	// Primary is the figure the rows' bytes are (totals.Size, the default,
	// or totals.AllocatedBytes), which names the bytes' column.
	Primary string
}

// WriteView writes the adaptive view's lines (tree.Select) with
// tree.Render, under the table's headings: the bytes' column is the
// primary figure's (size, or on disk) and the count's a local tree's files
// or objects.
func WriteView(w io.Writer, l ViewLayout, rows []tree.Row) error {
	t := tree.Layout{Label: l.Label, Width: l.Width, Color: l.Color, Loc: l.Loc, SizeHeading: FieldSize, CountHeading: FieldObjects}
	if l.Primary == totals.AllocatedBytes {
		t.SizeHeading = HeadingOnDisk
	}
	if l.Local {
		t.CountHeading = headingFiles
	}
	return tree.Render(w, t, rows)
}
