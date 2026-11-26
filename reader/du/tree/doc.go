// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package tree draws the view swath du shows on a terminal: the largest
// prefixes and objects at any depth, largest first, each under its parent,
// with size, count, share, a bar and the newest date, the rest of each
// parent's children folded into a "(N more)" row, fitted to the terminal's
// width. swath du draws its view with this package: Select chooses the
// rows from a Tree of totals, and Render writes them.
//
// The API is experimental: it may change in any release.
//
// # Totals
//
// A Tree is the total and the nodes under it, from whatever the caller
// counted: a live walk or an inventory. Select needs a node's parent
// prefix to place it, and a prefix's count of children to say how many a
// fold holds. A caller that summed prefixes with package du makes a Node
// of each du.Totals: Bytes from Size or Allocated, Objects from Files,
// Missing from SizeMissing (for Size) or AllocatedMissing plus
// Unidentified (for Allocated), Newest from Newest, and Children -1 unless
// it counted them; an object it shows is a Node of its own. Select chooses
// among the nodes given, so the view of a tree summed to two levels opens
// two levels at most.
//
// # Rows
//
// Select picks at most the rows asked for, from the largest down, and
// gives them in tree order; its rule and the floor below which a node
// gets no row are on Select. The bytes of the rows under a parent add up
// to the parent's: a fold carries the rest. Render draws any rows in that
// order, Select's or the caller's own.
//
// # Columns
//
// Render draws the built-in columns (size, count, share, bar, newest
// date, path) and the caller's: a Column is a heading, an alignment and
// one formatted cell per row. A fold row's cell is the caller's to fill
// with what the fold holds, or to leave empty. As the width shrinks,
// Render drops the bar, then the share, then the date, then the caller's
// columns from the last to the first; the size, the count and the path
// stay, a path cut in the middle where its row would pass the width. No
// row passes a width that holds the size, the count and the indented
// paths' ellipses.
//
// Cells, Clip, Size, Count and Grouped are the measures and forms the view
// uses, for a caller's own cells to match it.
package tree
