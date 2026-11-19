// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package du sums listing entries per requested key prefix by du's rules: space on
// disk with directories' own blocks, a file with several hard links once within each
// prefix, and no more than du -sB1 counts on an acyclic tree. It lists nothing: a
// caller feeds it a Walk's entries, live or from an inventory, in any order.
//
// The API is experimental: it may change in any release. swath du does not
// use this package: the command keeps its own tally, which sums every depth
// of one walk and counts a hard-linked file once per invocation, under its
// smallest name.
//
// # Prefixes
//
// Each prefix is summed as if it were the only one requested. An entry
// counts in every requested prefix its key begins with, its own entry
// included: a directory's own blocks, or an object at exactly a prefix's
// key. A prefix is "" or a key ending in "/", and matching costs one map
// lookup per "/" in the key, up to the longest prefix. Names outside every
// prefix count nowhere and are never remembered.
//
// # Kinds
//
// A local entry (KindFile, KindSymlink, KindOther) is counted only when it
// has AllocatedBytes and identity: Links equal to 1, or above 1 with Device
// and Inode. Otherwise it adds 1 to AllocatedMissing (checked first) or to
// Unidentified, and to no other figure but Symlinks, which counts every
// symbolic link's name. A counted file adds to Files, Size and Allocated
// and its time to Newest, once per file within each prefix; a counted
// symbolic link or other entry adds its size and space on disk the same
// way, and no count or time. An object adds to Files, Size and Newest, and
// no space on disk. A directory adds its own blocks to Allocated, or 1 to
// AllocatedMissing without them. KindPrefix and KindUnknown entries are
// ignored: one is synthetic, the other a gap the listing reports itself.
//
// # The guarantee
//
// For every prefix, nested ones included, Allocated is a lower bound of
// du -sB1 of the prefix on an acyclic traversal scope; Files and Size are
// lower bounds of their own values over every entry under the prefix, and
// Newest is never later than its value there. No figure decreases as Add
// takes entries, so totals taken from a walk that stopped early are lower
// bounds too. The missing counters are diagnostics, not bounds.
//
// Take a prefix the walk covered (no unread place under or above it) whose
// allocation evidence is complete (AllocatedMissing and Unidentified are
// 0). Then Allocated equals what du -sB1 reports for that directory, with
// three differences:
//
//   - A directory on another device is listed and not entered: its own
//     blocks count and its contents do not. GNU du -s enters it, and
//     du -sx leaves out the mount point's own blocks.
//   - The prefix's own directory entry counts only when it is fed. A live
//     walk of a non-empty prefix hands it on; a walk of the root does not,
//     so a caller feeds the root's entry, a KindDir at key "", itself. An
//     inventory has no row for its source root, so a total fed from one
//     leaves out the root's own blocks.
//   - A same-device bind mount of an ancestor is a cycle: swath's walk
//     enters it as an ordinary directory and counts it again, while GNU du
//     detects the cycle and skips it, so on such a tree Allocated can
//     exceed du's figure.
//
// Every equality and bound against du holds on acyclic traversal scopes
// only, and on a tree that does not change during the walk.
package du
