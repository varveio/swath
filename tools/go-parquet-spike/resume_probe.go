// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"
)

type checkpointFixture struct {
	RunID        int64  `json:"run_id"`
	NodeID       int64  `json:"node_id"`
	WriterID     int    `json:"writer_id"`
	ArgsHash     string `json:"args_hash"`
	InputJSONL   string `json:"input_jsonl"`
	BaselinePart string `json:"baseline_part"`
	BaselineMD5  string `json:"baseline_md5"`
}

func readCheckpointFixture(root string) (checkpointFixture, error) {
	data, err := os.ReadFile(filepath.Join(root, "fixture.json"))
	if err != nil {
		return checkpointFixture{}, err
	}
	var fixture checkpointFixture
	if err := json.Unmarshal(data, &fixture); err != nil {
		return checkpointFixture{}, err
	}
	if fixture.RunID <= 0 || fixture.NodeID <= 0 || fixture.WriterID != int(fixture.NodeID%2) ||
		fixture.ArgsHash == "" || fixture.InputJSONL == "" || fixture.BaselinePart == "" || fixture.BaselineMD5 == "" {
		return checkpointFixture{}, errors.New("incomplete Java checkpoint fixture")
	}
	return fixture, nil
}

// runResumeProbe is deliberately a one-range, bounded-corpus producer. Every
// selected stage emits a ready receipt and waits for an external SIGKILL, never
// substitutes a graceful close or a Go-only recovery for the Java oracle.
func runResumeProbe(args []string) error {
	flags := flag.NewFlagSet("resume-probe", flag.ContinueOnError)
	root := flags.String("root", "", "Java-created fixture dataset")
	stage := flags.String("stage", "", "page-commit, mid-part, part-durable, part-txn, part-commit, completed-tail or all-parts-commit")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *root == "" || flags.NArg() != 0 {
		return errors.New("resume-probe requires root and one stage")
	}
	switch *stage {
	case "page-commit", "mid-part", "part-durable", "part-txn", "part-commit", "completed-tail", "all-parts-commit":
	default:
		return fmt.Errorf("unknown crash stage %q", *stage)
	}
	fixture, err := readCheckpointFixture(*root)
	if err != nil {
		return err
	}
	rows, err := loadRows(fixture.InputJSONL)
	if err != nil {
		return err
	}
	if len(rows) != 513 {
		return errors.New("resume-probe requires the declared 513-row corpus")
	}
	for _, row := range rows {
		if row.RowType != "OBJECT" || row.VersionID != nil {
			return errors.New("resume-probe supports current objects only")
		}
	}
	ctx := context.Background()
	store, err := openCheckpoint(ctx, filepath.Join(*root, ".swath", "checkpoint.sqlite"))
	if err != nil {
		return err
	}
	defer store.close()
	var storedHash, mode, format string
	var sorted bool
	if err := store.conn.QueryRowContext(ctx,
		"SELECT args_hash, mode, output_format, sort_enabled FROM run_meta WHERE id=?", fixture.RunID).
		Scan(&storedHash, &mode, &format, &sorted); err != nil {
		return err
	}
	if storedHash != fixture.ArgsHash || mode != "OBJECTS" || format != "PARQUET" || sorted {
		return errors.New("Java checkpoint identity or output mode disagrees with fixture")
	}
	nodes, err := store.resumeNodes(ctx, fixture.RunID)
	if err != nil {
		return err
	}
	if len(nodes) != 1 || nodes[0].ID != fixture.NodeID || nodes[0].Kind != "RANGE" || nodes[0].RangeStart != nil || nodes[0].RangeEnd != nil {
		return errors.New("resume-probe requires one Java ROOT RANGE")
	}
	node := nodes[0]
	start := sort.Search(len(rows), func(i int) bool { return rows[i].Key > string(node.Cursor) })
	if start != 128 {
		return fmt.Errorf("Java durable floor resumes at row %d, want 128", start)
	}
	config := writerConfig{
		Layout: "direct", KeyEncoding: "dict", IntegerEncoding: "dict", PageStatistics: true,
		PageRows: 32, GroupRows: 64, SortMode: "none", Codec: "go",
	}
	sequence := 1
	partRows := make([]Row, 0, 128)
	for pageStart := start; pageStart < len(rows); {
		pageEnd := min(len(rows), pageStart+64)
		advance := []byte(rows[pageEnd-1].Key)
		completed := pageEnd == len(rows)
		if err := store.commitPage(ctx, node.ID, advance, completed); err != nil {
			return err
		}
		key := fmt.Sprintf("data/part-w%d-%05d.parquet", fixture.WriterID, sequence)
		ready := func() error {
			return waitForExternalKill(*stage, fixture, key, rows[pageEnd-1].Key)
		}
		if *stage == "page-commit" && pageStart == start || *stage == "completed-tail" && completed {
			return ready()
		}
		// The page enters the bounded part buffer only after its SQL commit.
		partRows = append(partRows, rows[pageStart:pageEnd]...)
		if len(partRows) == cap(partRows) || completed {
			ops := diskOps()
			if *stage == "mid-part" {
				create := ops.create
				ops.create = func(path string) (durableFile, error) {
					file, err := create(path)
					if err != nil {
						return nil, err
					}
					return &midPartBarrier{durableFile: file, remaining: 128, ready: ready}, nil
				}
			}
			part, err := writePart(filepath.Join(*root, filepath.FromSlash(key)), partRows, config, ops)
			if err != nil {
				return err
			}
			if *stage == "part-durable" {
				return ready()
			}
			var inserted func() error
			if *stage == "part-txn" {
				inserted = ready
			}
			if err := store.finalizePart(ctx, fixture.RunID, fixture.WriterID, key, part,
				[]durableAdvance{{NodeID: node.ID, Key: advance}}, inserted); err != nil {
				return err
			}
			if *stage == "part-commit" || *stage == "all-parts-commit" && completed {
				return ready()
			}
			partRows = partRows[:0]
			sequence++
		}
		pageStart = pageEnd
	}
	return fmt.Errorf("crash stage %s was not reached", *stage)
}

func waitForExternalKill(stage string, fixture checkpointFixture, key, cursor string) error {
	receipt := struct {
		Ready         bool   `json:"ready"`
		Stage         string `json:"stage"`
		RunID         int64  `json:"run_id"`
		NodeID        int64  `json:"node_id"`
		CandidatePart string `json:"candidate_part"`
		Cursor        string `json:"cursor"`
	}{true, stage, fixture.RunID, fixture.NodeID, key, cursor}
	if err := json.NewEncoder(os.Stdout).Encode(receipt); err != nil {
		return err
	}
	// A live timer also prevents Go from declaring the intentional barrier a
	// deadlock. The external driver must kill well before this watchdog expires.
	<-time.NewTimer(24 * time.Hour).C
	return errors.New("external kill barrier expired")
}

type midPartBarrier struct {
	durableFile
	remaining int
	ready     func() error
}

func (file *midPartBarrier) Write(data []byte) (int, error) {
	length := min(len(data), file.remaining)
	n, err := file.durableFile.Write(data[:length])
	file.remaining -= n
	if err != nil {
		return n, err
	}
	if n != length {
		return n, io.ErrShortWrite
	}
	if file.remaining == 0 {
		return n, file.ready()
	}
	return n, nil
}
