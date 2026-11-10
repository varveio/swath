// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"runtime/metrics"
	"time"
)

// heapEvery is how often sampleHeap reads the live heap.
const heapEvery = 100 * time.Millisecond

// heapLive is the runtime's live heap: the bytes the last garbage
// collection marked reachable (updated once per cycle).
const heapLive = "/gc/heap/live:bytes"

// sampleHeap samples the live heap until the returned function is called,
// which returns its peak (resources.peak_heap_live_bytes). Against
// --sort-buffer it shows the memory the budget does not see: pages a
// retained string keeps alive, the garbage collector's working set aside.
func sampleHeap() func() int64 {
	s := []metrics.Sample{{Name: heapLive}}
	read := func() int64 {
		metrics.Read(s)
		if s[0].Value.Kind() != metrics.KindUint64 {
			return 0
		}
		return int64(s[0].Value.Uint64())
	}
	stop, done := make(chan struct{}), make(chan int64)
	go func() {
		t := time.NewTicker(heapEvery)
		defer t.Stop()
		peak := read()
		for {
			select {
			case <-t.C:
				peak = max(peak, read())
			case <-stop:
				done <- max(peak, read())
				return
			}
		}
	}()
	return func() int64 {
		close(stop)
		return <-done
	}
}
