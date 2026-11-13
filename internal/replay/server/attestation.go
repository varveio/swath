// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"errors"
	"io/fs"
	"math/big"
	"os"
	"path/filepath"
	"strings"

	"github.com/varveio/swath/go/internal/replay/metrics"
)

// attestation reports the resource limits this process can see, for a
// controller to compare with the allocation it declared:
// runtime-attestation-v1, the cgroup v2 cpuset.cpus.effective, memory.max
// and memory.swap.max of the process's own cgroup, and
// /proc/self/status's Cpus_allowed_list. Each observation is a
// {value, error} pair; a missing, unreadable or empty source, or a limit
// that is not a byte count or "max", is an explicit error, never a guess.
type attestation struct {
	procSelfCgroup, procSelfStatus, cgroupMount string
}

func systemAttestation() attestation {
	return attestation{"/proc/self/cgroup", "/proc/self/status", "/sys/fs/cgroup"}
}

type observation struct {
	value, err string
	set        bool // value present
	number     bool
}

func obsError(e string) observation  { return observation{err: e} }
func obsString(v string) observation { return observation{value: v, set: true} }
func obsNumber(v string) observation { return observation{value: v, set: true, number: true} }
func (o observation) json(dst []byte) []byte {
	dst = append(dst, `{"value":`...)
	switch {
	case !o.set:
		dst = append(dst, "null"...)
	case o.number:
		dst = append(dst, o.value...)
	default:
		dst = metrics.AppendString(dst, o.value)
	}
	dst = append(dst, `,"error":`...)
	if o.set {
		dst = append(dst, "null"...)
	} else {
		dst = metrics.AppendString(dst, o.err)
	}
	return append(dst, '}')
}

func (a attestation) render() []byte {
	dir, dirErr := a.cgroupDirectory()
	var cpuset, memMax, swapMax observation
	if dir == "" {
		cpuset, memMax, swapMax = obsError(dirErr), obsError(dirErr), obsError(dirErr)
	} else {
		cpuset = readText(filepath.Join(dir, "cpuset.cpus.effective"))
		memMax = readLimit(filepath.Join(dir, "memory.max"))
		swapMax = readLimit(filepath.Join(dir, "memory.swap.max"))
	}
	out := []byte(`{"schema_version":"runtime-attestation-v1","cgroup_v2":{"directory":`)
	if dir == "" {
		out = append(out, "null"...)
	} else {
		out = metrics.AppendString(out, dir)
	}
	out = append(out, `,"cpuset_cpus_effective":`...)
	out = cpuset.json(out)
	out = append(out, `,"memory_max":`...)
	out = memMax.json(out)
	out = append(out, `,"memory_swap_max":`...)
	out = swapMax.json(out)
	out = append(out, `},"proc_self_status":{"cpus_allowed_list":`...)
	out = a.statusCPUs().json(out)
	return append(out, "}}"...)
}

func readError(kind, path string, err error) string {
	if errors.Is(err, fs.ErrNotExist) {
		return kind + " is missing: " + path
	}
	return kind + " is unreadable: " + path + " (" + err.Error() + ")"
}

func (a attestation) cgroupDirectory() (string, string) {
	b, err := os.ReadFile(a.procSelfCgroup)
	if err != nil {
		return "", readError("cgroup v2 membership file", a.procSelfCgroup, err)
	}
	for line := range strings.Lines(string(b)) {
		line = strings.TrimRight(line, "\r\n")
		membership, ok := strings.CutPrefix(line, "0::")
		if !ok {
			continue
		}
		if !strings.HasPrefix(membership, "/") {
			return "", "invalid cgroup v2 membership path: " + membership
		}
		mount, err := filepath.Abs(a.cgroupMount)
		if err != nil {
			return "", "invalid cgroup mount: " + a.cgroupMount
		}
		dir := filepath.Join(mount, membership[1:])
		if dir != mount && !strings.HasPrefix(dir, mount+string(filepath.Separator)) {
			return "", "cgroup v2 membership escapes its mount: " + membership
		}
		return dir, ""
	}
	return "", "cgroup v2 membership entry 0:: is missing: " + a.procSelfCgroup
}

func (a attestation) statusCPUs() observation {
	b, err := os.ReadFile(a.procSelfStatus)
	if err != nil {
		return obsError(readError("proc status file", a.procSelfStatus, err))
	}
	for line := range strings.Lines(string(b)) {
		if v, ok := strings.CutPrefix(line, "Cpus_allowed_list:"); ok {
			v = strings.TrimSpace(v)
			if v == "" {
				return obsError("Cpus_allowed_list is empty in " + a.procSelfStatus)
			}
			return obsString(v)
		}
	}
	return obsError("Cpus_allowed_list is missing from " + a.procSelfStatus)
}

func readText(path string) observation {
	b, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return obsError("missing: " + path)
		}
		return obsError("unreadable: " + path + " (" + err.Error() + ")")
	}
	v := strings.TrimSpace(string(b))
	if v == "" {
		return obsError("empty: " + path)
	}
	return obsString(v)
}

// readLimit reads a cgroup byte limit: a non-negative integer, or the
// kernel's literal "max" for unlimited.
func readLimit(path string) observation {
	o := readText(path)
	if !o.set || o.value == "max" {
		return o
	}
	n, ok := new(big.Int).SetString(o.value, 10)
	if !ok {
		return obsError("invalid cgroup limit in " + path + ": " + o.value)
	}
	if n.Sign() < 0 {
		return obsError("invalid negative cgroup limit in " + path + ": " + o.value)
	}
	return obsNumber(n.String())
}
