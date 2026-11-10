// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"os"
	"path"
	"slices"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

// physicalMemory is the host's RAM (sysinfo), 0 when unknown.
func physicalMemory() int64 {
	var si unix.Sysinfo_t
	if unix.Sysinfo(&si) != nil {
		return 0
	}
	return clampMemory(uint64(si.Totalram) * uint64(si.Unit))
}

// cgroupMemory is the least memory limit of the process's cgroup and its
// ancestors, v2 (memory.max) and v1 (memory.limit_in_bytes) alike; 0 when
// none is set or none can be read.
func cgroupMemory() int64 {
	cg, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		return 0
	}
	mi, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		return 0
	}
	return cgroupLimit(string(cg), string(mi), os.ReadFile)
}

// cgroupLimit is cgroupMemory from the contents of /proc/self/cgroup and
// /proc/self/mountinfo, reading the limit files with read. Each hierarchy
// that holds the memory controller (the v2 one, a v1 memory one) may be
// mounted several times, each mount a view from its own root: through
// every mount whose view holds the process's cgroup, each directory from
// that cgroup up to the mount point may set a limit, and the least of them
// all wins. A mount whose root is not the process's cgroup or an ancestor
// of it (mountRel) is skipped: a sibling's limit does not constrain the
// process. "max" and a v1 limit at or past 2^62 (its unlimited sentinel,
// rounded to the page size) are none.
func cgroupLimit(cgroup, mountinfo string, read func(string) ([]byte, error)) int64 {
	var least int64
	for _, line := range strings.Split(cgroup, "\n") {
		id, rest, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		ctrls, cgPath, ok := strings.Cut(rest, ":")
		if !ok {
			continue
		}
		v2 := id == "0" && ctrls == ""
		if !v2 && !hasMemory(ctrls) {
			continue
		}
		file := "memory.limit_in_bytes"
		if v2 {
			file = "memory.max"
		}
		for _, m := range cgroupMounts(mountinfo, v2) {
			rel, ok := mountRel(cgPath, m.root)
			if !ok {
				continue
			}
			for dir := path.Join(m.point, rel); ; dir = path.Dir(dir) {
				if b, err := read(path.Join(dir, file)); err == nil {
					if n := parseCgroupLimit(string(b)); n > 0 && (least == 0 || n < least) {
						least = n
					}
				}
				if dir == m.point || dir == "/" || !strings.HasPrefix(dir, m.point) {
					break
				}
			}
		}
	}
	return least
}

// mountRel is the cgroup p (as /proc/self/cgroup names it) relative to a
// mount whose root is root, both in the reader's cgroup namespace's
// coordinates: the kernel writes ".." components only at the front, for a
// cgroup above that namespace's root ("/../job", a mount root "/.."). It is
// false when the mount's root is not proven to be p or an ancestor of it
// (a sibling's subtree): its limits do not bind p. A root that is an
// ancestor of the namespace root whose path down to p cannot be known (p
// "/x" below a root "/..": the namespace root's own name is not shown) is
// "/": its own limit binds p, nothing below it is walked.
func mountRel(p, root string) (string, bool) {
	pu, pd, ok := cgroupSteps(p)
	ru, rd, rok := cgroupSteps(root)
	switch {
	case !ok || !rok:
		return "", false
	case ru == pu && len(rd) <= len(pd) && slices.Equal(rd, pd[:len(rd)]):
		return "/" + strings.Join(pd[len(rd):], "/"), true
	case ru > pu && len(rd) == 0:
		return "/", true
	}
	return "", false
}

// cgroupSteps splits a namespace-relative cgroup path into its leading ".."
// components (up) and the names below them (down); false for a ".." after
// a name, which the kernel never writes.
func cgroupSteps(p string) (up int, down []string, ok bool) {
	for c := range strings.SplitSeq(p, "/") {
		switch {
		case c == "" || c == ".":
		case c == ".." && len(down) == 0:
			up++
		case c == "..":
			return 0, nil, false
		default:
			down = append(down, c)
		}
	}
	return up, down, true
}

// hasMemory reports whether a v1 hierarchy's controller list holds memory.
func hasMemory(ctrls string) bool {
	for c := range strings.SplitSeq(ctrls, ",") {
		if c == "memory" {
			return true
		}
	}
	return false
}

// cgroupMount is one mount of a cgroup hierarchy: its root within the
// hierarchy and its mount point.
type cgroupMount struct{ root, point string }

// cgroupMounts are the cgroup2 mounts (v2), or the cgroup mounts holding
// the memory controller (v1), in mountinfo: "id parent major:minor root
// mountpoint options [optional...] - fstype source superoptions", root and
// mount point octal-escaped.
func cgroupMounts(mountinfo string, v2 bool) []cgroupMount {
	var out []cgroupMount
	for _, line := range strings.Split(mountinfo, "\n") {
		pre, post, found := strings.Cut(line, " - ")
		if !found {
			continue
		}
		f, g := strings.Fields(pre), strings.Fields(post)
		if len(f) < 5 || len(g) < 1 {
			continue
		}
		switch {
		case v2 && g[0] == "cgroup2":
		case !v2 && g[0] == "cgroup" && len(g) >= 3 && hasMemory(g[2]):
		default:
			continue
		}
		out = append(out, cgroupMount{root: unescapeMount(f[3]), point: unescapeMount(f[4])})
	}
	return out
}

// unescapeMount undoes mountinfo's octal escapes (\040 for a space).
func unescapeMount(s string) string {
	if !strings.Contains(s, `\`) {
		return s
	}
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+3 < len(s) {
			if n, err := strconv.ParseUint(s[i+1:i+4], 8, 8); err == nil {
				b.WriteByte(byte(n))
				i += 3
				continue
			}
		}
		b.WriteByte(s[i])
	}
	return b.String()
}

// parseCgroupLimit is a memory.max or memory.limit_in_bytes value; 0 for
// none ("max", the v1 sentinel) or a value that does not parse.
func parseCgroupLimit(s string) int64 {
	s = strings.TrimSpace(s)
	n, err := strconv.ParseUint(s, 10, 64)
	if err != nil || n >= 1<<62 {
		return 0
	}
	return int64(n)
}
