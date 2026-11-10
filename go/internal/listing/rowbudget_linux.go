// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"os"
	"path"
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
// all wins. A mount whose view does not hold it (the cgroup outside the
// mount's root, as a cgroup namespace's "/.." shows a cgroup above it)
// gives its own limits only. "max" and a v1 limit at or past 2^62 (its
// unlimited sentinel, rounded to the page size) are none.
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
				rel = "/"
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
// mount whose root is root, false when the mount's view does not hold it.
// The kernel writes ".." components only at the front, for a cgroup above
// the reader's cgroup namespace root ("/../job"); they are matched against
// the root as written ("/.."), never cleaned away first.
func mountRel(p, root string) (string, bool) {
	rel := p
	if root != "/" {
		var ok bool
		if rel, ok = strings.CutPrefix(p, root); !ok || rel != "" && rel[0] != '/' {
			return "", false
		}
	}
	if strings.Contains(rel+"/", "/../") {
		return "", false
	}
	return path.Clean("/" + rel), true
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
