"""Key bytes, and the projections that make a keyspace drawable."""

import collections

POS_BYTES = 8


# --------------------------------------------------------------------------- keys


def key_bytes(value):
    """A trace key as the bytes swath compares.

    The trace writes a key as a JSON string when it is valid UTF-8 and as
    ``{"hex": "..."}`` when it is not (docs/internals/trace.md), so every byte
    survives either way. ``None`` (an absent field) stays ``None``.
    """
    if value is None:
        return None
    if isinstance(value, dict):
        return bytes.fromhex(value.get("hex", ""))
    return value.encode("utf-8", errors="surrogatepass")


def key_text(value):
    """A trace key for display: its text, with any byte that is not UTF-8 as ``\\xNN``."""
    kb = key_bytes(value)
    return None if kb is None else kb.decode("utf-8", errors="backslashreplace")


def common_prefix(items):
    if not items:
        return b""
    lo, hi = min(items), max(items)
    n = 0
    while n < len(lo) and n < len(hi) and lo[n] == hi[n]:
        n += 1
    return lo[:n]


def _fraction(kb, skip):
    """The bytes of ``kb`` after ``skip``, read as a base-256 fraction in [0, 1)."""
    value, scale = 0.0, 1.0
    for byte in kb[skip:skip + POS_BYTES]:
        scale /= 256.0
        value += byte * scale
    return value


class MassAxis:
    """The measured key-mass CDF, as a projection of key bytes onto [0, 1].

    Built from ``(last, rows)`` pairs — one per page. Sorting them by key and
    accumulating the row counts gives the empirical distribution of objects across
    the keyspace; a key's position is its share of the bucket below it. Equal width
    is therefore equal objects, which is what makes a dense region legible instead
    of a hairline.

    A key falling between two measured page ends is interpolated by byte value inside
    that gap, so the projection stays monotone for the synthesized pivots and seed
    bounds that were never themselves a page's last key.
    """

    def __init__(self, points):
        merged = collections.OrderedDict()
        for key, rows in sorted(points):
            merged[key] = merged.get(key, 0) + rows
        self.keys = list(merged.keys())
        total = float(sum(merged.values())) or 1.0
        self.cum, running = [], 0.0
        for k in self.keys:
            running += merged[k]
            self.cum.append(running / total)
        self.count = len(self.keys)

    def pos(self, kb, default=0.0):
        """Position of a key in [0, 1] — the share of listed objects at or below it."""
        if kb is None or self.count == 0:
            return default
        lo, hi = 0, self.count
        while lo < hi:
            mid = (lo + hi) // 2
            if self.keys[mid] < kb:
                lo = mid + 1
            else:
                hi = mid
        idx = lo
        if idx >= self.count:
            return 1.0
        if self.keys[idx] == kb:
            return self.cum[idx]
        below = self.cum[idx - 1] if idx > 0 else 0.0
        above = self.cum[idx]
        left = self.keys[idx - 1] if idx > 0 else b""
        right = self.keys[idx]
        skip = len(common_prefix([left, right])) if idx > 0 else 0
        a, b, k = _fraction(left, skip), _fraction(right, skip), _fraction(kb, skip)
        frac = 0.0 if b <= a else min(1.0, max(0.0, (k - a) / (b - a)))
        return below + (above - below) * frac
