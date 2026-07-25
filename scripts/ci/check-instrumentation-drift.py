"""check-instrumentation-drift — CI guard for the "Instrument every new algorithm path" rule (AGENTS.md,
docs/internals/metrics-internals.md §5): every ``recordStealReason(category, reason)``
/ ``stealReasonCounter(category, reason)`` emission in ``swath-core/src/main/java`` must have a
matching row in the §5 CI-enforced registry table, and that table must not carry
"ghost" rows for counters no code actually emits (a row is exempt only if it is
tagged ``REMOVED <date>``).

Self-contained (stdlib only) so the public CI has no external package dependency.
The `.sh` wrapper CI invokes execs this file directly.

Usage:
    scripts/ci/check-instrumentation-drift.py [--repo-root PATH] [--self-test]
    scripts/ci/check-instrumentation-drift.sh [--self-test]  (CI entry point, unchanged)

Exit codes: 0 = clean, 1 = drift found, 2 = script error (e.g. can't find inputs).

How it resolves emission call sites
------------------------------------
Most call sites are two string literals: ``recordStealReason("CATEGORY", "reason")``.
Some pass a non-literal expression (a ternary of literals, or a local variable /
formal parameter). This script resolves those structurally instead of relying on a
hardcoded value list, so it keeps working as the code evolves:

  1. A literal arg is used directly.
  2. A string concatenation of one runtime-symbolic operand (a bare identifier or
     ``x.name()``) and one or more string literals, where at least one literal
     *trails* the symbolic operand (e.g. ``name + "_off"``), resolves to a FAMILY
     token: the literals verbatim with the symbolic operand collapsed to a ``<*>``
     placeholder (``<*>_off``). A doc row spells the same family with an
     ``<...>`` placeholder (``<name>_off``); the two match when the category and the
     literal prefix/suffix around the placeholder are identical. A literal-PREFIX +
     trailing-symbolic form (``"page_completed_at_" + level``) has no trailing
     affix and is NOT a family -- it falls through to rule 3, exactly as before.
  3. A non-literal arg expression that still contains quoted string literals
     anywhere in it (e.g. a ternary ``cond ? "a" : "b"``, including nested
     ternaries) contributes every literal found in the expression.
  4. A bare identifier (optionally ``x.name()``, i.e. an enum constant access) with
     *no* quoted literal in it is "symbolic" and is resolved by finding the
     smallest enclosing method body (via brace matching + a method-signature
     header regex) and:
       a. if the identifier is a LOCAL variable of that method, collecting the
          literals (quoted strings OR qualified enum-constant refs like
          ``Outcome.RETRY``) out of every assignment RHS to that variable in the
          method body ("pattern A" -- e.g. ``RunMetrics.recordChildMass``'s
          ``bucket``, or an enum/string local pair hoisted across branches and
          passed together later, e.g. ``Thief``'s ``pendingOutcome``/``pendingReason``);
       b. if the identifier is instead a FORMAL PARAMETER of that method, finding
          every call site of the method elsewhere in the tree and recursively
          resolving the argument expression at that parameter's position
          ("pattern B" -- e.g. ``SeedStep.recordSeed(String reason)``).
     A qualified reference like ``Outcome.NO_VICTIM`` resolves to the constant
     name ``NO_VICTIM`` (mirrors ``Outcome.name()``).
  5. When BOTH arguments of one call are symbolic and both resolve (pattern B) to
     parameters of the *same* wrapper method, the two are resolved together per
     wrapper call site (not as an independent cross product) so the two
     positions stay correlated -- e.g. ``Thief.record(Outcome outcome, String
     reason)``, whose ``outcome``/``reason`` pairs must not be cross-producted.
  6. Anything else (can't find a literal anywhere) is reported as a WARNING
     ("non-literal call site") rather than failing the build -- a human checks it.
"""

from __future__ import annotations

import argparse
import re
import sys
import tempfile
from dataclasses import dataclass, field
from pathlib import Path

EMIT_METHOD_NAMES = ("recordStealReason", "stealReasonCounter")

CONTROL_KEYWORDS = {
    "if", "for", "while", "switch", "catch", "do", "synchronized", "else", "try",
}

# A method-signature "header" -- the text immediately preceding a '{' -- looks like
# `<modifiers/annotations/return-type> name(<params>) [throws ...]`. Greedy pieces
# are non-greedy so `name`/`params` bind to the LAST `ident (...)` group in the
# header (closest to the brace), which is what we want for e.g. `record(...)`.
METHOD_SIG_RE = re.compile(
    r"^\s*(?:@\w+(?:\([^()]*\))?\s*)*"
    r"(?:(?:public|private|protected|static|final|synchronized|abstract|default|native)\s+)*"
    r"[\w$.<>\[\],?\s]+?\b(\w+)\s*\(([^()]*)\)\s*"
    r"(?:throws\s+[\w.,<>\s]+)?$"
)

STRING_LITERAL_RE = re.compile(r'"((?:[^"\\]|\\.)*)"')
QUALIFIED_CONST_RE = re.compile(r"^[A-Za-z_]\w*\.([A-Z][A-Za-z0-9_]*)$")
NAME_DOT_NAME_RE = re.compile(r"^([A-Za-z_]\w*)\.name\(\)$")
BARE_IDENT_RE = re.compile(r"^[A-Za-z_]\w*$")

# A "family" reason -- a runtime-symbolic key wrapped in fixed literal affixes, e.g.
# `name + "_off"` in the code or `<name>_off` in a doc row. Both sides normalize their
# `<...>` placeholder to this single marker so the family doc row and the family code
# token compare equal iff their category and the literal prefix/suffix around the
# placeholder match exactly (a plain-literal reason has no placeholder and is unchanged).
FAMILY_PLACEHOLDER = "<*>"
FAMILY_PLACEHOLDER_RE = re.compile(r"<[^<>]*>")


def canonical_family(reason: str) -> str:
    return FAMILY_PLACEHOLDER_RE.sub(FAMILY_PLACEHOLDER, reason)


MAX_RECURSION = 4


def scrub(text: str) -> str:
    """Blank out string/char literal interiors and comments, preserving length and
    newlines, so brace/paren/comma structure can be scanned without tripping over
    structural characters that merely appear inside a string or a comment."""
    out = list(text)
    i = 0
    n = len(text)
    while i < n:
        c = text[i]
        if c == "/" and i + 1 < n and text[i + 1] == "/":
            j = i
            while j < n and text[j] != "\n":
                out[j] = " "
                j += 1
            i = j
            continue
        if c == "/" and i + 1 < n and text[i + 1] == "*":
            j = i
            out[j] = " "
            out[j + 1] = " "
            j += 2
            while j + 1 < n and not (text[j] == "*" and text[j + 1] == "/"):
                if text[j] != "\n":
                    out[j] = " "
                j += 1
            if j + 1 < n:
                out[j] = " "
                out[j + 1] = " "
                j += 2
            i = j
            continue
        if c == '"':
            j = i + 1
            out[i] = " "
            while j < n and text[j] != '"' and text[j] != "\n":
                if text[j] == "\\" and j + 1 < n:
                    out[j] = " "
                    j += 1
                    if text[j] != "\n":
                        out[j] = " "
                    j += 1
                    continue
                out[j] = " "
                j += 1
            if j < n and text[j] == '"':
                out[j] = " "
                j += 1
            i = j
            continue
        if c == "'":
            j = i + 1
            out[i] = " "
            while j < n and text[j] != "'" and text[j] != "\n":
                if text[j] == "\\" and j + 1 < n:
                    out[j] = " "
                    j += 1
                    if text[j] != "\n":
                        out[j] = " "
                    j += 1
                    continue
                out[j] = " "
                j += 1
            if j < n and text[j] == "'":
                out[j] = " "
                j += 1
            i = j
            continue
        i += 1
    return "".join(out)


def find_matching_paren(scrubbed: str, open_idx: int) -> int | None:
    depth = 0
    for i in range(open_idx, len(scrubbed)):
        c = scrubbed[i]
        if c == "(":
            depth += 1
        elif c == ")":
            depth -= 1
            if depth == 0:
                return i
    return None


def split_top_level(orig: str, scrub_text: str) -> list[str]:
    """Split `orig` on commas that are at paren/bracket/brace depth 0 in `scrub_text`
    (same length/alignment as `orig`). Returns trimmed argument expression strings."""
    parts = []
    depth = 0
    start = 0
    for i, c in enumerate(scrub_text):
        if c in "([{":
            depth += 1
        elif c in ")]}":
            depth -= 1
        elif c == "," and depth == 0:
            parts.append(orig[start:i].strip())
            start = i + 1
    tail = orig[start:].strip()
    if tail or parts:
        parts.append(tail)
    return parts


def split_concat_atoms(expr: str) -> list[str]:
    """Split `expr` on top-level `+` string-concatenation operators (depth 0 in the
    scrubbed expression, so a `+` inside a nested call/paren or a string literal never
    splits). Returns the trimmed operand expressions; a single-element result means the
    expression is not a top-level concatenation."""
    s = scrub(expr)
    atoms = []
    depth = 0
    start = 0
    for i, c in enumerate(s):
        if c in "([{":
            depth += 1
        elif c in ")]}":
            depth -= 1
        elif c == "+" and depth == 0:
            atoms.append(expr[start:i].strip())
            start = i + 1
    atoms.append(expr[start:].strip())
    return atoms


def resolve_family_concat(expr: str) -> str | None:
    """If `expr` is a string concatenation of exactly one runtime-symbolic operand (a bare
    identifier or `x.name()`) and one or more string-literal operands where at least one
    literal *trails* the symbolic operand, return the FAMILY reason token: the literals
    spelled verbatim with the symbolic operand collapsed to `<*>` (e.g. `name + "_off"` ->
    ``<*>_off``, ``"pre_" + name + "_off"`` -> ``pre_<*>_off``). Returns None otherwise --
    including a literal-prefix + trailing-symbolic form (e.g. ``"page_completed_at_" +
    level``), which has no literal suffix and stays a plain literal-prefix reason resolved
    by the ordinary any-literal rule, exactly as before."""
    atoms = split_concat_atoms(expr)
    if len(atoms) < 2:
        return None
    parts: list[str] = []
    symbolic_idx = -1
    literal_count = 0
    for idx, a in enumerate(atoms):
        lm = re.match(r'^"((?:[^"\\]|\\.)*)"$', a)
        if lm:
            parts.append(lm.group(1))
            literal_count += 1
            continue
        base = a
        nm = NAME_DOT_NAME_RE.match(a)
        if nm:
            base = nm.group(1)
        if BARE_IDENT_RE.match(base):
            if symbolic_idx != -1:
                return None  # more than one symbolic operand -- not a single-key family
            symbolic_idx = idx
            parts.append(FAMILY_PLACEHOLDER)
            continue
        return None  # an operand that is neither a plain literal nor a bare symbol
    if symbolic_idx == -1 or literal_count == 0:
        return None
    # Require a literal AFTER the symbolic operand: that trailing affix is the case the
    # blanket any-literal rule mishandles (it would contribute the bare suffix). A purely
    # leading literal (symbolic operand trailing) is left to that rule's prefix extraction.
    if symbolic_idx == len(atoms) - 1:
        return None
    return "".join(parts)


def brace_spans(scrubbed: str) -> list[tuple[int, int]]:
    stack: list[int] = []
    spans = []
    for i, c in enumerate(scrubbed):
        if c == "{":
            stack.append(i)
        elif c == "}":
            if stack:
                spans.append((stack.pop(), i))
    return spans


@dataclass
class JavaFile:
    path: Path
    text: str
    scrub: str
    spans: list[tuple[int, int]] = field(default_factory=list)
    # (open_paren_idx, close_paren_idx) of every method DECLARATION's parameter
    # list in this file -- computed once at load time (see
    # `compute_decl_param_spans`) and used to tell a call site apart from the
    # declaration itself when hunting for a wrapper method's callers.
    decl_param_spans: set[tuple[int, int]] = field(default_factory=set)

    def line_of(self, idx: int) -> int:
        return self.text.count("\n", 0, idx) + 1


def compute_decl_param_spans(jf: JavaFile) -> set[tuple[int, int]]:
    spans = set()
    for (o, c) in jf.spans:
        header = extract_header(jf, o)
        if not header or header.startswith("new ") or re.search(r"\bnew\s*$", header):
            continue
        m = METHOD_SIG_RE.match(header)
        if not m or m.group(1) in CONTROL_KEYWORDS:
            continue
        # `header` == jf.scrub[boundary+1:o]; group(2) (the params) is bracketed by
        # the '(' / ')' immediately surrounding it within that same substring, so
        # its global offsets are the local match offsets shifted by the header's
        # start position (o - len(header) after the leading-whitespace strip --
        # recompute the boundary directly instead of relying on strip() offsets).
        boundary = o - 1
        while boundary >= 0 and jf.scrub[boundary] not in ";{}":
            boundary -= 1
        raw_header = jf.scrub[boundary + 1:o]
        lstrip_off = len(raw_header) - len(raw_header.lstrip())
        open_paren = boundary + 1 + lstrip_off + (m.start(2) - 1)
        close_paren = boundary + 1 + lstrip_off + m.end(2)
        spans.add((open_paren, close_paren))
    return spans


def load_java_files(src_root: Path) -> dict[Path, JavaFile]:
    files = {}
    for p in sorted(src_root.rglob("*.java")):
        text = p.read_text(encoding="utf-8")
        s = scrub(text)
        jf = JavaFile(p, text, s, brace_spans(s))
        jf.decl_param_spans = compute_decl_param_spans(jf)
        files[p] = jf
    return files


def extract_header(jf: JavaFile, open_idx: int) -> str:
    """The text immediately preceding `open_idx` (a '(' or '{' position) back to the
    previous top-level `;`/`{`/`}`, taken from the SCRUBBED text so any Javadoc/
    comment sitting in between (which may itself contain brace-like `{@code}`
    text) never leaks into the method-signature match below."""
    boundary = -1
    for i in range(open_idx - 1, -1, -1):
        if jf.scrub[i] in ";{}":
            boundary = i
            break
    header = jf.scrub[boundary + 1:open_idx]
    return header.strip()


def enclosing_method(jf: JavaFile, pos: int):
    """Returns (name, params_str, body_start, body_end, is_private) for the smallest
    enclosing method-like block containing `pos`, or None. `is_private` is a
    heuristic off the `private` modifier in the header, used to scope a caller
    search to the declaring file (a private method has no callers elsewhere,
    and a common short name -- e.g. `record` -- would otherwise collide with
    unrelated same-named methods on other types across the tree)."""
    containing = [s for s in jf.spans if s[0] < pos < s[1]]
    containing.sort(key=lambda s: s[1] - s[0])
    for (o, c) in containing:
        header = extract_header(jf, o)
        if not header or header.startswith("new ") or re.search(r"\bnew\s*$", header):
            continue
        m = METHOD_SIG_RE.match(header)
        if not m:
            continue
        name = m.group(1)
        if name in CONTROL_KEYWORDS:
            continue
        is_private = bool(re.search(r"(?<![A-Za-z0-9_])private(?![A-Za-z0-9_])", header))
        return name, m.group(2), o, c, is_private
    return None


def param_names(params_str: str) -> list[str]:
    names = []
    for raw in split_top_level(params_str, params_str):
        raw = raw.strip()
        if not raw:
            continue
        raw = re.sub(r"\bfinal\b", "", raw).strip()
        tokens = raw.replace("[]", " ").split()
        if tokens:
            names.append(tokens[-1])
    return names


def literals_in_expr(expr: str) -> list[str]:
    return [m.group(1) for m in STRING_LITERAL_RE.finditer(expr)]


QUALIFIED_CONST_FINDALL_RE = re.compile(r"\b[A-Za-z_]\w*\.([A-Z][A-Za-z0-9_]*)\b")


def literals_and_consts_in_expr(expr: str) -> list[str]:
    """Every quoted string literal OR qualified enum-constant reference (``Ident.CONST``)
    found anywhere in `expr` -- a ternary mixing the two, or (the common Pattern-A case
    below) a single assignment RHS that is just one or the other. This mirrors the two
    literal *shapes* a directly-passed call argument already resolves via rules 1/1.5 in
    `resolve_single_arg`, so an enum-typed local's per-branch assignments (e.g. a hoisted
    `pendingOutcome = Outcome.RETRY;` on one branch and `pendingOutcome = Outcome.UNSPLITTABLE;`
    on another) are traced the same way a directly-passed `Outcome.RETRY` argument already
    is -- structural, not a hardcoded name list."""
    found = literals_in_expr(expr)
    found.extend(m.group(1) for m in QUALIFIED_CONST_FINDALL_RE.finditer(expr))
    return found


@dataclass
class ResolveResult:
    literals: set[str] = field(default_factory=set)
    warning: str | None = None


def resolve_single_arg(files: dict[Path, JavaFile], jf: JavaFile, call_pos: int,
                        expr: str, depth: int) -> ResolveResult:
    expr = expr.strip()
    # Rule 1: a single simple string literal.
    m = re.match(r'^"((?:[^"\\]|\\.)*)"$', expr)
    if m:
        return ResolveResult(literals={m.group(1)})
    # Rule 1.5: a qualified constant reference, e.g. Outcome.NO_VICTIM.
    m = QUALIFIED_CONST_RE.match(expr)
    if m:
        return ResolveResult(literals={m.group(1)})
    # Rule 1.7: a symbolic key wrapped in a trailing string-literal affix, e.g.
    # `name + "_off"` -> the family token `<*>_off` (checked before Rule 2 so the
    # affix is not contributed as a bare-suffix literal). See `resolve_family_concat`.
    fam = resolve_family_concat(expr)
    if fam is not None:
        return ResolveResult(literals={fam})
    # Rule 2: any quoted literal(s) anywhere in the expression (ternaries, etc).
    lits = literals_in_expr(expr)
    if lits:
        return ResolveResult(literals=set(lits))
    # Rule 3: symbolic identifier -- strip a trailing `.name()`.
    base = expr
    nm = NAME_DOT_NAME_RE.match(expr)
    if nm:
        base = nm.group(1)
    if not BARE_IDENT_RE.match(base):
        return ResolveResult(warning=f"{jf.path}:{jf.line_of(call_pos)}: "
                                      f"non-literal argument `{expr}`")
    if depth > MAX_RECURSION:
        return ResolveResult(warning=f"{jf.path}:{jf.line_of(call_pos)}: "
                                      f"recursion limit resolving `{expr}`")
    method = enclosing_method(jf, call_pos)
    if method is None:
        return ResolveResult(warning=f"{jf.path}:{jf.line_of(call_pos)}: "
                                      f"non-literal argument `{expr}` (no enclosing method found)")
    mname, mparams, body_start, body_end, is_private = method
    params = param_names(mparams)
    if base in params:
        scope = {jf.path: jf} if is_private else files
        return resolve_via_callers(scope, mname, params.index(base), depth)
    # Pattern A: local variable -- scan assignments within the method body. Each RHS is
    # resolved for both literal shapes (quoted string OR qualified enum constant), so an
    # enum-typed local hoisted across branches (e.g. `pendingOutcome`) is traced exactly
    # like a directly-passed `Outcome.RETRY` argument would be.
    body_text = jf.text[body_start:body_end]
    literals: set[str] = set()
    assign_re = re.compile(r"(?<![.\w])" + re.escape(base) + r"\s*=\s*([^;]+);")
    for am in assign_re.finditer(body_text):
        literals.update(literals_and_consts_in_expr(am.group(1)))
    if literals:
        return ResolveResult(literals=literals)
    return ResolveResult(warning=f"{jf.path}:{jf.line_of(call_pos)}: "
                                  f"non-literal argument `{expr}` "
                                  f"(local `{base}` has no literal assignment found)")


def resolve_via_callers(files: dict[Path, JavaFile], method_name: str, param_idx: int,
                         depth: int) -> ResolveResult:
    literals: set[str] = set()
    warnings: list[str] = []
    call_re = re.compile(r"(?<![A-Za-z0-9_])" + re.escape(method_name) + r"\s*\(")
    for jf in files.values():
        for cm in call_re.finditer(jf.scrub):
            open_idx = cm.end() - 1
            close_idx = find_matching_paren(jf.scrub, open_idx)
            if close_idx is None:
                continue
            if (open_idx, close_idx) in jf.decl_param_spans:
                continue  # this is the declaration itself, not a call site
            inner_orig = jf.text[open_idx + 1:close_idx]
            inner_scrub = jf.scrub[open_idx + 1:close_idx]
            args = split_top_level(inner_orig, inner_scrub)
            if param_idx >= len(args):
                continue
            r = resolve_single_arg(files, jf, cm.start(), args[param_idx], depth + 1)
            literals |= r.literals
            if r.warning:
                warnings.append(r.warning)
    result = ResolveResult(literals=literals)
    if not literals and warnings:
        result.warning = "; ".join(warnings)
    return result


@dataclass
class EmissionSite:
    file: Path
    line: int
    pairs: set[tuple[str, str]]
    warning: str | None = None


def resolve_call_pairs(files: dict[Path, JavaFile], jf: JavaFile, call_pos: int,
                        args: list[str]) -> EmissionSite:
    line = jf.line_of(call_pos)
    if len(args) != 2:
        return EmissionSite(jf.path, line, set(),
                             f"{jf.path}:{line}: expected 2 args, found {len(args)}: {args}")

    def is_symbolic(e: str) -> bool:
        e = e.strip()
        if re.match(r'^"((?:[^"\\]|\\.)*)"$', e):
            return False
        if literals_in_expr(e):
            return False
        return True

    a0, a1 = args[0].strip(), args[1].strip()
    if is_symbolic(a0) and is_symbolic(a1):
        # Both symbolic: check whether both are params of the *same* enclosing
        # wrapper method -- if so resolve them per-call-site (correlated), not as
        # an independent cross product (see module docstring, point 5).
        base0 = a0
        m0 = NAME_DOT_NAME_RE.match(a0)
        if m0:
            base0 = m0.group(1)
        base1 = a1
        m1 = NAME_DOT_NAME_RE.match(a1)
        if m1:
            base1 = m1.group(1)
        if BARE_IDENT_RE.match(base0) and BARE_IDENT_RE.match(base1):
            method = enclosing_method(jf, call_pos)
            if method is not None:
                mname, mparams, _, _, is_private = method
                params = param_names(mparams)
                if base0 in params and base1 in params:
                    scope = {jf.path: jf} if is_private else files
                    pairs, warn = resolve_correlated_pair(scope, mname, params.index(base0),
                                                           params.index(base1))
                    return EmissionSite(jf.path, line, pairs, warn)

    r0 = resolve_single_arg(files, jf, call_pos, a0, 0)
    r1 = resolve_single_arg(files, jf, call_pos, a1, 0)
    warns = [w for w in (r0.warning, r1.warning) if w]
    warning = "; ".join(warns) if warns else None
    pairs = {(c, r) for c in (r0.literals or set()) for r in (r1.literals or set())}
    return EmissionSite(jf.path, line, pairs, warning)


def resolve_correlated_pair(files: dict[Path, JavaFile], method_name: str,
                             idx0: int, idx1: int) -> tuple[set[tuple[str, str]], str | None]:
    pairs: set[tuple[str, str]] = set()
    warnings: list[str] = []
    call_re = re.compile(r"(?<![A-Za-z0-9_])" + re.escape(method_name) + r"\s*\(")
    for jf in files.values():
        for cm in call_re.finditer(jf.scrub):
            open_idx = cm.end() - 1
            close_idx = find_matching_paren(jf.scrub, open_idx)
            if close_idx is None:
                continue
            if (open_idx, close_idx) in jf.decl_param_spans:
                continue
            inner_orig = jf.text[open_idx + 1:close_idx]
            inner_scrub = jf.scrub[open_idx + 1:close_idx]
            args = split_top_level(inner_orig, inner_scrub)
            if max(idx0, idx1) >= len(args):
                continue
            r0 = resolve_single_arg(files, jf, cm.start(), args[idx0], MAX_RECURSION)
            r1 = resolve_single_arg(files, jf, cm.start(), args[idx1], MAX_RECURSION)
            if r0.warning or r1.warning or not r0.literals or not r1.literals:
                warnings.append(f"{jf.path}:{jf.line_of(cm.start())}: "
                                 f"could not resolve correlated pair for {method_name}(...)")
                continue
            for c in r0.literals:
                for r in r1.literals:
                    pairs.add((c, r))
    return pairs, ("; ".join(warnings) if warnings else None)


def find_emission_sites(files: dict[Path, JavaFile]) -> list[EmissionSite]:
    sites = []
    call_re = re.compile(
        r"(?<![A-Za-z0-9_])(?:" + "|".join(re.escape(n) for n in EMIT_METHOD_NAMES) + r")\s*\(")
    for jf in files.values():
        for cm in call_re.finditer(jf.scrub):
            open_idx = cm.end() - 1
            close_idx = find_matching_paren(jf.scrub, open_idx)
            if close_idx is None:
                continue
            if (open_idx, close_idx) in jf.decl_param_spans:
                continue  # a method named e.g. `record(...)` that merely mentions
                          # `recordStealReason(` as a substring never matches here
                          # (word-boundary guarded); this guards the rarer case of
                          # a declaration whose OWN signature paren happens to
                          # equal a matched span (defensive, not expected to fire).
            enclosing = enclosing_method(jf, cm.start())
            if enclosing is not None and enclosing[0] in EMIT_METHOD_NAMES:
                # This call is inside the RunMetrics.recordStealReason/
                # stealReasonCounter *definitions* themselves -- e.g.
                # `recordStealReason` calling `stealReasonCounter(outcome, reason)`
                # with its own formal parameters. That's the generic dispatch
                # plumbing, not an emission site with a fixed pair; skip it (its
                # callers are themselves scanned as separate, real emission sites).
                continue
            inner_orig = jf.text[open_idx + 1:close_idx]
            inner_scrub = jf.scrub[open_idx + 1:close_idx]
            args = split_top_level(inner_orig, inner_scrub)
            sites.append(resolve_call_pairs(files, jf, cm.start(), args))
    return sites


# ---------------------------------------------------------------------------
# §5 doc table parsing.
# ---------------------------------------------------------------------------

TABLE_START = "<!-- ci:steal-reason-table:start -->"
TABLE_END = "<!-- ci:steal-reason-table:end -->"
REMOVED_RE = re.compile(r"REMOVED\s+\d{4}-\d{2}-\d{2}")


@dataclass
class DocRow:
    category: str
    reason: str
    removed: bool
    line: int


def parse_doc_table(doc_path: Path) -> list[DocRow]:
    text = doc_path.read_text(encoding="utf-8")
    lines = text.splitlines()
    try:
        start = next(i for i, l in enumerate(lines) if TABLE_START in l)
        end = next(i for i, l in enumerate(lines) if TABLE_END in l)
    except StopIteration:
        raise SystemExit(f"error: could not find {TABLE_START}/{TABLE_END} markers in {doc_path}")
    rows = []
    for i in range(start + 1, end):
        line = lines[i]
        if not line.strip().startswith("|"):
            continue
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if len(cells) < 3:
            continue
        category, reason, status = cells[0], cells[1], cells[2]
        if category.lower() in ("category",) or set(category) <= {"-", ":"}:
            continue
        cat = category.strip("`")
        rsn = reason.strip("`")
        removed = bool(REMOVED_RE.search(status))
        rows.append(DocRow(cat, rsn, removed, i + 1))
    return rows


# ---------------------------------------------------------------------------
# Main.
# ---------------------------------------------------------------------------

# Multi-module layout: the root is now an aggregator with no production
# src/ -- what used to be one `src/main/java` tree is split across these modules.
# swath-replay-server is deliberately EXCLUDED: its FixtureMetrics#recordStealReason
# was never scanned pre-reorg either (it always lived in its own top-level module,
# never under the root's `src/`), so excluding it here is a no-op vs. prior behavior,
# not a new gap -- the guard's job is the LISTING engine's instrumentation, not the
# dev-tool replay server's.
SCANNED_MODULES = ("swath-model", "swath-core", "swath-s3", "swath-cli")


def run_check(repo_root: Path) -> int:
    src_roots = [repo_root / m / "src" / "main" / "java" for m in SCANNED_MODULES]
    doc_path = repo_root / "docs" / "internals" / "metrics-internals.md"
    existing_roots = [r for r in src_roots if r.is_dir()]
    if not existing_roots:
        # None of the expected module src roots exist -- either a genuinely broken
        # repo layout, or (the common case for scratch/self-test trees) a synthetic
        # tree that only needs SOME of the module dirs; the per-module skip below
        # handles the latter, so this only fires when NOTHING was found at all.
        for r in src_roots:
            print(f"error: {r} not found", file=sys.stderr)
        return 2
    if not doc_path.is_file():
        print(f"error: {doc_path} not found", file=sys.stderr)
        return 2

    files: dict[Path, JavaFile] = {}
    for src_root in existing_roots:
        files.update(load_java_files(src_root))
    sites = find_emission_sites(files)

    code_pairs: set[tuple[str, str]] = set()
    warnings: list[str] = []
    for s in sites:
        code_pairs |= {(c, canonical_family(r)) for (c, r) in s.pairs}
        if s.warning:
            warnings.append(s.warning)

    try:
        doc_rows = parse_doc_table(doc_path)
    except SystemExit as e:
        print(str(e), file=sys.stderr)
        return 2

    live_doc_pairs = {(r.category, canonical_family(r.reason)) for r in doc_rows if not r.removed}
    removed_doc_pairs = {(r.category, canonical_family(r.reason)) for r in doc_rows if r.removed}

    undocumented = sorted(code_pairs - live_doc_pairs)
    ghosts = sorted((live_doc_pairs - code_pairs) - removed_doc_pairs)
    # A REMOVED row whose pair is still live in code is also drift: the code
    # still emits it, so it must not be marked REMOVED.
    wrongly_removed = sorted(removed_doc_pairs & code_pairs)

    if warnings:
        print("WARNING: non-literal recordStealReason/stealReasonCounter call site(s) "
              "(human review, not build-failing):")
        for w in sorted(set(warnings)):
            print(f"  - {w}")

    drift = False
    if undocumented:
        drift = True
        print(f"\nUNDOCUMENTED COUNTERS ({len(undocumented)}) -- emitted by code, "
              f"missing from the §5 registry table in {doc_path}:")
        for cat, reason in undocumented:
            print(f"  - {cat}.{reason}")
    if ghosts:
        drift = True
        print(f"\nGHOST ROWS ({len(ghosts)}) -- in the §5 registry table but no code "
              f"emits them (mark REMOVED <date> if intentionally retired):")
        for cat, reason in ghosts:
            print(f"  - {cat}.{reason}")
    if wrongly_removed:
        drift = True
        print(f"\nWRONGLY-REMOVED ROWS ({len(wrongly_removed)}) -- marked REMOVED in the "
              f"table but code still emits them:")
        for cat, reason in wrongly_removed:
            print(f"  - {cat}.{reason}")

    if drift:
        print(f"\ninstrumentation-drift guard: FAIL ({len(undocumented)} undocumented, "
              f"{len(ghosts)} ghost, {len(wrongly_removed)} wrongly-removed)")
        return 1

    print(f"instrumentation-drift guard: OK ({len(code_pairs)} live counter pairs, "
          f"{len(removed_doc_pairs)} REMOVED rows, {len(warnings)} non-literal site(s) "
          f"flagged for human review)")
    return 0


def self_test_ghost_and_undocumented() -> int:
    """Synthetic smoke test of the guard itself: builds a scratch tree with (a) an
    undocumented emission and (b) a ghost doc row, and verifies both are caught."""
    print("self-test: constructing a scratch tree with injected drift...")
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        src = root / "swath-core" / "src" / "main" / "java" / "dev" / "swath" / "x"
        src.mkdir(parents=True)
        (src / "Sample.java").write_text(
            "package io.varve.swath.x;\n"
            "class Sample {\n"
            "    void run(Metrics metrics) {\n"
            "        metrics.recordStealReason(\"DOCUMENTED\", \"present\");\n"
            "        metrics.recordStealReason(\"SELFTEST\", \"injected_undocumented\");\n"
            "    }\n"
            "}\n",
            encoding="utf-8",
        )
        docs = root / "docs" / "internals"
        docs.mkdir(parents=True)
        (docs / "metrics-internals.md").write_text(
            "# doc\n\n"
            f"{TABLE_START}\n"
            "| category | reason | status |\n"
            "|---|---|---|\n"
            "| `DOCUMENTED` | `present` | |\n"
            "| `GHOST` | `never_emitted` | |\n"
            f"{TABLE_END}\n",
            encoding="utf-8",
        )
        rc = run_check(root)
        if rc != 1:
            print(f"self-test FAILED: expected exit 1 (drift), got {rc}", file=sys.stderr)
            return 1
        print("self-test: injected undocumented-counter + ghost-row drift both detected. PASS")
        return 0


def self_test_hoisted_enum_local_pattern() -> int:
    """Regression for a hoist that routes an emission through an ENUM-typed local (assigned a
    qualified constant like ``Outcome.RETRY`` on each branch) and a STRING-typed local (assigned
    a literal reason on each branch), both later passed together to a private correlated wrapper
    (mirroring ``Thief``'s ``pendingOutcome``/``pendingReason`` -> ``record(pendingOutcome,
    pendingReason)``), used to misreport the wrapper's live pairs as ghost doc rows -- Pattern
    A's local-variable literal scan only recognized quoted-string assignment RHS values, not
    qualified enum-constant ones. Builds a scratch tree with exactly that shape (branch-hoisted
    locals of BOTH kinds, feeding a private two-arg wrapper) and asserts the guard resolves
    every emitted pair cleanly against a doc table that documents them (rc == 0, no ghosts, no
    non-literal warning) -- this would have failed (ghost rows for `RETRY.a`/`RETRY.b`) before
    the `literals_and_consts_in_expr` fix."""
    print("self-test: constructing a scratch tree with a hoisted enum-local/string-local pair...")
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        src = root / "swath-core" / "src" / "main" / "java" / "dev" / "swath" / "y"
        src.mkdir(parents=True)
        (src / "Hoist.java").write_text(
            "package io.varve.swath.y;\n"
            "class Hoist {\n"
            "    enum Outcome { RETRY, CHILD_CREATED }\n"
            "    private final Metrics metrics;\n"
            "    Hoist(Metrics metrics) { this.metrics = metrics; }\n"
            "    Outcome attempt(boolean condA, boolean condB) {\n"
            "        Outcome pendingOutcome = null;\n"
            "        String pendingReason = null;\n"
            "        if (condA) {\n"
            "            pendingOutcome = Outcome.RETRY;\n"
            "            pendingReason = \"a\";\n"
            "        } else if (condB) {\n"
            "            pendingOutcome = Outcome.RETRY;\n"
            "            pendingReason = \"b\";\n"
            "        }\n"
            "        if (pendingOutcome != null) {\n"
            "            return record(pendingOutcome, pendingReason);\n"
            "        }\n"
            "        return record(Outcome.CHILD_CREATED, \"split_committed\");\n"
            "    }\n"
            "    private Outcome record(Outcome outcome, String reason) {\n"
            "        metrics.recordStealReason(outcome.name(), reason);\n"
            "        return outcome;\n"
            "    }\n"
            "}\n",
            encoding="utf-8",
        )
        docs = root / "docs" / "internals"
        docs.mkdir(parents=True)
        (docs / "metrics-internals.md").write_text(
            "# doc\n\n"
            f"{TABLE_START}\n"
            "| category | reason | status |\n"
            "|---|---|---|\n"
            "| `RETRY` | `a` | |\n"
            "| `RETRY` | `b` | |\n"
            "| `CHILD_CREATED` | `split_committed` | |\n"
            f"{TABLE_END}\n",
            encoding="utf-8",
        )
        rc = run_check(root)
        if rc != 0:
            print(f"self-test FAILED: expected exit 0 (no drift), got {rc}", file=sys.stderr)
            return 1
        print("self-test: hoisted enum-local/string-local pair resolved with no false ghosts. PASS")
        return 0


def self_test_family_concat_documented() -> int:
    """A concat emission (``name + "_off"``, mirroring ``EngineToggles.recordOffMarks``'s
    per-disabled-toggle mark) resolves to the family token ``<*>_off`` and matches a family
    doc row spelled ``<name>_off`` -- one row covers the whole ``TOGGLE.<name>_off`` family, no
    drift (rc == 0). Before family resolution this shape mis-reported the bare-suffix pair
    ``TOGGLE._off`` as undocumented while every per-toggle doc row went ghost."""
    print("self-test: constructing a scratch tree with a concat family emission + family doc row...")
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        src = root / "swath-core" / "src" / "main" / "java" / "dev" / "swath" / "z"
        src.mkdir(parents=True)
        (src / "Fam.java").write_text(
            "package io.varve.swath.z;\n"
            "import java.util.List;\n"
            "class Fam {\n"
            "    void run(Metrics metrics, List<String> off) {\n"
            "        for (String name : off) {\n"
            "            metrics.recordStealReason(\"TOGGLE\", name + \"_off\");\n"
            "        }\n"
            "    }\n"
            "}\n",
            encoding="utf-8",
        )
        docs = root / "docs" / "internals"
        docs.mkdir(parents=True)
        (docs / "metrics-internals.md").write_text(
            "# doc\n\n"
            f"{TABLE_START}\n"
            "| category | reason | status |\n"
            "|---|---|---|\n"
            "| `TOGGLE` | `<name>_off` | one ablation mark per disabled toggle key | |\n"
            f"{TABLE_END}\n",
            encoding="utf-8",
        )
        rc = run_check(root)
        if rc != 0:
            print(f"self-test FAILED: expected exit 0 (no drift), got {rc}", file=sys.stderr)
            return 1
        print("self-test: concat family emission matched its `<name>_off` family doc row. PASS")
        return 0


def self_test_family_concat_undocumented() -> int:
    """A concat family emission with NO family doc row is caught as one undocumented family
    token (rc == 1). The plain documented emission alongside it keeps the missing family row
    the sole drift, so the failure is unambiguously the family token, not the plain pair."""
    print("self-test: constructing a scratch tree with a concat family emission and no family row...")
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        src = root / "swath-core" / "src" / "main" / "java" / "dev" / "swath" / "z"
        src.mkdir(parents=True)
        (src / "Fam.java").write_text(
            "package io.varve.swath.z;\n"
            "import java.util.List;\n"
            "class Fam {\n"
            "    void run(Metrics metrics, List<String> off) {\n"
            "        metrics.recordStealReason(\"DOCUMENTED\", \"present\");\n"
            "        for (String name : off) {\n"
            "            metrics.recordStealReason(\"TOGGLE\", name + \"_off\");\n"
            "        }\n"
            "    }\n"
            "}\n",
            encoding="utf-8",
        )
        docs = root / "docs" / "internals"
        docs.mkdir(parents=True)
        (docs / "metrics-internals.md").write_text(
            "# doc\n\n"
            f"{TABLE_START}\n"
            "| category | reason | status |\n"
            "|---|---|---|\n"
            "| `DOCUMENTED` | `present` | |\n"
            f"{TABLE_END}\n",
            encoding="utf-8",
        )
        rc = run_check(root)
        if rc != 1:
            print(f"self-test FAILED: expected exit 1 (drift), got {rc}", file=sys.stderr)
            return 1
        print("self-test: undocumented `<*>_off` family token detected as drift. PASS")
        return 0


def self_test_family_row_ghost() -> int:
    """A family doc row (`<name>_off`) with no matching concat emission is a ghost (rc == 1),
    exactly as a plain ghost row is today. The plain documented emission keeps the ghost family
    row the sole drift."""
    print("self-test: constructing a scratch tree with a family doc row and no concat emission...")
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        src = root / "swath-core" / "src" / "main" / "java" / "dev" / "swath" / "z"
        src.mkdir(parents=True)
        (src / "Fam.java").write_text(
            "package io.varve.swath.z;\n"
            "class Fam {\n"
            "    void run(Metrics metrics) {\n"
            "        metrics.recordStealReason(\"DOCUMENTED\", \"present\");\n"
            "    }\n"
            "}\n",
            encoding="utf-8",
        )
        docs = root / "docs" / "internals"
        docs.mkdir(parents=True)
        (docs / "metrics-internals.md").write_text(
            "# doc\n\n"
            f"{TABLE_START}\n"
            "| category | reason | status |\n"
            "|---|---|---|\n"
            "| `DOCUMENTED` | `present` | |\n"
            "| `TOGGLE` | `<name>_off` | one ablation mark per disabled toggle key | |\n"
            f"{TABLE_END}\n",
            encoding="utf-8",
        )
        rc = run_check(root)
        if rc != 1:
            print(f"self-test FAILED: expected exit 1 (drift), got {rc}", file=sys.stderr)
            return 1
        print("self-test: ghost `<name>_off` family doc row detected as drift. PASS")
        return 0


def self_test() -> int:
    results = [
        self_test_ghost_and_undocumented(),
        self_test_hoisted_enum_local_pattern(),
        self_test_family_concat_documented(),
        self_test_family_concat_undocumented(),
        self_test_family_row_ghost(),
    ]
    return 1 if any(results) else 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=None,
                         help="repo root (default: inferred from this script's location)")
    parser.add_argument("--self-test", action="store_true",
                         help="run the synthetic drift smoke test instead of the real check")
    args = parser.parse_args()

    if args.self_test:
        return self_test()

    # scripts/ci/check-instrumentation-drift.py -> parents[2] == repo root.
    repo_root = args.repo_root or Path(__file__).resolve().parents[2]
    return run_check(repo_root)


if __name__ == "__main__":
    sys.exit(main())
