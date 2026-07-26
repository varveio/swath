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

Two more mechanisms sit alongside rules 1-6, added when some reasons became closed enums
(io.varve.swath.engine.policy's PivotMechanism/RetryReason/NoVictimReason/
UnsplittableReason) or a plain data record (Engagement) instead of bare literals:

  7. A ``receiver.accessor().code()`` expression (an enum-typed record-component
     accessor chain, e.g. ``noVictim.reason().code()``) or a qualified
     ``EnumType.CONSTANT.code()`` reference resolves via a ``TypeIndex`` built once per
     run: every ``record`` declaration's components, and every enum with a no-arg
     ``code()`` method ("coded enum"), indexed to its constants' literal codes.
     ``receiver``'s declared type is found the same way as rule 4's identifiers (a
     local, an `instanceof` binding, a for-each element, or -- also checked here -- a
     FORMAL PARAMETER of the enclosing method), then its accessor is looked up as a
     record component. If that component's type is a coded enum, this is resolved by
     tracing which constant(s) are ACTUALLY assigned to that component at every
     ``new ReceiverType(...)`` construction site in the tree (an assignment may itself be
     a local, a formal parameter resolved via rule 4's caller search, or -- rule 4 did not
     cover this -- an INSTANCE FIELD assigned across several methods of its enclosing
     type, e.g. `ThiefPolicy.Attempt#mechanism`, whose scan therefore widens from the one
     method containing the construction site to the type that declares the field; see
     `enclosing_type_body`). Enumerating the WHOLE enum (every declared constant,
     regardless of what is actually constructed) is a fallback used ONLY when the
     receiver's record type has NO construction site anywhere in the tree -- nothing to
     narrow against. Issue #21 was exactly the gap between these two: unconditional whole-
     enum enumeration credited every constant to every category reaching it through any
     accessor chain, so the guard could silently validate a (category, reason) doc row no
     code path actually emitted (the dangerous direction: a ghost row fails loudly, a
     spuriously-resolved one does not) -- most concretely when the SAME coded enum type is
     the declared type of a same-named component shared across multiple record types, each
     feeding a different category. Fixed by narrowing through construction sites first, as
     described above; the whole-enum fallback remains, but only for a type that is never
     constructed at all, where there is nothing to narrow against and no wrong-category
     risk (no category reaches it through construction, only through the receiver's own,
     single, unambiguous category at that one call site).
  8. Two no-arg accessor calls on the SAME local, both symbolic (e.g. ``e.category()``
     / ``e.reason()`` inside a ``for (Engagement e : engagements)`` loop) resolve via
     every ``new RecordType(...)`` construction site for that local's record type,
     correlating the constructor args at the matching component positions -- reusing
     rule 5's correlated-pair machinery one level removed. A construction site's own
     args are often themselves symbolic params of an enclosing wrapper (e.g.
     ``ThiefPolicy.addEngagement(String category, String reason)``), so this recurses
     back through rule 5 rather than resolving the two positions independently, which
     would silently cross-product unrelated categories and reasons together.
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


TYPE_HEADER_RE = re.compile(r"\b(?:class|interface|enum|record)\s+\w+")


def enclosing_type_body(jf: JavaFile, pos: int) -> tuple[int, int] | None:
    """The smallest enclosing TYPE (class/interface/enum/record) body containing `pos`
    -- as opposed to `enclosing_method`, which finds the smallest enclosing METHOD.
    Used to widen an unresolved identifier's assignment scan from one method to the
    whole type that declares it (see `resolve_single_arg`'s Pattern C: a bare
    identifier that turns out to be an INSTANCE FIELD assigned across several methods
    of its class, e.g. `ThiefPolicy.Attempt#mechanism`, rather than a local scoped to
    the one method containing the emission/construction site)."""
    containing = [s for s in jf.spans if s[0] < pos < s[1]]
    containing.sort(key=lambda s: s[1] - s[0])
    for (o, c) in containing:
        header = extract_header(jf, o)
        if header and TYPE_HEADER_RE.search(header):
            return o + 1, c
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


# ---------------------------------------------------------------------------
# Type index: record components + "coded enums" (an enum with a no-arg `code()`
# accessor, whose constants each carry a literal string as their first constructor
# argument -- io.varve.swath.engine.policy's PivotMechanism/RetryReason/NoVictimReason/
# UnsplittableReason all follow this shape). Built once per `run_check` and threaded
# through resolution so a call-site expression shaped `receiver.accessor().code()` (an
# enum-typed record-component accessor chain) resolves by ENUMERATING the enum type's
# declared constants rather than chasing where the receiver's runtime value came from --
# per the policy split, the reasons are now closed enums, and a new constant
# can't hide from an enumeration the way it could hide from dataflow tracing.
# ---------------------------------------------------------------------------

def split_typed_params(orig: str, scrub_text: str) -> list[tuple[str, str]]:
    """Split a record/method parameter list into ``(simple_type, name)`` pairs, splitting
    on commas at ``()[]{}<>`` depth 0 in `scrub_text` (so a generic type argument's own
    comma, e.g. a hypothetical ``Map<String, Integer>`` component, never splits)."""
    parts = []
    depth = 0
    start = 0
    for i, c in enumerate(scrub_text):
        if c in "([{<":
            depth += 1
        elif c in ")]}>":
            depth -= 1
        elif c == "," and depth == 0:
            parts.append(orig[start:i].strip())
            start = i + 1
    tail = orig[start:].strip()
    if tail or parts:
        parts.append(tail)
    out = []
    for p in parts:
        if not p:
            continue
        p = re.sub(r"\bfinal\b", "", p).strip()
        tokens = p.replace("[]", " ").split()
        if len(tokens) < 2:
            continue
        name = tokens[-1]
        type_str = " ".join(tokens[:-1])
        simple = re.sub(r"<.*>", "", type_str, flags=re.DOTALL).strip()
        simple = simple.rsplit(".", 1)[-1]
        out.append((simple, name))
    return out


RECORD_DECL_RE = re.compile(r"\brecord\s+(\w+)\s*\(")
ENUM_DECL_RE = re.compile(r"\benum\s+(\w+)\b[^{;]*\{")
CONST_DECL_RE = re.compile(r"(?<![.\w])([A-Z][A-Z0-9_]*)\s*\(")


def find_top_level_terminator(scrub_text: str, start: int, end: int, terminator: str) -> int:
    """The index of the first `terminator` char at `()[]{}` depth 0 in
    `scrub_text[start:end]`, or `end` if none (the whole span is the search region)."""
    depth = 0
    for i in range(start, end):
        c = scrub_text[i]
        if c in "([{":
            depth += 1
        elif c in ")]}":
            depth -= 1
        elif c == terminator and depth == 0:
            return i
    return end


@dataclass
class TypeIndex:
    # RecordTypeName -> {componentName: simpleComponentTypeName}, in declaration order.
    record_components: dict[str, dict[str, str]] = field(default_factory=dict)
    # "Coded enum" TypeName -> {constantName: literalCode}. Only enums with a no-arg
    # `code()` method are included -- a plain `Outcome`-style enum (no `code()`) is not.
    coded_enums: dict[str, dict[str, str]] = field(default_factory=dict)


def build_type_index(files: dict[Path, JavaFile]) -> TypeIndex:
    record_components: dict[str, dict[str, str]] = {}
    coded_enums: dict[str, dict[str, str]] = {}
    for jf in files.values():
        span_by_open_here = {o: c for (o, c) in jf.spans}

        for m in RECORD_DECL_RE.finditer(jf.scrub):
            open_idx = m.end() - 1
            close_idx = find_matching_paren(jf.scrub, open_idx)
            if close_idx is None:
                continue
            comps: dict[str, str] = {}
            for type_name, comp_name in split_typed_params(
                    jf.text[open_idx + 1:close_idx], jf.scrub[open_idx + 1:close_idx]):
                comps[comp_name] = type_name
            record_components[m.group(1)] = comps

        for m in ENUM_DECL_RE.finditer(jf.scrub):
            brace_open = m.end() - 1
            brace_close = span_by_open_here.get(brace_open)
            if brace_close is None:
                continue
            body = jf.scrub[brace_open + 1:brace_close]
            if not re.search(r"(?<![.\w])code\s*\(\s*\)", body):
                continue  # not a "coded" enum -- no code() accessor, so nothing to enumerate
            const_region_end = find_top_level_terminator(
                jf.scrub, brace_open + 1, brace_close, ";")
            consts: dict[str, str] = {}
            for cm in CONST_DECL_RE.finditer(jf.scrub, brace_open + 1, const_region_end):
                c_open = cm.end() - 1
                c_close = find_matching_paren(jf.scrub, c_open)
                if c_close is None or c_close > const_region_end:
                    continue
                lits = literals_in_expr(jf.text[c_open + 1:c_close])
                if lits:
                    consts[cm.group(1)] = lits[0]
            if consts:
                coded_enums[m.group(1)] = consts
    return TypeIndex(record_components=record_components, coded_enums=coded_enums)


FOR_EACH_TYPE_RE_TMPL = r"for\s*\(\s*(\w+)\s+{ident}\s*:"
INSTANCEOF_BIND_RE_TMPL = r"\binstanceof\s+(\w+)\s+{ident}\b"
PLAIN_DECL_TYPE_RE_TMPL = r"(?<![.\w])(\w+)\s+{ident}\s*=(?!=)"


def find_local_type(jf: JavaFile, pos: int, ident: str) -> str | None:
    """The declared/bound simple type name of local `ident` at `pos`'s enclosing method,
    trying (in order) a formal PARAMETER of that method (e.g. `Thief#commit`'s `Commit
    commit` parameter -- easy to miss since a parameter's name can coincide with the
    method's own, as it does there), a for-each element declaration, an `instanceof`
    pattern binding, and a plain local declaration -- the shapes this codebase's emission
    sites actually bind a record-typed local through. Returns `None` if none match (the
    identifier is e.g. a field, or bound in a shape this does not recognize)."""
    method = enclosing_method(jf, pos)
    if method is None:
        return None
    _, mparams, body_start, body_end, _ = method
    for type_name, name in split_typed_params(mparams, scrub(mparams)):
        if name == ident:
            return type_name
    body = jf.scrub[body_start:body_end]
    ident_esc = re.escape(ident)
    for tmpl in (FOR_EACH_TYPE_RE_TMPL, INSTANCEOF_BIND_RE_TMPL, PLAIN_DECL_TYPE_RE_TMPL):
        m = re.search(tmpl.format(ident=ident_esc), body)
        if m:
            return m.group(1)
    return None


ACCESSOR_CHAIN_CODE_RE = re.compile(r"^(\w+)\.(\w+)\(\)\.code\(\)$")
QUALIFIED_CONST_CODE_RE = re.compile(r"^(\w+)\.([A-Z][A-Za-z0-9_]*)\.code\(\)$")
ACCESSOR_CALL_RE = re.compile(r"^(\w+)\.(\w+)\(\)$")


def resolve_constructed_component_codes(
        files: dict[Path, JavaFile], recv_type: str, comp_idx: int, consts: dict[str, str],
        type_index: TypeIndex, depth: int) -> tuple[set[str], bool]:
    """For every ``new recv_type(...)`` construction site in the tree, resolve the
    constructor argument at `comp_idx` (the coded-enum-typed component's position) to the
    constant NAME(s) actually assigned there, and map each through `consts` to its code.
    Returns ``(codes, found_any_construction_site)`` -- the second element tells the
    caller whether "no codes resolved" means "genuinely nothing reaches this component"
    (found_any_construction_site True, codes empty is a legitimate empty/unresolvable
    result) versus "this type is never constructed anywhere" (False, the only case that
    should fall back to full-enum enumeration -- see `resolve_enum_code_expr`)."""
    call_re = re.compile(r"(?<![A-Za-z0-9_])new\s+" + re.escape(recv_type) + r"\s*\(")
    codes: set[str] = set()
    found_site = False
    for jf in files.values():
        for cm in call_re.finditer(jf.scrub):
            open_idx = cm.end() - 1
            close_idx = find_matching_paren(jf.scrub, open_idx)
            if close_idx is None:
                continue
            inner_orig = jf.text[open_idx + 1:close_idx]
            inner_scrub = jf.scrub[open_idx + 1:close_idx]
            args = split_top_level(inner_orig, inner_scrub)
            if comp_idx >= len(args):
                continue
            found_site = True
            r = resolve_single_arg(files, jf, cm.start(), args[comp_idx], depth + 1, type_index)
            for name in r.literals:
                if name in consts:
                    codes.add(consts[name])
    return codes, found_site


def resolve_enum_code_expr(files: dict[Path, JavaFile], jf: JavaFile, call_pos: int, expr: str,
                            type_index: TypeIndex, depth: int) -> set[str] | None:
    """Resolve an enum-`.code()` expression to the codes that ACTUALLY reach it, not the
    enum type's whole constant set (issue #21: crediting every declared constant to every
    category reaching it through an accessor chain let the guard silently validate a doc
    row no code path emits -- the dangerous direction, since a ghost row fails loudly and
    a spuriously-resolved one does not):

      * ``receiver.accessor().code()`` -- `receiver`'s declared type (see
        `find_local_type`) must be a record with a component named `accessor` whose
        declared type is a "coded enum". Resolved by finding every construction site of
        that record type and tracing which constant(s) are actually assigned to the
        `accessor` component there (`resolve_constructed_component_codes`) -- reusing the
        same local/param/field resolution `resolve_single_arg` already does, not a new
        dataflow pass. Falls back to enumerating the WHOLE enum only when the record type
        has NO construction site anywhere in the tree (nothing to narrow against).
      * ``EnumType.CONSTANT.code()`` -- a qualified constant reference; resolves to just
        that one constant's code, exactly as `Outcome.NO_VICTIM` resolves to `NO_VICTIM`
        via the existing qualified-constant rule. Already narrow; unaffected by #21.

    Returns `None` (not a recognized enum-`.code()` shape) for anything else, falling
    through to the existing rules unchanged.
    """
    m = QUALIFIED_CONST_CODE_RE.match(expr)
    if m:
        type_name, const_name = m.group(1), m.group(2)
        consts = type_index.coded_enums.get(type_name)
        if consts and const_name in consts:
            return {consts[const_name]}
        return None
    m = ACCESSOR_CHAIN_CODE_RE.match(expr)
    if m:
        receiver, accessor = m.group(1), m.group(2)
        recv_type = find_local_type(jf, call_pos, receiver)
        if recv_type is None:
            return None
        comps = type_index.record_components.get(recv_type)
        if not comps or accessor not in comps:
            return None
        enum_type = comps[accessor]
        consts = type_index.coded_enums.get(enum_type)
        if not consts:
            return None
        comp_idx = list(comps.keys()).index(accessor)
        codes, found_site = resolve_constructed_component_codes(
                files, recv_type, comp_idx, consts, type_index, depth)
        if found_site:
            return codes  # narrowed to what's actually constructed -- may legitimately be empty
        return set(consts.values())  # recv_type is never constructed anywhere -- nothing to narrow against
    return None


def resolve_two_args(files: dict[Path, JavaFile], jf: JavaFile, call_pos: int, a0: str, a1: str,
                      type_index: TypeIndex, depth: int = 0) -> tuple[set[tuple[str, str]], str | None]:
    """Resolve one call/construction site's two argument expressions into a CORRELATED set
    of (arg0, arg1) pairs -- applying every rule that keeps the two positions linked
    (record-accessor pairs via construction sites, same-wrapper-method params via
    callers) before ever falling back to an independent cross product. This is the single
    place both `resolve_call_pairs` (a `recordStealReason`/`stealReasonCounter` emission
    site) and `resolve_correlated_pair_via_sites` (one step of either correlation rule,
    which itself calls back into this for the site it just found) resolve a 2-arg site --
    so correlation composes across multiple hops (e.g. `new Engagement(category, reason)`
    inside `addEngagement`, whose `category`/`reason` are THAT method's params, correlated
    again via ITS callers) instead of only ever applying once.

    `depth` bounds the correlation recursion itself (construction site -> wrapper params
    -> wrapper's callers -> ...), separate from `MAX_RECURSION` (which bounds a single
    symbolic VALUE's own dataflow chase in `resolve_single_arg`) -- pathological mutual
    wrapping could otherwise recurse without end.
    """
    if depth > MAX_RECURSION:
        return set(), f"{jf.path}:{jf.line_of(call_pos)}: correlation recursion limit resolving `{a0}`, `{a1}`"

    def is_symbolic(e: str) -> bool:
        e = e.strip()
        if re.match(r'^"((?:[^"\\]|\\.)*)"$', e):
            return False
        if literals_in_expr(e):
            return False
        return True

    a0s, a1s = a0.strip(), a1.strip()
    if is_symbolic(a0s) and is_symbolic(a1s):
        rec_result = try_resolve_record_accessor_pair(files, jf, call_pos, a0s, a1s, type_index, depth)
        if rec_result is not None:
            return rec_result
        base0 = a0s
        m0 = NAME_DOT_NAME_RE.match(a0s)
        if m0:
            base0 = m0.group(1)
        base1 = a1s
        m1 = NAME_DOT_NAME_RE.match(a1s)
        if m1:
            base1 = m1.group(1)
        if BARE_IDENT_RE.match(base0) and BARE_IDENT_RE.match(base1):
            method = enclosing_method(jf, call_pos)
            if method is not None:
                mname, mparams, _, _, is_private = method
                params = param_names(mparams)
                if base0 in params and base1 in params:
                    scope = {jf.path: jf} if is_private else files
                    return resolve_correlated_pair(scope, mname, params.index(base0),
                                                    params.index(base1), type_index, depth + 1)

    r0 = resolve_single_arg(files, jf, call_pos, a0s, 0, type_index)
    r1 = resolve_single_arg(files, jf, call_pos, a1s, 0, type_index)
    warns = [w for w in (r0.warning, r1.warning) if w]
    warning = "; ".join(warns) if warns else None
    pairs = {(c, r) for c in (r0.literals or set()) for r in (r1.literals or set())}
    return pairs, warning


def resolve_correlated_pair_via_sites(files: dict[Path, JavaFile], call_re: re.Pattern,
                                       idx0: int, idx1: int, type_index: TypeIndex,
                                       label: str, depth: int) -> tuple[set[tuple[str, str]], str | None]:
    """Shared correlated-pair machinery: for every call site matching `call_re` (a wrapper
    method call, or a record's implicit constructor invocation), resolve the two argument
    expressions at `idx0`/`idx1` via `resolve_two_args` (TOGETHER, not as an independent
    cross product -- see the module docstring's point 5, and `resolve_two_args`'s own
    docstring for why this must recurse through it rather than call
    `resolve_single_arg` on each position independently) and union the resulting pairs
    across every site. Used both by `resolve_correlated_pair` (wrapper methods) and
    `try_resolve_record_accessor_pair` (record construction sites)."""
    pairs: set[tuple[str, str]] = set()
    warnings: list[str] = []
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
            site_pairs, warn = resolve_two_args(files, jf, cm.start(), args[idx0], args[idx1],
                                                 type_index, depth)
            if warn or not site_pairs:
                warnings.append(f"{jf.path}:{jf.line_of(cm.start())}: "
                                 f"could not resolve correlated pair for {label}(...)")
                continue
            pairs |= site_pairs
    return pairs, ("; ".join(warnings) if warnings else None)


def try_resolve_record_accessor_pair(
        files: dict[Path, JavaFile], jf: JavaFile, call_pos: int, a0: str, a1: str,
        type_index: TypeIndex, depth: int = 0) -> tuple[set[tuple[str, str]], str | None] | None:
    """If `a0`/`a1` are both no-arg accessor calls on the SAME local (``e.category()`` /
    ``e.reason()``) whose declared type (see `find_local_type`) is a record with
    components matching those accessor names, resolve the pair by finding every
    ``new RecordType(...)`` construction site in the tree and correlating its
    constructor args at the matching positions (e.g. `Engagement`'s construction sites
    are the ``addEngagement("X", "y")`` call sites, reached transitively through
    `Engagement`'s constructor params -- the same wrapper-method machinery already
    used for `Thief.record`, just entered via a constructor instead of a method).
    Returns `None` (not this shape) for anything else."""
    m0 = ACCESSOR_CALL_RE.match(a0)
    m1 = ACCESSOR_CALL_RE.match(a1)
    if not m0 or not m1:
        return None
    recv0, acc0 = m0.group(1), m0.group(2)
    recv1, acc1 = m1.group(1), m1.group(2)
    if recv0 != recv1:
        return None
    recv_type = find_local_type(jf, call_pos, recv0)
    if recv_type is None:
        return None
    comps = type_index.record_components.get(recv_type)
    if not comps or acc0 not in comps or acc1 not in comps:
        return None
    names = list(comps.keys())
    idx0, idx1 = names.index(acc0), names.index(acc1)
    call_re = re.compile(r"(?<![A-Za-z0-9_])new\s+" + re.escape(recv_type) + r"\s*\(")
    return resolve_correlated_pair_via_sites(files, call_re, idx0, idx1, type_index,
                                              label=f"new {recv_type}", depth=depth + 1)


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
                        expr: str, depth: int, type_index: TypeIndex) -> ResolveResult:
    expr = expr.strip()
    # Rule 1: a single simple string literal.
    m = re.match(r'^"((?:[^"\\]|\\.)*)"$', expr)
    if m:
        return ResolveResult(literals={m.group(1)})
    # Rule 1.5: a qualified constant reference, e.g. Outcome.NO_VICTIM.
    m = QUALIFIED_CONST_RE.match(expr)
    if m:
        return ResolveResult(literals={m.group(1)})
    # Rule 1.6: an enum-typed `.code()` expression -- either a qualified constant
    # (`NoVictimReason.NO_SPLITTABLE_VICTIM.code()`) or a record-component accessor chain
    # on a local of a recognized type (`noVictim.reason().code()`, `commit.mechanism()
    # .code()`). Resolved by tracing which constant(s) actually reach the component at its
    # construction sites, falling back to the whole enum only if none exist -- see
    # `resolve_enum_code_expr`.
    enum_lits = resolve_enum_code_expr(files, jf, call_pos, expr, type_index, depth)
    if enum_lits is not None:
        return ResolveResult(literals=enum_lits)
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
        return resolve_via_callers(scope, mname, params.index(base), depth, type_index)
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
    # Pattern C: `base` resolved as neither a param nor a local of the ENCLOSING METHOD --
    # it may be an INSTANCE FIELD assigned across several methods of the enclosing TYPE
    # instead (e.g. `ThiefPolicy.Attempt#mechanism`, set in one method and read at a `new
    # Commit(...)` site in another). Widen the same assignment scan to the smallest
    # enclosing type's whole body.
    type_body = enclosing_type_body(jf, call_pos)
    if type_body is not None:
        t_start, t_end = type_body
        for am in assign_re.finditer(jf.text[t_start:t_end]):
            literals.update(literals_and_consts_in_expr(am.group(1)))
        if literals:
            return ResolveResult(literals=literals)
    return ResolveResult(warning=f"{jf.path}:{jf.line_of(call_pos)}: "
                                  f"non-literal argument `{expr}` "
                                  f"(local/field `{base}` has no literal assignment found)")


def resolve_via_callers(files: dict[Path, JavaFile], method_name: str, param_idx: int,
                         depth: int, type_index: TypeIndex) -> ResolveResult:
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
            r = resolve_single_arg(files, jf, cm.start(), args[param_idx], depth + 1, type_index)
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
                        args: list[str], type_index: TypeIndex) -> EmissionSite:
    line = jf.line_of(call_pos)
    if len(args) != 2:
        return EmissionSite(jf.path, line, set(),
                             f"{jf.path}:{line}: expected 2 args, found {len(args)}: {args}")
    pairs, warning = resolve_two_args(files, jf, call_pos, args[0], args[1], type_index)
    return EmissionSite(jf.path, line, pairs, warning)


def resolve_correlated_pair(files: dict[Path, JavaFile], method_name: str,
                             idx0: int, idx1: int, type_index: TypeIndex,
                             depth: int = 0) -> tuple[set[tuple[str, str]], str | None]:
    call_re = re.compile(r"(?<![A-Za-z0-9_])" + re.escape(method_name) + r"\s*\(")
    return resolve_correlated_pair_via_sites(files, call_re, idx0, idx1, type_index,
                                              label=method_name, depth=depth)


def find_emission_sites(files: dict[Path, JavaFile], type_index: TypeIndex) -> list[EmissionSite]:
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
            sites.append(resolve_call_pairs(files, jf, cm.start(), args, type_index))
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
    type_index = build_type_index(files)
    sites = find_emission_sites(files, type_index)

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


def self_test_enum_code_accessor_pattern() -> int:
    """Regression for the policy-seam shape that broke this guard against the real
    thief-brain extraction: a closed enum with a no-arg ``code()`` accessor (mirroring
    ``PivotMechanism``/``RetryReason``/``NoVictimReason``/``UnsplittableReason``), reached
    two ways -- an `instanceof`-pattern-bound local's record-component accessor chain
    (``outcome.reason().code()``, mirroring ``noVictim.reason().code()``/``commit
    .mechanism().code()``) and a qualified constant (``Reason.ALPHA.code()``, mirroring
    ``NoVictimReason.NO_SPLITTABLE_VICTIM.code()``). Before the enum-enumeration rule,
    BOTH shapes fell through to a non-literal warning and the code's live `CAT.alpha`/
    `CAT.beta` pairs never resolved, ghosting their doc rows even though they were
    faithfully documented (rc == 1). Asserts rc == 0 with no warning."""
    print("self-test: constructing a scratch tree with an enum-.code() accessor chain...")
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        src = root / "swath-core" / "src" / "main" / "java" / "dev" / "swath" / "w"
        src.mkdir(parents=True)
        (src / "W.java").write_text(
            "package io.varve.swath.w;\n"
            "class W {\n"
            "    enum Reason {\n"
            "        ALPHA(\"alpha\"), BETA(\"beta\");\n"
            "        private final String code;\n"
            "        Reason(String code) { this.code = code; }\n"
            "        public String code() { return code; }\n"
            "    }\n"
            "    record Outcome(Reason reason) {}\n"
            "    void run(Metrics metrics, Object selection) {\n"
            "        if (selection instanceof Outcome outcome) {\n"
            "            metrics.recordStealReason(\"CAT\", outcome.reason().code());\n"
            "        }\n"
            "        metrics.recordStealReason(\"CAT\", Reason.ALPHA.code());\n"
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
            "| `CAT` | `alpha` | |\n"
            "| `CAT` | `beta` | |\n"
            f"{TABLE_END}\n",
            encoding="utf-8",
        )
        rc = run_check(root)
        if rc != 0:
            print(f"self-test FAILED: expected exit 0 (no drift), got {rc}", file=sys.stderr)
            return 1
        print("self-test: enum-.code() accessor chain and qualified-constant .code() "
              "both resolved by enumeration, no false ghosts. PASS")
        return 0


def self_test_record_accessor_pair_via_collection() -> int:
    """Regression for the OTHER policy-seam shape that broke this guard: a two-String-
    component record iterated in a collection loop (``for (Mark m : marks) {
    metrics.recordStealReason(m.category(), m.reason()); }``, mirroring `Thief
    .applyEngagements`'s loop over `List<Engagement>`), whose actual literal pairs live
    at the record's construction sites reached only THROUGH a wrapper method's own
    params (mirroring `ThiefPolicy.addEngagement`). Before this resolved, the pair fell
    through to a non-literal warning; naively resolving `category()`/`reason()`
    independently (rather than correlated through the construction site AND its
    wrapper) would instead silently cross-product unrelated categories and reasons
    together -- this asserts the exact correlated pairs, not just rc == 0, so that
    regression cannot hide behind a coincidentally-clean exit code."""
    print("self-test: constructing a scratch tree with a record-accessor pair over a collection...")
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        src = root / "swath-core" / "src" / "main" / "java" / "dev" / "swath" / "v"
        src.mkdir(parents=True)
        (src / "V.java").write_text(
            "package io.varve.swath.v;\n"
            "import java.util.List;\n"
            "class V {\n"
            "    record Mark(String category, String reason) {}\n"
            "    private final Metrics metrics;\n"
            "    V(Metrics metrics) { this.metrics = metrics; }\n"
            "    void emit(List<Mark> marks) {\n"
            "        for (Mark m : marks) {\n"
            "            metrics.recordStealReason(m.category(), m.reason());\n"
            "        }\n"
            "    }\n"
            "    private void addMark(List<Mark> out, String category, String reason) {\n"
            "        out.add(new Mark(category, reason));\n"
            "    }\n"
            "    void caller(List<Mark> out) {\n"
            "        addMark(out, \"CAT\", \"one\");\n"
            "        addMark(out, \"OTHER\", \"two\");\n"
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
            "| `CAT` | `one` | |\n"
            "| `OTHER` | `two` | |\n"
            f"{TABLE_END}\n",
            encoding="utf-8",
        )
        rc = run_check(root)
        if rc != 0:
            print(f"self-test FAILED: expected exit 0 (no drift), got {rc}", file=sys.stderr)
            return 1
        # Not just rc == 0: assert the resolved pairs are exactly the correlated ones,
        # so a future change that resolves category()/reason() independently (silently
        # cross-producting CAT.two/OTHER.one into existence) is still caught even though
        # both would happen to also be "documented" in a table listing all four.
        files = load_java_files(root / "swath-core" / "src" / "main" / "java")
        type_index = build_type_index(files)
        sites = find_emission_sites(files, type_index)
        pairs = {p for s in sites for p in s.pairs}
        expected = {("CAT", "one"), ("OTHER", "two")}
        if pairs != expected:
            print(f"self-test FAILED: expected exactly {expected}, resolved {pairs}", file=sys.stderr)
            return 1
        print("self-test: record-accessor pair over a collection resolved via construction "
              "sites, correlated (not cross-producted). PASS")
        return 0


def self_test_enum_code_never_constructed_constant_ghosts() -> int:
    """Issue #21, repro 1: a coded enum `Reason { ALPHA, BETA, DEAD }` where code only
    ever constructs the record with ALPHA/BETA -- DEAD is declared but never assigned to
    the component anywhere. Before narrowing, `receiver.accessor().code()` credited the
    WHOLE enum (including DEAD) to the category, so a doc row `CAT.dead_never_constructed`
    resolved as live instead of ghost -- the guard silently validating a row no code path
    emits, the dangerous direction (a ghost row fails loudly; this failed not at all).
    Asserts the exact resolved code pairs (never `dead_never_constructed`) AND that
    `run_check` reports the doc row as a genuine ghost."""
    print("self-test: constructing a scratch tree with a declared-but-never-constructed "
          "coded-enum constant...")
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        src = root / "swath-core" / "src" / "main" / "java" / "dev" / "swath" / "u"
        src.mkdir(parents=True)
        (src / "U.java").write_text(
            "package io.varve.swath.u;\n"
            "import java.util.List;\n"
            "class U {\n"
            "    enum Reason {\n"
            "        ALPHA(\"alpha\"), BETA(\"beta\"), DEAD(\"dead_never_constructed\");\n"
            "        private final String code;\n"
            "        Reason(String code) { this.code = code; }\n"
            "        public String code() { return code; }\n"
            "    }\n"
            "    record Outcome(Reason reason) {}\n"
            "    void run(Metrics metrics, Object selection) {\n"
            "        if (selection instanceof Outcome outcome) {\n"
            "            metrics.recordStealReason(\"CAT\", outcome.reason().code());\n"
            "        }\n"
            "    }\n"
            "    void build(List<Outcome> out) {\n"
            "        out.add(new Outcome(Reason.ALPHA));\n"
            "        out.add(new Outcome(Reason.BETA));\n"
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
            "| `CAT` | `alpha` | |\n"
            "| `CAT` | `beta` | |\n"
            "| `CAT` | `dead_never_constructed` | |\n"
            f"{TABLE_END}\n",
            encoding="utf-8",
        )
        java_root = root / "swath-core" / "src" / "main" / "java"
        files = load_java_files(java_root)
        type_index = build_type_index(files)
        sites = find_emission_sites(files, type_index)
        pairs = {p for s in sites for p in s.pairs}
        expected = {("CAT", "alpha"), ("CAT", "beta")}
        if pairs != expected:
            print(f"self-test FAILED: expected exactly {expected}, resolved {pairs}", file=sys.stderr)
            return 1
        rc = run_check(root)
        if rc != 1:
            print(f"self-test FAILED: expected exit 1 (the never-constructed row must ghost), "
                  f"got {rc}", file=sys.stderr)
            return 1
        print("self-test: declared-but-never-constructed constant correctly ghosts "
              "instead of silently resolving. PASS")
        return 0


def self_test_enum_code_cross_record_mismatch_ghosts() -> int:
    """Issue #21, repro 2: two DIFFERENT record types (`RecA`, `RecB`) share the same
    coded-enum component type -- `RecA` only ever constructed with ALPHA (feeding
    category CAT_A), `RecB` only ever constructed with BETA (feeding CAT_B). Before
    narrowing, both accessor chains credited the WHOLE enum to BOTH categories, so a
    wrong doc row `CAT_A.beta` was silently accepted (no ghost, no undocumented) while
    the unrelated `CAT_B.alpha` -- absent from the doc table on purpose here, to isolate
    the artifact -- showed as UNDOCUMENTED: a true-positive complaint, but about a pair
    the code never actually emits, an artifact of the same over-widening rather than a
    real gap. Asserts the exact resolved pairs (each category credited only its own
    record's actual constant) AND that `run_check` now ghosts exactly the wrong row
    (`CAT_A.beta`) with nothing left undocumented."""
    print("self-test: constructing a scratch tree with two record types sharing one coded enum...")
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        src = root / "swath-core" / "src" / "main" / "java" / "dev" / "swath" / "t"
        src.mkdir(parents=True)
        (src / "T.java").write_text(
            "package io.varve.swath.t;\n"
            "import java.util.List;\n"
            "class T {\n"
            "    enum Reason {\n"
            "        ALPHA(\"alpha\"), BETA(\"beta\");\n"
            "        private final String code;\n"
            "        Reason(String code) { this.code = code; }\n"
            "        public String code() { return code; }\n"
            "    }\n"
            "    record RecA(Reason reason) {}\n"
            "    record RecB(Reason reason) {}\n"
            "    void run(Metrics metrics, Object a, Object b) {\n"
            "        if (a instanceof RecA recA) {\n"
            "            metrics.recordStealReason(\"CAT_A\", recA.reason().code());\n"
            "        }\n"
            "        if (b instanceof RecB recB) {\n"
            "            metrics.recordStealReason(\"CAT_B\", recB.reason().code());\n"
            "        }\n"
            "    }\n"
            "    void build(List<RecA> as, List<RecB> bs) {\n"
            "        as.add(new RecA(Reason.ALPHA));\n"
            "        bs.add(new RecB(Reason.BETA));\n"
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
            "| `CAT_A` | `alpha` | |\n"
            "| `CAT_A` | `beta` | |\n"
            "| `CAT_B` | `beta` | |\n"
            f"{TABLE_END}\n",
            encoding="utf-8",
        )
        java_root = root / "swath-core" / "src" / "main" / "java"
        files = load_java_files(java_root)
        type_index = build_type_index(files)
        sites = find_emission_sites(files, type_index)
        pairs = {p for s in sites for p in s.pairs}
        expected = {("CAT_A", "alpha"), ("CAT_B", "beta")}
        if pairs != expected:
            print(f"self-test FAILED: expected exactly {expected}, resolved {pairs}", file=sys.stderr)
            return 1
        rc = run_check(root)
        if rc != 1:
            print(f"self-test FAILED: expected exit 1 (CAT_A.beta must ghost), got {rc}",
                  file=sys.stderr)
            return 1
        print("self-test: cross-record coded-enum sharing no longer lets a wrong "
              "category/reason pairing slide through. PASS")
        return 0


def self_test() -> int:
    results = [
        self_test_ghost_and_undocumented(),
        self_test_hoisted_enum_local_pattern(),
        self_test_family_concat_documented(),
        self_test_family_concat_undocumented(),
        self_test_family_row_ghost(),
        self_test_enum_code_accessor_pattern(),
        self_test_record_accessor_pair_via_collection(),
        self_test_enum_code_never_constructed_constant_ghosts(),
        self_test_enum_code_cross_record_mismatch_ghosts(),
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
