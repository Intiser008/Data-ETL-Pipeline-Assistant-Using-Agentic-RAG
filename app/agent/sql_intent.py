from __future__ import annotations

from dataclasses import dataclass
from typing import Tuple
import re

from sqlglot import parse_one, exp

_AGG_NAMES = {"count", "sum", "avg", "min", "max"}
_LITERAL_RE = re.compile(r"(?P<num>\b\d+(\.\d+)?\b)|(?P<str>'[^']*'|\"[^\"]*\")", re.I)


@dataclass(frozen=True)
class SQLIntentFeatures:
    tables: Tuple[str, ...]
    projections: Tuple[str, ...]
    aggregates: Tuple[str, ...]
    group_by: Tuple[str, ...]
    predicate_fields: Tuple[str, ...]
    join_edges: Tuple[Tuple[str, str], ...]
    windows: Tuple[str, ...]
    distinct: bool
    order_by: Tuple[str, ...]
    shape: str  # normalized SQL skeleton (literals & limits removed)


def _norm_identifier(name: str | None) -> str:
    return (name or "").strip().strip('"').lower()


def _remove_literals_and_limit(sql: str) -> str:
    s = _LITERAL_RE.sub("?", sql)
    s = re.sub(r"\blimit\s+\d+\b", "limit ?", s, flags=re.I)
    return " ".join(s.lower().split())


def _collect_tables(tree: exp.Expression) -> Tuple[str, ...]:
    return tuple(sorted({_norm_identifier(t.name) for t in tree.find_all(exp.Table) if getattr(t, "name", None)}))


def _collect_proj(tree: exp.Expression) -> Tuple[str, ...]:
    out: set[str] = set()
    sel = tree.find(exp.Select)
    if not sel:
        return tuple()
    for item in sel.expressions or []:
        if isinstance(item, exp.Alias):
            base = item.this
        else:
            base = item
        if isinstance(base, exp.Column):
            out.add(_norm_identifier(base.name))
        elif isinstance(base, exp.Func):
            out.add(_norm_identifier(base.name))
        else:
            # fallback textual signature
            out.add(_norm_identifier(getattr(base, "name", None)) or base.sql(dialect="postgres")[:64].lower())
    return tuple(sorted(out))


def _collect_aggs(tree: exp.Expression) -> Tuple[str, ...]:
    names: set[str] = set()
    for fn in tree.find_all(exp.Func):
        nm = _norm_identifier(fn.name)
        if nm in _AGG_NAMES:
            names.add(nm)
    return tuple(sorted(names))


def _collect_group_by(tree: exp.Expression) -> Tuple[str, ...]:
    grp = tree.find(exp.Group)
    if not grp:
        return tuple()
    cols: set[str] = set()
    for e in grp.expressions or []:
        if isinstance(e, exp.Column):
            cols.add(_norm_identifier(e.name))
        else:
            cols.add(e.sql(dialect="postgres").lower())
    return tuple(sorted(cols))


def _collect_predicate_fields(tree: exp.Expression) -> Tuple[str, ...]:
    where = tree.find(exp.Where)
    if not where:
        return tuple()
    cols: set[str] = set()
    for col in where.find_all(exp.Column):
        if col.name:
            cols.add(_norm_identifier(col.name))
    return tuple(sorted(cols))


def _collect_joins(tree: exp.Expression) -> Tuple[Tuple[str, str], ...]:
    edges: set[tuple[str, str]] = set()
    for j in tree.find_all(exp.Join):
        tables = [t.name for t in (j.find_all(exp.Table) or []) if getattr(t, "name", None)]
        if len(tables) >= 2:
            a, b = sorted({_norm_identifier(t) for t in tables})[:2]
            edges.add((a, b))
    return tuple(sorted(edges))


def _collect_windows(tree: exp.Expression) -> Tuple[str, ...]:
    names: set[str] = set()
    for w in tree.find_all(exp.Window):
        names.add(w.sql(dialect="postgres").lower())
    return tuple(sorted(names))


def _collect_distinct(tree: exp.Expression) -> bool:
    sel = tree.find(exp.Select)
    return bool(sel and sel.args.get("distinct"))


def _collect_order(tree: exp.Expression) -> Tuple[str, ...]:
    order = tree.find(exp.Order)
    if not order:
        return tuple()
    cols: set[str] = set()
    for e in order.expressions or []:
        if isinstance(e, exp.Ordered) and isinstance(e.this, exp.Column):
            cols.add(_norm_identifier(e.this.name))
        else:
            cols.add(e.sql(dialect="postgres").lower())
    return tuple(sorted(cols))


def extract_intent_features(sql: str) -> SQLIntentFeatures:
    tree = parse_one(sql, read="postgres")
    return SQLIntentFeatures(
        tables=_collect_tables(tree),
        projections=_collect_proj(tree),
        aggregates=_collect_aggs(tree),
        group_by=_collect_group_by(tree),
        predicate_fields=_collect_predicate_fields(tree),
        join_edges=_collect_joins(tree),
        windows=_collect_windows(tree),
        distinct=_collect_distinct(tree),
        order_by=_collect_order(tree),
        shape=_remove_literals_and_limit(sql),
    )


def same_intent(a_sql: str, b_sql: str) -> bool:
    try:
        a = extract_intent_features(a_sql)
        b = extract_intent_features(b_sql)
    except Exception:
        return False
    hard_equal = (
        a.tables == b.tables
        and a.join_edges == b.join_edges
        and a.aggregates == b.aggregates
        and a.group_by == b.group_by
        and a.projections == b.projections
        and a.predicate_fields == b.predicate_fields
        and a.distinct == b.distinct
        and a.windows == b.windows
    )
    shape_similar = a.shape == b.shape
    return hard_equal or shape_similar


