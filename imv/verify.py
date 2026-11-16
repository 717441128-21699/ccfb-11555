"""一致性校验:每个视图用全量 SQL 从基表重算,与增量维护的物化结果逐行对比。

校验 SQL 由视图定义自动生成(与引擎的增量逻辑是两条独立代码路径),
因此两边结果一致才能说明增量维护是正确的。
"""
from __future__ import annotations

import time
from collections import Counter
from dataclasses import dataclass, field


@dataclass
class ViewCheck:
    name: str
    ok: bool
    rows_expected: int
    rows_actual: int
    elapsed: float
    mismatches: list = field(default_factory=list)   # [('missing'|'extra', row, n)]


def _norm(rows):
    return [tuple(round(x, 6) if isinstance(x, float) else x for x in r) for r in rows]


def verify_all(engine, conn=None) -> list[ViewCheck]:
    """对全部派生视图做全量重算对比。调用前应保证源库与引擎都处于静止点
    (发生器暂停且引擎已排空积压),或传入独立连接在读事务中取快照。"""
    own = conn is None
    conn = conn or engine.conn
    if own:
        conn.execute('BEGIN')
    try:
        ctx = {}
        results = []
        for name in engine.topo:
            dfn = engine.defs[name]
            node = engine.nodes[name]
            sql = dfn.to_sql(ctx)
            ctx[name] = (sql, node.schema)
            if dfn.kind == 'source':
                continue
            t0 = time.perf_counter()
            expected = _norm(conn.execute(sql).fetchall())
            cols = ', '.join(f'"{c}"' for c in node.schema)
            actual = []
            for r in conn.execute(f'SELECT {cols}, __cnt__ FROM "vw_{name}"'):
                actual.extend([tuple(r[:-1])] * r[-1])
            actual = _norm(actual)
            ce, ca = Counter(expected), Counter(actual)
            ok = ce == ca
            mism = []
            if not ok:
                for row, n in (ce - ca).most_common(3):
                    mism.append(('missing', row, n))
                for row, n in (ca - ce).most_common(3):
                    mism.append(('extra', row, n))
            results.append(ViewCheck(name, ok, len(expected), len(actual),
                                     time.perf_counter() - t0, mism))
        return results
    finally:
        if own:
            conn.execute('COMMIT')


def format_results(results) -> str:
    lines = []
    for r in results:
        mark = '✓ 一致' if r.ok else '✗ 不一致'
        lines.append(f'  {mark}  {r.name:<16} 行数 {r.rows_actual}/{r.rows_expected} '
                     f'(全量重算 {r.elapsed:.2f}s)')
        for kind, row, n in r.mismatches:
            lines.append(f'      {kind} x{n}: {row}')
    return '\n'.join(lines)
