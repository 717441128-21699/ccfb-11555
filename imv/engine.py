"""增量物化视图引擎。

数据流模型:每个算子节点接收"行增量" (row, sign),sign=+1 表示插入、
-1 表示删除(UPDATE = 删旧行 + 插新行),处理后向下游发射自己的行增量。
带符号计数(多重集语义)使各算子可以任意组合、层层传播。

崩溃恢复:所有物化视图(vw_*)与 checkpoint 在同一个 SQLite 事务中落盘。
重启后把算子内部状态精确重建到 checkpoint 时刻:
- 直接消费源表的节点(连接/聚合):重放 change_log 的已确认前缀
  [1..checkpoint] 重建 —— 源表当前内容反映的是 max(seq) 时刻,直接用会
  与"从 checkpoint 续跑"重复计算,必须重放已确认前缀;
- 下游节点:从父视图的物化表重建(vw_* 与 checkpoint 原子一致)。
重建只恢复内存状态、不写 vw 表;随后从 checkpoint+1 继续增量应用,
不重复、不遗漏。
"""
from __future__ import annotations

import json
import time
from collections import Counter, deque

from . import db as dbmod
from .views import Agg, Filter, Join, Map, compile_row_expr


# ---------------------------------------------------------------- 算子节点

class SourceNode:
    kind = 'source'

    def __init__(self, name, schema):
        self.name = name
        self.schema = schema

    def reset(self):
        pass


class FilterNode:
    kind = 'filter'

    def __init__(self, name, dfn: Filter, pschema, env):
        self.name = name
        self.schema = list(pschema)
        self.pred = compile_row_expr(dfn.pred_py, pschema, env)

    def reset(self):
        pass

    def process(self, deltas):
        pred = self.pred
        return [(r, s) for r, s in deltas if pred(r)]


class MapNode:
    kind = 'map'

    def __init__(self, name, dfn: Map, pschema, env):
        self.name = name
        self.schema = list(pschema) + [dfn.new_col]
        self.fn = compile_row_expr(dfn.expr_py, pschema, env)

    def reset(self):
        pass

    def process(self, deltas):
        fn = self.fn
        return [(r + (fn(r),), s) for r, s in deltas]


class JoinNode:
    """两表等值连接。两侧各维护 连接键 -> {行: 重数} 的索引。

    同一批两侧都有增量时,按 Δout = ΔL⋈R_new + L_old⋈ΔR 计算,
    交叉项 ΔL⋈ΔR 恰好被包含一次,不会重复也不会遗漏。
    """

    kind = 'join'

    def __init__(self, name, dfn: Join, lschema, rschema):
        self.name = name
        self.lk = lschema.index(dfn.left_on)
        self.rk = rschema.index(dfn.right_on)
        self.schema = list(dfn.cols)
        self.getters = []
        for c in dfn.cols:
            in_l, in_r = c in lschema, c in rschema
            if in_l and (not in_r or c == dfn.left_on):
                self.getters.append((0, lschema.index(c)))
            elif in_r:
                self.getters.append((1, rschema.index(c)))
            else:
                raise ValueError(f'{name}: 输出列 {c} 找不到来源')
        self.reset()

    def reset(self):
        self.lidx = {}   # jkey -> {left_row: cnt}
        self.ridx = {}   # jkey -> {right_row: cnt}

    @staticmethod
    def _add(idx, k, row, s):
        d = idx.get(k)
        if d is None:
            if s > 0:
                idx[k] = {row: s}
            return
        nv = d.get(row, 0) + s
        if nv:
            d[row] = nv
        else:
            del d[row]
            if not d:
                del idx[k]

    def _join(self, l, r):
        return tuple(l[i] if side == 0 else r[i] for side, i in self.getters)

    def process(self, ld, rd):
        out = []
        lidx, ridx = self.lidx, self.ridx
        # 1) 先把右侧增量并入右索引(右索引变为 R_new)
        for row, s in rd:
            self._add(ridx, row[self.rk], row, s)
        # 2) ΔL ⋈ R_new
        for row, s in ld:
            d = ridx.get(row[self.lk])
            if d:
                for rr, c in d.items():
                    out.append((self._join(row, rr), s * c))
        # 3) L_old ⋈ ΔR(左索引尚未更新)
        for row, s in rd:
            d = lidx.get(row[self.rk])
            if d:
                for lr, c in d.items():
                    out.append((self._join(lr, row), s * c))
        # 4) 左侧增量并入左索引
        for row, s in ld:
            self._add(lidx, row[self.lk], row, s)
        return out


class _GS:
    """一个分组的聚合状态。"""
    __slots__ = ('cnt', 'sums', 'mins', 'maxs', 'ms', 'dm', 'dcnt')

    def __init__(self, n_sum, n_min, n_max, n_cd):
        self.cnt = 0
        self.sums = [0] * n_sum
        self.mins = [None] * n_min
        self.maxs = [None] * n_max
        self.ms = [{} for _ in range(n_min + n_max)]  # MIN/MAX 的值多重集
        self.dm = [{} for _ in range(n_cd)]           # DISTINCT 的值->计数
        self.dcnt = [0] * n_cd


class AggNode:
    """分组聚合。

    - SUM/COUNT/AVG:累加器,AVG 由 SUM/COUNT 现算
    - MIN/MAX:每个分组维护 值->重数 的多重集;当前极值被删光时从多重集
      中取下一个极值,正确回退(需求 2)
    - COUNT DISTINCT:每个分组维护 值->重数,重数 0<->1 跳变时增减
      distinct 计数(需求 4)
    """

    kind = 'agg'

    def __init__(self, name, dfn: Agg, pschema):
        self.name = name
        self.gidx = [pschema.index(c) for c in dfn.group_by]
        self.schema = list(dfn.group_by) + [a.out for a in dfn.aggs]
        self.sum_slots = []   # (slot, col_idx)
        self.min_slots = []
        self.max_slots = []
        self.cd_slots = []
        self.out_plan = []    # (kind, slot)
        ns = nmi = nma = nc = 0
        for a in dfn.aggs:
            f = a.func
            if f == 'COUNT':
                self.out_plan.append(('count', None))
            elif f == 'SUM':
                self.sum_slots.append((ns, pschema.index(a.col)))
                self.out_plan.append(('sum', ns)); ns += 1
            elif f == 'AVG':
                self.sum_slots.append((ns, pschema.index(a.col)))
                self.out_plan.append(('avg', ns)); ns += 1
            elif f == 'MIN':
                self.min_slots.append((nmi, pschema.index(a.col)))
                self.out_plan.append(('min', nmi)); nmi += 1
            elif f == 'MAX':
                self.max_slots.append((nma, pschema.index(a.col)))
                self.out_plan.append(('max', nma)); nma += 1
            elif f == 'COUNT_DISTINCT':
                self.cd_slots.append((nc, pschema.index(a.col)))
                self.out_plan.append(('cd', nc)); nc += 1
            else:
                raise ValueError(f'不支持的聚合 {f}')
        self.n_sum, self.n_min, self.n_max, self.n_cd = ns, nmi, nma, nc
        self.reset()

    def reset(self):
        self.state = {}       # gkey(tuple) -> _GS

    def _new_gs(self):
        return _GS(self.n_sum, self.n_min, self.n_max, self.n_cd)

    def _emit(self, gk, st):
        out = list(gk)
        for kind, slot in self.out_plan:
            if kind == 'count':
                out.append(st.cnt)
            elif kind == 'sum':
                out.append(st.sums[slot])
            elif kind == 'avg':
                out.append(st.sums[slot] / st.cnt)
            elif kind == 'min':
                out.append(st.mins[slot])
            elif kind == 'max':
                out.append(st.maxs[slot])
            else:
                out.append(st.dcnt[slot])
        return tuple(out)

    def process(self, deltas):
        gidx = self.gidx
        touched = {}
        for row, s in deltas:
            gk = tuple(row[i] for i in gidx)
            lst = touched.get(gk)
            if lst is None:
                touched[gk] = [(row, s)]
            else:
                lst.append((row, s))
        out = []
        state = self.state
        for gk, rows in touched.items():
            st = state.get(gk)
            old = self._emit(gk, st) if st is not None else None
            if st is None:
                st = self._new_gs()
                state[gk] = st
            for row, s in rows:
                st.cnt += s
                for slot, ci in self.sum_slots:
                    st.sums[slot] += row[ci] * s
                for slot, ci in self.min_slots:
                    v = row[ci]
                    ms = st.ms[slot]
                    nv = ms.get(v, 0) + s
                    if nv:
                        ms[v] = nv
                    else:
                        del ms[v]
                    cur = st.mins[slot]
                    if s > 0 and (cur is None or v < cur):
                        st.mins[slot] = v
                for slot, ci in self.max_slots:
                    v = row[ci]
                    ms = st.ms[self.n_min + slot]
                    nv = ms.get(v, 0) + s
                    if nv:
                        ms[v] = nv
                    else:
                        del ms[v]
                    cur = st.maxs[slot]
                    if s > 0 and (cur is None or v > cur):
                        st.maxs[slot] = v
                for slot, ci in self.cd_slots:
                    v = row[ci]
                    dm = st.dm[slot]
                    ov = dm.get(v, 0)
                    nv = ov + s
                    if nv == 0:
                        del dm[v]
                        st.dcnt[slot] -= 1
                    else:
                        dm[v] = nv
                        if ov == 0:
                            st.dcnt[slot] += 1
            # 极值回退:当前 min/max 的重数归零时,从多重集重算(需求 2)
            for slot, _ in self.min_slots:
                cur = st.mins[slot]
                if cur is not None and cur not in st.ms[slot]:
                    ms = st.ms[slot]
                    st.mins[slot] = min(ms) if ms else None
            for slot, _ in self.max_slots:
                cur = st.maxs[slot]
                if cur is not None and cur not in st.ms[self.n_min + slot]:
                    ms = st.ms[self.n_min + slot]
                    st.maxs[slot] = max(ms) if ms else None
            if st.cnt <= 0:
                del state[gk]
                new = None
            else:
                new = self._emit(gk, st)
            if old is not None and old != new:
                out.append((old, -1))
            if new is not None and new != old:
                out.append((new, +1))
        return out


# ---------------------------------------------------------------- 引擎

class Engine:
    def __init__(self, db_path, defs, env=None):
        self.db_path = db_path
        self.defs = {d.name: d for d in defs}
        self.topo = self._topo_sort(defs)
        self.conn = dbmod.connect(db_path)
        self.env = env or {}
        self.nodes = {}
        self.children = {d.name: [] for d in defs}
        for d in defs:
            for p in d.parents():
                self.children[p].append(d.name)
        self._build_nodes()
        self._create_vw_tables()
        row = self.conn.execute("SELECT v FROM meta WHERE k='checkpoint'").fetchone()
        self.checkpoint = int(row[0]) if row else 0
        self.applied_total = 0
        self.stream = deque(maxlen=400)   # 最近应用的变更(控制台"变更流")
        self.perf = deque(maxlen=1200)    # (时间, 变更数, 耗时秒)
        self.crash_hook = None            # 测试用:在写事务里模拟崩溃

    # ---------- 构建 ----------

    @staticmethod
    def _topo_sort(defs):
        by_name = {d.name: d for d in defs}
        order, seen = [], set()

        def visit(n):
            if n in seen:
                return
            for p in by_name[n].parents():
                visit(p)
            seen.add(n)
            order.append(n)

        for d in defs:
            visit(d.name)
        return order

    def _build_nodes(self):
        for name in self.topo:
            d = self.defs[name]
            if d.kind == 'source':
                self.nodes[name] = SourceNode(name, list(d.cols))
            elif d.kind == 'join':
                self.nodes[name] = JoinNode(
                    name, d, self.nodes[d.left].schema, self.nodes[d.right].schema)
            elif d.kind == 'filter':
                self.nodes[name] = FilterNode(name, d, self.nodes[d.src].schema, self.env)
            elif d.kind == 'map':
                self.nodes[name] = MapNode(name, d, self.nodes[d.src].schema, self.env)
            elif d.kind == 'agg':
                self.nodes[name] = AggNode(name, d, self.nodes[d.src].schema)
            else:
                raise ValueError(d.kind)

    def _create_vw_tables(self):
        cur = self.conn.cursor()
        for name in self.topo:
            if self.defs[name].kind == 'source':
                continue
            cols = self.nodes[name].schema
            col_defs = ', '.join(f'"{c}"' for c in cols)
            pk = ', '.join(f'"{c}"' for c in cols)
            cur.execute(
                f'CREATE TABLE IF NOT EXISTS "vw_{name}" '
                f'({col_defs}, __cnt__ INTEGER NOT NULL, PRIMARY KEY ({pk}))')
        self.conn.execute('COMMIT') if self.conn.in_transaction else None

    # ---------- 崩溃恢复(需求 7) ----------

    # 有内部状态、需要重建的节点类型(filter/map 无状态,跳过)
    _STATEFUL = ('join', 'agg')

    def recover(self):
        """重启后:把全部算子状态精确重建到 checkpoint 时刻。

        1) 直接消费源表的节点:重放 change_log[1..checkpoint] 的源表增量
           (不能用源表当前内容 —— 那是 max(seq) 时刻,会重复计算);
        2) 其余节点:从父视图的物化表 vw_* 重建(与 checkpoint 原子一致)。
        """
        row = self.conn.execute("SELECT v FROM meta WHERE k='checkpoint'").fetchone()
        self.checkpoint = ckpt = int(row[0]) if row else 0
        for name in self.topo:
            self.nodes[name].reset()

        # 第一轮:重放已确认前缀,喂给直接挂在源表上的有状态节点
        src_consumers = []   # (tbl, node, side)  side: 'l' / 'r' / '-'
        for name in self.topo:
            d = self.defs[name]
            node = self.nodes[name]
            if node.kind not in self._STATEFUL:
                continue
            for p in d.parents():
                if self.defs[p].kind == 'source':
                    side = '-'
                    if node.kind == 'join':
                        side = 'l' if d.left == p else 'r'
                    src_consumers.append((p, node, side))
        if src_consumers and ckpt > 0:
            lo = 0
            while lo < ckpt:
                rows = self.conn.execute(
                    'SELECT seq, tbl, op, old_row, new_row FROM change_log '
                    'WHERE seq > ? AND seq <= ? ORDER BY seq LIMIT ?',
                    (lo, ckpt, 100000)).fetchall()
                if not rows:
                    break
                by_tbl = {}
                for _, tbl, op, old_j, new_j in rows:
                    ds = by_tbl.setdefault(tbl, [])
                    if op != 'INSERT':
                        ds.append((self._row_of(tbl, json.loads(old_j)), -1))
                    if op != 'DELETE':
                        ds.append((self._row_of(tbl, json.loads(new_j)), +1))
                for tbl, node, side in src_consumers:
                    ds = by_tbl.get(tbl)
                    if not ds:
                        continue
                    if node.kind == 'join':
                        node.process(ds, []) if side == 'l' else node.process([], ds)
                    else:
                        node.process(ds)
                lo = rows[-1][0]

        # 第二轮:派生父视图 -> 从 vw_* 物化表重建(内容即 checkpoint 时刻)
        for name in self.topo:
            d = self.defs[name]
            node = self.nodes[name]
            if node.kind not in self._STATEFUL:
                continue
            for p in d.parents():
                if self.defs[p].kind == 'source':
                    continue   # 第一轮已处理
                for chunk in self._current_deltas(p):
                    if node.kind == 'join':
                        node.process(chunk, []) if d.left == p else node.process([], chunk)
                    else:
                        node.process(chunk)

    def _current_deltas(self, name, chunk_size=100000):
        """父视图当前内容,以 (row, 重数) 增量形式产出。"""
        node = self.nodes[name]
        cols = ', '.join(f'"{c}"' for c in node.schema)
        if self.defs[name].kind == 'source':
            sql = f'SELECT {cols} FROM "{name}"'
            cur = self.conn.execute(sql)
            while True:
                rows = cur.fetchmany(chunk_size)
                if not rows:
                    break
                yield [(tuple(r), 1) for r in rows]
        else:
            sql = f'SELECT {cols}, __cnt__ FROM "vw_{name}"'
            cur = self.conn.execute(sql)
            while True:
                rows = cur.fetchmany(chunk_size)
                if not rows:
                    break
                yield [(tuple(r[:-1]), r[-1]) for r in rows]

    # ---------- 增量应用 ----------

    def max_seq(self):
        return self.conn.execute('SELECT COALESCE(MAX(seq),0) FROM change_log').fetchone()[0]

    def lag(self):
        return self.max_seq() - self.checkpoint

    def _row_of(self, tbl, d):
        cols = dbmod.SOURCE_TABLES[tbl]
        return tuple(d[c] for c in cols)

    def apply_pending(self, batch=20000):
        """从 change_log 读一批完整事务,传播增量,并在一个 SQLite 事务里
        落盘全部视图更新 + checkpoint(需求 1、6、7)。返回应用的变更条数。"""
        rows = self.conn.execute(
            'SELECT seq, tx_id, tbl, op, old_row, new_row FROM change_log '
            'WHERE seq > ? ORDER BY seq LIMIT ?', (self.checkpoint, batch)).fetchall()
        if not rows:
            return 0
        if len(rows) == batch:
            # 裁剪到完整事务边界:末尾可能截断了一个事务
            last_tx = rows[-1][1]
            while rows and rows[-1][1] == last_tx:
                rows.pop()
            if not rows:
                # 单个事务超过 batch 大小:完整取出该事务
                rows = self.conn.execute(
                    'SELECT seq, tx_id, tbl, op, old_row, new_row FROM change_log '
                    'WHERE tx_id = ? ORDER BY seq', (last_tx,)).fetchall()
        t0 = time.perf_counter()
        n_changes = 0
        all_outputs = {}
        i = 0
        n = len(rows)
        while i < n:
            tx_id = rows[i][1]
            src_deltas = {}
            j = i
            while j < n and rows[j][1] == tx_id:      # 同一事务的变更一起应用
                seq, _, tbl, op, old_j, new_j = rows[j]
                old = json.loads(old_j) if old_j else None
                new = json.loads(new_j) if new_j else None
                d = src_deltas.setdefault(tbl, [])
                if op != 'INSERT':
                    d.append((self._row_of(tbl, old), -1))
                if op != 'DELETE':
                    d.append((self._row_of(tbl, new), +1))
                self.stream.appendleft(self._stream_line(seq, tx_id, tbl, op, old, new))
                j += 1
            outputs = self._propagate(src_deltas)
            for name, ds in outputs.items():
                if ds:
                    all_outputs.setdefault(name, []).extend(ds)
            n_changes += j - i
            i = j
        new_ckpt = rows[-1][0]
        conn = self.conn
        conn.execute('BEGIN IMMEDIATE')
        try:
            if self.crash_hook:
                self.crash_hook('before_flush')
            self._flush(all_outputs)
            conn.execute(
                "INSERT INTO meta(k, v) VALUES('checkpoint', ?) "
                "ON CONFLICT(k) DO UPDATE SET v = excluded.v", (str(new_ckpt),))
            if self.crash_hook:
                self.crash_hook('before_commit')
            conn.execute('COMMIT')
        except BaseException:
            conn.execute('ROLLBACK')
            raise
        self.checkpoint = new_ckpt
        self.applied_total += n_changes
        self.perf.append((time.time(), n_changes, time.perf_counter() - t0))
        return n_changes

    def _propagate(self, src_deltas):
        pending = {}        # 单输入节点 -> deltas
        join_pending = {}   # join 节点 -> {'l': [...], 'r': [...]}
        outputs = {}

        def deliver(name, deltas):
            for child in self.children[name]:
                cd = self.defs[child]
                if cd.kind == 'join':
                    jp = join_pending.setdefault(child, {'l': [], 'r': []})
                    jp['l' if cd.left == name else 'r'].extend(deltas)
                else:
                    pending.setdefault(child, []).extend(deltas)

        for name, ds in src_deltas.items():
            deliver(name, ds)
        for name in self.topo:
            node = self.nodes[name]
            if node.kind == 'source':
                continue
            if node.kind == 'join':
                p = join_pending.get(name)
                if not p or (not p['l'] and not p['r']):
                    continue
                out = node.process(p['l'], p['r'])
            else:
                d = pending.get(name)
                if not d:
                    continue
                out = node.process(d)
            if out:
                outputs[name] = out
                deliver(name, out)
        return outputs

    def _flush(self, outputs):
        """把各视图的输出增量合并后写入 vw_* 物化表。"""
        cur = self.conn.cursor()
        for name, deltas in outputs.items():
            net = Counter()
            for row, s in deltas:
                net[row] += s
            touched = [row for row, n in net.items() if n]
            if not touched:
                continue
            node = self.nodes[name]
            tbl = f'"vw_{name}"'
            ncol = len(node.schema)
            ph = ','.join('?' * (ncol + 1))
            upsert = (f'INSERT INTO {tbl} VALUES({ph}) ON CONFLICT DO UPDATE '
                      f'SET __cnt__ = __cnt__ + excluded.__cnt__')
            key_eq = ' AND '.join(f'"{c}"=?' for c in node.schema)
            delete = f'DELETE FROM {tbl} WHERE {key_eq} AND __cnt__ <= 0'
            cur.executemany(upsert, [(*r, net[r]) for r in touched])
            cur.executemany(delete, touched)

    # ---------- 控制台展示 ----------

    @staticmethod
    def _stream_line(seq, tx_id, tbl, op, old, new):
        if tbl == 'orders':
            if op == 'INSERT':
                return (f'#{seq} tx{tx_id} +订单 {new["order_id"]} '
                        f'买家{new["user_id"]} ¥{new["amount"]/100:.2f} {new["status"]}')
            if op == 'DELETE':
                return f'#{seq} tx{tx_id} -订单 {old["order_id"]}'
            return (f'#{seq} tx{tx_id} ~订单 {new["order_id"]} '
                    f'¥{old["amount"]/100:.2f}->¥{new["amount"]/100:.2f} '
                    f'{old["status"]}->{new["status"]}')
        if op == 'INSERT':
            return f'#{seq} tx{tx_id} +用户 {new["user_id"]} {new["city"]}'
        if op == 'DELETE':
            return f'#{seq} tx{tx_id} -用户 {old["user_id"]} {old["city"]}'
        return f'#{seq} tx{tx_id} ~用户 {new["user_id"]} {old["city"]}->{new["city"]}'

    def run_loop(self, stop, pause=None, busy=None, batch=5000, idle_sleep=0.05):
        """持续应用变更(控制台模式的后台线程)。"""
        while not stop.is_set():
            if pause is not None and pause.is_set():
                time.sleep(0.05)
                continue
            if busy is not None:
                busy.set()
            try:
                n = self.apply_pending(batch)
            finally:
                if busy is not None:
                    busy.clear()
            if n == 0:
                time.sleep(idle_sleep)
