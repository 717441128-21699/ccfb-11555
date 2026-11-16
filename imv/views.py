"""视图定义层:声明式描述视图 DAG,并能生成用于一致性校验的全量 SQL。

支持的算子:
- Source  源表
- Join    两表等值连接(单连接键)
- Filter  行过滤
- Map     追加派生列
- Agg     分组聚合(COUNT / SUM / AVG / MIN / MAX / COUNT_DISTINCT)
"""
from __future__ import annotations

import re
from dataclasses import dataclass


def compile_row_expr(expr: str, cols: list[str], extra_env: dict | None = None):
    """把 'amount>100 and status==\'paid\'' 编译成 f(row_tuple) -> value。

    列名被替换为 row[i],避免每行构造 dict(百万级行数下快几个数量级)。
    """
    code = expr
    for i, c in sorted(enumerate(cols), key=lambda t: -len(t[1])):
        code = re.sub(rf'\b{re.escape(c)}\b', f'__r[{i}]', code)
    env = {'__builtins__': {}}
    if extra_env:
        env.update(extra_env)
    return eval(f'lambda __r: {code}', env)


@dataclass
class AggSpec:
    func: str          # COUNT / SUM / AVG / MIN / MAX / COUNT_DISTINCT
    col: str | None    # COUNT(*) 时为 None
    out: str           # 输出列名

    def sql(self) -> str:
        if self.func == 'COUNT':
            return f'COUNT(*) AS "{self.out}"'
        if self.func == 'COUNT_DISTINCT':
            return f'COUNT(DISTINCT {self.col}) AS "{self.out}"'
        return f'{self.func}({self.col}) AS "{self.out}"'


class ViewDef:
    name: str
    kind: str = '?'

    def parents(self) -> list[str]:
        raise NotImplementedError

    def to_sql(self, ctx: dict) -> str:
        """生成该视图的全量重算 SQL。ctx: name -> (sql, out_cols)。"""
        raise NotImplementedError

    def describe(self) -> str:
        raise NotImplementedError


@dataclass
class Source(ViewDef):
    name: str
    cols: list[str]
    kind = 'source'

    def parents(self):
        return []

    def to_sql(self, ctx):
        return f"SELECT {', '.join(self.cols)} FROM {self.name}"

    def describe(self):
        return f'源表 {self.name}{tuple(self.cols)}'


@dataclass
class Join(ViewDef):
    name: str
    left: str
    right: str
    left_on: str
    right_on: str
    cols: list[str]          # 输出列(来自两侧父视图,重名时取连接键/左侧)
    kind = 'join'

    def parents(self):
        return [self.left, self.right]

    def to_sql(self, ctx):
        lsql, lcols = ctx[self.left]
        rsql, rcols = ctx[self.right]
        sel = []
        for c in self.cols:
            in_l, in_r = c in lcols, c in rcols
            if in_l and (not in_r or c == self.left_on):
                sel.append(f'l.{c} AS {c}')
            elif in_r:
                sel.append(f'r.{c} AS {c}')
            else:
                raise ValueError(f'join 输出列 {c} 在两侧父视图中都找不到')
        return (f"SELECT {', '.join(sel)} FROM ({lsql}) l "
                f"JOIN ({rsql}) r ON l.{self.left_on} = r.{self.right_on}")

    def describe(self):
        return f'{self.left} ⋈ {self.right} ON {self.left_on}={self.right_on} -> {self.cols}'


@dataclass
class Filter(ViewDef):
    name: str
    src: str
    pred_sql: str            # 校验用 SQL 谓词
    pred_py: str             # 引擎用 Python 表达式(列名为变量)
    kind = 'filter'

    def parents(self):
        return [self.src]

    def to_sql(self, ctx):
        s, _ = ctx[self.src]
        return f'SELECT * FROM ({s}) WHERE {self.pred_sql}'

    def describe(self):
        return f'{self.src} WHERE {self.pred_sql}'


@dataclass
class Map(ViewDef):
    name: str
    src: str
    new_col: str
    expr_sql: str            # 校验用 SQL 表达式
    expr_py: str             # 引擎用 Python 表达式
    kind = 'map'

    def parents(self):
        return [self.src]

    def to_sql(self, ctx):
        s, _ = ctx[self.src]
        return f'SELECT *, {self.expr_sql} AS {self.new_col} FROM ({s})'

    def describe(self):
        expr = self.expr_sql if len(self.expr_sql) <= 36 else self.expr_sql[:33] + '…'
        return f'{self.src} + [{self.new_col} = {expr}]'


@dataclass
class Agg(ViewDef):
    name: str
    src: str
    group_by: list[str]
    aggs: list[AggSpec]
    kind = 'agg'

    def parents(self):
        return [self.src]

    def to_sql(self, ctx):
        s, _ = ctx[self.src]
        gb = ', '.join(self.group_by)
        exprs = ', '.join(a.sql() for a in self.aggs)
        return f'SELECT {gb}, {exprs} FROM ({s}) GROUP BY {gb}'

    def describe(self):
        aggs = ', '.join(f'{a.func}({a.col or "*"})->{a.out}' for a in self.aggs)
        return f'{self.src} GROUP BY {self.group_by}: {aggs}'
