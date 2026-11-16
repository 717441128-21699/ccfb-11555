"""控制台:rich 实时仪表盘。

五个面板:视图定义 / 变更流 / 视图内容 / 一致性校验 / 性能曲线。
按键:q 退出 | 空格 暂停/恢复发生器 | v 立即全量校验 | x 模拟进程崩溃
"""
from __future__ import annotations

import threading
import time
from collections import deque

from rich.align import Align
from rich.console import Group
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from . import db as dbmod
from .verify import verify_all

SPARK = '▁▂▃▄▅▆▇█'


def sparkline(data, width):
    if not data:
        return ' ' * width
    data = list(data)[-width:]
    mx = max(data) or 1
    s = ''.join(SPARK[min(7, int(v / mx * 7))] for v in data)
    return s.rjust(width) if len(s) < width else s


def _money(cents):
    return f'¥{cents/100:,.0f}' if cents is not None else '-'


class Dashboard:
    def __init__(self, engine, sim, control, db_path):
        self.engine = engine
        self.sim = sim
        self.control = control
        self.conn = dbmod.connect(db_path)
        self.verify_results = None
        self.verify_msg = '尚未校验 — 按 v 触发(会短暂停顿发生器)'
        self.lag_hist = deque(maxlen=200)
        self.tps_hist = deque(maxlen=200)
        self.t0 = time.time()
        self._verify_thread = None

    # ---------- 后台校验 ----------

    def _max_seq(self):
        # 用控制台自己的连接查;engine.conn 只能由引擎线程使用
        return self.conn.execute(
            'SELECT COALESCE(MAX(seq),0) FROM change_log').fetchone()[0]

    def _lag(self):
        return self._max_seq() - self.engine.checkpoint

    def kick_verify(self):
        if self._verify_thread and self._verify_thread.is_alive():
            return
        self._verify_thread = threading.Thread(target=self._do_verify, daemon=True)
        self._verify_thread.start()

    def _do_verify(self):
        c = self.control
        self.verify_msg = '校验中:暂停发生器,等待引擎排空…'
        c['pause_sim'].set()
        # 等引擎把积压应用完
        deadline = time.time() + 120
        while self._lag() > 0 and time.time() < deadline:
            time.sleep(0.2)
        c['pause_engine'].set()
        while c['engine_busy'].is_set():
            time.sleep(0.05)
        try:
            t0 = time.perf_counter()
            self.verify_results = verify_all(self.engine)
            dt = time.perf_counter() - t0
            ok = all(r.ok for r in self.verify_results)
            self.verify_msg = (f'{"全部一致 ✓" if ok else "存在不一致 ✗"} '
                               f'(全量重算耗时 {dt:.1f}s,'
                               f' {time.strftime("%H:%M:%S")})')
        finally:
            c['pause_engine'].clear()
            c['pause_sim'].clear()

    # ---------- 渲染 ----------

    def render(self):
        layout = Layout()
        layout.split_column(
            Layout(name='header', size=3),
            Layout(name='body', ratio=1),
            Layout(name='footer', size=8),
        )
        layout['body'].split_row(
            Layout(name='left', ratio=2),
            Layout(name='right', ratio=3),
        )
        layout['left'].split_column(
            Layout(name='views', ratio=3),
            Layout(name='consistency', ratio=2),
        )
        layout['right'].split_column(
            Layout(name='stream', ratio=3),
            Layout(name='content', ratio=2),
        )
        layout['header'].update(self._header())
        layout['views'].update(self._views_panel())
        layout['consistency'].update(self._consistency_panel())
        layout['stream'].update(self._stream_panel())
        layout['content'].update(self._content_panel())
        layout['footer'].update(self._footer())
        return layout

    def _header(self):
        e = self.engine
        lag = self._lag()
        self.lag_hist.append(lag)
        tps = 0.0
        if len(e.perf) >= 2:
            (t1, n1, _), (t2, n2, _) = e.perf[0], e.perf[-1]
            tot_n = sum(p[1] for p in e.perf)
            span = e.perf[-1][0] - e.perf[0][0]
            tps = tot_n / span if span > 0 else 0.0
        self.tps_hist.append(tps)
        paused = ' [yellow]发生器已暂停[/]' if self.control['pause_sim'].is_set() else ''
        txt = (f'[bold cyan]增量物化视图引擎[/]  已应用 [bold]{e.applied_total:,}[/] 条变更  '
               f'checkpoint [bold]{e.checkpoint:,}[/]  积压 [bold]{lag:,}[/]  '
               f'吞吐 [bold]{tps:,.0f}[/] 条/秒  运行 {time.time()-self.t0:.0f}s{paused}')
        return Panel(Align.center(txt), style='on default')

    def _views_panel(self):
        lines = Text()
        tree = (
            'orders, users\n'
            '   └─⋈─ v_order_user\n'
            '          ├─ v_paid ─ v_city_gmv\n'
            '          │           └─ v_city_region ─ v_region_gmv\n'
            '          └─ v_status_stats\n'
        )
        lines.append(tree, style='cyan')
        for name in self.engine.topo:
            d = self.engine.defs[name]
            if d.kind == 'source':
                continue
            lines.append(f'● {name}\n', style='bold green')
            lines.append(f'    {d.describe()}\n', style='dim')
        return Panel(lines, title='视图定义(DAG)', border_style='green')

    def _stream_panel(self):
        lines = Text()
        for line in list(self.engine.stream)[:16]:
            style = 'white'
            if ' -' in line[:40] and ('-订单' in line or '-用户' in line):
                style = 'red'
            elif '+订单' in line or '+用户' in line:
                style = 'green'
            elif '~订单' in line or '~用户' in line:
                style = 'yellow'
            lines.append(line + '\n', style=style)
        return Panel(lines, title='变更流(最新在上)', border_style='blue')

    def _content_panel(self):
        cur = self.conn.cursor()
        t = Table(expand=True, header_style='bold magenta')
        for c in ('城市', '订单数', 'GMV', '客单价', '最小单', '最大单', '去重买家'):
            t.add_column(c, justify='right' if c != '城市' else 'left')
        try:
            rows = cur.execute(
                'SELECT city, orders, gmv, avg_amount, min_amount, max_amount, buyers '
                'FROM vw_v_city_gmv ORDER BY gmv DESC LIMIT 6').fetchall()
            for city, orders, gmv, avg, mn, mx, buyers in rows:
                t.add_row(city, f'{orders:,}', _money(gmv), _money(avg),
                          _money(mn), _money(mx), f'{buyers:,}')
        except Exception as ex:
            t.add_row(f'(查询失败 {ex})')
        extra = Text()
        try:
            extra.append('区域大屏: ', style='bold')
            for r in cur.execute('SELECT region, gmv, orders, buyer_city_sum '
                                 'FROM vw_v_region_gmv ORDER BY gmv DESC'):
                extra.append(f'{r[0]} {_money(r[1])}/{r[2]:,}单   ')
            extra.append('\n状态分布: ', style='bold')
            for r in cur.execute('SELECT status, cnt, gmv FROM vw_v_status_stats '
                                 'ORDER BY cnt DESC'):
                extra.append(f'{r[0]} {r[1]:,}单 {_money(r[2])}   ')
        except Exception:
            pass
        return Panel(Group(t, Text(), extra), title='视图内容(城市 GMV Top6 / 区域 / 状态)',
                     border_style='magenta')

    def _consistency_panel(self):
        lines = Text()
        lines.append(self.verify_msg + '\n\n', style='bold')
        if self.verify_results:
            for r in self.verify_results:
                if r.ok:
                    lines.append(f'  ✓ {r.name:<16} {r.rows_actual:,} 行一致\n',
                                 style='green')
                else:
                    lines.append(f'  ✗ {r.name:<16} 期望 {r.rows_expected:,} '
                                 f'实际 {r.rows_actual:,}\n', style='bold red')
                    for kind, row, n in r.mismatches[:2]:
                        lines.append(f'      {kind} x{n}: {row}\n', style='red')
        return Panel(lines, title='一致性校验(增量 vs 全量重算)', border_style='yellow')

    def _footer(self):
        w = 46
        tps = sparkline(self.tps_hist, w)
        lag = sparkline(self.lag_hist, w)
        txt = Text()
        txt.append(f'吞吐/秒 |{tps}| {self.tps_hist[-1] if self.tps_hist else 0:,.0f}\n')
        txt.append(f'积压    |{lag}| {self.lag_hist[-1] if self.lag_hist else 0:,}\n')
        txt.append('\n')
        txt.append('按键:', style='bold')
        txt.append(' q 退出   空格 暂停/恢复发生器   v 立即一致性校验   '
                   'x 模拟进程崩溃(重启后从 checkpoint 续跑)')
        return Panel(txt, title='性能曲线', border_style='cyan')


def run_dashboard(engine, sim, control, db_path, duration=None):
    import msvcrt

    dash = Dashboard(engine, sim, control, db_path)
    t_end = time.time() + duration if duration else None
    with Live(dash.render(), refresh_per_second=1, screen=True) as live:
        while True:
            live.update(dash.render())
            time.sleep(0.25)
            if t_end and time.time() > t_end:
                break
            while msvcrt.kbhit():
                ch = msvcrt.getwch()
                if ch == 'q':
                    return
                if ch == ' ':
                    p = control['pause_sim']
                    p.clear() if p.is_set() else p.set()
                elif ch == 'v':
                    dash.kick_verify()
                elif ch == 'x':
                    import os
                    os._exit(1)   # 真·崩溃:不提交、不清理
