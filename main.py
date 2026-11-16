"""命令行入口。

  py main.py init      [--db imv.db] [--fresh]           建库(表/触发器/物化视图)
  py main.py seed      [--users N] [--orders M]        造种子数据(经 change_log)
  py main.py run                          控制台:实时发生负载 + 引擎 + 仪表盘
  py main.py bench     [--orders M] [--changes K] [--verify-every K]
                                             验收:百万级数据,每隔 K 条全量对比
  py main.py verify                                    一次性全量一致性校验
  py main.py crashtest [--changes N]                   反复崩溃-重启,验证不重复不遗漏
"""
from __future__ import annotations

import argparse
import os
import random
import subprocess
import sys
import threading
import time

from imv import db as dbmod
from imv.domain import MAP_ENV, build_view_defs
from imv.engine import Engine
from imv.simulator import Simulator
from imv.verify import format_results, verify_all


def ensure_db(db_path, fresh=False):
    if fresh:
        for suffix in ('', '-wal', '-shm'):
            try:
                os.remove(db_path + suffix)
            except FileNotFoundError:
                pass
    if not os.path.exists(db_path):
        dbmod.init_db(db_path)
        Engine(db_path, build_view_defs(), env=MAP_ENV)  # 建 vw_* 物化表
        return True
    return False


def eng_recover(eng):
    t0 = time.perf_counter()
    eng.recover()
    dt = time.perf_counter() - t0
    if eng.checkpoint:
        print(f'[引擎] 从 checkpoint={eng.checkpoint:,} 恢复,'
              f'算子状态重建耗时 {dt:.1f}s')


def cmd_init(args):
    created = ensure_db(args.db, fresh=args.fresh)
    print(f'[init] {args.db} {"已重建" if created else "已存在,未改动"}')


def cmd_seed(args):
    ensure_db(args.db)
    sim = Simulator(args.db, seed=args.seed)
    sim.load_state()
    print(f'[seed] users={args.users:,} orders={args.orders:,} ...')
    dt = sim.seed(args.users, args.orders,
                  progress=lambda m: print(f'\r  {m}   ', end='', flush=True))
    print(f'\n[seed] 完成,耗时 {dt:.1f}s')


def cmd_bench(args):
    """验收主流程:造数 -> 引擎增量消费 -> 每隔一段全量重算对比。"""
    ensure_db(args.db, fresh=args.fresh)
    eng = Engine(args.db, build_view_defs(), env=MAP_ENV)
    eng_recover(eng)
    sim = Simulator(args.db, seed=args.seed)
    sim.load_state()

    nu, no = sim.counts()
    if no == 0:
        print(f'[bench] 造种子数据: {args.users:,} 用户 / {args.orders:,} 订单')
        dt = sim.seed(args.users, args.orders,
                      progress=lambda m: print(f'\r  {m}   ', end='', flush=True))
        print(f'\n[bench] 种子数据完成,耗时 {dt:.1f}s')
    else:
        print(f'[bench] 复用已有数据: {nu:,} 用户 / {no:,} 订单')

    def drain(tag):
        t0 = time.perf_counter()
        n0 = eng.applied_total
        while eng.apply_pending():
            pass
        dt = time.perf_counter() - t0
        n = eng.applied_total - n0
        if n:
            print(f'[引擎] {tag}: 应用 {n:,} 条变更,耗时 {dt:.1f}s '
                  f'({n/dt:,.0f} 条/秒),checkpoint={eng.checkpoint:,}')
        return n

    drain('消费种子数据变更流')

    print('[校验] 基线(种子数据后):')
    base = verify_all(eng)
    print(format_results(base))
    if not all(r.ok for r in base):
        print('!! 基线校验失败'); sys.exit(1)

    print(f'[bench] 开始生成 {args.changes:,} 条随机变更,'
          f'每 {args.verify_every:,} 条全量对比一次')
    remaining = args.changes
    round_no = 0
    t_start = time.perf_counter()
    while remaining > 0:
        step = min(args.verify_every, remaining)
        round_no += 1
        t0 = time.perf_counter()
        sim.make_changes(step)
        gen_dt = time.perf_counter() - t0
        drain(f'第{round_no}轮 (生成 {step:,} 条变更耗时 {gen_dt:.1f}s)')
        res = verify_all(eng)
        ok = all(r.ok for r in res)
        print(f'[校验] 第{round_no}轮 @变更{sim.made:,}: '
              + ('全部一致 ✓' if ok else '不一致 ✗'))
        if not ok:
            print(format_results(res))
            print('!! 校验失败,增量结果与全量重算不一致')
            sys.exit(1)
        remaining -= step

    tot = time.perf_counter() - t_start
    print(f'\n[bench] 完成:{args.changes:,} 条随机变更全部增量应用,'
          f'{round_no} 轮全量对比均完全一致,总耗时 {tot:.1f}s')
    if eng.perf:
        tps = [n / dt for _, n, dt in eng.perf if dt > 0]
        tps.sort()
        print(f'[引擎] 批次吞吐: 中位 {tps[len(tps)//2]:,.0f} 条/秒,'
              f'峰值 {tps[-1]:,.0f} 条/秒')


def cmd_verify(args):
    eng = Engine(args.db, build_view_defs(), env=MAP_ENV)
    eng_recover(eng)
    n = 0
    while eng.apply_pending():
        n += 1
    if n:
        print(f'[引擎] 先排空积压({n} 批)')
    res = verify_all(eng)
    print(format_results(res))
    ok = all(r.ok for r in res)
    print('全部一致 ✓' if ok else '存在不一致 ✗')
    sys.exit(0 if ok else 1)


def cmd_run(args):
    ensure_db(args.db)
    eng = Engine(args.db, build_view_defs(), env=MAP_ENV)
    eng_recover(eng)
    sim = Simulator(args.db, seed=args.seed)
    sim.load_state()
    nu, no = sim.counts()
    if no == 0:
        print(f'[run] 空库,先造 {args.users:,} 用户 / {args.orders:,} 订单种子数据')
        sim.seed(args.users, args.orders,
                 progress=lambda m: print(f'\r  {m}   ', end='', flush=True))
        print()

    control = {
        'stop': threading.Event(),
        'pause_sim': threading.Event(),
        'pause_engine': threading.Event(),
        'engine_busy': threading.Event(),
    }
    th_eng = threading.Thread(
        target=eng.run_loop,
        args=(control['stop'], control['pause_engine'], control['engine_busy']),
        kwargs={'batch': 5000}, daemon=True)
    th_sim = threading.Thread(
        target=sim.run_loop,
        args=(control['stop'], control['pause_sim']),
        kwargs={'rate': args.rate}, daemon=True)
    th_eng.start()
    th_sim.start()
    print(f'[run] 控制台启动,发生器目标 {args.rate:,} 变更/秒')
    try:
        from imv.console import run_dashboard
        run_dashboard(eng, sim, control, args.db, duration=args.duration)
    finally:
        control['stop'].set()
        th_eng.join(timeout=5)
        th_sim.join(timeout=5)
        print(f'[run] 退出。已应用 {eng.applied_total:,} 条,'
              f'checkpoint={eng.checkpoint:,}')


def cmd_crashtest(args):
    """反复"崩溃-重启":子进程在写事务中随机 os._exit,父进程重启它直到
    全部变更应用完,最后全量校验 —— 验证不重复、不遗漏(需求 7)。"""
    ensure_db(args.db, fresh=args.fresh)
    sim = Simulator(args.db, seed=args.seed)
    sim.load_state()
    nu, no = sim.counts()
    if no == 0:
        print(f'[crashtest] 造种子数据: {args.users:,} 用户 / {args.orders:,} 订单')
        sim.seed(args.users, args.orders)
    print(f'[crashtest] 预生成 {args.changes:,} 条随机变更(引擎暂不消费)')
    sim.make_changes(args.changes)

    crashes = 0
    attempt = 0
    while True:
        attempt += 1
        r = subprocess.run(
            [sys.executable, 'main.py', '--db', args.db, 'worker',
             '--crash-prob', str(args.crash_prob)],
            cwd=os.path.dirname(os.path.abspath(__file__)))
        if r.returncode == 0:
            break
        crashes += 1
        print(f'[crashtest] 第 {crashes} 次崩溃,重启进程从 checkpoint 恢复…')
    print(f'[crashtest] 全部变更应用完毕,共模拟崩溃 {crashes} 次')

    eng = Engine(args.db, build_view_defs(), env=MAP_ENV)
    eng_recover(eng)
    res = verify_all(eng)
    print(format_results(res))
    if all(r.ok for r in res):
        print(f'[crashtest] PASS ✓ —— {crashes} 次崩溃恢复后,'
              f'增量结果与全量重算完全一致(不重复不遗漏)')
        sys.exit(0)
    print('[crashtest] FAIL ✗')
    sys.exit(1)


def cmd_worker(args):
    """crashtest 的子进程:应用变更,并随机在写事务中途 os._exit 模拟崩溃。"""
    rng = random.Random()
    eng = Engine(args.db, build_view_defs(), env=MAP_ENV)
    eng.recover()

    def maybe_crash(phase):
        if rng.random() < args.crash_prob:
            print(f'[worker] !!! 在 {phase} 模拟进程崩溃', flush=True)
            os._exit(2)

    eng.crash_hook = maybe_crash
    while True:
        n = eng.apply_pending(batch=rng.randint(500, 5000))
        if n == 0:
            break
        if rng.random() < args.crash_prob:
            print('[worker] !!! 批次间模拟进程崩溃', flush=True)
            os._exit(2)
    print(f'[worker] 变更流已排空,checkpoint={eng.checkpoint:,}', flush=True)


def main():
    ap = argparse.ArgumentParser(description='增量物化视图引擎')
    ap.add_argument('--db', default='imv.db')
    sub = ap.add_subparsers(dest='cmd', required=True)

    p = sub.add_parser('init')
    p.add_argument('--fresh', action='store_true')

    p = sub.add_parser('seed')
    p.add_argument('--users', type=int, default=200_000)
    p.add_argument('--orders', type=int, default=1_000_000)
    p.add_argument('--seed', type=int, default=42)

    p = sub.add_parser('run')
    p.add_argument('--users', type=int, default=50_000)
    p.add_argument('--orders', type=int, default=200_000)
    p.add_argument('--rate', type=int, default=2000, help='目标变更速率(条/秒)')
    p.add_argument('--seed', type=int, default=42)
    p.add_argument('--duration', type=float, default=None, help='演示秒数(默认一直跑)')

    p = sub.add_parser('bench')
    p.add_argument('--users', type=int, default=200_000)
    p.add_argument('--orders', type=int, default=1_000_000)
    p.add_argument('--changes', type=int, default=1_000_000)
    p.add_argument('--verify-every', type=int, default=100_000)
    p.add_argument('--seed', type=int, default=42)
    p.add_argument('--fresh', action='store_true')

    p = sub.add_parser('verify')

    p = sub.add_parser('crashtest')
    p.add_argument('--users', type=int, default=20_000)
    p.add_argument('--orders', type=int, default=100_000)
    p.add_argument('--changes', type=int, default=200_000)
    p.add_argument('--crash-prob', type=float, default=0.15)
    p.add_argument('--seed', type=int, default=42)
    p.add_argument('--fresh', action='store_true')

    p = sub.add_parser('worker')
    p.add_argument('--crash-prob', type=float, default=0.15)

    args = ap.parse_args()
    {
        'init': cmd_init,
        'seed': cmd_seed,
        'bench': cmd_bench,
        'verify': cmd_verify,
        'run': cmd_run,
        'crashtest': cmd_crashtest,
        'worker': cmd_worker,
    }[args.cmd](args)


if __name__ == '__main__':
    main()
