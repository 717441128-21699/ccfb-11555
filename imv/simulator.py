"""负载发生器:造种子数据 + 随机增删改,全部经触发器进入 change_log。

变更混合比例模拟真实订单系统:
- 45% 新订单插入
- 20% 改订单金额 / 10% 改订单状态(paid<->cancelled,考验过滤视图)
- 15% 删除订单
- 4%  新用户 / 4% 用户改城市(连接右侧变化,GMV 在城市间迁移)
- 2%  删除用户(同事务级联删其订单 —— 多表多行事务,需求 6)
约 15% 的事务打包 2~8 条变更,检验事务原子性。
"""
from __future__ import annotations

import random
import time

from . import db as dbmod
from .domain import CITIES, ORDER_STATUSES


class Simulator:
    def __init__(self, db_path, seed=42):
        self.conn = dbmod.connect(db_path)
        self.writer = dbmod.Writer(self.conn)
        self.rng = random.Random(seed)
        self.user_ids = []
        self.order_ids = []
        self.next_user_id = 1
        self.next_order_id = 1
        self.ts = 1_700_000_000
        self.made = 0          # 已生成的变更条数

    # ---------- 状态 ----------

    def load_state(self):
        """从当前库恢复 id 空间(重启后续写)。"""
        self.user_ids = [r[0] for r in self.conn.execute('SELECT user_id FROM users')]
        self.order_ids = [r[0] for r in self.conn.execute('SELECT order_id FROM orders')]
        self.next_user_id = (max(self.user_ids) + 1) if self.user_ids else 1
        self.next_order_id = (max(self.order_ids) + 1) if self.order_ids else 1

    def counts(self):
        u = self.conn.execute('SELECT COUNT(*) FROM users').fetchone()[0]
        o = self.conn.execute('SELECT COUNT(*) FROM orders').fetchone()[0]
        return u, o

    # ---------- 种子数据 ----------

    def seed(self, n_users, n_orders, tx_size=10000, progress=None):
        rng = self.rng
        t0 = time.perf_counter()
        done_u = done_o = 0
        while done_u < n_users:
            k = min(tx_size, n_users - done_u)
            with self.writer.tx():
                for _ in range(k):
                    uid = self.next_user_id
                    self.next_user_id += 1
                    self.conn.execute(
                        'INSERT INTO users VALUES (?,?,?,?)',
                        (uid, rng.choice(CITIES), 1 if rng.random() < 0.15 else 0,
                         f'user_{uid}'))
                    self.user_ids.append(uid)
            done_u += k
            if progress:
                progress(f'种子用户 {done_u}/{n_users}')
        while done_o < n_orders:
            k = min(tx_size, n_orders - done_o)
            with self.writer.tx():
                for _ in range(k):
                    oid = self.next_order_id
                    self.next_order_id += 1
                    self.ts += rng.randint(1, 60)
                    self.conn.execute(
                        'INSERT INTO orders VALUES (?,?,?,?,?)',
                        (oid, rng.choice(self.user_ids),
                         rng.randint(100, 100000), self._rand_status(), self.ts))
                    self.order_ids.append(oid)
            done_o += k
            if progress:
                progress(f'种子订单 {done_o}/{n_orders}  '
                         f'({done_o/(time.perf_counter()-t0):.0f} 行/秒)')
        return time.perf_counter() - t0

    def _rand_status(self):
        r = self.rng.random()
        if r < 0.8:
            return 'paid'
        if r < 0.9:
            return 'pending'
        return 'cancelled'

    # ---------- 随机变更 ----------

    def make_changes(self, n, bundle_prob=0.15):
        """生成 n 条变更;约 bundle_prob 的事务打包 2~8 条(需求 6 的检验)。"""
        left = n
        while left > 0:
            k = 1 if self.rng.random() > bundle_prob else self.rng.randint(2, 8)
            k = min(k, left)
            with self.writer.tx():
                for _ in range(k):
                    self._one_change()
            left -= k
            self.made += k

    def _one_change(self):
        rng = self.rng
        r = rng.random()
        if r < 0.45 or not self.order_ids:
            self._insert_order()
        elif r < 0.65:
            self._update_order_amount()
        elif r < 0.75:
            self._update_order_status()
        elif r < 0.90:
            self._delete_order()
        elif r < 0.94:
            self._insert_user()
        elif r < 0.98:
            self._update_user_city()
        else:
            self._delete_user_cascade()

    def _insert_order(self):
        rng = self.rng
        oid = self.next_order_id
        self.next_order_id += 1
        self.ts += rng.randint(1, 60)
        self.conn.execute(
            'INSERT INTO orders VALUES (?,?,?,?,?)',
            (oid, rng.choice(self.user_ids), rng.randint(100, 100000),
             self._rand_status(), self.ts))
        self.order_ids.append(oid)

    def _update_order_amount(self):
        idx = self.rng.randrange(len(self.order_ids))
        self.conn.execute('UPDATE orders SET amount=? WHERE order_id=?',
                          (self.rng.randint(100, 100000), self.order_ids[idx]))

    def _update_order_status(self):
        idx = self.rng.randrange(len(self.order_ids))
        self.conn.execute('UPDATE orders SET status=? WHERE order_id=?',
                          (self.rng.choice(ORDER_STATUSES), self.order_ids[idx]))

    def _delete_order(self):
        idx = self.rng.randrange(len(self.order_ids))
        oid = self.order_ids[idx]
        self.order_ids[idx] = self.order_ids[-1]
        self.order_ids.pop()
        self.conn.execute('DELETE FROM orders WHERE order_id=?', (oid,))

    def _insert_user(self):
        rng = self.rng
        uid = self.next_user_id
        self.next_user_id += 1
        self.conn.execute('INSERT INTO users VALUES (?,?,?,?)',
                          (uid, rng.choice(CITIES),
                           1 if rng.random() < 0.15 else 0, f'user_{uid}'))
        self.user_ids.append(uid)

    def _update_user_city(self):
        uid = self.rng.choice(self.user_ids)
        self.conn.execute('UPDATE users SET city=? WHERE user_id=?',
                          (self.rng.choice(CITIES), uid))

    def _delete_user_cascade(self):
        """删除用户并同事务级联删除其全部订单 —— 一个事务里跨表多行变更。"""
        idx = self.rng.randrange(len(self.user_ids))
        uid = self.user_ids[idx]
        # 先查出该用户的订单(用于维护本地 id 列表),再同事务删除
        oids = [r[0] for r in self.conn.execute(
            'SELECT order_id FROM orders WHERE user_id=?', (uid,))]
        self.conn.execute('DELETE FROM orders WHERE user_id=?', (uid,))
        self.conn.execute('DELETE FROM users WHERE user_id=?', (uid,))
        self.user_ids[idx] = self.user_ids[-1]
        self.user_ids.pop()
        if oids:
            dead = set(oids)
            self.order_ids = [o for o in self.order_ids if o not in dead]

    # ---------- 持续负载(控制台模式) ----------

    def run_loop(self, stop, pause, rate=2000):
        per_tick = max(1, rate // 10)
        while not stop.is_set():
            if pause.is_set():
                time.sleep(0.1)
                continue
            t0 = time.perf_counter()
            self.make_changes(per_tick)
            dt = time.perf_counter() - t0
            if dt < 0.1:
                time.sleep(0.1 - dt)
