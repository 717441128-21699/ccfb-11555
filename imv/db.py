"""SQLite 源库:业务表 + 触发器 CDC 变更日志 + 事务上下文。

设计要点:
- 写入方在事务开始时把 _tx_ctx.tx_id 加一,触发器把该 tx_id 连同变更写入
  change_log。数据修改与日志记录处于同一个 SQLite 事务,要么一起提交、
  要么一起回滚 —— 这是"同一事务的变更要么都体现要么都不体现"的根基。
- change_log.seq 为 AUTOINCREMENT,单写入方下严格递增,即提交顺序。
"""
from __future__ import annotations

import sqlite3
from contextlib import contextmanager

# 源表及其列(顺序即行元组顺序)
SOURCE_TABLES = {
    'users':  ['user_id', 'city', 'vip', 'name'],
    'orders': ['order_id', 'user_id', 'amount', 'status', 'ts'],
}

DDL = """
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;

CREATE TABLE IF NOT EXISTS users (
  user_id INTEGER PRIMARY KEY,
  city    TEXT NOT NULL,
  vip     INTEGER NOT NULL DEFAULT 0,
  name    TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS orders (
  order_id INTEGER PRIMARY KEY,
  user_id  INTEGER NOT NULL,
  amount   INTEGER NOT NULL,          -- 金额,单位:分(整数,SUM 才能精确)
  status   TEXT NOT NULL,             -- paid / pending / cancelled
  ts       INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_orders_user ON orders(user_id);

-- 事务上下文:写入方在事务开头递tx_id,触发器同事务内读取
CREATE TABLE IF NOT EXISTS _tx_ctx (
  id    INTEGER PRIMARY KEY CHECK (id = 1),
  tx_id INTEGER NOT NULL
);
INSERT OR IGNORE INTO _tx_ctx (id, tx_id) VALUES (1, 0);

-- 变更日志(CDC)
CREATE TABLE IF NOT EXISTS change_log (
  seq     INTEGER PRIMARY KEY AUTOINCREMENT,
  tx_id   INTEGER NOT NULL,
  tbl     TEXT NOT NULL,
  op      TEXT NOT NULL,              -- INSERT / UPDATE / DELETE
  old_row TEXT,                       -- JSON
  new_row TEXT                        -- JSON
);
CREATE INDEX IF NOT EXISTS idx_change_log_tx ON change_log(tx_id);

-- 引擎元数据(checkpoint 等)
CREATE TABLE IF NOT EXISTS meta (
  k TEXT PRIMARY KEY,
  v TEXT
);
"""


def _trigger_ddl(tbl: str, cols: list[str]) -> str:
    new_obj = 'json_object(' + ','.join(f"'{c}',NEW.{c}" for c in cols) + ')'
    old_obj = 'json_object(' + ','.join(f"'{c}',OLD.{c}" for c in cols) + ')'
    tx = '(SELECT tx_id FROM _tx_ctx WHERE id = 1)'
    return f"""
CREATE TRIGGER IF NOT EXISTS trg_{tbl}_ai AFTER INSERT ON {tbl} BEGIN
  INSERT INTO change_log (tx_id, tbl, op, old_row, new_row)
  VALUES ({tx}, '{tbl}', 'INSERT', NULL, {new_obj});
END;
CREATE TRIGGER IF NOT EXISTS trg_{tbl}_au AFTER UPDATE ON {tbl} BEGIN
  INSERT INTO change_log (tx_id, tbl, op, old_row, new_row)
  VALUES ({tx}, '{tbl}', 'UPDATE', {old_obj}, {new_obj});
END;
CREATE TRIGGER IF NOT EXISTS trg_{tbl}_ad AFTER DELETE ON {tbl} BEGIN
  INSERT INTO change_log (tx_id, tbl, op, old_row, new_row)
  VALUES ({tx}, '{tbl}', 'DELETE', {old_obj}, NULL);
END;
"""


def schema_ddl() -> str:
    parts = [DDL]
    for t, c in SOURCE_TABLES.items():
        parts.append(_trigger_ddl(t, c))
    return '\n'.join(parts)


def connect(db_path: str) -> sqlite3.Connection:
    # isolation_level=None:手动控制事务边界(BEGIN IMMEDIATE / COMMIT)
    # check_same_thread=False:连接由创建它的对象(引擎/发生器)在其工作线程里
    # 独占使用,调用方保证同一连接不跨线程并发访问。
    conn = sqlite3.connect(db_path, timeout=60, isolation_level=None,
                           check_same_thread=False)
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('PRAGMA synchronous=NORMAL')
    conn.execute('PRAGMA busy_timeout=60000')
    conn.execute('PRAGMA temp_store=MEMORY')
    return conn


def init_db(db_path: str) -> None:
    conn = connect(db_path)
    conn.executescript(schema_ddl())
    conn.close()


class Writer:
    """业务写入方:一个 tx() 块里的所有数据修改共享同一个 tx_id。"""

    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    @contextmanager
    def tx(self):
        conn = self.conn
        conn.execute('BEGIN IMMEDIATE')
        conn.execute('UPDATE _tx_ctx SET tx_id = tx_id + 1 WHERE id = 1')
        tx_id = conn.execute('SELECT tx_id FROM _tx_ctx WHERE id = 1').fetchone()[0]
        try:
            yield tx_id
        except BaseException:
            conn.execute('ROLLBACK')
            raise
        else:
            conn.execute('COMMIT')
