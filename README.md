# imv — 增量物化视图引擎

源库用 SQLite 模拟的订单系统:高频增删改经触发器进入变更日志(CDC),
引擎按提交顺序把变更翻译成行级增量,沿视图 DAG 逐层传播,物化视图
(GMV / 客单价 / 去重买家 …)始终与源库保持一致 —— 不再每五分钟全量重算。

## 快速开始

```bash
py main.py init --fresh                 # 建库:业务表 + CDC 触发器 + 物化视图表
py main.py run                          # 控制台:实时负载 + 引擎 + 仪表盘
py main.py bench --fresh                # 验收:100 万订单 + 100 万随机变更,每 10 万条全量对比
py main.py crashtest --fresh            # 反复"崩溃-重启",验证不重复不遗漏
py main.py verify                       # 一次性全量一致性校验
```

控制台按键:`q` 退出 · `空格` 暂停/恢复发生器 · `v` 立即全量校验 · `x` 模拟进程崩溃

仪表盘五个面板:视图定义(DAG)/ 变更流 / 视图内容 / 一致性校验 / 性能曲线。

## 架构

```
业务写入 ──tx──> 源表(users/orders)
                └─ 触发器 ──> change_log(seq, tx_id, op, old, new)   ← 同事务提交
                                     │
引擎:读 change_log[checkpoint..] ──> 行增量 (row, ±1) ──> DAG 传播
   Join ⋈ / Filter σ / Map π / Agg Σ ──> vw_* 物化表 + checkpoint   ← 同一事务落盘
```

### 对应需求

1. **变更日志按提交顺序应用** — `change_log.seq` 全局递增;引擎按 seq 顺序、
   以完整事务为单位消费(`imv/engine.py: apply_pending`)。
2. **SUM/COUNT/AVG/MIN/MAX 增量维护** — SUM/COUNT 是累加器,AVG=SUM/COUNT;
   MIN/MAX 每组维护 *值→重数* 多重集,当前极值被删光时从多重集取下一个,
   正确回退(`imv/engine.py: AggNode`)。
3. **两表连接只算受影响行** — 两侧各维护 `连接键→{行:重数}` 索引,一边变化
   只探测另一边同键的行:`Δout = ΔL⋈R_new + L_old⋈ΔR`(`JoinNode`)。
4. **COUNT DISTINCT 增量维护** — 每组维护 *值→重数*,重数 0↔1 跳变时
   ±1 distinct 计数。
5. **视图上再定义视图** — 每个算子输出都是 (行,±1) 增量流,下游算子无差别
   消费,天然层层传播(城市 GMV → 区域 GMV)。
6. **事务原子性** — 写入方的数据修改与 change_log 记录在同一 SQLite 事务;
   引擎按 tx_id 分组,一个事务的变更一起应用、一起落盘。
7. **崩溃恢复** — vw_* 与 checkpoint 同一事务提交;重启后把算子状态精确
   重建到 checkpoint 时刻(直接消费源表的节点重放已确认前缀
   change_log[1..checkpoint],下游节点从父视图物化表重建),再从
   checkpoint+1 继续,不重复不遗漏。

### 一致性校验

每个视图由定义自动生成**全量重算 SQL**(与增量引擎是两条独立代码路径),
逐行对比物化结果 —— 必须完全一致(金额以"分"存整数,SUM 精确;
AVG 比较保留 6 位小数)。

## 演示视图 DAG

```
orders, users
   └─⋈─ v_order_user          连接:订单 ⋈ 用户
          ├─ v_paid           过滤:status='paid'
          │   └─ v_city_gmv   聚合:COUNT/SUM/AVG/MIN/MAX + COUNT DISTINCT(买家)
          │        └─ v_city_region ─ v_region_gmv   视图之上的视图
          └─ v_status_stats   按订单状态统计
```

## 文件

| 文件 | 说明 |
|---|---|
| `imv/db.py` | 源表、CDC 触发器、事务上下文 |
| `imv/views.py` | 视图定义(声明式)+ 校验 SQL 生成 |
| `imv/engine.py` | 增量引擎:算子、传播、checkpoint、恢复 |
| `imv/simulator.py` | 种子数据 + 随机变更负载 |
| `imv/verify.py` | 全量重算对比 |
| `imv/console.py` | rich 控制台仪表盘 |
| `main.py` | CLI:init / seed / run / bench / verify / crashtest |

## 已知边界(演示取舍)

- 等值连接为单连接键;聚合的分组键/度量列不允许 NULL。
- 算子状态在内存中(百万行级约几百 MB);持久化的是 vw_* 与 checkpoint,
  重启靠"重放已确认前缀 + 父视图物化表"重建,代价是 O(checkpoint) 的重放。
- 单写入方( SQLite 本身串行化写事务);多写者需要给 change_log 加
  事务边界标记再按 tx 排序。
