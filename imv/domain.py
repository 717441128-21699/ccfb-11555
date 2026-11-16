"""演示域:城市/区域维度 + 运营大屏的视图 DAG 定义。

DAG:
  users ──┐
          ├─⋈─ v_order_user ──┬─ v_paid(过滤) ─ v_city_gmv(聚合) ─ v_city_region(派生列) ─ v_region_gmv(聚合)
  orders ─┘                   └─ v_status_stats(聚合)

覆盖的需求:
- v_city_gmv:SUM/COUNT/AVG/MIN/MAX + COUNT DISTINCT(需求 2、4),输入是连接结果(需求 3)
- v_region_gmv:视图之上再定义视图,层层传播(需求 5)
"""
from __future__ import annotations

from .views import Agg, AggSpec, Filter, Join, Map, Source

CITIES = [
    '北京', '上海', '广州', '深圳', '杭州', '成都', '重庆', '武汉', '西安', '南京',
    '苏州', '天津', '长沙', '郑州', '青岛', '沈阳', '宁波', '昆明', '大连', '厦门',
    '合肥', '佛山', '福州', '哈尔滨', '济南', '温州', '石家庄', '泉州', '南宁', '贵阳',
]

REGION_OF = {
    '北京': '华北', '天津': '华北', '石家庄': '华北',
    '上海': '华东', '杭州': '华东', '南京': '华东', '苏州': '华东', '宁波': '华东',
    '合肥': '华东', '福州': '华东', '济南': '华东', '青岛': '华东', '厦门': '华东',
    '温州': '华东', '泉州': '华东',
    '广州': '华南', '深圳': '华南', '佛山': '华南', '南宁': '华南',
    '武汉': '华中', '长沙': '华中', '郑州': '华中',
    '成都': '西南', '重庆': '西南', '昆明': '西南', '贵阳': '西南',
    '西安': '西北',
    '沈阳': '东北', '大连': '东北', '哈尔滨': '东北',
}

ORDER_STATUSES = ['paid', 'pending', 'cancelled']


def _region_case_sql() -> str:
    whens = ' '.join(f"WHEN '{c}' THEN '{r}'" for c, r in REGION_OF.items())
    return f'CASE city {whens} END'


def build_view_defs():
    return [
        Source('users', ['user_id', 'city', 'vip', 'name']),
        Source('orders', ['order_id', 'user_id', 'amount', 'status', 'ts']),

        # 需求 3:两表连接,任一边变化只影响相关行
        Join('v_order_user', 'orders', 'users', 'user_id', 'user_id',
             ['order_id', 'user_id', 'amount', 'status', 'city', 'vip']),

        Filter('v_paid', 'v_order_user', "status = 'paid'", "status == 'paid'"),

        # 需求 2 + 4:全聚合算子 + COUNT DISTINCT
        Agg('v_city_gmv', 'v_paid', ['city'], [
            AggSpec('COUNT', None, 'orders'),
            AggSpec('SUM', 'amount', 'gmv'),
            AggSpec('AVG', 'amount', 'avg_amount'),
            AggSpec('MIN', 'amount', 'min_amount'),
            AggSpec('MAX', 'amount', 'max_amount'),
            AggSpec('COUNT_DISTINCT', 'user_id', 'buyers'),
        ]),

        Map('v_city_region', 'v_city_gmv', 'region',
            _region_case_sql(), "__REGION__.get(city, '其他')"),

        # 需求 5:视图之上再定义视图(区域大屏 = 城市大屏的汇总)
        Agg('v_region_gmv', 'v_city_region', ['region'], [
            AggSpec('SUM', 'gmv', 'gmv'),
            AggSpec('SUM', 'orders', 'orders'),
            AggSpec('SUM', 'buyers', 'buyer_city_sum'),
            AggSpec('MAX', 'max_amount', 'max_order'),
        ]),

        # 从连接视图分出的另一支:按订单状态统计(不过滤)
        Agg('v_status_stats', 'v_order_user', ['status'], [
            AggSpec('COUNT', None, 'cnt'),
            AggSpec('SUM', 'amount', 'gmv'),
            AggSpec('AVG', 'amount', 'avg_amount'),
        ]),
    ]


# Map 算子 Python 表达式里引用的环境
MAP_ENV = {'__REGION__': REGION_OF}
