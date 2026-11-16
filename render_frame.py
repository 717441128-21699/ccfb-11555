"""渲染一帧仪表盘到文本,用于检查布局(非交互)。"""
import sys, threading
sys.path.insert(0, '.')
from imv.engine import Engine
from imv.domain import build_view_defs, MAP_ENV
from imv.simulator import Simulator
from imv.console import Dashboard
from rich.console import Console

eng = Engine('demo.db', build_view_defs(), env=MAP_ENV)
eng.recover()
sim = Simulator('demo.db')
control = {k: threading.Event() for k in
           ('stop', 'pause_sim', 'pause_engine', 'engine_busy')}
dash = Dashboard(eng, sim, control, 'demo.db')
dash.kick_verify()
import time
time.sleep(6)  # 等校验完成
con = Console(record=True, width=150, height=48)
con.print(dash.render())
open('frame.txt', 'w', encoding='utf-8').write(con.export_text())
print('written frame.txt')
