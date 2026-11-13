"""业务模块路由汇总。

这里统一按别名导入再暴露 ROUTERS：模块名有可能和内置名撞车（某个业务模块就叫 dict、list
这种名字时），按名字直接 import 会把内置类型覆盖掉，函数注解在运行时求值就会报
'module' object is not subscriptable。
"""
from __future__ import annotations

from app.routers import datasource as router_datasource
from app.routers import parserule as router_parserule
from app.routers import proxyconfig as router_proxyconfig
from app.routers import ratelimiter as router_ratelimiter
from app.routers import taskcrawl as router_taskcrawl
from app.routers import pipeline as router_pipeline
from app.routers import robotflow as router_robotflow
from app.routers import retrypolicy as router_retrypolicy
from app.routers import resultcheck as router_resultcheck
from app.routers import alertrule as router_alertrule
from app.routers import diagnose as router_diagnose
from app.routers import scheduler as router_scheduler
from app.routers import exportconf as router_exportconf
from app.routers import lmcfg as router_lmcfg
from app.routers import blacklist as router_blacklist
from app.routers import storagelog as router_storagelog
from app.routers import templatelib as router_templatelib
from app.routers import datalabel as router_datalabel
from app.routers import exportrun as router_exportrun

ROUTERS = [router_datasource, router_parserule, router_proxyconfig, router_ratelimiter, router_taskcrawl, router_pipeline, router_robotflow, router_retrypolicy, router_resultcheck, router_alertrule, router_diagnose, router_scheduler, router_exportconf, router_lmcfg, router_blacklist, router_storagelog, router_templatelib, router_datalabel, router_exportrun]
