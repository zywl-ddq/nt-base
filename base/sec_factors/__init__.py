# -*- coding: utf-8 -*-
"""秒级公共因子包。

注册制：新增因子 = 在本目录加一个文件（实现 SecFactor 接口），
然后在 main.py 的 SecFactorLoop 注册一行即可。系统运行中也可热插拔。
"""
from base.sec_factors.obi import OBIFactor
from base.sec_factors.cvd import CVDFactor

__all__ = ["OBIFactor", "CVDFactor"]
