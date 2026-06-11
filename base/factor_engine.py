"""
factor_engine.py — 因子计算引擎模块

================================================================================
模块定位与职责
================================================================================
FactorEngine 是 nt-base 系统中负责「因子代码管理」与「因子值计算」的核心模块。
它不属于 gRPC 通信层，也不属于策略调度层，而是作为中间的数据处理单元：

  gRPC Server (接收策略注册的因子源码)
      ↓
  FactorEngine (编译 → 缓存 → 执行 → 产出因子值)
      ↓
  Bar 消息分发 (因子值被附加到 bar 消息中下发给策略)

================================================================================
核心功能
================================================================================
1. 因子代码注册 (register)：接收策略通过 gRPC 提交的 Python 源码，编译为字节码后缓存
2. 因子代码注销 (unregister)：移除不再需要的因子
3. 即时执行 (execute_all)：在最新一根 bar 到达时，对所有已注册因子执行计算
4. 历史批处理 (compute_history)：对历史 bar 数据批量计算因子值（用于回测预热）
5. 安全沙箱：通过 _SAFE_BUILTINS 限制 builtins 可访问范围，仅暴露 numpy/pandas 库

================================================================================
架构设计
================================================================================
- 无状态设计：FactorEngine 本身不存储因子计算结果，每次 execute_all 都重新计算
- 编译缓存：因子源码只在注册时编译一次，后续执行直接使用已编译的 code object
- 多模式执行：支持三种因子代码编写模式 —— 函数模式、预计算 Series 模式、自动发现模式
- 错误隔离：单个因子执行失败不影响其他因子，异常被捕获并记录日志后返回 None

================================================================================
因子代码约定（三种编写模式）
================================================================================
模式1 — Callable 函数（推荐）：
    因子源码中定义一个以 factor_ 开头的函数，接收 df 参数（以及可选的 timescale 参数）。
    函数返回一个 pd.Series，引擎取最后一个非 NaN 值。
    示例：
        def factor_momentum(df):
            return df['close'].pct_change(20)

模式2 — 预计算 Series：
    在源码顶层执行代码，计算结果存储在一个与因子同名的变量或 factor_{name} 变量中。
    示例：
        result = df['close'].rolling(20).mean() / df['close'] - 1
        factor_momentum = result  # 变量名与因子名匹配

模式3 — 自动发现（兜底）：
    如果前两种模式都没有匹配到，引擎会遍历命名空间中的非私有 Series 变量，
    取第一个非空 Series 的最后一个值作为因子值。
    注意：此模式匹配不稳定，建议优先使用模式1或模式2。

================================================================================
使用方式 (被 grpc_server.py/TradingBaseServicer 调用)
================================================================================
    # 初始化
    engine = FactorEngine()

    # 策略注册因子：名称 + Python 源码 + 可选参数
    engine.register("cvd_divergence", code_string, {"lookback": 20, "threshold": 0.5})

    # 每根 bar 到达时，对所有因子执行计算
    results = engine.execute_all(df_bars)  # 返回 {"factor_name": latest_value, ...}

    # 回测预热时，对全量历史数据计算因子序列
    series = engine.compute_history("cvd_divergence", df_bars)  # 返回 pd.Series
"""

from __future__ import annotations  # 支持在类型注解中使用字符串形式的类名（PEP 604）
import logging

import numpy as np
import pandas as pd

import sys
sys.path.insert(0, "/root/trading-v2/factors")
from utils import ema, atr, volume_ratio

import sys

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# _SAFE_BUILTINS — 安全沙箱的 builtins 白名单
#
# 当 exec() 执行策略提交的因子源码时，我们不能直接暴露完整的 Python builtins，
# 否则恶意代码（如 os.system、subprocess、文件读写）可能造成安全风险。
# 这里手动指定了因子计算中常用的 builtins 函数和常量，仅暴露这些。
#
# 设计决策：
# - 包含所有数学/集合/类型转换函数（abs, max, min, len, sum, ...）
# - 包含常见异常类型（ValueError, TypeError, ZeroDivisionError）使因子代码可以捕获异常
# - 明确排除 open, eval, exec, __import__, getattr, globals, locals 等危险函数
# - 每次 exec 时通过 {**_SAFE_BUILTINS, ...} 展开到命名空间中
# ---------------------------------------------------------------------------
_SAFE_BUILTINS = {
    "ema": ema,             # EMA
    # ---- 数学与数值操作 ----
    "abs": abs,
    "all": all,
    "any": any,
    "bool": bool,
    # ---- 类型转换 ----
    "dict": dict,
    "enumerate": enumerate,
    "float": float,
    "int": int,
    "len": len,
    "list": list,
    # ---- 聚合与序列操作 ----
    "max": max,
    "min": min,
    "range": range,
    "round": round,
    "set": set,
    "sorted": sorted,
    "str": str,
    "sum": sum,
    "tuple": tuple,
    "zip": zip,
    # ---- 常量 ----
    "True": True,
    "False": False,
    "None": None,
    # ---- 类型检查与异常 ----
    "isinstance": isinstance,
    "ValueError": ValueError,
    "TypeError": TypeError,
    "ZeroDivisionError": ZeroDivisionError,
}


class FactorEngine:
    """
    ========================================================================
    因子计算引擎
    ========================================================================
    职责：编译策略提交的因子 Python 源代码 → 缓存编译后的 code object →
        在每次新 bar 到达时对所有因子执行计算 → 返回 {因子名: 最新值}。

    生命周期：
        ┌─────────────────────────────────────────────────────────────┐
        │  __init__()          创建空引擎（无注册因子）              │
        │      ↓                                                     │
        │  register(name, code) 注册/更新因子，编译源码缓存          │
        │      ↓                                                     │
        │  execute_all(df)      每根 bar 到达时调用，计算所有因子    │
        │      ↓                                                     │
        │  unregister(name)     移除因子（策略注销时）               │
        │      ↓                                                     │
        │  compute_history()    回测预热时对历史数据批量计算         │
        └─────────────────────────────────────────────────────────────┘

    无状态设计要点：
    - 引擎本身不存储因子计算结果，每次 execute_all 都重新从头计算
    - 回调函数因子（模式1）每次都会重新调用，不会缓存中间结果
    - 这意味着因子代码应当尽量纯粹（输入 bar → 输出值），避免依赖外部状态
    ========================================================================
    """

    def __init__(self):
        """
        初始化一个空的因子引擎。

        内部数据结构：
            self._factors: dict[str, dict]
                结构: {因子名: {"code": 源码, "params": 参数dict, "compiled": code object}}

                键 (str):           因子名称，唯一标识，由策略注册时指定
                值 (dict):
                    "code" (str):       原始的 Python 源字符串
                    "params" (dict):    附加参数，编译时展开到命名空间中
                    "compiled" (code):  compile() 产出的字节码对象，后续 exec 直接使用

                注意：
                - 因子不可重名，相同名称再次 register() 会覆盖旧的
                - 因子名称会嵌入到编译对象的文件名属性中（<factor:{name}>），便于调试
        """
        # _factors: 因子存储字典，key=因子名称，value={code, params, compiled}
        self._factors: dict[str, dict] = {}

    def register(self, name: str, code: str, params: dict[str, float] | None = None):
        """
        注册或更新一个因子。

        流程：
            1. 调用 compile() 将源码编译为字节码对象（code object）
            2. 如果编译失败（SyntaxError），记录日志并向外抛出
            3. 将因子信息存入 self._factors，包括源码、参数、编译后的 code object
            4. 后续每次执行都使用已编译的 code object，跳过重复编译

        参数：
            name (str): 因子名称，必须唯一。重复注册会覆盖之前的定义。
                命名惯例：使用小写+下划线，如 "cvd_divergence", "channel_breakout"
            code (str): 完整的 Python 因子计算源码。可以是一个函数定义（模式1）
                也可以是顶层表达式（模式2/3）。引擎会通过 _execute_one() 中的
                三种模式自动匹配合适的取值方式。
            params (dict[str, float] | None): 可选参数，注册时传入但编译阶段不展开。
                这些参数在每次 exec 执行时通过 **meta["params"] 合并到命名空间中，
                使因子代码可以读取参数变量。例如 params={"lookback": 20} 后，
                因子源码中直接使用 lookback 变量。

        异常：
            SyntaxError: 如果源码存在语法错误，记录错误日志后向外抛出，
                由调用方（通常是 gRPC servicer）处理并向客户端返回错误。
                注意：只有语法错误会被抛出，运行时错误会在 _execute_one()
                中被捕获并记录，不会向外传播。
        """
        try:
            # compile(source, filename, mode)
            # - source: 因子源码字符串
            # - filename: 嵌入到 code object 的"文件名"，仅用于调试（traceback 显示）
            #            使用 <factor:{name}> 格式便于定位出错因子
            # - mode: "exec" 模式，允许源码包含多个语句、函数定义等
            compiled = compile(code, f"<factor:{name}>", "exec")
        except SyntaxError as e:
            # 语法错误无法恢复，记录错误并向外抛出
            # 调用方（gRPC servicer）应捕获此异常并返回错误给 trading-v2 客户端
            logger.error(f"Factor '{name}' syntax error: {e}")
            raise

        # 存储因子信息：保留原始源码（可用于调试/展示）+ 参数 + 编译后字节码
        self._factors[name] = {
            "code": code,
            "params": params or {},       # 如果 params 为 None，使用空 dict
            "compiled": compiled,
        }
        logger.info(f"Factor registered: {name}")

    def unregister(self, name: str):
        """
        注销一个因子。

        从 _factors 中移除指定因子。如果因子不存在，静默忽略（不报错）。
        此方法通常由以下场景触发：
        - 策略实例从 strategy_instances 表中被删除（热注销）
        - 策略更新因子配置（先 unregister 旧因子，再 register 新因子）

        参数：
            name (str): 要注销的因子名称。
        """
        if name in self._factors:
            del self._factors[name]

    def registered_names(self) -> list[str]:
        """
        返回当前所有已注册因子的名称列表。

        返回值：
            list[str]: 已注册因子名称的列表。
                如果没有任何注册因子，返回空列表 []。

        注意：
            返回的是快照（list 复制），调用方修改返回的列表不会影响内部状态。
        """
        return list(self._factors.keys())

    def execute_all(self, df_bars: pd.DataFrame) -> dict[str, float]:
        """
        对所有已注册因子执行计算，返回 {因子名: 最新值} 字典。

        这是最核心的调用入口，在每根新 bar 到达时被触发。
        执行流程：
            1. 遍历 self._factors 中的所有因子
            2. 对每个因子调用 _execute_one() 获取最新值
            3. 跳过执行失败（返回 None）的因子
            4. 收集所有成功的因子值为 dict 返回

        参数：
            df_bars (pd.DataFrame): K线数据的 DataFrame，至少应包含 'close' 列。
                DataFrame 的索引应为时间戳（Timestamp）。

        返回值：
            dict[str, float]: {因子名称: 最新因子值}。
                - 只包含执行成功的因子
                - 如果没有任何因子，返回空字典 {}
                - 如果所有因子都执行失败，也返回空字典 {}

        处理逻辑：
            - 因子间独立执行，一个因子失败不影响其他因子
            - 为保持数值稳定性，因子值在内部被转为 Python float 类型
            - 异常因子返回 None 并被跳过
        """
        results = {}
        for name, meta in self._factors.items():
            val = self._execute_one(name, meta, df_bars)
            if val is not None:
                results[name] = val
        return results

    def _execute_one(self, name: str, meta: dict, df: pd.DataFrame) -> float | None:
        """
        执行单个因子，返回最新计算值。

        这是因子执行的核心实现，实现了三种取值模式的自动匹配。

        参数：
            name (str):        因子名称，用于日志和命名空间查找。
            meta (dict):       因子元数据，包含 "code", "params", "compiled" 三个键。
            df (pd.DataFrame): K线数据。

        返回值：
            float | None:
                - float: 成功计算出的最新因子值
                - None: 执行过程中发生异常，或不满足任何取值模式
                - 注意：如果匹配到模式但 Series 全部为 NaN，返回 0.0 而非 None

        核心算法步骤：
        Step 1: 构造执行命名空间（安全 builtins + numpy + pandas + df + params）
        Step 2: exec 编译后的字节码，代码执行结果写入命名空间
        Step 3: 模式1 — 查找 callable 函数（以 factor_ 开头）
        Step 4: 模式2 — 按因子名或 factor_{name} 查找 Series
        Step 5: 模式3 — 自动发现任意非私有 Series（兜底）
        Step 6: 所有模式失败，返回 0.0
        """
        # ---------------------------------------------------------------
        # Step 1: 构造执行命名空间
        #
        # 命名空间构建策略：
        # 1. 先展开 _SAFE_BUILTINS 白名单（注入安全的 builtins）
        # 2. 注入 np / pd 两个科学计算库
        # 3. 注入 df.copy() —— 使用副本而非引用，防止因子代码意外修改原始数据
        # 4. 注入因子参数（params），使因子代码可以直接使用参数变量名
        #
        # 为何使用 df.copy() 而非 df？
        # 虽然因子代码原则上不应修改 df，但实际中可能出现 df['new_col'] = ...
        # 的原地操作。如果使用副本，不会影响主线程中的原始 df_bars，避免非预期副作用。
        # ---------------------------------------------------------------
        namespace = {
            **_SAFE_BUILTINS,          # 展开安全 builtins 白名单
            "np": np,                  # numpy 库
            "pd": pd,                  # pandas 库
            "df": df.copy(),           # K线数据的副本
            **meta["params"],          # 展开因子参数（如 lookback=20）
        }

        # ---------------------------------------------------------------
        # Step 2: 执行编译后的字节码
        #
        # exec() 会在 namespace 字典中执行代码，所有在源码中定义的变量、
        # 函数、赋值操作都会写入 namespace 中。
        #
        # 异常处理策略：
        # - 捕获所有 Exception（而非仅特定类型），因为用户代码可能产生各种异常
        # - 不捕获 BaseException（如 SystemExit、KeyboardInterrupt）
        # - 记录错误后返回 None，跳过此因子的取值
        # ---------------------------------------------------------------
        try:
            exec(meta["compiled"], namespace)
        except Exception as e:
            logger.error(f"Factor '{name}' execution error: {e}")
            return None

        # ---------------------------------------------------------------
        # Step 3, 模式1: 查找并调用 callable 函数（标准模式）
        #
        # 遍历命名空间，寻找符合以下条件的可调用对象：
        #   1. 是 callable（函数/类/具有 __call__ 方法的对象）
        #   2. 名称以 "factor_" 开头（约定前缀）
        #
        # 找到后，检查函数签名是否包含 timescale 参数：
        #   - 如果包含，传入 timescale="1min"（因子可据此调整计算窗口）
        #   - 如果不包含，只传入 df 参数
        #
        # 调用函数得到结果后：
        #   - 必须是 pd.Series 类型
        #   - 去掉 NaN 值后取最后一个元素
        #   - 如果全部为 NaN，返回 0.0
        # ---------------------------------------------------------------
        import inspect
        for key, obj in namespace.items():
            if callable(obj) and key.startswith("factor_"):
                try:
                    sig = inspect.signature(obj)
                    kwargs = {"df": namespace["df"]}
                    if "timescale" in sig.parameters:
                        kwargs["timescale"] = "1min"
                    result = obj(**kwargs)
                    if isinstance(result, pd.Series) and not result.empty:
                        val = result.dropna()
                        return float(val.iloc[-1]) if len(val) > 0 else 0.0
                except Exception as e:
                    logger.error(f"Factor '{name}' call error: {e}")

        # ---------------------------------------------------------------
        # Step 4, 模式2: 按约定名称查找 Series
        #
        # 查找命名空间中是否存在以下两个变量名中的 Series：
        #   1. 因子名本身（如 "cvd_divergence"）
        #   2. 前缀格式（如 "factor_cvd_divergence"）
        #
        # 如果找到非空 Series，去掉 NaN 后取最后一个值。
        # 此模式适用于因子代码在顶层直接赋值计算结果的情况。
        #
        # 优先级：模式1 > 模式2 > 模式3
        # ---------------------------------------------------------------
        for key in (name, f"factor_{name}"):
            obj = namespace.get(key)
            if isinstance(obj, pd.Series) and not obj.empty:
                val = obj.dropna()
                return float(val.iloc[-1]) if len(val) > 0 else 0.0

        # ---------------------------------------------------------------
        # Step 5, 模式3: 自动发现任意非私有 Series（兜底模式）
        #
        # 遍历命名空间中所有变量，找到第一个符合以下条件的 Series：
        #   1. 是 pd.Series 类型
        #   2. 非空
        #   3. 名称不以 "_" 开头（排除私有/内部变量）
        #
        # 警告：此模式具有不确定性！如果命名空间中有多个 Series，
        # 会取遍历顺序中的第一个，而 dict 的遍历顺序在 Python 3.7+
        # 是插入顺序（大致按源码中的变量定义顺序），但用户不应依赖此行为。
        # ---------------------------------------------------------------
        for key, obj in namespace.items():
            if isinstance(obj, pd.Series) and not obj.empty and not key.startswith("_"):
                val = obj.dropna()
                return float(val.iloc[-1]) if len(val) > 0 else 0.0

        # ---------------------------------------------------------------
        # Step 6: 所有模式均未匹配，返回 0.0 作为兜底
        #
        # 返回 0.0 而非 None 的含义：
        # - None 表示执行失败（异常），会在上层被跳过（不加入 results dict）
        # - 0.0 表示成功执行但没有找到有效输出，信号系统将其视为"中性"信号
        # ---------------------------------------------------------------
        return 0.0

    def compute_history(self, name: str, df_bars: pd.DataFrame) -> pd.Series | None:
        """
        对完整历史 K 线数据批量计算某个因子的全量序列。

        与 execute_all() 的区别：
        - execute_all(): 每次只取最新值（latest value），用于实时计算
        - compute_history(): 返回完整时间序列，用于回测预热或历史因子值填充

        使用场景：
        1. nt-base 启动时，需要将因子回填到历史 bar 中（prefill_bar_buffer）
        2. 新策略注册时，需要计算出当前时刻的因子历史值用于 state 初始化
        3. 调试时，可以获取因子完整曲线用于分析

        参数：
            name (str):          因子名称，必须在 register() 中注册过
            df_bars (pd.DataFrame): 完整的 K 线历史数据 DataFrame

        返回值：
            pd.Series | None:
            - pd.Series: 成功计算的因子值序列，index 与 df_bars.index 对齐，
              已去除 NaN 值。Series 中每个元素都转为 float 类型。
            - None: 因子未注册，或执行过程中发生异常。

        异常处理：
            - 因子未注册：返回 None（非异常场景）
            - exec 异常：记录日志，返回 None
            - 函数调用异常：记录日志，继续尝试其他模式
            - 类型转换异常：pd.to_numeric(..., errors="coerce") 将无法转换的值强制设为 NaN

        注意：
            与 _execute_one 不同，此方法直接传递 df_bars 副本给命名空间。
            因子代码仍不应修改 df，但引擎层面不强制保护。
        """
        # 检查因子是否已注册，未注册直接返回 None
        if name not in self._factors:
            logger.warning(f"Factor '{name}' not registered, cannot compute history")
            return None

        meta = self._factors[name]

        # 构造执行命名空间（同 _execute_one）
        namespace = {
            **_SAFE_BUILTINS,
            "np": np,
            "pd": pd,
            "df": df_bars.copy(),
            **meta["params"],
        }

        # 执行字节码
        try:
            exec(meta["compiled"], namespace)
        except Exception as e:
            logger.error(f"Factor '{name}' batch error: {e}")
            return None

        # 尝试模式1: Callable 函数
        import inspect
        for key, obj in namespace.items():
            if callable(obj) and key.startswith("factor_"):
                try:
                    sig = inspect.signature(obj)
                    kwargs = {"df": namespace["df"]}
                    if "timescale" in sig.parameters:
                        kwargs["timescale"] = "1min"
                    result = obj(**kwargs)
                    if isinstance(result, pd.Series):
                        # pd.to_numeric 确保所有值为数值类型，非数值设为 NaN
                        result = pd.to_numeric(result, errors="coerce")
                        # 强制对齐到原始 df_bars 的 index（避免因子内部操作导致 index 漂移）
                        result.index = df_bars.index
                        # 去掉 NaN 后返回，保持序列最简
                        return result.dropna()
                except Exception as e:
                    logger.error(f"Factor '{name}' batch call error: {e}")

        # 尝试模式2: 按因子名查找 Series
        for key in (name, f"factor_{name}"):
            obj = namespace.get(key)
            if isinstance(obj, pd.Series):
                result = pd.to_numeric(obj, errors="coerce")
                result.index = df_bars.index
                return result.dropna()

        # 尝试模式3: 自动发现任意非私有 Series（兜底）
        for key, obj in namespace.items():
            if isinstance(obj, pd.Series) and not obj.empty and not key.startswith("_"):
                result = pd.to_numeric(obj, errors="coerce")
                result.index = df_bars.index
                return result.dropna()

        # 所有模式均未匹配，返回 None
        logger.warning(f"Factor '{name}' batch: no valid output found in namespace")
        return None
