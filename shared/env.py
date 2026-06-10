"""
Module:    shared/env
Purpose:   Centralized environment configuration loader.
           Reads .env file once at import time, exposes typed AppCfg dataclass.
           All other modules MUST import cfg from here — never call os.getenv directly.

Interface: cfg: AppCfg           — typed configuration singleton
           assert_required()     — raises RuntimeError if critical secrets missing

Configuration Sections:
  BinanceCfg     — exchange API credentials
  TelegramCfg    — bot token and admin chat
  TimescaleCfg   — database host/port/user/password, exposes .dsn property
  LLMCfg         — LLM provider/model/API key
  AppCfg         — top-level: mode, symbols, risk params, sub-configs

Security:
  Secrets sourced from environment only (os.environ).
  .env file parsed defensively (strips inline comments, respects existing env vars).
  assert_required() must be called at every entrypoint before trading begins.

Author:    nt-base system
Version:   1.0.0
"""
from __future__ import annotations
"""
集中式环境配置加载器。

本模块在导入时自动读取 /root/nt-base/.env 文件，将配置项加载到操作系统的环境变量中，
然后通过一组带类型的 dataclass 对外暴露配置对象。

核心设计原则：
  1. 所有其他模块必须从本模块导入 cfg 对象来访问配置，禁止直接调用 os.getenv。
  2. 配置加载在模块导入时完成（_load_env_file 在模块底部被调用），保证全局单例。
  3. 使用冻结 dataclass（frozen=True）保证配置在运行期不可修改。
  4. .env 文件的解析策略为"已有环境变量优先"，不会覆盖系统已设置的值。
"""


import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

# 项目根目录：/root/nt-base（由当前文件 shared/env.py 向上两层得到）
ROOT = Path(__file__).resolve().parent.parent
# .env 配置文件路径：/root/nt-base/.env
ENV_FILE = ROOT / ".env"


def _load_env_file(path: Path) -> None:
    """
    轻量级 .env 文件解析器（无外部依赖）。

    解析规则（由安全到宽松排序）：
      1. 如果文件不存在，直接返回（静默跳过）。
      2. 跳过空行和以 # 开头的注释行。
      3. 跳过不包含 = 的行（格式不合法）。
      4. 用 = 分割 key 和 value（最多分割一次，允许 value 中包含 =）。
      5. 对 key 做 strip 去除前后空白。
      6. 对 value 做 lstrip 去除左侧空白（保留原始缩进风格）。
      7. 如果 value 不是以引号开头（单引号或双引号），则视为非引用值，
         查找其中的 # 字符作为行内注释的开始并截断。
      8. 对 value 做 strip 并移除首尾的引号（单引号或双引号）。
      9. 安全性检查：只在 os.environ 中不存在该 key 或现有的值包含 # 时才覆写。
         这防止了 systemd EnvironmentFile 解析器遗留的行内注释问题
         （systemd 不会去除值中的 # 注释后缀）。
    """
    if not path.exists():
        return
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        k, v = line.split("=", 1)
        k = k.strip()
        v = v.lstrip()
        # 去除行内注释：只有非引用值（不以引号开头的）才做此处理
        if not (v.startswith('"') or v.startswith("'")):
            # 任何 # 都视为注释开始（非引用值的场景）
            hash_idx = v.find("#")
            if hash_idx >= 0:
                v = v[:hash_idx]
        v = v.strip().strip('"').strip("'")
        if not k:
            continue
        # 防御性处理：如果 systemd 的 EnvironmentFile 解析器在值中遗留了行内注释
        # （systemd 不会去除值中 ' #...' 的部分），用我们清理后的值覆盖。
        existing = os.environ.get(k)
        if existing is None or "#" in existing:
            os.environ[k] = v


# === 模块导入时自动加载 .env 文件 ===
# 这保证了所有后续导入 shared.env 的模块都能立即获取到完整的配置。
_load_env_file(ENV_FILE)


def _get(key: str, default: str = "") -> str:
    """
    从环境变量中读取字符串值，如果不存在返回默认值。
    参数:
        key: 环境变量名
        default: 默认值（空字符串）
    返回:
        str: 环境变量的值，或默认值
    """
    return os.environ.get(key, default)


def _getint(key: str, default: int) -> int:
    """
    从环境变量中读取整数值，如果不存在或无法解析返回默认值。
    参数:
        key: 环境变量名
        default: 默认值
    返回:
        int: 解析后的整数，或默认值
    """
    try:
        return int(os.environ.get(key, str(default)))
    except ValueError:
        return default


def _getfloat(key: str, default: float) -> float:
    """
    从环境变量中读取浮点数值，如果不存在或无法解析返回默认值。
    参数:
        key: 环境变量名
        default: 默认值
    返回:
        float: 解析后的浮点数，或默认值
    """
    try:
        return float(os.environ.get(key, str(default)))
    except ValueError:
        return default


@dataclass(frozen=True)
class BinanceCfg:
    """
    Binance 交易所 API 配置（冻结 dataclass）。
    字段说明:
        api_key:    Binance API Key（环境变量: BINANCE_API_KEY）
        api_secret: Binance API Secret（环境变量: BINANCE_API_SECRET）
    安全说明: 密钥只从环境变量读取，不会硬编码在代码中。
              在生产环境中应通过 systemd EnvironmentFile 或手动 export 设置。
    """
    api_key: str = field(default_factory=lambda: _get("BINANCE_API_KEY"))
    api_secret: str = field(default_factory=lambda: _get("BINANCE_API_SECRET"))


@dataclass(frozen=True)
class TelegramCfg:
    """
    Telegram 通知机器人配置（冻结 dataclass）。
    字段说明:
        bot_token:     Telegram Bot Token（环境变量: TELEGRAM_BOT_TOKEN）
                       BotFather 创建机器人时获取。
        admin_chat_id: 管理员聊天 ID（环境变量: TELEGRAM_ADMIN_CHAT_ID）
                       通知消息发送的目标 chat ID，一般为管理员个人或群组。
    """
    bot_token: str = field(default_factory=lambda: _get("TELEGRAM_BOT_TOKEN"))
    admin_chat_id: int = field(default_factory=lambda: _getint("TELEGRAM_ADMIN_CHAT_ID", 0))


@dataclass(frozen=True)
class TimescaleCfg:
    """
    TimescaleDB 数据库连接配置（冻结 dataclass）。
    字段说明:
        host:     数据库主机地址（环境变量: TS_HOST，默认: 127.0.0.1 本地 Docker 容器）
        port:     数据库端口（环境变量: TS_PORT，默认: 5432 PostgreSQL 标准端口）
        user:     数据库用户名（环境变量: TS_USER，默认: nautilus_admin）
        password: 数据库密码（环境变量: TS_PASSWORD，无默认值，必须配置）
        database: 数据库名（环境变量: TS_DB，默认: trading_data）
    属性:
        dsn: 返回完整的 PostgreSQL 连接字符串
             postgresql://user:password@host:port/database
             用于 asyncpg.create_pool() 建立数据库连接池。
    """
    host: str = field(default_factory=lambda: _get("TS_HOST", "127.0.0.1"))
    port: int = field(default_factory=lambda: _getint("TS_PORT", 5432))
    user: str = field(default_factory=lambda: _get("TS_USER", "nautilus_admin"))
    password: str = field(default_factory=lambda: _get("TS_PASSWORD"))
    database: str = field(default_factory=lambda: _get("TS_DB", "trading_data"))

    @property
    def dsn(self) -> str:
        """
        返回完整的 PostgreSQL 连接字符串（DSN）。
        格式: postgresql://user:password@host:port/database
        用于: asyncpg.create_pool(dsn=...)
        """
        return (
            f"postgresql://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )


@dataclass(frozen=True)
class LLMCfg:
    """
    LLM（大语言模型）服务配置（冻结 dataclass）。
    字段说明:
        provider:          LLM 提供商名称（环境变量: LLM_PROVIDER，默认: zhipu 智谱AI）
        model:             模型名称（环境变量: LLM_MODEL，默认: glm-5.1 智谱GLM-5.1）
        api_key:           API 密钥（环境变量: ZHIPU_API_KEY，无默认值，必须配置）
        base_url:          API 请求基础 URL（环境变量: ZHIPU_BASE_URL）
                           默认: https://open.bigmodel.cn/api/paas/v4 智谱开放平台
        monthly_budget_usd: 每月预算上限 USD（环境变量: LLM_MONTHLY_BUDGET_USD，默认: 50.0）
    """
    provider: str = field(default_factory=lambda: _get("LLM_PROVIDER", "zhipu"))
    model: str = field(default_factory=lambda: _get("LLM_MODEL", "glm-5.1"))
    api_key: str = field(default_factory=lambda: _get("ZHIPU_API_KEY"))
    base_url: str = field(
        default_factory=lambda: _get(
            "ZHIPU_BASE_URL", "https://open.bigmodel.cn/api/paas/v4"
        )
    )
    monthly_budget_usd: float = field(
        default_factory=lambda: _getfloat("LLM_MONTHLY_BUDGET_USD", 50.0)
    )


@dataclass(frozen=True)
class AppCfg:
    """
    应用顶层配置（冻结 dataclass）。
    这是所有子配置的容器，对外暴露为全局单例 cfg。

    交易模式:
        mode:           "sandbox" 或 "live"（环境变量: MODE，默认: sandbox）
        trading_mode:   "live" / "sandbox" / "data_only"（环境变量: TRADING_MODE，默认: live）
                        data_only 模式只采集行情和入库，不下单。

    初始资金:
        sandbox_initial_usdt: 沙盘初始 USDT 资金（环境变量: SANDBOX_INITIAL_USDT，默认: 1000.0）
        live_initial_usdt:    实盘初始 USDT 资金（环境变量: LIVE_INITIAL_USDT，默认: 1000.0）

    交易品种:
        primary_symbol:  主交易品种（环境变量: PRIMARY_SYMBOL，默认: SOLUSDT）
        data_symbols:    行情数据订阅品种列表（环境变量: DATA_SYMBOLS，逗号分隔的字符串）
                         例如: "SOLUSDT,BTCUSDT" 表示同时订阅 SOL 和 BTC 的行情。

    费用和风控:
        taker_fee:            吃单手续费率（环境变量: TAKER_FEE，默认: 0.0005 = 0.05%）
        max_daily_symbols:    每日最大交易品种数（环境变量: MAX_DAILY_SYMBOLS，默认: 2）
        hard_floor_equity_ratio: 硬性止损线（环境变量: HARD_FLOOR_EQUITY_RATIO，默认: 0.5）
                                 当权益低于初始资金的 50% 时触发硬止损。

    扫描器配置（用于热注册策略的品种扫描）:
        scanner_top_n:       扫描 TOP N 品种（环境变量: SCANNER_TOP_N，默认: 5）
        scanner_utc_hour:    扫描执行 UTC 小时（环境变量: SCANNER_UTC_HOUR，默认: 12）
        scanner_utc_minute:  扫描执行 UTC 分钟（环境变量: SCANNER_UTC_MINUTE，默认: 0）

    循环间隔:
        infer_interval_sec:  因子推理间隔秒数（环境变量: INFER_INTERVAL_SEC，默认: 5）
        check_interval_sec:  风控检查间隔秒数（环境变量: CHECK_INTERVAL_SEC，默认: 1）

    RD-Agent 相关:
        rdagent_loop_hours:  RD-Agent 循环间隔小时数（环境变量: RDAGENT_LOOP_HOURS，默认: 6）
        nautilus_py:        Nautilus Python 解释器路径（环境变量: NAUTILUS_PY）
                            默认: /root/miniconda3/envs/nautilus/bin/python
        rdagent_py:         RD-Agent Python 解释器路径（环境变量: RDAGENT_PY）
                            默认: /root/miniconda3/envs/rdagent/bin/python

    子配置实例（嵌套 dataclass）:
        binance:   BinanceCfg 实例  — Binance 交易所 API 配置
        telegram:  TelegramCfg 实例 — Telegram 通知配置
        timescale: TimescaleCfg 实例 — 数据库连接配置
        llm:       LLMCfg 实例      — 大语言模型配置
    """
    mode: Literal["sandbox", "live"] = field(
        default_factory=lambda: _get("MODE", "sandbox")  # type: ignore[arg-type]
    )
    trading_mode: Literal["live", "sandbox", "data_only"] = field(
        default_factory=lambda: _get("TRADING_MODE", "live")  # type: ignore[arg-type]
    )
    sandbox_initial_usdt: float = field(
        default_factory=lambda: _getfloat("SANDBOX_INITIAL_USDT", 1000.0)
    )
    live_initial_usdt: float = field(
        default_factory=lambda: _getfloat("LIVE_INITIAL_USDT", 1000.0)
    )

    primary_symbol: str = field(
        default_factory=lambda: _get("PRIMARY_SYMBOL", "SOLUSDT")
    )
    data_symbols: tuple[str, ...] = field(
        default_factory=lambda: tuple(
            s.strip() for s in _get("DATA_SYMBOLS", "").split(",") if s.strip()
        )
    )
    taker_fee: float = field(default_factory=lambda: _getfloat("TAKER_FEE", 0.0005))
    max_daily_symbols: int = field(
        default_factory=lambda: _getint("MAX_DAILY_SYMBOLS", 2)
    )
    scanner_top_n: int = field(default_factory=lambda: _getint("SCANNER_TOP_N", 5))
    scanner_utc_hour: int = field(default_factory=lambda: _getint("SCANNER_UTC_HOUR", 12))
    scanner_utc_minute: int = field(
        default_factory=lambda: _getint("SCANNER_UTC_MINUTE", 0)
    )
    infer_interval_sec: int = field(
        default_factory=lambda: _getint("INFER_INTERVAL_SEC", 5)
    )
    check_interval_sec: int = field(
        default_factory=lambda: _getint("CHECK_INTERVAL_SEC", 1)
    )

    rdagent_loop_hours: int = field(
        default_factory=lambda: _getint("RDAGENT_LOOP_HOURS", 6)
    )
    hard_floor_equity_ratio: float = field(
        default_factory=lambda: _getfloat("HARD_FLOOR_EQUITY_RATIO", 0.5)
    )

    nautilus_py: str = field(
        default_factory=lambda: _get(
            "NAUTILUS_PY", "/root/miniconda3/envs/nautilus/bin/python"
        )
    )
    rdagent_py: str = field(
        default_factory=lambda: _get(
            "RDAGENT_PY", "/root/miniconda3/envs/rdagent/bin/python"
        )
    )

    binance: BinanceCfg = field(default_factory=BinanceCfg)
    telegram: TelegramCfg = field(default_factory=TelegramCfg)
    timescale: TimescaleCfg = field(default_factory=TimescaleCfg)
    llm: LLMCfg = field(default_factory=LLMCfg)


# === 全局单例配置对象 ===
# 模块导入后立即创建，所有其他模块通过 from shared.env import cfg 获取。
cfg = AppCfg()


def assert_required() -> None:
    """
    检查关键密钥是否已配置，在入口点（main.py）中调用。

    检查项:
        1. Binance API Key 和 Secret: 如果为空则交易无法执行。
        2. Telegram Bot Token 和 Admin Chat ID: 如果为空则通知无法发送。
        3. TimescaleDB 密码: 如果为空则数据库连接失败。

    如果任一缺失，抛出 RuntimeError 异常，阻止服务启动。
    这是在系统启动时进行的安全检查，避免在运行时才发现配置不全。
    """
    missing: list[str] = []
    if not cfg.binance.api_key or not cfg.binance.api_secret:
        missing.append("BINANCE_API_KEY/SECRET")
    if not cfg.telegram.bot_token or not cfg.telegram.admin_chat_id:
        missing.append("TELEGRAM_BOT_TOKEN/ADMIN_CHAT_ID")
    if not cfg.timescale.password:
        missing.append("TS_PASSWORD")
    if missing:
        raise RuntimeError(f"Missing env vars: {', '.join(missing)}")
