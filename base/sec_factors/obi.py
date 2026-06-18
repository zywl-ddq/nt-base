# -*- coding: utf-8 -*-
"""
base/sec_factors/obi.py — Order Book Imbalance（订单簿失衡）秒级因子
==================================================================
取 L2 盘口 top N 档，计算 (Σbid_qty − Σask_qty) / (Σbid_qty + Σask_qty)，
归一化到 [-1, 1]：>0 买盘挂单占优，<0 卖盘占优。

示例因子（验证框架用），可替换/删除。
"""
from __future__ import annotations


class OBIFactor:
    def __init__(self, interval_sec: int = 3, symbol: str = "SOLUSDT-PERP", levels: int = 5):
        self.name = "obi"
        self.interval_sec = interval_sec
        self.symbol = symbol
        self.levels = levels
        self._book: dict = {}  # 最近一次盘口快照 {bids: {price:size}, asks: {price:size}}

    def on_tick(self, tick) -> None:
        pass  # OBI 基于盘口，不用 tick

    def on_book(self, books: dict) -> None:
        """books = dm_actor._books（dict[sid, {bids, asks}]）。取本 symbol 的盘口。"""
        book = books.get(self.symbol) if books else None
        if book is None and books:
            # 兼容 sid 后缀（如 SOLUSDT-PERP.BINANCE）或单 symbol 系统：取第一个
            book = next(iter(books.values()))
        self._book = book or {}

    def compute(self) -> float:
        bids = self._book.get("bids", {})
        asks = self._book.get("asks", {})
        # bid 价格降序（高价位在前），ask 价格升序（低价位在前），各取 top N
        top_bids = sorted(bids.items(), key=lambda x: -x[0])[: self.levels]
        top_asks = sorted(asks.items(), key=lambda x: x[0])[: self.levels]
        bid_qty = sum(float(s) for _, s in top_bids)
        ask_qty = sum(float(s) for _, s in top_asks)
        total = bid_qty + ask_qty
        if total <= 0:
            return 0.0
        return (bid_qty - ask_qty) / total
