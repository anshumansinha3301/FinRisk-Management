from __future__ import annotations
import asyncio
import collections
import time
import typing as t

def now_ns() -> int:
    return time.perf_counter_ns()

class RiskManager:
    def __init__(self, position_limit: float = 1_000_000.0, loss_limit: float = 100_000.0):
        self.position_limit = position_limit
        self.loss_limit = loss_limit
        self.positions: dict[str, float] = {}
        self.pnl: float = 0.0

    def allowed(self, symbol: str, size: float, price: float) -> bool:
        pos = self.positions.get(symbol, 0.0)
        projected = pos + size
        if abs(projected) > self.position_limit:
            return False
        projected_pnl = self.pnl - abs(size) * price * 0.001
        if projected_pnl < -self.loss_limit:
            return False
        return True

    def apply_fill(self, symbol: str, size: float, price: float, side: str) -> None:
        self.positions[symbol] = self.positions.get(symbol, 0.0) + size if side == "BUY" else self.positions.get(symbol, 0.0) - size
        self.pnl += size * price if side == "SELL" else -size * price

class OrderGateway:
    async def send_order(self, symbol: str, size: float, side: str, price: float | None = None) -> dict:
        raise NotImplementedError

class DemoOrderGateway(OrderGateway):
    def __init__(self, latency_ns_mean: int = 50_000):
        self.latency_ns_mean = latency_ns_mean
    async def send_order(self, symbol: str, size: float, side: str, price: float | None = None) -> dict:
        await asyncio.sleep(self.latency_ns_mean / 1e9)
        return {"status": "ACK","symbol": symbol,"size": size,"side": side,"price": price,"ts_ns": now_ns()}

class MarketEvent(t.TypedDict):
    ts_ns: int
    symbol: str
    bid: float
    ask: float
    seq: int

class RingBuffer:
    def __init__(self, size: int = 4096):
        self._dq = collections.deque(maxlen=size)
    def put(self, item) -> None:
        self._dq.append(item)
    def get_batch(self, max_items: int = 64) -> list:
        out = []
        for _ in range(min(max_items, len(self._dq))):
            out.append(self._dq.popleft())
        return out
    def __len__(self) -> int:
        return len(self._dq)

async def simple_strategy(feed: RingBuffer, gateway: OrderGateway, risk: RiskManager):
    spread_threshold = 0.5
    while True:
        batch = feed.get_batch(16)
        for event in batch:
            spread = event["ask"] - event["bid"]
            if spread > spread_threshold and risk.allowed(event["symbol"], 1, event["ask"]):
                ack = await gateway.send_order(event["symbol"], 1, "BUY", event["ask"])
                risk.apply_fill(event["symbol"], 1, event["ask"], "BUY")
        await asyncio.sleep(0)

import random
async def demo_feed_producer(feed: RingBuffer, symbol: str = "XYZ"):
    seq = 0
    while True:
        seq += 1
        mid = 100 + random.uniform(-1, 1)
        event: MarketEvent = {"ts_ns": now_ns(),"symbol": symbol,"bid": mid - 0.25,"ask": mid + 0.25 + random.uniform(0, 1),"seq": seq}
        feed.put(event)
        await asyncio.sleep(0.001)

async def main_demo():
    feed = RingBuffer(4096)
    gateway = DemoOrderGateway()
    risk = RiskManager()
    await asyncio.gather(demo_feed_producer(feed), simple_strategy(feed, gateway, risk))

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--demo", action="store_true")
    args = parser.parse_args()
    if args.demo:
        asyncio.run(main_demo())
    else:
        print("Run with --demo to test locally.")
