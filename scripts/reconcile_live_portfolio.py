# -*- coding: utf-8 -*-
"""Reconcile live portfolio vs paper target and emit delta orders."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List


def _load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _normalize_positions(items: List[Dict[str, Any]]) -> Dict[str, float]:
    out: Dict[str, float] = {}
    for item in items or []:
        symbol = str(item.get("symbol") or item.get("code") or "").strip().upper()
        if not symbol:
            continue
        qty = float(item.get("quantity") or item.get("qty") or 0.0)
        if qty == 0:
            continue
        out[symbol] = qty
    return out


def _build_delta_orders(
    *,
    live_positions: Dict[str, float],
    target_positions: Dict[str, float],
    min_delta_shares: float,
) -> List[Dict[str, Any]]:
    symbols = sorted(set(live_positions) | set(target_positions))
    orders: List[Dict[str, Any]] = []
    for symbol in symbols:
        live_qty = float(live_positions.get(symbol, 0.0))
        target_qty = float(target_positions.get(symbol, 0.0))
        delta = target_qty - live_qty
        if abs(delta) < min_delta_shares:
            continue
        if delta > 0:
            side = "buy"
            order_qty = delta
        else:
            side = "sell"
            order_qty = abs(delta)
        orders.append(
            {
                "symbol": symbol,
                "side": side,
                "order_qty": int(order_qty),
                "live_qty": live_qty,
                "target_qty": target_qty,
                "delta_qty": delta,
                "reason": "rebalance_to_paper_target",
            }
        )
    return orders


def main() -> int:
    parser = argparse.ArgumentParser(description="Reconcile live portfolio with paper target.")
    parser.add_argument("--live-json", required=True, help="Path to live portfolio snapshot JSON.")
    parser.add_argument(
        "--paper-json",
        default="data/paper_trading_result.json",
        help="Path to paper result JSON (default: data/paper_trading_result.json).",
    )
    parser.add_argument(
        "--min-delta-shares",
        type=float,
        default=1.0,
        help="Ignore tiny deltas below this share amount.",
    )
    parser.add_argument(
        "--output-json",
        default="data/reconcile_orders.json",
        help="Output path for delta orders JSON.",
    )
    args = parser.parse_args()

    live = _load_json(Path(args.live_json))
    paper = _load_json(Path(args.paper_json))

    live_positions = _normalize_positions(live.get("positions") or [])
    target_positions = _normalize_positions(
        (((paper.get("result") or {}).get("account_snapshot") or {}).get("positions") or [])
    )

    orders = _build_delta_orders(
        live_positions=live_positions,
        target_positions=target_positions,
        min_delta_shares=max(0.0, float(args.min_delta_shares)),
    )

    payload = {
        "live_as_of": live.get("as_of"),
        "paper_run_date": (paper.get("result") or {}).get("run_date"),
        "strategy": {
            "name": (paper.get("strategy") or {}).get("strategy_name"),
            "version": (paper.get("strategy") or {}).get("strategy_version"),
        },
        "live_positions": live_positions,
        "target_positions": target_positions,
        "delta_orders": orders,
        "summary": {
            "order_count": len(orders),
            "buy_count": sum(1 for o in orders if o["side"] == "buy"),
            "sell_count": sum(1 for o in orders if o["side"] == "sell"),
        },
    }

    out_path = Path(args.output_json)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"reconcile_orders={len(orders)}")
    print(f"reconcile_output={out_path.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
