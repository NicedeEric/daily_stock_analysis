# -*- coding: utf-8 -*-
"""Reconcile live portfolio vs paper target and emit delta orders."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List, Optional


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


def _normalize_live_prices(items: List[Dict[str, Any]]) -> Dict[str, float]:
    out: Dict[str, float] = {}
    for item in items or []:
        symbol = str(item.get("symbol") or item.get("code") or "").strip().upper()
        if not symbol:
            continue
        price_raw = item.get("last_price")
        if price_raw is None:
            price_raw = item.get("price")
        if price_raw is None:
            continue
        try:
            price = float(price_raw)
        except (TypeError, ValueError):
            continue
        if price > 0:
            out[symbol] = price
    return out


def _normalize_decisions(items: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for item in items or []:
        symbol = str(item.get("symbol") or item.get("code") or "").strip().upper()
        if not symbol:
            continue
        out[symbol] = item
    return out


def _to_float(value: Any) -> Optional[float]:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    return numeric


def _build_delta_orders(
    *,
    live_positions: Dict[str, float],
    live_prices: Dict[str, float],
    target_positions: Dict[str, float],
    decision_map: Dict[str, Dict[str, Any]],
    min_delta_shares: float,
) -> List[Dict[str, Any]]:
    symbols = sorted(set(live_positions) | set(target_positions))
    orders: List[Dict[str, Any]] = []
    for symbol in symbols:
        live_qty = float(live_positions.get(symbol, 0.0))
        target_qty = float(target_positions.get(symbol, 0.0))
        delta = target_qty - live_qty
        if abs(delta) < min_delta_shares:
            delta = 0.0
        decision = decision_map.get(symbol) or {}
        paper_action = str(decision.get("action") or "").strip().lower()
        paper_final_decision = str(decision.get("final_decision") or "").strip().lower()

        stop_loss = _to_float(decision.get("stop_loss"))
        take_profit = _to_float(decision.get("take_profit"))
        current_price = live_prices.get(symbol)
        if current_price is None:
            # Fallback for users who only pass quantity in LIVE_PORTFOLIO_JSON.
            current_price = _to_float(decision.get("analysis_close"))

        forced_target_qty: Optional[float] = None
        forced_reason: Optional[str] = None
        reason_priority = 0

        if (
            live_qty > 0
            and current_price is not None
            and stop_loss is not None
            and stop_loss > 0
            and current_price <= stop_loss
        ):
            forced_target_qty = 0.0
            forced_reason = "stop_loss_triggered"
            reason_priority = 1
        elif (
            live_qty > 0
            and current_price is not None
            and take_profit is not None
            and take_profit > 0
            and current_price >= take_profit
        ):
            forced_target_qty = 0.0
            forced_reason = "take_profit_triggered"
            reason_priority = 2
        elif live_qty > 0 and (paper_action == "sell" or paper_final_decision == "sell"):
            forced_target_qty = 0.0
            forced_reason = "strategy_sell_signal"
            reason_priority = 3

        effective_target_qty = target_qty
        if forced_target_qty is not None:
            effective_target_qty = forced_target_qty
        elif (
            target_qty <= 0
            and live_qty > 0
            and paper_final_decision in {"", "hold"}
            and paper_action not in {"sell"}
        ):
            # User-approved behavior: keep live holdings on hold/no-signal.
            effective_target_qty = live_qty

        delta = effective_target_qty - live_qty
        if abs(delta) < min_delta_shares:
            continue

        if delta > 0:
            side = "buy"
            order_qty = delta
            if forced_reason:
                reconcile_reason = forced_reason
            elif paper_action == "buy" or paper_final_decision == "buy":
                reconcile_reason = "strategy_buy_signal"
                reason_priority = 4
            else:
                reconcile_reason = "target_rebalance"
                reason_priority = 5
        else:
            side = "sell"
            order_qty = abs(delta)
            if forced_reason:
                reconcile_reason = forced_reason
            elif paper_action == "sell" or paper_final_decision == "sell":
                reconcile_reason = "strategy_sell_signal"
                reason_priority = 3
            else:
                reconcile_reason = "target_rebalance"
                reason_priority = 5

        # Keep execution list clean when tiny floating deltas round to zero shares.
        order_qty_int = int(round(order_qty))
        if order_qty_int <= 0:
            continue

        orders.append(
            {
                "symbol": symbol,
                "side": side,
                "order_qty": order_qty_int,
                "live_qty": live_qty,
                "target_qty": target_qty,
                "effective_target_qty": effective_target_qty,
                "delta_qty": delta,
                "reason": reconcile_reason,
                "reason_priority": reason_priority,
                "live_price": current_price,
                "paper_action": decision.get("action"),
                "paper_status": decision.get("status"),
                "paper_final_decision": decision.get("final_decision"),
                "paper_final_score": decision.get("final_score"),
                "paper_rule_score": decision.get("rule_score"),
                "paper_reasons": decision.get("reasons") or [],
                "paper_analysis_close": decision.get("analysis_close"),
                "paper_ideal_buy": decision.get("ideal_buy"),
                "paper_secondary_buy": decision.get("secondary_buy"),
                "paper_signal_date": decision.get("signal_date"),
                "paper_stop_loss": stop_loss,
                "paper_take_profit": take_profit,
                "paper_position_advice": decision.get("position_advice") or {},
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
        default=0.0,
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

    live_items = live.get("positions") or []
    live_positions = _normalize_positions(live_items)
    live_prices = _normalize_live_prices(live_items)
    target_positions = _normalize_positions(
        (((paper.get("result") or {}).get("account_snapshot") or {}).get("positions") or [])
    )
    decision_map = _normalize_decisions(paper.get("decisions") or [])

    orders = _build_delta_orders(
        live_positions=live_positions,
        live_prices=live_prices,
        target_positions=target_positions,
        decision_map=decision_map,
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
