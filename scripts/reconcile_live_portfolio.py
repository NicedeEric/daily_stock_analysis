# -*- coding: utf-8 -*-
"""Reconcile live portfolio vs paper target and emit delta orders."""

from __future__ import annotations

import argparse
import json
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from sqlalchemy import desc, or_, select

from src.storage import AnalysisHistory, DatabaseManager


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


def _normalize_live_meta(items: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for item in items or []:
        symbol = str(item.get("symbol") or item.get("code") or "").strip().upper()
        if not symbol:
            continue
        meta = out.setdefault(symbol, {})

        price_raw = item.get("last_price")
        price_source = "last_price"
        if price_raw is None:
            price_raw = item.get("price")
            price_source = "price"
        if price_raw is not None:
            try:
                price = float(price_raw)
            except (TypeError, ValueError):
                price = None
            if price is not None and price > 0:
                meta["live_price"] = price
                meta["live_price_source"] = price_source

        avg_raw = item.get("avg_cost")
        if avg_raw is None:
            avg_raw = item.get("average_cost")
        if avg_raw is None:
            avg_raw = item.get("cost_basis_per_share")
        if avg_raw is not None:
            try:
                avg_cost = float(avg_raw)
            except (TypeError, ValueError):
                avg_cost = None
            if avg_cost is not None and avg_cost > 0:
                meta["live_avg_cost"] = avg_cost
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


def _parse_iso_date(value: Any) -> Optional[date]:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return datetime.strptime(raw[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


def _extract_position_advice(raw_result: Any) -> Dict[str, str]:
    if isinstance(raw_result, dict):
        payload = raw_result
    elif raw_result:
        try:
            payload = json.loads(raw_result)
        except Exception:
            payload = {}
    else:
        payload = {}
    if not isinstance(payload, dict):
        return {}
    dashboard = payload.get("dashboard")
    core = dashboard.get("core_conclusion") if isinstance(dashboard, dict) else None
    pos = core.get("position_advice") if isinstance(core, dict) else None
    if not isinstance(pos, dict):
        return {}
    out: Dict[str, str] = {}
    no_position = str(pos.get("no_position") or "").strip()
    has_position = str(pos.get("has_position") or "").strip()
    if no_position:
        out["no_position"] = no_position
    if has_position:
        out["has_position"] = has_position
    return out


def _history_to_signal(row: AnalysisHistory) -> Dict[str, Any]:
    return {
        "code": str(getattr(row, "code", "") or "").upper(),
        "final_score": getattr(row, "final_score", None),
        "rule_score": getattr(row, "rule_score", None),
        "final_decision": getattr(row, "final_decision", None),
        "ideal_buy": getattr(row, "ideal_buy", None),
        "secondary_buy": getattr(row, "secondary_buy", None),
        "stop_loss": getattr(row, "stop_loss", None),
        "take_profit": getattr(row, "take_profit", None),
        "analysis_close": getattr(row, "analysis_close", None),
        "signal_date": getattr(row, "analysis_date", None).isoformat() if getattr(row, "analysis_date", None) else None,
        "position_advice": _extract_position_advice(getattr(row, "raw_result", None)),
    }


def _needs_history_fill(item: Dict[str, Any]) -> bool:
    return any(
        item.get(key) in (None, "", [])
        for key in (
            "final_score",
            "rule_score",
            "stop_loss",
            "take_profit",
            "ideal_buy",
            "secondary_buy",
            "signal_date",
        )
    )


def _merge_signal(base: Dict[str, Any], fallback: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(base)
    for key, value in fallback.items():
        if merged.get(key) in (None, "", []):
            merged[key] = value
    existing_advice = merged.get("position_advice")
    if not isinstance(existing_advice, dict) or not existing_advice:
        merged["position_advice"] = fallback.get("position_advice") or {}
    return merged


def _load_history_map(symbols: Iterable[str], paper_run_date: Optional[date]) -> Dict[str, Dict[str, Any]]:
    requested = sorted({str(symbol or "").strip().upper() for symbol in symbols if str(symbol or "").strip()})
    if not requested:
        return {}

    db = DatabaseManager.get_instance()
    history_map: Dict[str, Dict[str, Any]] = {}
    with db.get_session() as session:
        for symbol in requested:
            conditions = [AnalysisHistory.code == symbol]
            if paper_run_date is not None:
                conditions.append(
                    or_(
                        AnalysisHistory.analysis_date.is_(None),
                        AnalysisHistory.analysis_date <= paper_run_date,
                    )
                )
            row = session.execute(
                select(AnalysisHistory)
                .where(*conditions)
                .order_by(
                    desc(AnalysisHistory.analysis_date),
                    desc(AnalysisHistory.created_at),
                )
                .limit(1)
            ).scalars().first()
            if row is not None:
                history_map[symbol] = _history_to_signal(row)
    return history_map


def _enrich_decision_map(
    decision_map: Dict[str, Dict[str, Any]],
    *,
    symbols: Iterable[str],
    paper_run_date: Optional[date],
) -> Dict[str, Dict[str, Any]]:
    out = {str(symbol).upper(): dict(item) for symbol, item in (decision_map or {}).items()}
    history_map = _load_history_map(symbols, paper_run_date)
    for symbol in {str(symbol or "").strip().upper() for symbol in symbols if str(symbol or "").strip()}:
        current = out.get(symbol) or {"code": symbol}
        if symbol not in out or _needs_history_fill(current):
            fallback = history_map.get(symbol)
            if fallback:
                out[symbol] = _merge_signal(current, fallback)
    return out


def _build_delta_orders(
    *,
    live_positions: Dict[str, float],
    live_meta: Optional[Dict[str, Dict[str, Any]]] = None,
    live_prices: Optional[Dict[str, float]] = None,
    target_positions: Dict[str, float],
    decision_map: Dict[str, Dict[str, Any]],
    min_delta_shares: float,
) -> List[Dict[str, Any]]:
    symbols = sorted(set(live_positions) | set(target_positions))
    orders: List[Dict[str, Any]] = []
    live_meta = live_meta or {}
    live_prices = live_prices or {}
    for symbol in symbols:
        live_qty = float(live_positions.get(symbol, 0.0))
        target_qty = float(target_positions.get(symbol, 0.0))
        delta = target_qty - live_qty
        if abs(delta) < min_delta_shares:
            delta = 0.0
        decision = decision_map.get(symbol) or {}
        paper_action = str(decision.get("action") or "").strip().lower()
        paper_final_decision = str(decision.get("final_decision") or "").strip().lower()
        meta = live_meta.get(symbol) or {}

        stop_loss = _to_float(decision.get("stop_loss"))
        take_profit = _to_float(decision.get("take_profit"))
        current_price = meta.get("live_price")
        live_price_source = meta.get("live_price_source")
        if current_price is None:
            current_price = live_prices.get(symbol)
            if current_price is not None:
                live_price_source = "last_price"
        if current_price is None:
            # Fallback for users who only pass quantity in LIVE_PORTFOLIO_JSON.
            current_price = _to_float(decision.get("analysis_close"))
            if current_price is not None:
                live_price_source = "paper_analysis_close"

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
                "live_price_source": live_price_source,
                "live_avg_cost": meta.get("live_avg_cost"),
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


def _build_signal_rows(
    *,
    live_positions: Dict[str, float],
    live_meta: Dict[str, Dict[str, Any]],
    target_positions: Dict[str, float],
    decision_map: Dict[str, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    symbols = sorted(set(live_positions) | set(target_positions) | set(decision_map))
    for symbol in symbols:
        decision = decision_map.get(symbol) or {}
        live_qty = float(live_positions.get(symbol, 0.0))
        target_qty = float(target_positions.get(symbol, 0.0))
        meta = live_meta.get(symbol) or {}
        live_price = meta.get("live_price")
        live_price_source = meta.get("live_price_source")
        if live_price is None:
            live_price = _to_float(decision.get("analysis_close"))
            if live_price is not None:
                live_price_source = "paper_analysis_close"

        rows.append(
            {
                "symbol": symbol,
                "live_qty": live_qty,
                "target_qty": target_qty,
                "live_price": live_price,
                "live_price_source": live_price_source,
                "live_avg_cost": meta.get("live_avg_cost"),
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
                "paper_stop_loss": _to_float(decision.get("stop_loss")),
                "paper_take_profit": _to_float(decision.get("take_profit")),
                "paper_position_advice": decision.get("position_advice") or {},
            }
        )
    return rows


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
    paper_run_date = _parse_iso_date((paper.get("result") or {}).get("run_date"))

    live_items = live.get("positions") or []
    live_positions = _normalize_positions(live_items)
    live_meta = _normalize_live_meta(live_items)
    target_positions = _normalize_positions(
        (((paper.get("result") or {}).get("account_snapshot") or {}).get("positions") or [])
    )
    decision_map = _enrich_decision_map(
        _normalize_decisions(paper.get("decisions") or []),
        symbols=set(live_positions) | set(target_positions),
        paper_run_date=paper_run_date,
    )

    orders = _build_delta_orders(
        live_positions=live_positions,
        live_meta=live_meta,
        target_positions=target_positions,
        decision_map=decision_map,
        min_delta_shares=max(0.0, float(args.min_delta_shares)),
    )
    signal_rows = _build_signal_rows(
        live_positions=live_positions,
        live_meta=live_meta,
        target_positions=target_positions,
        decision_map=decision_map,
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
        "signal_rows": signal_rows,
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
