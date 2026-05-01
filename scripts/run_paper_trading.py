# -*- coding: utf-8 -*-
"""Run daily signal-driven paper trading for one strategy."""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from sqlalchemy import and_, select

from src.services.paper_trading_service import PaperTradingService
from src.storage import DatabaseManager, PaperStrategyDecision


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run signal-driven paper trading.")
    parser.add_argument("--strategy-name", default=os.getenv("PAPER_STRATEGY_NAME", "signal_portfolio"))
    parser.add_argument("--strategy-version", default=os.getenv("PAPER_STRATEGY_VERSION", "v1_us"))
    parser.add_argument("--initial-capital", type=float, default=float(os.getenv("PAPER_INITIAL_CAPITAL", "20000")))
    parser.add_argument("--market", default=os.getenv("PAPER_MARKET", "us"))
    parser.add_argument("--base-currency", default=os.getenv("PAPER_BASE_CURRENCY", "USD"))
    parser.add_argument("--run-date", default=os.getenv("PAPER_RUN_DATE", ""))
    parser.add_argument("--max-positions", type=int, default=int(os.getenv("PAPER_MAX_POSITIONS", "5")))
    parser.add_argument("--max-position-pct", type=float, default=float(os.getenv("PAPER_MAX_POSITION_PCT", "0.20")))
    parser.add_argument("--cash-reserve-pct", type=float, default=float(os.getenv("PAPER_CASH_RESERVE_PCT", "0.20")))
    parser.add_argument("--min-buy-score", type=int, default=int(os.getenv("PAPER_MIN_BUY_SCORE", "70")))
    parser.add_argument("--min-rule-score", type=int, default=int(os.getenv("PAPER_MIN_RULE_SCORE", "65")))
    parser.add_argument("--sell-score-threshold", type=int, default=int(os.getenv("PAPER_SELL_SCORE_THRESHOLD", "40")))
    parser.add_argument("--trade-fee-usd", type=float, default=float(os.getenv("PAPER_TRADE_FEE_USD", "1.3")))
    parser.add_argument("--slippage-bps", type=float, default=float(os.getenv("PAPER_SLIPPAGE_BPS", "5")))
    parser.add_argument("--lookback-days", type=int, default=int(os.getenv("PAPER_SIGNAL_LOOKBACK_DAYS", "3")))
    parser.add_argument("--notify", default=os.getenv("PAPER_NOTIFY", "false"))
    parser.add_argument("--output-json", default=os.getenv("PAPER_OUTPUT_JSON", "data/paper_trading_result.json"))
    return parser


def _parse_run_date(raw: str):
    candidate = (raw or "").strip()
    if not candidate:
        return None
    return datetime.strptime(candidate[:10], "%Y-%m-%d").date()


def _dump_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _as_bool(value: str) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _build_notify_message(strategy: Dict[str, Any], result: Dict[str, Any]) -> str:
    account = result.get("account_snapshot") or {}
    run_date = result.get("run_date") or "-"
    strategy_title = f"{strategy.get('strategy_name')}:{strategy.get('strategy_version')}"
    total_equity = account.get("total_equity")
    cash = account.get("cash")
    lines = [
        "Paper Trading Daily Update",
        f"Date: {run_date}",
        f"Strategy: {strategy_title}",
        f"Status: {result.get('status', '-')}",
        f"Executed: {result.get('executed', 0)} | Skipped: {result.get('skipped', 0)} | Errors: {result.get('errors', 0)}",
    ]
    if total_equity is not None:
        lines.append(f"Total Equity: {float(total_equity):.2f}")
    if cash is not None:
        lines.append(f"Cash: {float(cash):.2f}")
    return "\n".join(lines)


def _fetch_decisions_for_run(strategy: Dict[str, Any], result: Dict[str, Any]) -> list[Dict[str, Any]]:
    run_date = datetime.strptime(str(result.get("run_date"))[:10], "%Y-%m-%d").date()
    strategy_id = int(strategy["id"])
    db = DatabaseManager.get_instance()
    items: list[Dict[str, Any]] = []
    with db.get_session() as session:
        rows = session.execute(
            select(PaperStrategyDecision).where(
                and_(
                    PaperStrategyDecision.strategy_id == strategy_id,
                    PaperStrategyDecision.run_date == run_date,
                )
            )
        ).scalars().all()
    for row in rows:
        snapshot = {}
        reasons = []
        if getattr(row, "signal_snapshot_json", None):
            try:
                snapshot = json.loads(row.signal_snapshot_json) or {}
            except Exception:
                snapshot = {}
        if getattr(row, "reason_codes_json", None):
            try:
                reasons = json.loads(row.reason_codes_json) or []
            except Exception:
                reasons = []
        items.append(
            {
                "code": str(row.code),
                "action": str(row.action or "").lower(),
                "status": str(row.status or "").lower(),
                "final_score": snapshot.get("final_score"),
                "rule_score": snapshot.get("rule_score"),
                "final_decision": snapshot.get("final_decision"),
                "qty": getattr(row, "execution_quantity", None),
                "price": getattr(row, "execution_price", None),
                "reasons": reasons,
            }
        )
    return items


def _format_decisions_block(decisions: list[Dict[str, Any]], max_lines: int = 20) -> str:
    if not decisions:
        return "Decisions: none"
    buckets = {"buy": [], "sell": [], "skip": [], "error": []}
    for item in decisions:
        action = item.get("action") or "skip"
        if action not in buckets:
            action = "skip"
        buckets[action].append(item)
    lines = [
        f"Decisions: buy {len(buckets['buy'])} | sell {len(buckets['sell'])} | skip {len(buckets['skip'])} | error {len(buckets['error'])}",
    ]
    emitted = 1
    for action in ("buy", "sell", "skip", "error"):
        rows = buckets[action]
        if not rows:
            continue
        lines.append(f"[{action.upper()}]")
        emitted += 1
        for row in rows:
            reason = ",".join((row.get("reasons") or [])[:2]) or "-"
            qty = row.get("qty")
            price = row.get("price")
            qty_text = f"{float(qty):.0f}" if qty is not None else "-"
            price_text = f"{float(price):.2f}" if price is not None else "-"
            lines.append(
                f"{row['code']} score:{row.get('final_score','-')} rule:{row.get('rule_score','-')} qty:{qty_text} px:{price_text} reason:{reason}"
            )
            emitted += 1
            if emitted >= max_lines:
                lines.append("...truncated")
                return "\n".join(lines)
    return "\n".join(lines)


def main() -> int:
    args = _build_parser().parse_args()
    service = PaperTradingService()
    config_override = {
        "max_positions": args.max_positions,
        "max_position_pct": args.max_position_pct,
        "cash_reserve_pct": args.cash_reserve_pct,
        "min_buy_score": args.min_buy_score,
        "min_rule_score": args.min_rule_score,
        "sell_score_threshold": args.sell_score_threshold,
        "trade_fee_usd": args.trade_fee_usd,
        "slippage_bps": args.slippage_bps,
        "lookback_days": args.lookback_days,
        "market": args.market,
        "execution_mode": "next_open",
    }
    strategy = service.ensure_strategy(
        strategy_name=args.strategy_name,
        strategy_version=args.strategy_version,
        initial_capital=args.initial_capital,
        base_currency=args.base_currency,
        market=args.market,
        config_override=config_override,
    )
    result = service.run_daily(
        strategy_name=args.strategy_name,
        strategy_version=args.strategy_version,
        run_date=_parse_run_date(args.run_date),
    )
    payload = {"strategy": strategy, "result": result}
    output_path = Path(args.output_json)
    _dump_json(output_path, payload)
    print(f"paper_trading_strategy={strategy['strategy_name']}:{strategy['strategy_version']}")
    print(f"paper_trading_status={result.get('status')}")
    print(f"paper_trading_executed={result.get('executed')}")
    print(f"paper_trading_skipped={result.get('skipped')}")
    print(f"paper_trading_errors={result.get('errors')}")
    print(f"paper_trading_output={output_path.resolve()}")
    if _as_bool(args.notify):
        from src.notification import NotificationService
        decisions = _fetch_decisions_for_run(strategy, result)
        notify_message = _build_notify_message(strategy, result)
        notify_message = f"{notify_message}\n\n{_format_decisions_block(decisions)}"
        ok = NotificationService().send_to_telegram(notify_message)
        print(f"paper_trading_notify_telegram={'ok' if ok else 'failed'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
