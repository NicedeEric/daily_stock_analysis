"""Formatting helpers for paper trading and reconcile notifications."""

from __future__ import annotations

from typing import Any, Dict, Iterable, List


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _fmt_currency(value: Any, default: str = "-") -> str:
    if value is None:
        return default
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    return f"${number:,.2f}"


def _fmt_int(value: Any, default: str = "-") -> str:
    if value is None:
        return default
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    return f"{int(round(number))}"


def _fmt_pct(value: Any, digits: int = 1, default: str = "-") -> str:
    if value is None:
        return default
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    return f"{number:.{digits}f}%"


def _build_action_summary(decisions: Iterable[Dict[str, Any]]) -> Dict[str, int]:
    summary = {"buy": 0, "sell": 0, "hold": 0, "skip": 0, "other": 0}
    for row in decisions:
        action = str(row.get("action") or "").strip().lower()
        if action not in summary:
            action = "other"
        summary[action] += 1
    return summary


def _humanize_reason(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    return text.replace("_", " ")


def build_paper_trading_message(
    *,
    strategy: Dict[str, Any],
    result: Dict[str, Any],
    decisions: List[Dict[str, Any]],
    max_action_rows: int = 10,
    max_position_rows: int = 8,
) -> str:
    account = result.get("account_snapshot") or {}
    total_equity = _as_float(account.get("total_equity"))
    cash = _as_float(account.get("total_cash"))
    market_value = _as_float(account.get("total_market_value"))
    realized_pnl = _as_float(account.get("realized_pnl"))
    unrealized_pnl = _as_float(account.get("unrealized_pnl"))
    exposure_pct = (market_value / total_equity * 100.0) if total_equity > 0 else 0.0
    cash_pct = (cash / total_equity * 100.0) if total_equity > 0 else 0.0
    strategy_title = f"{strategy.get('strategy_name') or '-'} / {strategy.get('strategy_version') or '-'}"
    action_summary = _build_action_summary(decisions)

    lines = [
        "*Paper Trading Daily Update*",
        "",
        f"*Date* {result.get('run_date') or '-'}    *Status* {result.get('status') or '-'}",
        f"*Strategy* `{strategy_title}`",
        (
            f"*Signals* {int(result.get('signals') or 0)}    "
            f"*Planned* B {int(result.get('planned_buys') or 0)} / S {int(result.get('planned_sells') or 0)}    "
            f"*Executed* {int(result.get('executed') or 0)}"
        ),
        (
            f"*Skipped* {int(result.get('skipped') or 0)}    "
            f"*Errors* {int(result.get('errors') or 0)}"
        ),
        (
            f"*Equity* {_fmt_currency(total_equity)}    *Cash* {_fmt_currency(cash)} "
            f"({_fmt_pct(cash_pct)})"
        ),
        (
            f"*Exposure* {_fmt_pct(exposure_pct)}    *PnL* "
            f"{_fmt_currency(realized_pnl + unrealized_pnl)}"
        ),
        (
            f"*PnL Split* Realized {_fmt_currency(realized_pnl)} / "
            f"Unrealized {_fmt_currency(unrealized_pnl)}"
        ),
        "",
        "*Action Mix*",
        (
            f"- Buy {action_summary['buy']} | Sell {action_summary['sell']} | "
            f"Hold {action_summary['hold']} | Skip {action_summary['skip']}"
        ),
        "",
        "*Today's Orders*",
    ]

    actionable = [
        row for row in decisions if str(row.get("action") or "").lower() in {"buy", "sell", "skip"}
    ]
    actionable.sort(
        key=lambda row: (
            0 if str(row.get("action") or "").lower() in {"buy", "sell"} else 1,
            -(int(row.get("final_score") or 0)),
            str(row.get("code") or ""),
        )
    )
    if not actionable:
        lines.append("- No actionable orders today. Portfolio stays unchanged.")
    else:
        for idx, row in enumerate(actionable[:max_action_rows], start=1):
            action = str(row.get("action") or "-").upper()
            code = str(row.get("code") or "-")
            qty = _fmt_int(row.get("qty"))
            price = _fmt_currency(row.get("price"))
            notional = _fmt_currency(row.get("notional"))
            score = row.get("final_score", "-")
            rule_score = row.get("rule_score", "-")
            reason_list = [_humanize_reason(item) for item in (row.get("reasons") or []) if str(item).strip()]
            reason_text = ", ".join(reason_list[:2]) if reason_list else "signal review"
            lines.append(
                f"{idx}. {action} {code} | qty {qty} | px {price} | value {notional} | "
                f"score {score} / rule {rule_score}"
            )
            lines.append(f"   reason: {reason_text}")
        if len(actionable) > max_action_rows:
            lines.append(f"- ... {len(actionable) - max_action_rows} more rows omitted")

    positions = account.get("positions") or []
    positions = sorted(
        positions,
        key=lambda item: _as_float(item.get("market_value_base") or item.get("market_value"), 0.0),
        reverse=True,
    )
    lines.extend(["", "*Current Positions*"])
    if not positions:
        lines.append("- No open paper positions.")
    else:
        for idx, position in enumerate(positions[:max_position_rows], start=1):
            symbol = position.get("symbol") or "-"
            qty = _as_float(position.get("quantity"))
            avg_cost = _as_float(position.get("avg_cost"))
            last_price = _as_float(position.get("last_price"))
            market_val = _as_float(position.get("market_value_base") or position.get("market_value"))
            weight_pct = (market_val / total_equity * 100.0) if total_equity > 0 else 0.0
            pnl_pct = ((last_price - avg_cost) / avg_cost * 100.0) if avg_cost > 0 else 0.0
            lines.append(
                f"{idx}. {symbol} | qty {_fmt_int(qty)} | last {_fmt_currency(last_price)} | "
                f"avg {_fmt_currency(avg_cost)} | pnl {_fmt_pct(pnl_pct, digits=2)} | "
                f"wt {_fmt_pct(weight_pct, digits=2)}"
            )
        if len(positions) > max_position_rows:
            lines.append(f"- ... {len(positions) - max_position_rows} more positions omitted")

    return "\n".join(lines)


def build_reconcile_message(payload: Dict[str, Any], max_rows: int = 20) -> str:
    summary = payload.get("summary") or {}
    strategy = payload.get("strategy") or {}
    orders = list(payload.get("delta_orders") or [])
    buys = [row for row in orders if str(row.get("side") or "").lower() == "buy"]
    sells = [row for row in orders if str(row.get("side") or "").lower() == "sell"]

    lines = [
        "*Paper Reconcile Update*",
        "",
        f"*Live As-Of* {payload.get('live_as_of') or '-'}    *Paper Date* {payload.get('paper_run_date') or '-'}",
        f"*Strategy* `{(strategy.get('name') or '-')} / {(strategy.get('version') or '-')}`",
        (
            f"*Orders* {int(summary.get('order_count') or 0)} total    "
            f"*Buy* {int(summary.get('buy_count') or 0)}    "
            f"*Sell* {int(summary.get('sell_count') or 0)}"
        ),
        "",
    ]

    if not orders:
        lines.append("*Delta Orders*")
        lines.append("- Live portfolio already matches paper target.")
        return "\n".join(lines)

    def _append_order_section(title: str, items: List[Dict[str, Any]], current_rows: int) -> int:
        if not items:
            return current_rows
        lines.append(f"*{title}*")
        for row in items:
            if current_rows >= max_rows:
                break
            current_rows += 1
            lines.append(
                f"{current_rows}. {str(row.get('symbol') or '-').upper()} | "
                f"qty {_fmt_int(row.get('order_qty'))} | "
                f"live {_fmt_int(row.get('live_qty'))} -> target {_fmt_int(row.get('target_qty'))}"
            )
        lines.append("")
        return current_rows

    shown = 0
    shown = _append_order_section("Buy To Add", buys, shown)
    shown = _append_order_section("Sell To Reduce", sells, shown)
    if len(orders) > shown:
        lines.append(f"- ... {len(orders) - shown} more orders omitted")
    return "\n".join(lines).rstrip()
