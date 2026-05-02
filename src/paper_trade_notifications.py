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


def _clip(text: Any, width: int) -> str:
    raw = str(text or "")
    if len(raw) <= width:
        return raw.ljust(width)
    if width <= 1:
        return raw[:width]
    return f"{raw[:width - 1]}…"


def _code_line(columns: List[tuple[Any, int]]) -> str:
    return "`" + " ".join(_clip(value, width) for value, width in columns) + "`"


def _build_reason_detail(row: Dict[str, Any]) -> str:
    reasons = [_humanize_reason(item) for item in (row.get("reasons") or []) if str(item).strip()]
    if not reasons:
        return "signal review"

    if "missing_entry_price" in {str(item).strip() for item in (row.get("reasons") or [])}:
        detail_parts: List[str] = ["missing entry price"]
        analysis_close = row.get("analysis_close")
        ideal_buy = row.get("ideal_buy")
        secondary_buy = row.get("secondary_buy")
        signal_date = row.get("signal_date")
        if analysis_close is not None:
            label = "last close"
            if signal_date:
                label = f"last close ({signal_date})"
            detail_parts.append(f"{label} {_fmt_currency(analysis_close)}")
        if ideal_buy is not None:
            detail_parts.append(f"ideal buy {_fmt_currency(ideal_buy)}")
        if secondary_buy is not None:
            detail_parts.append(f"secondary buy {_fmt_currency(secondary_buy)}")
        return " | ".join(detail_parts)

    return ", ".join(reasons[:2])


def _build_reconcile_reason_detail(row: Dict[str, Any]) -> str:
    reasons = [_humanize_reason(item) for item in (row.get("paper_reasons") or []) if str(item).strip()]
    action = str(row.get("paper_action") or "").strip().lower()
    final_decision = str(row.get("paper_final_decision") or "").strip().lower()
    status = str(row.get("paper_status") or "").strip().lower()
    side = str(row.get("side") or "").strip().lower()
    reconcile_reason = str(row.get("reason") or "").strip().lower()

    if reconcile_reason == "stop_loss_triggered":
        detail = "stop loss triggered"
        stop_loss = row.get("paper_stop_loss")
        live_price = row.get("live_price")
        if stop_loss is not None and live_price is not None:
            detail = f"{detail} ({_fmt_currency(live_price)} <= {_fmt_currency(stop_loss)})"
    elif reconcile_reason == "take_profit_triggered":
        detail = "take profit triggered"
        take_profit = row.get("paper_take_profit")
        live_price = row.get("live_price")
        if take_profit is not None and live_price is not None:
            detail = f"{detail} ({_fmt_currency(live_price)} >= {_fmt_currency(take_profit)})"
    elif reconcile_reason == "strategy_sell_signal":
        if reasons:
            detail = ", ".join(reasons[:2])
        elif action:
            detail = f"paper action {action}"
        else:
            detail = f"paper decision {final_decision or 'sell'}"
    elif reconcile_reason == "strategy_buy_signal":
        if reasons:
            detail = ", ".join(reasons[:2])
        elif action:
            detail = f"paper action {action}"
        else:
            detail = f"paper decision {final_decision or 'buy'}"
    elif action == side or final_decision == side:
        if reasons:
            detail = ", ".join(reasons[:2])
        elif action:
            detail = f"paper action {action}"
        else:
            detail = f"paper decision {final_decision}"
    elif reconcile_reason == "target_rebalance":
        detail = "live position differs from paper target sizing"
    elif reasons:
        detail = ", ".join(reasons[:2])
    else:
        detail = "rebalance to paper target"

    extras: List[str] = []
    if final_decision and reconcile_reason in {"target_rebalance", "hold_no_trigger"}:
        extras.append(f"decision {final_decision}")
    if status and reconcile_reason in {"strategy_sell_signal", "strategy_buy_signal"}:
        extras.append(f"status {status}")
    return " | ".join([detail] + extras) if extras else detail


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
        lines.append(
            _code_line(
                [
                    ("ACT", 4),
                    ("CODE", 6),
                    ("QTY", 5),
                    ("PRICE", 9),
                    ("VALUE", 10),
                    ("SCORE", 5),
                    ("RULE", 4),
                ]
            )
        )
        for idx, row in enumerate(actionable[:max_action_rows], start=1):
            action = str(row.get("action") or "-").upper()
            code = str(row.get("code") or "-")
            qty = _fmt_int(row.get("qty"))
            price = _fmt_currency(row.get("price"))
            notional = _fmt_currency(row.get("notional"))
            score = row.get("final_score", "-")
            rule_score = row.get("rule_score", "-")
            reason_text = _build_reason_detail(row)
            lines.append(
                f"{idx}. "
                + _code_line(
                    [
                        (action, 4),
                        (code, 6),
                        (qty, 5),
                        (price, 9),
                        (notional, 10),
                        (score, 5),
                        (rule_score, 4),
                    ]
                )
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
        lines.append(
            _code_line(
                [
                    ("CODE", 6),
                    ("QTY", 5),
                    ("LAST", 9),
                    ("AVG", 9),
                    ("PNL%", 7),
                    ("WT%", 7),
                ]
            )
        )
        for idx, position in enumerate(positions[:max_position_rows], start=1):
            symbol = position.get("symbol") or "-"
            qty = _as_float(position.get("quantity"))
            avg_cost = _as_float(position.get("avg_cost"))
            last_price = _as_float(position.get("last_price"))
            market_val = _as_float(position.get("market_value_base") or position.get("market_value"))
            weight_pct = (market_val / total_equity * 100.0) if total_equity > 0 else 0.0
            pnl_pct = ((last_price - avg_cost) / avg_cost * 100.0) if avg_cost > 0 else 0.0
            lines.append(
                f"{idx}. "
                + _code_line(
                    [
                        (symbol, 6),
                        (_fmt_int(qty), 5),
                        (_fmt_currency(last_price), 9),
                        (_fmt_currency(avg_cost), 9),
                        (_fmt_pct(pnl_pct, digits=2), 7),
                        (_fmt_pct(weight_pct, digits=2), 7),
                    ]
                )
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
        lines.append(
            _code_line(
                [
                    ("CODE", 6),
                    ("QTY", 5),
                    ("LIVE", 5),
                    ("TARGET", 6),
                    ("SCORE", 5),
                    ("RULE", 4),
                ]
            )
        )
        for row in items:
            if current_rows >= max_rows:
                break
            current_rows += 1
            lines.append(
                f"{current_rows}. "
                + _code_line(
                    [
                        (str(row.get("symbol") or "-").upper(), 6),
                        (_fmt_int(row.get("order_qty")), 5),
                        (_fmt_int(row.get("live_qty")), 5),
                        (_fmt_int(row.get("target_qty")), 6),
                        (row.get("paper_final_score", "-"), 5),
                        (row.get("paper_rule_score", "-"), 4),
                    ]
                )
            )
            lines.append(f"   reason: {_build_reconcile_reason_detail(row)}")
        lines.append("")
        return current_rows

    shown = 0
    shown = _append_order_section("Buy To Add", buys, shown)
    shown = _append_order_section("Sell To Reduce", sells, shown)
    if len(orders) > shown:
        lines.append(f"- ... {len(orders) - shown} more orders omitted")
    return "\n".join(lines).rstrip()
