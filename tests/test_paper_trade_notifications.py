from datetime import date
import unittest

from src.paper_trade_notifications import (
    build_paper_trading_message,
    build_reconcile_message,
)
from src.services.paper_trading_service import PaperStrategyConfig, PaperTradingService


class PaperTradeNotificationsTestCase(unittest.TestCase):
    def test_build_paper_trading_message_includes_actionable_sections(self):
        message = build_paper_trading_message(
            strategy={"strategy_name": "signal_portfolio", "strategy_version": "v1_us"},
            result={
                "run_date": "2026-05-02",
                "status": "ok",
                "signals": 5,
                "planned_buys": 1,
                "planned_sells": 1,
                "executed": 2,
                "skipped": 1,
                "errors": 0,
                "account_snapshot": {
                    "total_equity": 20100,
                    "total_cash": 4100,
                    "total_market_value": 16000,
                    "realized_pnl": 80,
                    "unrealized_pnl": 120,
                    "positions": [
                        {
                            "symbol": "NVDA",
                            "quantity": 12,
                            "avg_cost": 150,
                            "last_price": 165,
                            "market_value_base": 1980,
                        }
                    ],
                },
            },
            decisions=[
                {
                    "code": "NVDA",
                    "action": "buy",
                    "final_score": 84,
                    "rule_score": 78,
                    "qty": 3,
                    "price": 165.12,
                    "notional": 495.36,
                    "reasons": ["rule_score_ok", "cash_ready"],
                },
                {
                    "code": "AAPL",
                    "action": "skip",
                    "final_score": 69,
                    "rule_score": 63,
                    "qty": None,
                    "price": None,
                    "notional": None,
                    "reasons": ["already_held_or_no_slot"],
                },
            ],
        )

        self.assertIn("*Paper Trading Daily Update*", message)
        self.assertIn("*Action Mix*", message)
        self.assertIn("`ACT", message)
        self.assertIn("1. `BUY", message)
        self.assertIn("reason: rule score ok, cash ready", message)
        self.assertIn("*Strategy* `signal_portfolio / v1_us`", message)
        self.assertIn("*Current Positions*", message)
        self.assertIn("`CODE", message)
        self.assertNotIn("| Date | Strategy |", message)

    def test_build_paper_trading_message_shows_missing_entry_context(self):
        message = build_paper_trading_message(
            strategy={"strategy_name": "signal_portfolio", "strategy_version": "v1_us"},
            result={
                "run_date": "2026-05-02",
                "status": "ok",
                "signals": 1,
                "planned_buys": 1,
                "planned_sells": 0,
                "executed": 0,
                "skipped": 1,
                "errors": 0,
                "account_snapshot": {
                    "total_equity": 20000,
                    "total_cash": 20000,
                    "total_market_value": 0,
                    "realized_pnl": 0,
                    "unrealized_pnl": 0,
                    "positions": [],
                },
            },
            decisions=[
                {
                    "code": "GEV",
                    "action": "buy",
                    "final_score": 81,
                    "rule_score": 77,
                    "qty": None,
                    "price": None,
                    "notional": None,
                    "analysis_close": 412.35,
                    "ideal_buy": 405.0,
                    "secondary_buy": 398.5,
                    "signal_date": "2026-05-01",
                    "reasons": ["missing_entry_price"],
                }
            ],
        )

        self.assertIn("missing entry price", message)
        self.assertIn("last close (2026-05-01) $412.35", message)
        self.assertIn("ideal buy $405.00", message)
        self.assertIn("secondary buy $398.50", message)

    def test_build_reconcile_message_groups_buy_and_sell(self):
        message = build_reconcile_message(
            {
                "live_as_of": "2026-05-02",
                "paper_run_date": "2026-05-02",
                "strategy": {"name": "signal_portfolio", "version": "v1_us"},
                "delta_orders": [
                    {
                        "symbol": "AAPL",
                        "side": "buy",
                        "order_qty": 5,
                        "live_qty": 0,
                        "target_qty": 5,
                        "paper_final_score": 78,
                        "paper_rule_score": 70,
                        "paper_action": "buy",
                        "paper_status": "executed",
                        "paper_final_decision": "buy",
                        "paper_reasons": ["executed"],
                        "live_price": 188.52,
                        "live_price_source": "paper_analysis_close",
                        "paper_ideal_buy": 186.0,
                        "paper_secondary_buy": 182.5,
                        "paper_signal_date": "2026-05-01",
                        "paper_position_advice": {
                            "no_position": "enter on controlled pullbacks near the buy zone",
                        },
                    },
                    {
                        "symbol": "TSLA",
                        "side": "sell",
                        "order_qty": 2,
                        "live_qty": 4,
                        "target_qty": 2,
                        "paper_final_score": 39,
                        "paper_rule_score": 44,
                        "paper_action": "sell",
                        "paper_status": "executed",
                        "paper_final_decision": "sell",
                        "paper_reasons": ["executed"],
                        "live_price": 171.25,
                        "live_price_source": "last_price",
                        "live_avg_cost": 193.4,
                        "paper_stop_loss": 175.0,
                        "paper_take_profit": 215.0,
                        "paper_position_advice": {
                            "has_position": "cut the position if price keeps trading below the stop",
                        },
                    },
                ],
                "summary": {"order_count": 2, "buy_count": 1, "sell_count": 1},
            }
        )

        self.assertIn("*Paper Reconcile Update*", message)
        self.assertIn("*Buy To Add*", message)
        self.assertIn("*Sell To Reduce*", message)
        self.assertIn("*Action Context*", message)
        self.assertIn("`CODE   QTY   LIVE  TARGET SL", message)
        self.assertIn("1. `AAPL", message)
        self.assertIn("2. `TSLA", message)
        self.assertIn("$175.00", message)
        self.assertIn("$215.00", message)
        self.assertIn("reason: executed", message)
        self.assertIn("status: PX* $188.52 | EN $186.00 / $182.50 | SIG 2026-05-01 | paper-close fallback", message)
        self.assertIn("status: PX $171.25 | AVG $193.40 | SL $175.00 | TP $215.00", message)
        self.assertIn("advice: enter on controlled pullbacks near the buy zone", message)
        self.assertIn("advice: cut the position if price keeps trading below the stop", message)

    def test_build_reconcile_message_distinguishes_target_mismatch_from_sell_signal(self):
        message = build_reconcile_message(
            {
                "live_as_of": "2026-05-02",
                "paper_run_date": "2026-05-02",
                "strategy": {"name": "signal_portfolio", "version": "v1_us"},
                "delta_orders": [
                    {
                        "symbol": "MSFT",
                        "side": "sell",
                        "order_qty": 3,
                        "live_qty": 3,
                        "target_qty": 0,
                        "reason": "target_rebalance",
                        "paper_final_score": 66,
                        "paper_rule_score": 62,
                        "paper_action": "skip",
                        "paper_status": "skipped",
                        "paper_final_decision": "hold",
                        "paper_reasons": [],
                    }
                ],
                "summary": {"order_count": 1, "buy_count": 0, "sell_count": 1},
            }
        )

        self.assertIn("live position differs from paper target sizing", message)
        self.assertIn("decision hold", message)
        self.assertNotIn("paper target has no position", message)

    def test_build_reconcile_message_includes_position_advice_for_holders(self):
        message = build_reconcile_message(
            {
                "live_as_of": "2026-05-02",
                "paper_run_date": "2026-05-02",
                "strategy": {"name": "signal_portfolio", "version": "v1_us"},
                "delta_orders": [
                    {
                        "symbol": "AAPL",
                        "side": "sell",
                        "order_qty": 1,
                        "live_qty": 2,
                        "target_qty": 0,
                        "reason": "strategy_sell_signal",
                        "paper_action": "sell",
                        "paper_final_decision": "sell",
                        "paper_reasons": ["signal_flip"],
                        "paper_position_advice": {
                            "no_position": "wait for clearer setup",
                            "has_position": "reduce position and lock gains",
                        },
                    }
                ],
                "summary": {"order_count": 1, "buy_count": 0, "sell_count": 1},
            }
        )
        self.assertIn("advice: reduce position and lock gains", message)

    def test_build_reconcile_message_includes_live_position_status_and_levels(self):
        message = build_reconcile_message(
            {
                "live_as_of": "2026-05-05",
                "paper_run_date": "2026-05-05",
                "strategy": {"name": "signal_portfolio", "version": "v1_us"},
                "delta_orders": [],
                "signal_rows": [
                    {
                        "symbol": "CRWD",
                        "live_qty": 4,
                        "target_qty": 4,
                        "live_price": 455.64,
                        "live_price_source": "last_price",
                        "live_avg_cost": 456.19,
                        "paper_final_score": 72,
                        "paper_rule_score": 66,
                        "paper_final_decision": "hold",
                        "paper_stop_loss": 430.0,
                        "paper_take_profit": 490.0,
                        "paper_position_advice": {
                            "has_position": "hold above the stop and scale out near target",
                        },
                    }
                ],
                "summary": {"order_count": 0, "buy_count": 0, "sell_count": 0},
            }
        )
        self.assertIn("*Live Positions*", message)
        self.assertIn("status: PX $455.64 | AVG $456.19 | SL $430.00 | TP $490.00", message)
        self.assertIn("advice: hold above the stop and scale out near target", message)

    def test_build_reconcile_message_includes_entry_watchlist_and_price_fallback(self):
        message = build_reconcile_message(
            {
                "live_as_of": "2026-05-05",
                "paper_run_date": "2026-05-05",
                "strategy": {"name": "signal_portfolio", "version": "v1_us"},
                "delta_orders": [],
                "signal_rows": [
                    {
                        "symbol": "GEV",
                        "live_qty": 0,
                        "target_qty": 1,
                        "live_price": 1072.09,
                        "live_price_source": "paper_analysis_close",
                        "paper_final_score": 73,
                        "paper_rule_score": 71,
                        "paper_final_decision": "buy",
                        "paper_ideal_buy": 1072.09,
                        "paper_secondary_buy": 1065.24,
                        "paper_signal_date": "2026-05-02",
                        "paper_position_advice": {
                            "no_position": "wait for entry near the buy zone",
                        },
                    }
                ],
                "summary": {"order_count": 0, "buy_count": 0, "sell_count": 0},
            }
        )
        self.assertIn("*Entry Watchlist*", message)
        self.assertIn("status: PX* $1,072.09 | EN $1,072.09 / $1,065.24 | SIG 2026-05-02 | paper-close fallback", message)
        self.assertIn("advice: wait for entry near the buy zone", message)

    def test_analysis_close_execution_mode_uses_analysis_close_price(self):
        service = PaperTradingService.__new__(PaperTradingService)
        price = service._resolve_entry_price(
            {
                "code": "GEV",
                "signal_date": date(2026, 5, 1),
                "analysis_close": 1072.09,
            },
            run_date=date(2026, 5, 2),
            config=PaperStrategyConfig(execution_mode="analysis_close"),
        )
        self.assertEqual(price, 1072.09)


if __name__ == "__main__":
    unittest.main()
