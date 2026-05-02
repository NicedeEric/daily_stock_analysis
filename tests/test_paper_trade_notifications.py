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
                    },
                ],
                "summary": {"order_count": 2, "buy_count": 1, "sell_count": 1},
            }
        )

        self.assertIn("*Paper Reconcile Update*", message)
        self.assertIn("*Buy To Add*", message)
        self.assertIn("*Sell To Reduce*", message)
        self.assertIn("1. `AAPL", message)
        self.assertIn("2. `TSLA", message)
        self.assertIn("reason: executed", message)

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
