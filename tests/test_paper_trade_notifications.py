import unittest

from src.reports.paper_trade_notifications import (
    build_paper_trading_message,
    build_reconcile_message,
)


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
        self.assertIn("1. BUY NVDA", message)
        self.assertIn("reason: rule score ok, cash ready", message)
        self.assertIn("*Strategy* `signal_portfolio / v1_us`", message)
        self.assertIn("*Current Positions*", message)
        self.assertNotIn("| Date | Strategy |", message)

    def test_build_reconcile_message_groups_buy_and_sell(self):
        message = build_reconcile_message(
            {
                "live_as_of": "2026-05-02",
                "paper_run_date": "2026-05-02",
                "strategy": {"name": "signal_portfolio", "version": "v1_us"},
                "delta_orders": [
                    {"symbol": "AAPL", "side": "buy", "order_qty": 5, "live_qty": 0, "target_qty": 5},
                    {"symbol": "TSLA", "side": "sell", "order_qty": 2, "live_qty": 4, "target_qty": 2},
                ],
                "summary": {"order_count": 2, "buy_count": 1, "sell_count": 1},
            }
        )

        self.assertIn("*Paper Reconcile Update*", message)
        self.assertIn("*Buy To Add*", message)
        self.assertIn("*Sell To Reduce*", message)
        self.assertIn("1. AAPL | qty 5 | live 0 -> target 5", message)
        self.assertIn("2. TSLA | qty 2 | live 4 -> target 2", message)


if __name__ == "__main__":
    unittest.main()
