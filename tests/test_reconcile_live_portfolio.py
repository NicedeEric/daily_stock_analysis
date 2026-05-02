import unittest

from scripts.reconcile_live_portfolio import _build_delta_orders


class ReconcileLivePortfolioTestCase(unittest.TestCase):
    def test_reconcile_generates_sell_on_strategy_sell_signal(self):
        orders = _build_delta_orders(
            live_positions={"TSLA": 4},
            live_prices={"TSLA": 90},
            target_positions={"TSLA": 0},
            decision_map={
                "TSLA": {
                    "action": "sell",
                    "final_decision": "sell",
                    "final_score": 39,
                    "rule_score": 44,
                }
            },
            min_delta_shares=1.0,
        )
        self.assertEqual(len(orders), 1)
        order = orders[0]
        self.assertEqual(order["side"], "sell")
        self.assertEqual(order["order_qty"], 4)
        self.assertEqual(order["reason"], "strategy_sell_signal")

    def test_reconcile_generates_sell_on_take_profit_even_when_hold(self):
        orders = _build_delta_orders(
            live_positions={"GEV": 5},
            live_prices={"GEV": 120},
            target_positions={"GEV": 5},
            decision_map={
                "GEV": {
                    "action": "hold",
                    "final_decision": "hold",
                    "take_profit": 115,
                    "stop_loss": 80,
                }
            },
            min_delta_shares=1.0,
        )
        self.assertEqual(len(orders), 1)
        order = orders[0]
        self.assertEqual(order["side"], "sell")
        self.assertEqual(order["order_qty"], 5)
        self.assertEqual(order["reason"], "take_profit_triggered")

    def test_reconcile_generates_delta_buy_when_live_below_target(self):
        orders = _build_delta_orders(
            live_positions={"AAPL": 3},
            live_prices={"AAPL": 210},
            target_positions={"AAPL": 5},
            decision_map={
                "AAPL": {
                    "action": "buy",
                    "final_decision": "buy",
                    "final_score": 80,
                    "rule_score": 70,
                }
            },
            min_delta_shares=1.0,
        )
        self.assertEqual(len(orders), 1)
        order = orders[0]
        self.assertEqual(order["side"], "buy")
        self.assertEqual(order["order_qty"], 2)
        self.assertEqual(order["reason"], "strategy_buy_signal")

    def test_reconcile_keeps_hold_positions_when_no_sell_signal(self):
        orders = _build_delta_orders(
            live_positions={"AAPL": 2},
            live_prices={"AAPL": 205},
            target_positions={"AAPL": 0},
            decision_map={
                "AAPL": {
                    "action": "hold",
                    "final_decision": "hold",
                    "final_score": 68,
                    "rule_score": 62,
                }
            },
            min_delta_shares=0.0,
        )
        self.assertEqual(orders, [])

    def test_reconcile_filters_zero_share_orders_after_rounding(self):
        orders = _build_delta_orders(
            live_positions={"JPM": 0.4},
            live_prices={"JPM": 240},
            target_positions={"JPM": 0.0},
            decision_map={"JPM": {"action": "sell", "final_decision": "sell"}},
            min_delta_shares=0.0,
        )
        self.assertEqual(orders, [])


if __name__ == "__main__":
    unittest.main()
