from datetime import date
from types import SimpleNamespace
import unittest
from unittest.mock import Mock

from src.services.paper_trading_service import PaperTradingService


class PaperTradingTopUpTestCase(unittest.TestCase):
    def test_top_up_strategy_cash_records_cash_ledger(self):
        portfolio_service = Mock()
        portfolio_service.record_cash_ledger.return_value = {"id": 321}
        service = PaperTradingService(
            db_manager=Mock(),
            portfolio_service=portfolio_service,
            stock_repo=Mock(),
        )
        service._load_strategy = Mock(
            return_value=SimpleNamespace(
                account_id=7,
                base_currency="USD",
            )
        )

        result = service.top_up_strategy_cash(
            strategy_name="signal_portfolio",
            strategy_version="v1_us",
            amount=80000,
            event_date=date(2026, 5, 7),
        )

        portfolio_service.record_cash_ledger.assert_called_once_with(
            account_id=7,
            event_date=date(2026, 5, 7),
            direction="in",
            amount=80000.0,
            currency="USD",
            note="paper_strategy_topup:signal_portfolio:v1_us",
        )
        self.assertEqual(result["ledger_id"], 321)
        self.assertEqual(result["amount"], 80000.0)
        self.assertEqual(result["event_date"], "2026-05-07")


if __name__ == "__main__":
    unittest.main()
