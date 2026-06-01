from __future__ import annotations

from contextlib import contextmanager
from datetime import date, datetime
from types import SimpleNamespace
import unittest
from unittest.mock import Mock

from src.services.paper_trading_service import PaperTradingService


class _FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def scalars(self):
        return self

    def all(self):
        return list(self._rows)


class _FakeSession:
    def __init__(self, rows):
        self._rows = rows

    def execute(self, _query):
        return _FakeResult(self._rows)


class _FakeDb:
    def __init__(self, rows):
        self._rows = rows

    @contextmanager
    def get_session(self):
        yield _FakeSession(self._rows)


class PaperTradingUniverseFilterTestCase(unittest.TestCase):
    def test_load_latest_signals_respects_allowed_symbols(self):
        rows = [
            SimpleNamespace(
                id=1,
                code="INTL",
                name="Intel",
                created_at=datetime(2026, 6, 2, 8, 0, 0),
                final_score=72,
                sentiment_score=72,
                rule_score=66,
                llm_score=70,
                final_decision="buy",
                ideal_buy=20.0,
                secondary_buy=19.5,
                stop_loss=18.0,
                take_profit=24.0,
                analysis_close=20.5,
                analysis_date=date(2026, 6, 2),
                raw_result=None,
                context_snapshot=None,
            ),
            SimpleNamespace(
                id=2,
                code="MU",
                name="Micron",
                created_at=datetime(2026, 6, 2, 8, 1, 0),
                final_score=74,
                sentiment_score=74,
                rule_score=68,
                llm_score=71,
                final_decision="buy",
                ideal_buy=110.0,
                secondary_buy=108.0,
                stop_loss=102.0,
                take_profit=125.0,
                analysis_close=111.0,
                analysis_date=date(2026, 6, 2),
                raw_result=None,
                context_snapshot=None,
            ),
        ]

        service = PaperTradingService(
            db_manager=_FakeDb(rows),
            portfolio_service=Mock(),
            stock_repo=Mock(),
        )

        signals = service._load_latest_signals(
            run_date=date(2026, 6, 2),
            market="us",
            lookback_days=3,
            allowed_symbols={"MU"},
        )

        self.assertEqual([item["code"] for item in signals], ["MU"])


if __name__ == "__main__":
    unittest.main()
