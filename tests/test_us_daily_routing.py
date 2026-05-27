import unittest

import pandas as pd

from data_provider.base import DataFetchError, DataFetcherManager


def _sample_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": ["2026-05-23", "2026-05-26"],
            "open": [75.0, 76.0],
            "high": [76.0, 77.0],
            "low": [74.5, 75.5],
            "close": [75.6, 76.2],
            "volume": [1000, 1200],
            "amount": [75600, 91440],
            "pct_chg": [0.0, 0.79],
        }
    )


class _DummyDailyFetcher:
    def __init__(self, name: str, priority: int, *, result=None, error: Exception | None = None):
        self.name = name
        self.priority = priority
        self.result = result
        self.error = error
        self.calls = []

    def get_daily_data(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        if self.error is not None:
            raise self.error
        return self.result


class _DummyLongbridgeFetcher(_DummyDailyFetcher):
    def _is_available(self) -> bool:
        return True


class TestUsDailyRouting(unittest.TestCase):
    def test_us_daily_history_prefers_yfinance_even_when_longbridge_is_available(self):
        longbridge = _DummyLongbridgeFetcher(
            "LongbridgeFetcher",
            0,
            result=_sample_df(),
        )
        yfinance = _DummyDailyFetcher(
            "YfinanceFetcher",
            1,
            result=_sample_df(),
        )

        manager = DataFetcherManager(fetchers=[longbridge, yfinance])
        df, source = manager.get_daily_data("MU", days=5)

        self.assertFalse(df.empty)
        self.assertEqual(source, "YfinanceFetcher")
        self.assertEqual(len(yfinance.calls), 1)
        self.assertEqual(longbridge.calls, [])

    def test_us_daily_history_does_not_fallback_to_longbridge_when_yfinance_fails(self):
        longbridge = _DummyLongbridgeFetcher(
            "LongbridgeFetcher",
            0,
            result=_sample_df(),
        )
        yfinance = _DummyDailyFetcher(
            "YfinanceFetcher",
            1,
            error=DataFetchError("yfinance unavailable"),
        )

        manager = DataFetcherManager(fetchers=[longbridge, yfinance])

        with self.assertRaises(DataFetchError):
            manager.get_daily_data("MU", days=5)

        self.assertEqual(len(yfinance.calls), 1)
        self.assertEqual(longbridge.calls, [])


if __name__ == "__main__":
    unittest.main()
