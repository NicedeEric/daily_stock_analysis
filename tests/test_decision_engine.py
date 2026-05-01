# -*- coding: utf-8 -*-
"""Tests for deterministic multi-factor decision blending."""

from __future__ import annotations

import sys
import unittest
from unittest.mock import MagicMock

try:
    import litellm  # noqa: F401
except ModuleNotFoundError:
    sys.modules["litellm"] = MagicMock()

from src.analyzer import AnalysisResult
from src.services.decision_engine import StockDecisionEngine
from src.stock_analyzer import BuySignal, TrendAnalysisResult, TrendStatus


class TestDecisionEngine(unittest.TestCase):
    def test_technical_only_rule_score_preserves_legacy_behavior(self) -> None:
        engine = StockDecisionEngine()
        result = AnalysisResult(
            code="600519",
            name="贵州茅台",
            sentiment_score=84,
            trend_prediction="看多",
            operation_advice="买入",
            decision_type="buy",
            analysis_summary="LLM wants to buy",
            dashboard={},
        )
        trend_result = TrendAnalysisResult(
            code="600519",
            trend_status=TrendStatus.BEAR,
            buy_signal=BuySignal.SELL,
            signal_score=28,
        )

        payload = engine.apply(result, trend_result, "zh")

        self.assertEqual(payload.rule_score, 28)
        self.assertEqual(payload.rule_decision, "sell")
        self.assertEqual(payload.factor_scores["technical"], 28)
        self.assertIsNone(payload.factor_scores["fundamental"])
        self.assertNotIn("multi_factor_rule_blend_applied", payload.adjustments)

    def test_multi_factor_scores_raise_rule_confidence_when_context_is_supportive(self) -> None:
        engine = StockDecisionEngine()
        result = AnalysisResult(
            code="AAPL",
            name="Apple",
            sentiment_score=60,
            trend_prediction="Neutral",
            operation_advice="Hold",
            decision_type="hold",
            analysis_summary="Baseline hold",
            report_language="en",
            dashboard={},
        )
        trend_result = TrendAnalysisResult(
            code="AAPL",
            trend_status=TrendStatus.WEAK_BULL,
            buy_signal=BuySignal.BUY,
            signal_score=66,
        )
        fundamental_context = {
            "valuation": {"data": {"pe_ratio": 22.0, "pb_ratio": 3.2}},
            "growth": {"data": {"revenue_yoy": 16.5, "net_profit_yoy": 19.3}},
            "earnings": {
                "data": {
                    "forecast_summary": "growth improving and beat expectations",
                    "dividend": {"ttm_dividend_yield_pct": 3.4},
                }
            },
            "institution": {"data": {"institution_holding_change": 1.5}},
            "capital_flow": {"data": {"net_inflow": 1234567}},
            "coverage": {
                "valuation": "ok",
                "growth": "ok",
                "earnings": "ok",
                "institution": "ok",
                "capital_flow": "ok",
            },
            "errors": [],
        }
        news_context = "\n".join(
            [
                "Latest news: company won a major partnership and earnings beat expectations.",
                "Social Sentiment Intelligence for AAPL (Reddit / X / Polymarket)",
                "Buzz Score: 80/100",
                "Sentiment Score: 0.4",
            ]
        )

        payload = engine.apply(
            result,
            trend_result,
            "en",
            fundamental_context=fundamental_context,
            news_context=news_context,
        )

        self.assertGreaterEqual(payload.rule_score or 0, 69)
        self.assertEqual(payload.rule_decision, "buy")
        self.assertEqual(payload.factor_scores["technical"], 66)
        self.assertIsNotNone(payload.factor_scores["fundamental"])
        self.assertIsNotNone(payload.factor_scores["event"])
        self.assertIsNotNone(payload.factor_scores["sentiment"])
        self.assertGreater(payload.factor_weights.get("technical", 0.0), payload.factor_weights.get("sentiment", 0.0))
        self.assertIn("multi_factor_rule_blend_applied", payload.adjustments)


if __name__ == "__main__":
    unittest.main()
