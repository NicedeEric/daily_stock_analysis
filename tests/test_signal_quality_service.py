# -*- coding: utf-8 -*-
"""Tests for structured signal extraction helpers."""

from __future__ import annotations

import json
import unittest
from datetime import datetime
from types import SimpleNamespace

from src.services.signal_quality_service import SignalQualityService


class TestSignalQualityService(unittest.TestCase):
    """Pure extraction and normalization tests."""

    def test_extract_signal_snapshot_prefers_decision_engine(self) -> None:
        record = SimpleNamespace(
            id=1,
            code="600519",
            name="贵州茅台",
            created_at=datetime(2026, 3, 18, 18, 0, 0),
            context_snapshot=json.dumps({"enhanced_context": {"date": "2026-03-18"}}),
            raw_result=json.dumps(
                {
                    "sentiment_score": 78,
                    "operation_advice": "持有",
                    "decision_type": "hold",
                    "dashboard": {
                        "decision_engine": {
                            "final_score": 45,
                            "final_decision": "sell",
                            "final_action": "减仓",
                            "llm_score": 86,
                            "rule_score": 28,
                            "rule_decision": "sell",
                            "factor_scores": {"technical": 28, "risk": 34},
                            "factor_weights": {"technical": 0.8, "risk": 0.2},
                            "adjustments": ["llm_buy_overridden_to_sell"],
                        }
                    },
                }
            ),
            sentiment_score=78,
            operation_advice="持有",
            stop_loss=1480.0,
            take_profit=1650.0,
        )

        snapshot = SignalQualityService.extract_signal_snapshot(record)
        self.assertTrue(snapshot.has_decision_engine)
        self.assertEqual(snapshot.final_score, 45)
        self.assertEqual(snapshot.final_decision, "sell")
        self.assertEqual(snapshot.final_action, "减仓")
        self.assertEqual(snapshot.rule_score, 28)
        self.assertEqual(snapshot.factor_scores["technical"], 28)
        self.assertAlmostEqual(snapshot.factor_weights["technical"], 0.8, places=4)
        self.assertEqual(snapshot.score_band, "40-54")
        self.assertEqual(snapshot.analysis_date.isoformat(), "2026-03-18")

    def test_extract_signal_snapshot_falls_back_to_legacy_fields(self) -> None:
        record = SimpleNamespace(
            id=2,
            code="000001",
            name="平安银行",
            created_at=datetime(2026, 3, 10, 18, 0, 0),
            context_snapshot=None,
            raw_result=json.dumps(
                {
                    "sentiment_score": 72,
                    "operation_advice": "买入",
                    "decision_type": "buy",
                }
            ),
            sentiment_score=72,
            operation_advice="买入",
            stop_loss=None,
            take_profit=None,
        )

        snapshot = SignalQualityService.extract_signal_snapshot(record)
        self.assertFalse(snapshot.has_decision_engine)
        self.assertEqual(snapshot.final_score, 72)
        self.assertEqual(snapshot.final_decision, "buy")
        self.assertEqual(snapshot.final_action, "买入")
        self.assertEqual(snapshot.score_band, "70-84")
        self.assertEqual(snapshot.analysis_date.isoformat(), "2026-03-10")

    def test_score_band_boundaries(self) -> None:
        self.assertEqual(SignalQualityService.score_band(85), "85-100")
        self.assertEqual(SignalQualityService.score_band(70), "70-84")
        self.assertEqual(SignalQualityService.score_band(55), "55-69")
        self.assertEqual(SignalQualityService.score_band(40), "40-54")
        self.assertEqual(SignalQualityService.score_band(39), "0-39")
