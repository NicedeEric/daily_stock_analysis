# -*- coding: utf-8 -*-
"""Historical structured-signal evaluation helpers."""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional

from sqlalchemy import desc, select

from src.agent.protocols import normalize_decision_signal
from src.core.backtest_engine import BacktestEngine, EvaluationConfig
from src.report_language import infer_decision_type_from_advice
from src.repositories.backtest_repo import BacktestRepository
from src.repositories.stock_repo import StockRepository
from src.storage import AnalysisHistory, DatabaseManager


@dataclass
class SignalSnapshot:
    """Normalized, version-tolerant signal snapshot for one analysis row."""

    analysis_id: int
    code: str
    name: str
    analysis_date: Optional[date]
    created_at: Optional[str]
    has_decision_engine: bool
    final_score: int
    final_decision: str
    final_action: str
    llm_score: Optional[int]
    llm_decision: Optional[str]
    llm_action: Optional[str]
    rule_score: Optional[int]
    rule_decision: Optional[str]
    rule_action: Optional[str]
    factor_scores: Dict[str, Optional[int]]
    factor_weights: Dict[str, float]
    score_band: str
    suggested_position: Optional[str]
    adjustments: List[str]
    stop_loss: Optional[float]
    take_profit: Optional[float]


class SignalQualityService:
    """Evaluate stored analysis_history rows by structured signal quality."""

    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        self.db = db_manager or DatabaseManager.get_instance()
        self.stock_repo = StockRepository(self.db)
        self.backtest_repo = BacktestRepository(self.db)

    def evaluate_history(
        self,
        *,
        code: Optional[str] = None,
        days: int = 365,
        limit: int = 500,
        eval_window_days: int = 10,
        neutral_band_pct: float = 2.0,
    ) -> Dict[str, Any]:
        records = self._load_history_records(code=code, days=days, limit=limit)
        config = EvaluationConfig(
            eval_window_days=int(eval_window_days),
            neutral_band_pct=float(neutral_band_pct),
            engine_version="signal_quality_v1",
        )

        detail_rows: List[Dict[str, Any]] = []
        adjustment_counter: Counter[str] = Counter()
        by_code_buckets: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        by_decision_buckets: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        by_score_band_buckets: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

        structured_count = 0
        completed_count = 0
        insufficient_count = 0
        missing_price_count = 0

        for record in records:
            snapshot = self.extract_signal_snapshot(record)
            if snapshot.has_decision_engine:
                structured_count += 1
            adjustment_counter.update(snapshot.adjustments)

            row = asdict(snapshot)
            row["analysis_date"] = snapshot.analysis_date.isoformat() if snapshot.analysis_date else None

            evaluation = self._evaluate_snapshot(snapshot, config)
            row.update(evaluation)
            detail_rows.append(row)

            status = evaluation.get("eval_status")
            if status == "completed":
                completed_count += 1
                by_code_buckets[snapshot.code].append(row)
                by_decision_buckets[snapshot.final_decision].append(row)
                by_score_band_buckets[snapshot.score_band].append(row)
            elif status == "insufficient_data":
                insufficient_count += 1
            else:
                missing_price_count += 1

        total_records = len(records)
        summary = {
            "meta": {
                "code": code,
                "days": int(days),
                "limit": int(limit),
                "eval_window_days": int(eval_window_days),
                "neutral_band_pct": float(neutral_band_pct),
                "generated_at": datetime.now().isoformat(timespec="seconds"),
            },
            "coverage": {
                "total_records": total_records,
                "structured_signal_records": structured_count,
                "structured_signal_coverage_pct": self._ratio(structured_count, total_records),
                "completed_evaluations": completed_count,
                "insufficient_data_records": insufficient_count,
                "missing_price_records": missing_price_count,
            },
            "overall": self._summarize_rows(detail_rows),
            "by_decision": {
                decision: self._summarize_rows(rows)
                for decision, rows in sorted(by_decision_buckets.items())
            },
            "by_score_band": {
                band: self._summarize_rows(rows)
                for band, rows in sorted(by_score_band_buckets.items())
            },
            "top_codes": self._summarize_top_codes(by_code_buckets),
            "adjustment_breakdown": dict(adjustment_counter.most_common()),
            "details": detail_rows,
        }
        return summary

    def _load_history_records(
        self,
        *,
        code: Optional[str],
        days: int,
        limit: int,
    ) -> List[AnalysisHistory]:
        cutoff_dt = datetime.now() - timedelta(days=max(days, 1))
        with self.db.get_session() as session:
            conditions = [AnalysisHistory.created_at >= cutoff_dt]
            if code:
                conditions.append(AnalysisHistory.code == code)
            query = (
                select(AnalysisHistory)
                .where(*conditions)
                .order_by(desc(AnalysisHistory.created_at))
                .limit(int(limit))
            )
            return list(session.execute(query).scalars().all())

    def _evaluate_snapshot(
        self,
        snapshot: SignalSnapshot,
        config: EvaluationConfig,
    ) -> Dict[str, Any]:
        if snapshot.analysis_date is None:
            return {"eval_status": "missing_analysis_date"}

        start_daily = self.stock_repo.get_start_daily(code=snapshot.code, analysis_date=snapshot.analysis_date)
        if start_daily is None or start_daily.close is None:
            return {"eval_status": "missing_start_price"}

        forward_bars = self.stock_repo.get_forward_bars(
            code=snapshot.code,
            analysis_date=start_daily.date,
            eval_window_days=config.eval_window_days,
        )
        evaluation = BacktestEngine.evaluate_single(
            operation_advice=snapshot.final_action,
            analysis_date=start_daily.date,
            start_price=float(start_daily.close),
            forward_bars=forward_bars,
            stop_loss=snapshot.stop_loss,
            take_profit=snapshot.take_profit,
            config=config,
        )
        evaluation["start_price"] = float(start_daily.close)
        return evaluation

    @classmethod
    def extract_signal_snapshot(cls, record: AnalysisHistory) -> SignalSnapshot:
        raw_payload = cls._safe_json_loads(getattr(record, "raw_result", None))
        dashboard = raw_payload.get("dashboard") if isinstance(raw_payload.get("dashboard"), dict) else {}
        decision_engine = dashboard.get("decision_engine") if isinstance(dashboard.get("decision_engine"), dict) else {}

        final_score = cls._safe_int(
            decision_engine.get("final_score"),
            cls._safe_int(raw_payload.get("sentiment_score"), cls._safe_int(record.sentiment_score, 50)),
        )
        final_decision = normalize_decision_signal(
            decision_engine.get("final_decision")
            or raw_payload.get("decision_type")
            or infer_decision_type_from_advice(
                decision_engine.get("final_action")
                or raw_payload.get("operation_advice")
                or record.operation_advice,
                default="hold",
            )
        )
        final_action = str(
            decision_engine.get("final_action")
            or raw_payload.get("operation_advice")
            or record.operation_advice
            or ""
        )
        analysis_date = cls._resolve_analysis_date(record)

        return SignalSnapshot(
            analysis_id=int(getattr(record, "id", 0) or 0),
            code=str(getattr(record, "code", "") or ""),
            name=str(getattr(record, "name", "") or ""),
            analysis_date=analysis_date,
            created_at=getattr(record, "created_at", None).isoformat() if getattr(record, "created_at", None) else None,
            has_decision_engine=bool(decision_engine),
            final_score=final_score,
            final_decision=final_decision,
            final_action=final_action,
            llm_score=cls._safe_optional_int(decision_engine.get("llm_score")),
            llm_decision=cls._safe_optional_text(decision_engine.get("llm_decision")),
            llm_action=cls._safe_optional_text(decision_engine.get("llm_action")),
            rule_score=cls._safe_optional_int(decision_engine.get("rule_score")),
            rule_decision=cls._safe_optional_text(decision_engine.get("rule_decision")),
            rule_action=cls._safe_optional_text(decision_engine.get("rule_action")),
            factor_scores=cls._normalize_factor_scores(decision_engine.get("factor_scores")),
            factor_weights=cls._normalize_factor_weights(decision_engine.get("factor_weights")),
            score_band=cls.score_band(final_score),
            suggested_position=cls._safe_optional_text(decision_engine.get("suggested_position")),
            adjustments=cls._normalize_adjustments(decision_engine.get("adjustments")),
            stop_loss=cls._safe_optional_float(getattr(record, "stop_loss", None)),
            take_profit=cls._safe_optional_float(getattr(record, "take_profit", None)),
        )

    @staticmethod
    def score_band(score: int) -> str:
        if score >= 85:
            return "85-100"
        if score >= 70:
            return "70-84"
        if score >= 55:
            return "55-69"
        if score >= 40:
            return "40-54"
        return "0-39"

    @staticmethod
    def _resolve_analysis_date(record: AnalysisHistory) -> Optional[date]:
        parsed = BacktestRepository.parse_analysis_date_from_snapshot(getattr(record, "context_snapshot", None))
        if parsed is not None:
            return parsed
        created_at = getattr(record, "created_at", None)
        return created_at.date() if created_at else None

    @staticmethod
    def _safe_json_loads(value: Any) -> Dict[str, Any]:
        if isinstance(value, dict):
            return value
        if not value:
            return {}
        try:
            parsed = json.loads(value)
        except Exception:
            return {}
        return parsed if isinstance(parsed, dict) else {}

    @staticmethod
    def _safe_int(value: Any, default: int) -> int:
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _safe_optional_int(value: Any) -> Optional[int]:
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _safe_optional_float(value: Any) -> Optional[float]:
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            return None
        return parsed

    @staticmethod
    def _safe_optional_text(value: Any) -> Optional[str]:
        if value is None:
            return None
        text = str(value).strip()
        return text or None

    @staticmethod
    def _normalize_adjustments(value: Any) -> List[str]:
        if isinstance(value, list):
            return [str(item).strip() for item in value if str(item).strip()]
        if value is None:
            return []
        text = str(value).strip()
        return [text] if text else []

    @classmethod
    def _normalize_factor_scores(cls, value: Any) -> Dict[str, Optional[int]]:
        if not isinstance(value, dict):
            return {}
        normalized: Dict[str, Optional[int]] = {}
        for key, raw in value.items():
            text_key = str(key).strip()
            if not text_key:
                continue
            normalized[text_key] = cls._safe_optional_int(raw)
        return normalized

    @staticmethod
    def _normalize_factor_weights(value: Any) -> Dict[str, float]:
        if not isinstance(value, dict):
            return {}
        normalized: Dict[str, float] = {}
        for key, raw in value.items():
            text_key = str(key).strip()
            if not text_key:
                continue
            try:
                normalized[text_key] = round(float(raw), 4)
            except (TypeError, ValueError):
                continue
        return normalized

    @classmethod
    def _summarize_rows(cls, rows: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
        rows_list = list(rows)
        total = len(rows_list)
        completed = [row for row in rows_list if row.get("eval_status") == "completed"]

        direction_values = [
            row.get("direction_correct")
            for row in completed
            if row.get("direction_correct") is not None
        ]
        win_count = sum(1 for row in completed if row.get("outcome") == "win")
        loss_count = sum(1 for row in completed if row.get("outcome") == "loss")
        neutral_count = sum(1 for row in completed if row.get("outcome") == "neutral")

        return {
            "records": total,
            "completed": len(completed),
            "decision_accuracy_pct": cls._ratio(sum(1 for value in direction_values if value), len(direction_values)),
            "win_rate_pct": cls._ratio(win_count, len(completed)),
            "loss_rate_pct": cls._ratio(loss_count, len(completed)),
            "neutral_rate_pct": cls._ratio(neutral_count, len(completed)),
            "avg_stock_return_pct": cls._average(completed, "stock_return_pct"),
            "avg_simulated_return_pct": cls._average(completed, "simulated_return_pct"),
        }

    @classmethod
    def _summarize_top_codes(
        cls,
        buckets: Dict[str, List[Dict[str, Any]]],
        *,
        top_n: int = 10,
    ) -> List[Dict[str, Any]]:
        ranked = []
        for code, rows in buckets.items():
            item = {"code": code}
            item.update(cls._summarize_rows(rows))
            ranked.append(item)
        ranked.sort(
            key=lambda item: (
                item.get("completed", 0),
                item.get("avg_simulated_return_pct") if item.get("avg_simulated_return_pct") is not None else -9999,
            ),
            reverse=True,
        )
        return ranked[:top_n]

    @staticmethod
    def _ratio(numerator: int, denominator: int) -> Optional[float]:
        if denominator <= 0:
            return None
        return round((numerator / denominator) * 100, 2)

    @staticmethod
    def _average(rows: Iterable[Dict[str, Any]], key: str) -> Optional[float]:
        values = [float(row[key]) for row in rows if row.get(key) is not None]
        if not values:
            return None
        return round(sum(values) / len(values), 4)
