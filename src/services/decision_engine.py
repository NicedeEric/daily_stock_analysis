# -*- coding: utf-8 -*-
"""Deterministic post-processing for final stock decisions."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, TYPE_CHECKING

from src.agent.protocols import normalize_decision_signal
from src.report_language import infer_decision_type_from_advice, localize_operation_advice

if TYPE_CHECKING:
    from src.analyzer import AnalysisResult
    from src.stock_analyzer import TrendAnalysisResult


_BEARISH_TREND_NAMES = {"WEAK_BEAR", "BEAR", "STRONG_BEAR"}
_BULLISH_TREND_NAMES = {"WEAK_BULL", "BULL", "STRONG_BULL"}
_FACTOR_BASE_WEIGHTS: Dict[str, float] = {
    "technical": 0.55,
    "fundamental": 0.20,
    "event": 0.10,
    "sentiment": 0.05,
    "risk": 0.10,
}
_POSITIVE_EVENT_KEYWORDS: Tuple[str, ...] = (
    "增长",
    "超预期",
    "回购",
    "中标",
    "突破",
    "合作",
    "增持",
    "盈利改善",
    "improved",
    "beat",
    "upgrade",
    "partnership",
    "backlog",
    "surge",
)
_NEGATIVE_EVENT_KEYWORDS: Tuple[str, ...] = (
    "下滑",
    "不及预期",
    "减持",
    "诉讼",
    "处罚",
    "暴雷",
    "风险",
    "亏损",
    "调查",
    "downgrade",
    "miss",
    "lawsuit",
    "fraud",
    "warning",
    "probe",
)
_POSITIVE_EARNINGS_KEYWORDS: Tuple[str, ...] = (
    "预增",
    "扭亏",
    "高增长",
    "improving",
    "beat",
    "growth",
    "strong",
)
_NEGATIVE_EARNINGS_KEYWORDS: Tuple[str, ...] = (
    "预减",
    "预亏",
    "下滑",
    "decline",
    "miss",
    "weak",
    "loss",
)
_RISK_KEYWORDS: Tuple[str, ...] = (
    "风险",
    "诉讼",
    "处罚",
    "暴雷",
    "减持",
    "质押",
    "监管",
    "调查",
    "warning",
    "lawsuit",
    "probe",
    "fraud",
    "downgrade",
    "recall",
)


@dataclass
class DecisionEngineResult:
    """Stable decision metadata derived from LLM output plus rule signals."""

    llm_score: int
    llm_decision: str
    llm_action: str
    rule_score: Optional[int]
    rule_decision: Optional[str]
    rule_action: Optional[str]
    final_score: int
    final_decision: str
    final_action: str
    suggested_position: str
    adjustments: List[str]
    factor_scores: Dict[str, Optional[int]]
    factor_weights: Dict[str, float]
    factor_notes: Dict[str, List[str]]
    engine_version: str = "decision_engine_v2"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "engine_version": self.engine_version,
            "llm_score": self.llm_score,
            "llm_decision": self.llm_decision,
            "llm_action": self.llm_action,
            "rule_score": self.rule_score,
            "rule_decision": self.rule_decision,
            "rule_action": self.rule_action,
            "final_score": self.final_score,
            "final_decision": self.final_decision,
            "final_action": self.final_action,
            "suggested_position": self.suggested_position,
            "adjustments": list(self.adjustments),
            "factor_scores": dict(self.factor_scores),
            "factor_weights": dict(self.factor_weights),
            "factor_notes": {key: list(value) for key, value in self.factor_notes.items()},
        }


class StockDecisionEngine:
    """Blend LLM and multi-factor rule signals with a conservative policy."""

    RULE_WEIGHT = 0.7
    LLM_WEIGHT = 0.3

    def apply(
        self,
        result: "AnalysisResult",
        trend_result: Optional["TrendAnalysisResult"],
        report_language: str,
        *,
        fundamental_context: Optional[Dict[str, Any]] = None,
        news_context: Optional[str] = None,
    ) -> DecisionEngineResult:
        llm_score = self._clamp_score(getattr(result, "sentiment_score", 50))
        llm_decision = normalize_decision_signal(
            getattr(result, "decision_type", None)
            or infer_decision_type_from_advice(getattr(result, "operation_advice", None), default="hold")
        )
        llm_action = str(
            getattr(result, "operation_advice", "")
            or localize_operation_advice(llm_decision, report_language)
        )

        (
            rule_score,
            rule_decision,
            rule_action,
            factor_scores,
            factor_weights,
            factor_notes,
        ) = self._build_rule_signal(
            trend_result=trend_result,
            report_language=report_language,
            fundamental_context=fundamental_context,
            news_context=news_context,
        )

        data_sources = str(getattr(result, "data_sources", "") or "")
        is_trend_fallback = "trend:fallback" in data_sources

        if is_trend_fallback and rule_score is not None and rule_decision is not None:
            final_score = rule_score
            final_decision = rule_decision
            final_action = rule_action or self._decision_to_action(final_decision, final_score, report_language)
            adjustments = ["trend_fallback_preserved"]
        elif rule_score is None or rule_decision is None:
            final_score = llm_score
            final_decision = llm_decision
            final_action = self._decision_to_action(final_decision, final_score, report_language)
            adjustments = ["llm_only_no_rule_signal"]
        else:
            final_score = round((rule_score * self.RULE_WEIGHT) + (llm_score * self.LLM_WEIGHT))
            final_decision = self._resolve_final_decision(
                trend_result=trend_result,
                llm_decision=llm_decision,
                llm_score=llm_score,
                rule_decision=rule_decision,
                rule_score=rule_score,
                blended_score=final_score,
            )
            adjustments = self._build_adjustments(
                trend_result=trend_result,
                llm_decision=llm_decision,
                rule_decision=rule_decision,
                final_decision=final_decision,
                factor_scores=factor_scores,
            )
            final_action = self._decision_to_action(final_decision, final_score, report_language)
        suggested_position = self._suggest_position(final_decision, final_score)

        result.sentiment_score = final_score
        result.decision_type = final_decision
        result.operation_advice = final_action

        dashboard = result.dashboard if isinstance(getattr(result, "dashboard", None), dict) else {}
        dashboard["sentiment_score"] = final_score
        dashboard["decision_type"] = final_decision
        dashboard["operation_advice"] = final_action
        decision_payload = DecisionEngineResult(
            llm_score=llm_score,
            llm_decision=llm_decision,
            llm_action=llm_action,
            rule_score=rule_score,
            rule_decision=rule_decision,
            rule_action=rule_action,
            final_score=final_score,
            final_decision=final_decision,
            final_action=final_action,
            suggested_position=suggested_position,
            adjustments=adjustments,
            factor_scores=factor_scores,
            factor_weights=factor_weights,
            factor_notes=factor_notes,
        )
        dashboard["decision_engine"] = decision_payload.to_dict()

        battle_plan = dashboard.get("battle_plan")
        if isinstance(battle_plan, dict) and not battle_plan.get("suggested_position"):
            battle_plan["suggested_position"] = suggested_position

        result.dashboard = dashboard
        return decision_payload

    def _build_rule_signal(
        self,
        *,
        trend_result: Optional["TrendAnalysisResult"],
        report_language: str,
        fundamental_context: Optional[Dict[str, Any]],
        news_context: Optional[str],
    ) -> Tuple[
        Optional[int],
        Optional[str],
        Optional[str],
        Dict[str, Optional[int]],
        Dict[str, float],
        Dict[str, List[str]],
    ]:
        factor_scores: Dict[str, Optional[int]] = {
            "technical": None,
            "fundamental": None,
            "event": None,
            "sentiment": None,
            "risk": None,
        }
        factor_notes: Dict[str, List[str]] = {
            "technical": [],
            "fundamental": [],
            "event": [],
            "sentiment": [],
            "risk": [],
        }

        technical_score = None
        technical_decision = None
        technical_action = None
        if trend_result is not None:
            technical_score = self._clamp_score(getattr(trend_result, "signal_score", 50))
            technical_decision = self._trend_to_decision(trend_result)
            technical_action = self._trend_to_action(trend_result, report_language)
            factor_scores["technical"] = technical_score
            factor_notes["technical"] = self._compact_notes(
                list(getattr(trend_result, "signal_reasons", []) or [])
                + list(getattr(trend_result, "risk_factors", []) or [])
                + ([f"technical_action:{technical_action}"] if technical_action else [])
            )

        fundamental_score, fundamental_notes = self._score_fundamental(fundamental_context)
        factor_scores["fundamental"] = fundamental_score
        factor_notes["fundamental"] = fundamental_notes

        event_score, event_notes = self._score_event(fundamental_context, news_context)
        factor_scores["event"] = event_score
        factor_notes["event"] = event_notes

        sentiment_score, sentiment_notes = self._score_sentiment(news_context)
        factor_scores["sentiment"] = sentiment_score
        factor_notes["sentiment"] = sentiment_notes

        risk_score, risk_notes = self._score_risk(trend_result, fundamental_context, news_context)
        factor_scores["risk"] = risk_score
        factor_notes["risk"] = risk_notes

        available_scores = {
            factor: score
            for factor, score in factor_scores.items()
            if score is not None
        }
        if not available_scores:
            return None, None, None, factor_scores, {}, factor_notes

        factor_weights = self._normalize_factor_weights(available_scores)
        rule_score = self._weighted_average(available_scores, factor_weights)
        rule_decision = self._resolve_rule_decision(
            technical_decision=technical_decision,
            technical_score=technical_score,
            blended_rule_score=rule_score,
            available_factor_count=len(available_scores),
        )
        if len(available_scores) <= 1 and technical_action:
            rule_action = technical_action
        else:
            rule_action = self._decision_to_action(rule_decision, rule_score, report_language) if rule_decision else None
        return rule_score, rule_decision, rule_action, factor_scores, factor_weights, factor_notes

    @staticmethod
    def _clamp_score(value: Any, default: int = 50) -> int:
        try:
            score = int(float(value))
        except (TypeError, ValueError):
            score = default
        return max(0, min(100, score))

    @staticmethod
    def _trend_to_decision(trend_result: "TrendAnalysisResult") -> str:
        signal_name = str(getattr(getattr(trend_result, "buy_signal", None), "name", "")).strip().lower()
        if signal_name in {"strong_buy", "buy"}:
            return "buy"
        if signal_name in {"sell", "strong_sell"}:
            return "sell"
        return "hold"

    @staticmethod
    def _trend_to_action(trend_result: "TrendAnalysisResult", report_language: str) -> str:
        buy_signal = getattr(trend_result, "buy_signal", None)
        signal_name = str(getattr(buy_signal, "name", "")).strip().lower()
        if signal_name:
            return localize_operation_advice(signal_name, report_language)
        signal_label = getattr(buy_signal, "value", None)
        if signal_label:
            return str(signal_label)
        return localize_operation_advice("watch", report_language)

    @staticmethod
    def _normalize_factor_weights(available_scores: Dict[str, int]) -> Dict[str, float]:
        raw_weights = {
            factor: _FACTOR_BASE_WEIGHTS.get(factor, 0.0)
            for factor in available_scores
        }
        weight_sum = sum(raw_weights.values())
        if weight_sum <= 0:
            equal_weight = round(1.0 / max(len(available_scores), 1), 4)
            return {factor: equal_weight for factor in available_scores}
        return {
            factor: round(weight / weight_sum, 4)
            for factor, weight in raw_weights.items()
        }

    @staticmethod
    def _weighted_average(scores: Dict[str, int], weights: Dict[str, float]) -> int:
        total = 0.0
        for factor, score in scores.items():
            total += float(score) * float(weights.get(factor, 0.0))
        return max(0, min(100, int(round(total))))

    def _resolve_rule_decision(
        self,
        *,
        technical_decision: Optional[str],
        technical_score: Optional[int],
        blended_rule_score: int,
        available_factor_count: int,
    ) -> str:
        if technical_decision is None:
            return self._score_to_decision(blended_rule_score)

        if available_factor_count <= 1 or technical_score is None:
            return technical_decision

        if technical_decision == "sell":
            if blended_rule_score <= 35:
                return "sell"
            return "hold"

        if technical_decision == "buy":
            if blended_rule_score >= 68:
                return "buy"
            return "hold"

        return self._score_to_decision(blended_rule_score)

    @staticmethod
    def _score_to_decision(score: int) -> str:
        if score >= 68:
            return "buy"
        if score <= 35:
            return "sell"
        return "hold"

    def _resolve_final_decision(
        self,
        trend_result: Optional["TrendAnalysisResult"],
        llm_decision: str,
        llm_score: int,
        rule_decision: str,
        rule_score: int,
        blended_score: int,
    ) -> str:
        trend_name = str(getattr(getattr(trend_result, "trend_status", None), "name", "")).strip().upper()

        if trend_name in _BEARISH_TREND_NAMES and llm_decision == "buy":
            return "hold" if rule_score >= 35 else "sell"

        if trend_name == "STRONG_BEAR":
            return "sell"

        if trend_name == "BEAR":
            return "sell" if rule_score <= 35 else "hold"

        if rule_decision == "sell":
            if llm_decision == "buy":
                return "hold" if rule_score >= 35 else "sell"
            return "sell" if blended_score <= 42 else "hold"

        if trend_name in _BULLISH_TREND_NAMES and llm_decision == "sell":
            return "buy" if rule_score >= 72 else "hold"

        if rule_decision == "buy":
            if llm_decision == "sell":
                return "buy" if blended_score >= 72 else "hold"
            return "buy" if blended_score >= 68 else "hold"

        if llm_decision == "buy" and rule_score < 60:
            return "hold"
        if llm_decision == "sell" and rule_score > 45:
            return "hold"

        if blended_score >= 70:
            return "buy"
        if blended_score <= 34:
            return "sell"
        if llm_score >= 72 and rule_score >= 60:
            return "buy"
        if llm_score <= 30 and rule_score <= 42:
            return "sell"
        return "hold"

    @staticmethod
    def _build_adjustments(
        trend_result: Optional["TrendAnalysisResult"],
        llm_decision: str,
        rule_decision: Optional[str],
        final_decision: str,
        factor_scores: Dict[str, Optional[int]],
    ) -> List[str]:
        adjustments: List[str] = []
        trend_name = str(getattr(getattr(trend_result, "trend_status", None), "name", "")).strip().upper()

        if llm_decision != final_decision:
            adjustments.append(f"llm_{llm_decision}_overridden_to_{final_decision}")
        if rule_decision and rule_decision != final_decision:
            adjustments.append(f"rule_{rule_decision}_tempered_to_{final_decision}")
        if trend_name in _BEARISH_TREND_NAMES and final_decision != "buy":
            adjustments.append("bearish_trend_guardrail_applied")
        if trend_name in _BULLISH_TREND_NAMES and llm_decision == "sell" and final_decision != "sell":
            adjustments.append("bullish_trend_guardrail_applied")
        if sum(1 for score in factor_scores.values() if score is not None) > 1:
            adjustments.append("multi_factor_rule_blend_applied")
        if not adjustments:
            adjustments.append("llm_and_rule_aligned")
        return adjustments

    @staticmethod
    def _decision_to_action(decision: str, score: int, report_language: str) -> str:
        if decision == "buy":
            token = "strong_buy" if score >= 82 else "buy"
        elif decision == "sell":
            token = "strong_sell" if score <= 20 else ("sell" if score <= 32 else "reduce")
        else:
            token = "hold" if score >= 55 else "watch"
        return localize_operation_advice(token, report_language)

    @staticmethod
    def _suggest_position(decision: str, score: int) -> str:
        if decision == "buy":
            if score >= 82:
                return "50%-70%"
            return "25%-40%"
        if decision == "sell":
            if score <= 20:
                return "0%-10%"
            return "10%-20%"
        if score >= 58:
            return "20%-35%"
        return "0%-20%"

    def _score_fundamental(
        self,
        fundamental_context: Optional[Dict[str, Any]],
    ) -> Tuple[Optional[int], List[str]]:
        if not isinstance(fundamental_context, dict):
            return None, []

        score = 50
        notes: List[str] = []
        used = False

        valuation = self._get_block_data(fundamental_context, "valuation")
        pe_ratio = self._safe_float(valuation.get("pe_ratio"))
        pb_ratio = self._safe_float(valuation.get("pb_ratio"))
        if pe_ratio is not None:
            used = True
            if 0 < pe_ratio <= 25:
                score += 5
                notes.append("valuation_pe_reasonable")
            elif pe_ratio >= 45:
                score -= 6
                notes.append("valuation_pe_expensive")
        if pb_ratio is not None:
            used = True
            if 0 < pb_ratio <= 4:
                score += 3
                notes.append("valuation_pb_supportive")
            elif pb_ratio >= 8:
                score -= 4
                notes.append("valuation_pb_stretched")

        growth = self._get_block_data(fundamental_context, "growth")
        revenue_yoy = self._safe_float(growth.get("revenue_yoy"))
        net_profit_yoy = self._safe_float(growth.get("net_profit_yoy"))
        if revenue_yoy is not None:
            used = True
            if revenue_yoy >= 15:
                score += 6
                notes.append("growth_revenue_strong")
            elif revenue_yoy >= 5:
                score += 3
                notes.append("growth_revenue_positive")
            elif revenue_yoy < 0:
                score -= 6
                notes.append("growth_revenue_negative")
        if net_profit_yoy is not None:
            used = True
            if net_profit_yoy >= 15:
                score += 8
                notes.append("growth_profit_strong")
            elif net_profit_yoy >= 5:
                score += 4
                notes.append("growth_profit_positive")
            elif net_profit_yoy < 0:
                score -= 8
                notes.append("growth_profit_negative")

        earnings = self._get_block_data(fundamental_context, "earnings")
        forecast_summary = str(earnings.get("forecast_summary") or "")
        if forecast_summary.strip():
            used = True
            score += 3 * self._keyword_balance(
                forecast_summary,
                _POSITIVE_EARNINGS_KEYWORDS,
                _NEGATIVE_EARNINGS_KEYWORDS,
            )
            notes.append("earnings_guidance_reviewed")
        dividend = earnings.get("dividend")
        if isinstance(dividend, dict):
            dividend_yield = self._safe_float(dividend.get("ttm_dividend_yield_pct"))
            if dividend_yield is not None:
                used = True
                if dividend_yield >= 3:
                    score += 3
                    notes.append("dividend_yield_supportive")
                elif dividend_yield >= 1:
                    score += 1
                    notes.append("dividend_yield_present")

        institution = self._get_block_data(fundamental_context, "institution")
        holding_change = self._safe_float(institution.get("institution_holding_change"))
        if holding_change is not None:
            used = True
            if holding_change > 0:
                score += 4
                notes.append("institution_accumulation")
            elif holding_change < 0:
                score -= 4
                notes.append("institution_distribution")

        if not used:
            return None, []
        return self._clamp_score(score), self._compact_notes(notes)

    def _score_event(
        self,
        fundamental_context: Optional[Dict[str, Any]],
        news_context: Optional[str],
    ) -> Tuple[Optional[int], List[str]]:
        score = 50
        notes: List[str] = []
        used = False

        text = str(news_context or "").strip()
        if text:
            event_balance = self._keyword_balance(text, _POSITIVE_EVENT_KEYWORDS, _NEGATIVE_EVENT_KEYWORDS)
            if event_balance != 0 or any(keyword in text.lower() for keyword in ("news", "情报", "intel")):
                used = True
                score += 4 * event_balance
                notes.append("news_event_signal_scanned")

        capital_flow = self._get_block_data(fundamental_context, "capital_flow")
        flow_value = self._safe_float(self._find_first_numeric(capital_flow, ("net_inflow", "main_net_inflow", "net_amount")))
        if flow_value is not None:
            used = True
            if flow_value > 0:
                score += 4
                notes.append("capital_flow_inflow")
            elif flow_value < 0:
                score -= 4
                notes.append("capital_flow_outflow")

        dragon_tiger = self._get_block_data(fundamental_context, "dragon_tiger")
        if isinstance(dragon_tiger.get("seats"), list) and dragon_tiger.get("seats"):
            used = True
            notes.append("dragon_tiger_activity_present")

        if not used:
            return None, []
        return self._clamp_score(score), self._compact_notes(notes)

    def _score_sentiment(self, news_context: Optional[str]) -> Tuple[Optional[int], List[str]]:
        text = str(news_context or "")
        if "social sentiment intelligence" not in text.lower():
            return None, []

        notes: List[str] = []
        values: List[float] = []

        for match in re.findall(r"Buzz Score:\s*([-+]?\d+(?:\.\d+)?)\s*/\s*100", text, flags=re.IGNORECASE):
            try:
                values.append(float(match))
            except ValueError:
                continue
        for match in re.findall(r"Sentiment Score:\s*([-+]?\d+(?:\.\d+)?)", text, flags=re.IGNORECASE):
            try:
                raw_value = float(match)
            except ValueError:
                continue
            if -1.0 <= raw_value <= 1.0:
                values.append(50.0 + raw_value * 50.0)
            else:
                values.append(raw_value)

        if not values:
            return None, []

        avg_score = sum(values) / len(values)
        notes.append("social_sentiment_signal_present")
        return self._clamp_score(avg_score), notes

    def _score_risk(
        self,
        trend_result: Optional["TrendAnalysisResult"],
        fundamental_context: Optional[Dict[str, Any]],
        news_context: Optional[str],
    ) -> Tuple[Optional[int], List[str]]:
        score = 70
        notes: List[str] = []
        used = False

        risk_factors = list(getattr(trend_result, "risk_factors", []) or []) if trend_result is not None else []
        if risk_factors:
            used = True
            score -= min(len(risk_factors), 4) * 8
            notes.extend(str(item).strip() for item in risk_factors if str(item).strip())

        if isinstance(fundamental_context, dict):
            coverage = fundamental_context.get("coverage")
            if isinstance(coverage, dict):
                failed_blocks = sum(
                    1 for value in coverage.values()
                    if str(value).strip().lower() == "failed"
                )
                if failed_blocks:
                    used = True
                    score -= min(failed_blocks, 3) * 4
                    notes.append("fundamental_block_failures")

            errors = fundamental_context.get("errors")
            if isinstance(errors, list) and errors:
                used = True
                score -= min(len(errors), 3) * 3
                notes.append("fundamental_pipeline_errors")

        text = str(news_context or "")
        risk_hits = self._count_keyword_hits(text, _RISK_KEYWORDS)
        if risk_hits:
            used = True
            score -= min(risk_hits, 5) * 4
            notes.append("news_risk_keywords_detected")

        if not used:
            return None, []
        return self._clamp_score(score), self._compact_notes(notes)

    @staticmethod
    def _get_block_data(fundamental_context: Optional[Dict[str, Any]], block_name: str) -> Dict[str, Any]:
        if not isinstance(fundamental_context, dict):
            return {}
        payload = fundamental_context.get(block_name)
        if not isinstance(payload, dict):
            return {}
        data = payload.get("data")
        return data if isinstance(data, dict) else {}

    @staticmethod
    def _safe_float(value: Any) -> Optional[float]:
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return None
        if numeric != numeric:
            return None
        return numeric

    @staticmethod
    def _count_keyword_hits(text: str, keywords: Tuple[str, ...]) -> int:
        lowered = text.lower()
        return sum(lowered.count(keyword.lower()) for keyword in keywords)

    def _keyword_balance(
        self,
        text: str,
        positive_keywords: Tuple[str, ...],
        negative_keywords: Tuple[str, ...],
    ) -> int:
        positive_hits = self._count_keyword_hits(text, positive_keywords)
        negative_hits = self._count_keyword_hits(text, negative_keywords)
        if positive_hits == 0 and negative_hits == 0:
            return 0
        return max(-2, min(2, positive_hits - negative_hits))

    def _find_first_numeric(self, payload: Dict[str, Any], keys: Tuple[str, ...]) -> Optional[float]:
        if not isinstance(payload, dict):
            return None
        for key in keys:
            direct_value = self._safe_float(payload.get(key))
            if direct_value is not None:
                return direct_value
        for value in payload.values():
            if isinstance(value, dict):
                nested = self._find_first_numeric(value, keys)
                if nested is not None:
                    return nested
        return None

    @staticmethod
    def _compact_notes(values: List[str], limit: int = 6) -> List[str]:
        notes: List[str] = []
        for value in values:
            text = str(value).strip()
            if not text or text in notes:
                continue
            notes.append(text)
            if len(notes) >= limit:
                break
        return notes
