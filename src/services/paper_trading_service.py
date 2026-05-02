# -*- coding: utf-8 -*-
"""Signal-driven paper trading service for US stock pool."""

from __future__ import annotations

import json
import logging
import math
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import and_, desc, select

from data_provider.us_index_mapping import is_us_stock_code
from src.repositories.backtest_repo import BacktestRepository
from src.repositories.stock_repo import StockRepository
from src.services.portfolio_service import PortfolioConflictError, PortfolioService
from src.storage import (
    AnalysisHistory,
    DatabaseManager,
    PaperStrategyDecision,
    PaperStrategyDefinition,
    StockDaily,
)

logger = logging.getLogger(__name__)


@dataclass
class PaperStrategyConfig:
    max_positions: int = 10
    max_position_pct: float = 0.20
    cash_reserve_pct: float = 0.50
    min_buy_score: int = 70
    min_rule_score: int = 65
    sell_score_threshold: int = 40
    trade_fee_usd: float = 1.30
    slippage_bps: float = 5.0
    execution_mode: str = "next_open"
    lookback_days: int = 3
    market: str = "us"

    @classmethod
    def from_payload(cls, payload: Optional[Dict[str, Any]]) -> "PaperStrategyConfig":
        if not isinstance(payload, dict):
            return cls()
        instance = cls()
        for field_name in instance.__dataclass_fields__.keys():
            if field_name not in payload:
                continue
            setattr(instance, field_name, payload[field_name])
        instance.max_positions = max(1, int(instance.max_positions))
        instance.max_position_pct = min(1.0, max(0.01, float(instance.max_position_pct)))
        instance.cash_reserve_pct = min(0.95, max(0.0, float(instance.cash_reserve_pct)))
        instance.min_buy_score = max(0, min(100, int(instance.min_buy_score)))
        instance.min_rule_score = max(0, min(100, int(instance.min_rule_score)))
        instance.sell_score_threshold = max(0, min(100, int(instance.sell_score_threshold)))
        instance.trade_fee_usd = max(0.0, float(instance.trade_fee_usd))
        instance.slippage_bps = max(0.0, float(instance.slippage_bps))
        instance.lookback_days = max(1, int(instance.lookback_days))
        instance.market = str(instance.market or "us").strip().lower() or "us"
        return instance

    def to_dict(self) -> Dict[str, Any]:
        return {
            "max_positions": self.max_positions,
            "max_position_pct": self.max_position_pct,
            "cash_reserve_pct": self.cash_reserve_pct,
            "min_buy_score": self.min_buy_score,
            "min_rule_score": self.min_rule_score,
            "sell_score_threshold": self.sell_score_threshold,
            "trade_fee_usd": self.trade_fee_usd,
            "slippage_bps": self.slippage_bps,
            "execution_mode": self.execution_mode,
            "lookback_days": self.lookback_days,
            "market": self.market,
        }


class PaperTradingService:
    """Execute one daily paper trading run from structured signals."""

    def __init__(
        self,
        db_manager: Optional[DatabaseManager] = None,
        portfolio_service: Optional[PortfolioService] = None,
        stock_repo: Optional[StockRepository] = None,
    ) -> None:
        self.db = db_manager or DatabaseManager.get_instance()
        self.portfolio_service = portfolio_service or PortfolioService()
        self.stock_repo = stock_repo or StockRepository(self.db)

    def ensure_strategy(
        self,
        *,
        strategy_name: str,
        strategy_version: str,
        initial_capital: float,
        base_currency: str = "USD",
        market: str = "us",
        config_override: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        strategy_name = str(strategy_name).strip()
        strategy_version = str(strategy_version).strip()
        if not strategy_name or not strategy_version:
            raise ValueError("strategy_name and strategy_version are required")
        initial_capital = float(initial_capital)
        if initial_capital <= 0:
            raise ValueError("initial_capital must be positive")

        market = str(market or "us").strip().lower() or "us"
        base_currency = str(base_currency or "USD").strip().upper() or "USD"
        cfg = PaperStrategyConfig.from_payload(config_override)
        cfg.market = market

        account_name = f"paper_{strategy_name}_{strategy_version}"
        account = self._get_or_create_account(
            name=account_name,
            market=market,
            base_currency=base_currency,
        )
        self._ensure_initial_capital(
            account_id=int(account["id"]),
            as_of=date.today(),
            initial_capital=initial_capital,
            currency=base_currency,
            note=f"paper_strategy_seed:{strategy_name}:{strategy_version}",
        )

        with self.db.get_session() as session:
            row = session.execute(
                select(PaperStrategyDefinition)
                .where(
                    and_(
                        PaperStrategyDefinition.strategy_name == strategy_name,
                        PaperStrategyDefinition.strategy_version == strategy_version,
                    )
                )
                .limit(1)
            ).scalar_one_or_none()
            if row is None:
                row = PaperStrategyDefinition(
                    strategy_name=strategy_name,
                    strategy_version=strategy_version,
                    account_id=int(account["id"]),
                    market=market,
                    base_currency=base_currency,
                    initial_capital=initial_capital,
                    status="active",
                    config_json=json.dumps(cfg.to_dict(), ensure_ascii=False),
                )
                session.add(row)
                session.commit()
                session.refresh(row)
            else:
                row.account_id = int(account["id"])
                row.market = market
                row.base_currency = base_currency
                row.initial_capital = initial_capital
                row.config_json = json.dumps(cfg.to_dict(), ensure_ascii=False)
                if not row.status:
                    row.status = "active"
                session.commit()

            return self._strategy_to_dict(row)

    def run_daily(
        self,
        *,
        strategy_name: str,
        strategy_version: str,
        run_date: Optional[date] = None,
    ) -> Dict[str, Any]:
        run_date = run_date or date.today()
        strategy = self._load_strategy(strategy_name=strategy_name, strategy_version=strategy_version)
        if strategy is None:
            raise ValueError(f"strategy not found: {strategy_name}:{strategy_version}")
        if strategy.status != "active":
            return {"status": "skipped", "reason": f"strategy_status={strategy.status}"}

        config = PaperStrategyConfig.from_payload(self._safe_json_loads(strategy.config_json))
        account_id = int(strategy.account_id)

        snapshot = self.portfolio_service.get_portfolio_snapshot(account_id=account_id, as_of=run_date)
        account_snapshot = (snapshot.get("accounts") or [{}])[0]
        positions = account_snapshot.get("positions") or []
        position_qty = {str(p.get("symbol", "")).upper(): float(p.get("quantity") or 0.0) for p in positions}
        total_equity = float(account_snapshot.get("total_equity") or 0.0)
        available_cash = float(account_snapshot.get("total_cash") or 0.0)

        signals = self._load_latest_signals(run_date=run_date, market=config.market, lookback_days=config.lookback_days)
        sells, buys = self._plan_actions(
            signals=signals,
            positions=position_qty,
            config=config,
        )

        executed = 0
        skipped = 0
        errors = 0

        # execute sells first
        for signal in sells:
            status = self._execute_signal(
                strategy_id=int(strategy.id),
                account_id=account_id,
                signal=signal,
                run_date=run_date,
                side="sell",
                quantity=float(position_qty.get(signal["code"], 0.0)),
                config=config,
            )
            if status == "executed":
                executed += 1
            elif status == "skipped":
                skipped += 1
            else:
                errors += 1

        # refresh snapshot after sells
        refreshed = self.portfolio_service.get_portfolio_snapshot(account_id=account_id, as_of=run_date)
        refreshed_account = (refreshed.get("accounts") or [{}])[0]
        available_cash = float(refreshed_account.get("total_cash") or available_cash)
        total_equity = float(refreshed_account.get("total_equity") or total_equity)
        refreshed_positions = refreshed_account.get("positions") or []
        held_symbols = {str(p.get("symbol", "")).upper() for p in refreshed_positions if float(p.get("quantity") or 0.0) > 0}

        max_new_positions = max(0, config.max_positions - len(held_symbols))
        buy_budget = max(0.0, available_cash - total_equity * config.cash_reserve_pct)
        if max_new_positions > 0 and buy_budget > 0:
            per_slot_budget = buy_budget / max_new_positions
        else:
            per_slot_budget = 0.0

        for signal in buys:
            if signal["code"] in held_symbols or max_new_positions <= 0:
                self._record_decision_only(
                    strategy_id=int(strategy.id),
                    run_date=run_date,
                    signal=signal,
                    action="skip",
                    target_weight=self._target_weight(signal["final_score"], config),
                    reason_codes=["already_held_or_no_slot"],
                    status="skipped",
                )
                skipped += 1
                continue

            entry_price = self._resolve_entry_price(signal, run_date=run_date, config=config)
            if entry_price is None or entry_price <= 0:
                self._record_decision_only(
                    strategy_id=int(strategy.id),
                    run_date=run_date,
                    signal=signal,
                    action="buy",
                    target_weight=self._target_weight(signal["final_score"], config),
                    reason_codes=["missing_entry_price"],
                    status="skipped",
                )
                skipped += 1
                continue

            target_value = min(
                total_equity * self._target_weight(signal["final_score"], config),
                per_slot_budget,
                available_cash,
            )
            quantity = math.floor(max(0.0, target_value - config.trade_fee_usd) / entry_price)
            if quantity <= 0:
                self._record_decision_only(
                    strategy_id=int(strategy.id),
                    run_date=run_date,
                    signal=signal,
                    action="buy",
                    target_weight=self._target_weight(signal["final_score"], config),
                    reason_codes=["insufficient_cash"],
                    status="skipped",
                )
                skipped += 1
                continue

            status = self._execute_signal(
                strategy_id=int(strategy.id),
                account_id=account_id,
                signal=signal,
                run_date=run_date,
                side="buy",
                quantity=float(quantity),
                config=config,
            )
            if status == "executed":
                executed += 1
                max_new_positions -= 1
                available_cash -= quantity * entry_price + config.trade_fee_usd
            elif status == "skipped":
                skipped += 1
            else:
                errors += 1

        end_snapshot = self.portfolio_service.get_portfolio_snapshot(account_id=account_id, as_of=run_date)
        return {
            "status": "ok",
            "strategy_name": strategy_name,
            "strategy_version": strategy_version,
            "run_date": run_date.isoformat(),
            "signals": len(signals),
            "planned_sells": len(sells),
            "planned_buys": len(buys),
            "executed": executed,
            "skipped": skipped,
            "errors": errors,
            "account_snapshot": (end_snapshot.get("accounts") or [{}])[0],
        }

    def _load_latest_signals(self, *, run_date: date, market: str, lookback_days: int) -> List[Dict[str, Any]]:
        cutoff = datetime.combine(run_date - timedelta(days=lookback_days), datetime.min.time())
        with self.db.get_session() as session:
            rows = session.execute(
                select(AnalysisHistory)
                .where(AnalysisHistory.created_at >= cutoff)
                .order_by(desc(AnalysisHistory.created_at))
                .limit(4000)
            ).scalars().all()

        latest_by_code: Dict[str, AnalysisHistory] = {}
        for row in rows:
            code = str(getattr(row, "code", "") or "").strip().upper()
            if not code:
                continue
            if market == "us" and not is_us_stock_code(code):
                continue
            if code in latest_by_code:
                continue
            latest_by_code[code] = row

        snapshots: List[Dict[str, Any]] = []
        for code, row in latest_by_code.items():
            signal_date = BacktestRepository.parse_analysis_date_from_snapshot(getattr(row, "context_snapshot", None))
            if signal_date is None and getattr(row, "created_at", None):
                signal_date = row.created_at.date()
            snapshots.append(
                {
                    "analysis_id": int(row.id),
                    "code": code,
                    "name": str(getattr(row, "name", "") or code),
                    "signal_date": signal_date,
                    "final_score": int(getattr(row, "final_score", None) or getattr(row, "sentiment_score", 50) or 50),
                    "rule_score": int(getattr(row, "rule_score", 0) or 0),
                    "llm_score": int(getattr(row, "llm_score", 0) or 0),
                    "final_decision": str(getattr(row, "final_decision", "") or "hold").strip().lower(),
                    "ideal_buy": self._safe_float(getattr(row, "ideal_buy", None)),
                    "secondary_buy": self._safe_float(getattr(row, "secondary_buy", None)),
                    "stop_loss": self._safe_float(getattr(row, "stop_loss", None)),
                    "take_profit": self._safe_float(getattr(row, "take_profit", None)),
                    "analysis_close": self._safe_float(getattr(row, "analysis_close", None)),
                    "created_at": row.created_at.isoformat() if getattr(row, "created_at", None) else None,
                }
            )
        return sorted(snapshots, key=lambda item: item.get("final_score", 0), reverse=True)

    def _plan_actions(
        self,
        *,
        signals: List[Dict[str, Any]],
        positions: Dict[str, float],
        config: PaperStrategyConfig,
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        sells: List[Dict[str, Any]] = []
        buys: List[Dict[str, Any]] = []
        held = {symbol for symbol, qty in positions.items() if qty > 0}
        for signal in signals:
            decision = str(signal.get("final_decision") or "hold").lower()
            score = int(signal.get("final_score") or 0)
            rule_score = int(signal.get("rule_score") or 0)
            code = signal["code"]
            if code in held:
                if decision == "sell" or score <= config.sell_score_threshold:
                    sells.append(signal)
                continue
            if decision != "buy":
                continue
            if score < config.min_buy_score:
                continue
            if rule_score < config.min_rule_score:
                continue
            buys.append(signal)
        return sells, buys

    def _execute_signal(
        self,
        *,
        strategy_id: int,
        account_id: int,
        signal: Dict[str, Any],
        run_date: date,
        side: str,
        quantity: float,
        config: PaperStrategyConfig,
    ) -> str:
        if quantity <= 0:
            self._record_decision_only(
                strategy_id=strategy_id,
                run_date=run_date,
                signal=signal,
                action=side,
                target_weight=self._target_weight(signal["final_score"], config),
                reason_codes=["non_positive_quantity"],
                status="skipped",
            )
            return "skipped"

        entry_price = self._resolve_entry_price(signal, run_date=run_date, config=config)
        if entry_price is None or entry_price <= 0:
            self._record_decision_only(
                strategy_id=strategy_id,
                run_date=run_date,
                signal=signal,
                action=side,
                target_weight=self._target_weight(signal["final_score"], config),
                reason_codes=["missing_entry_price"],
                status="skipped",
            )
            return "skipped"

        slippage_factor = 1.0 + (config.slippage_bps / 10000.0) * (1.0 if side == "buy" else -1.0)
        traded_price = max(0.0001, entry_price * slippage_factor)
        trade_notional = traded_price * quantity
        dedup_hash = f"paper:{strategy_id}:{run_date.isoformat()}:{signal['code']}:{side}"

        try:
            response = self.portfolio_service.record_trade(
                account_id=account_id,
                symbol=signal["code"],
                trade_date=run_date,
                side=side,
                quantity=float(quantity),
                price=float(traded_price),
                fee=float(config.trade_fee_usd),
                tax=0.0,
                currency="USD",
                market="us",
                trade_uid=dedup_hash,
                dedup_hash=dedup_hash,
                note=f"paper_strategy:{strategy_id}",
            )
            trade_id = int(response["id"])
            self._upsert_decision_row(
                strategy_id=strategy_id,
                run_date=run_date,
                signal=signal,
                action=side,
                target_weight=self._target_weight(signal["final_score"], config),
                reason_codes=["executed"],
                status="executed",
                executed_trade_id=trade_id,
                execution_price=float(traded_price),
                execution_quantity=float(quantity),
                execution_notional=float(trade_notional),
                error_message=None,
            )
            return "executed"
        except PortfolioConflictError as exc:
            self._upsert_decision_row(
                strategy_id=strategy_id,
                run_date=run_date,
                signal=signal,
                action=side,
                target_weight=self._target_weight(signal["final_score"], config),
                reason_codes=["duplicate_trade"],
                status="skipped",
                executed_trade_id=None,
                execution_price=float(traded_price),
                execution_quantity=float(quantity),
                execution_notional=float(trade_notional),
                error_message=str(exc),
            )
            return "skipped"
        except Exception as exc:
            logger.warning("paper trade execution failed: %s", exc)
            self._upsert_decision_row(
                strategy_id=strategy_id,
                run_date=run_date,
                signal=signal,
                action=side,
                target_weight=self._target_weight(signal["final_score"], config),
                reason_codes=["execution_error"],
                status="error",
                executed_trade_id=None,
                execution_price=float(traded_price),
                execution_quantity=float(quantity),
                execution_notional=float(trade_notional),
                error_message=str(exc),
            )
            return "error"

    def _record_decision_only(
        self,
        *,
        strategy_id: int,
        run_date: date,
        signal: Dict[str, Any],
        action: str,
        target_weight: float,
        reason_codes: List[str],
        status: str,
    ) -> None:
        self._upsert_decision_row(
            strategy_id=strategy_id,
            run_date=run_date,
            signal=signal,
            action=action,
            target_weight=target_weight,
            reason_codes=reason_codes,
            status=status,
            executed_trade_id=None,
            execution_price=None,
            execution_quantity=None,
            execution_notional=None,
            error_message=None,
        )

    def _resolve_entry_price(
        self,
        signal: Dict[str, Any],
        *,
        run_date: date,
        config: PaperStrategyConfig,
    ) -> Optional[float]:
        signal_date = signal.get("signal_date")
        if not isinstance(signal_date, date):
            return None
        if config.execution_mode != "next_open":
            return None
        bars = self.stock_repo.get_forward_bars(
            code=signal["code"],
            analysis_date=signal_date,
            eval_window_days=1,
        )
        if not bars:
            return None
        bar = bars[0]
        if getattr(bar, "date", None) and bar.date > run_date + timedelta(days=7):
            return None
        open_price = self._safe_float(getattr(bar, "open", None))
        close_price = self._safe_float(getattr(bar, "close", None))
        return open_price if open_price and open_price > 0 else close_price

    def _target_weight(self, score: int, config: PaperStrategyConfig) -> float:
        if score >= 85:
            return min(config.max_position_pct, 0.20)
        if score >= 78:
            return min(config.max_position_pct, 0.15)
        return min(config.max_position_pct, 0.10)

    def _get_or_create_account(self, *, name: str, market: str, base_currency: str) -> Dict[str, Any]:
        accounts = self.portfolio_service.list_accounts(include_inactive=True)
        for row in accounts:
            if str(row.get("name", "")).strip() == name:
                if not bool(row.get("is_active", True)):
                    self.portfolio_service.update_account(int(row["id"]), is_active=True)
                    refreshed = self.portfolio_service.list_accounts(include_inactive=True)
                    for item in refreshed:
                        if int(item["id"]) == int(row["id"]):
                            return item
                return row
        return self.portfolio_service.create_account(
            name=name,
            broker="paper",
            market=market,
            base_currency=base_currency,
            owner_id="paper",
        )

    def _ensure_initial_capital(
        self,
        *,
        account_id: int,
        as_of: date,
        initial_capital: float,
        currency: str,
        note: str,
    ) -> None:
        existing = self.portfolio_service.repo.list_cash_ledger(account_id, as_of=as_of)
        net_cash = 0.0
        for row in existing:
            amount = float(getattr(row, "amount", 0.0) or 0.0)
            direction = str(getattr(row, "direction", "") or "").strip().lower()
            net_cash += amount if direction == "in" else -amount
        if net_cash > 0:
            return
        self.portfolio_service.record_cash_ledger(
            account_id=account_id,
            event_date=as_of,
            direction="in",
            amount=float(initial_capital),
            currency=currency,
            note=note,
        )

    def _load_strategy(self, *, strategy_name: str, strategy_version: str) -> Optional[PaperStrategyDefinition]:
        with self.db.get_session() as session:
            return session.execute(
                select(PaperStrategyDefinition)
                .where(
                    and_(
                        PaperStrategyDefinition.strategy_name == strategy_name,
                        PaperStrategyDefinition.strategy_version == strategy_version,
                    )
                )
                .limit(1)
            ).scalar_one_or_none()

    def _upsert_decision_row(
        self,
        *,
        strategy_id: int,
        run_date: date,
        signal: Dict[str, Any],
        action: str,
        target_weight: float,
        reason_codes: List[str],
        status: str,
        executed_trade_id: Optional[int],
        execution_price: Optional[float],
        execution_quantity: Optional[float],
        execution_notional: Optional[float],
        error_message: Optional[str],
    ) -> None:
        with self.db.get_session() as session:
            row = session.execute(
                select(PaperStrategyDecision)
                .where(
                    and_(
                        PaperStrategyDecision.strategy_id == strategy_id,
                        PaperStrategyDecision.run_date == run_date,
                        PaperStrategyDecision.code == signal["code"],
                    )
                )
                .limit(1)
            ).scalar_one_or_none()
            payload = {
                "analysis_id": signal.get("analysis_id"),
                "final_score": signal.get("final_score"),
                "final_decision": signal.get("final_decision"),
                "rule_score": signal.get("rule_score"),
                "llm_score": signal.get("llm_score"),
                "ideal_buy": signal.get("ideal_buy"),
                "secondary_buy": signal.get("secondary_buy"),
                "stop_loss": signal.get("stop_loss"),
                "take_profit": signal.get("take_profit"),
                "analysis_close": signal.get("analysis_close"),
                "signal_date": signal.get("signal_date").isoformat() if isinstance(signal.get("signal_date"), date) else None,
                "created_at": signal.get("created_at"),
            }
            if row is None:
                row = PaperStrategyDecision(
                    strategy_id=strategy_id,
                    run_date=run_date,
                    code=signal["code"],
                    action=action,
                    signal_date=signal.get("signal_date"),
                    analysis_history_id=signal.get("analysis_id"),
                    target_weight=target_weight,
                    signal_snapshot_json=json.dumps(payload, ensure_ascii=False),
                    reason_codes_json=json.dumps(reason_codes, ensure_ascii=False),
                    status=status,
                    executed_trade_id=executed_trade_id,
                    execution_price=execution_price,
                    execution_quantity=execution_quantity,
                    execution_notional=execution_notional,
                    error_message=error_message,
                )
                session.add(row)
            else:
                row.action = action
                row.signal_date = signal.get("signal_date")
                row.analysis_history_id = signal.get("analysis_id")
                row.target_weight = target_weight
                row.signal_snapshot_json = json.dumps(payload, ensure_ascii=False)
                row.reason_codes_json = json.dumps(reason_codes, ensure_ascii=False)
                row.status = status
                row.executed_trade_id = executed_trade_id
                row.execution_price = execution_price
                row.execution_quantity = execution_quantity
                row.execution_notional = execution_notional
                row.error_message = error_message
            session.commit()

    @staticmethod
    def _safe_float(value: Any) -> Optional[float]:
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return None
        if math.isnan(numeric):
            return None
        return numeric

    @staticmethod
    def _safe_json_loads(value: Any) -> Dict[str, Any]:
        if isinstance(value, dict):
            return value
        if not value:
            return {}
        try:
            payload = json.loads(value)
        except Exception:
            return {}
        return payload if isinstance(payload, dict) else {}

    @staticmethod
    def _strategy_to_dict(row: PaperStrategyDefinition) -> Dict[str, Any]:
        return {
            "id": int(row.id),
            "strategy_name": row.strategy_name,
            "strategy_version": row.strategy_version,
            "account_id": int(row.account_id),
            "market": row.market,
            "base_currency": row.base_currency,
            "initial_capital": float(row.initial_capital),
            "status": row.status,
        }
