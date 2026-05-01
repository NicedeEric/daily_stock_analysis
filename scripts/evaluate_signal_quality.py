# -*- coding: utf-8 -*-
"""Evaluate structured signal quality from stored analysis history."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
import sys
from typing import Any, Dict, List

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import get_config
from src.services.signal_quality_service import SignalQualityService


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Evaluate stored analysis_history rows by structured signal quality and forward returns."
    )
    parser.add_argument("--code", help="Optional stock code filter, e.g. 600519", default=None)
    parser.add_argument("--days", type=int, default=365, help="Look back this many calendar days from now.")
    parser.add_argument("--limit", type=int, default=500, help="Maximum analysis rows to inspect.")
    parser.add_argument(
        "--window",
        type=int,
        default=None,
        help="Forward evaluation window in trading days. Defaults to BACKTEST_EVAL_WINDOW_DAYS.",
    )
    parser.add_argument(
        "--neutral-band",
        type=float,
        default=None,
        help="Neutral band percentage. Defaults to BACKTEST_NEUTRAL_BAND_PCT.",
    )
    parser.add_argument(
        "--output-json",
        default="data/signal_quality_summary.json",
        help="Where to write the JSON summary.",
    )
    parser.add_argument(
        "--output-csv",
        default="data/signal_quality_details.csv",
        help="Where to write the detailed CSV rows.",
    )
    return parser


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames: List[str] = []
    seen = set()
    for row in rows:
        for key in row.keys():
            if key not in seen:
                seen.add(key)
                fieldnames.append(key)
    with path.open("w", encoding="utf-8-sig", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            normalized = dict(row)
            if isinstance(normalized.get("adjustments"), list):
                normalized["adjustments"] = "|".join(normalized["adjustments"])
            writer.writerow(normalized)


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    config = get_config()
    eval_window_days = int(args.window or getattr(config, "backtest_eval_window_days", 10))
    neutral_band_pct = float(args.neutral_band or getattr(config, "backtest_neutral_band_pct", 2.0))

    service = SignalQualityService()
    summary = service.evaluate_history(
        code=args.code,
        days=int(args.days),
        limit=int(args.limit),
        eval_window_days=eval_window_days,
        neutral_band_pct=neutral_band_pct,
    )

    details = summary.pop("details", [])
    json_path = Path(args.output_json)
    csv_path = Path(args.output_csv)
    _write_json(json_path, summary)
    _write_csv(csv_path, details)

    coverage = summary.get("coverage", {})
    overall = summary.get("overall", {})
    print(f"DB: {Path(getattr(config, 'database_path', './data/stock_analysis.db')).resolve()}")
    print(f"Rows scanned: {coverage.get('total_records', 0)}")
    print(f"Structured signal coverage: {coverage.get('structured_signal_coverage_pct')}")
    print(f"Completed evaluations: {coverage.get('completed_evaluations', 0)}")
    print(f"Overall decision accuracy: {overall.get('decision_accuracy_pct')}")
    print(f"Overall avg simulated return: {overall.get('avg_simulated_return_pct')}")
    print(f"Summary written to: {json_path.resolve()}")
    print(f"Details written to: {csv_path.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
