# -*- coding: utf-8 -*-
"""Send paper reconcile summary to Telegram."""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import urllib.parse
import urllib.request
from pathlib import Path
from urllib.error import HTTPError

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.paper_trade_notifications import build_reconcile_message


def _to_plain_telegram_text(markdown_like_text: str) -> str:
    text = str(markdown_like_text or "")
    lines = []
    for raw_line in text.splitlines():
        line = re.sub(r"^\*(.+)\*$", r"\1", raw_line.strip())
        line = line.replace("`", "")
        lines.append(line)
    return "\n".join(lines).strip()


def _truncate_by_lines(text: str, max_chars: int = 3500) -> str:
    raw = str(text or "").strip()
    if len(raw) <= max_chars:
        return raw
    lines = raw.splitlines()
    kept: list[str] = []
    omitted = 0
    suffix = "\n... truncated"
    current_len = 0
    for idx, line in enumerate(lines):
        line_len = len(line) + (1 if kept else 0)
        remaining_lines = len(lines) - idx - 1
        reserved_suffix = len(suffix) if remaining_lines > 0 else 0
        if current_len + line_len + reserved_suffix > max_chars:
            omitted = len(lines) - idx
            break
        kept.append(line)
        current_len += line_len
    if omitted > 0:
        return "\n".join(kept) + suffix
    return "\n".join(kept)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Send paper reconcile result to Telegram.")
    parser.add_argument(
        "--input-json",
        default="data/reconcile_orders.json",
        help="Path to reconcile JSON payload.",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=20,
        help="Maximum number of delta orders to include in the message.",
    )
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    bot_token = str(os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
    chat_id = str(os.getenv("TELEGRAM_CHAT_ID") or "").strip()
    thread_id = str(os.getenv("TELEGRAM_MESSAGE_THREAD_ID") or "").strip()
    if not bot_token or not chat_id:
        print("telegram_status=skipped (missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID)")
        return 0

    payload = json.loads(Path(args.input_json).read_text(encoding="utf-8"))
    markdown_text = _truncate_by_lines(
        build_reconcile_message(payload, max_rows=max(1, int(args.max_rows))),
        max_chars=3500,
    )

    data = {
        "chat_id": chat_id,
        "text": markdown_text,
        "parse_mode": "Markdown",
        "disable_web_page_preview": True,
    }
    if thread_id:
        data["message_thread_id"] = thread_id

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    req = urllib.request.Request(
        url,
        data=urllib.parse.urlencode(data).encode("utf-8"),
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            body = resp.read().decode("utf-8", errors="ignore")
            print("telegram_status=ok")
            print(body[:300])
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="ignore")
        print(f"telegram_status=http_error_{exc.code}")
        print(body[:500])
        if exc.code == 400:
            fallback_text = _truncate_by_lines(_to_plain_telegram_text(markdown_text), max_chars=3500)
            fallback_data = {
                "chat_id": chat_id,
                "text": fallback_text,
                "disable_web_page_preview": True,
            }
            if thread_id:
                fallback_data["message_thread_id"] = thread_id
            fallback_req = urllib.request.Request(
                url,
                data=urllib.parse.urlencode(fallback_data).encode("utf-8"),
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )
            with urllib.request.urlopen(fallback_req, timeout=20) as resp:
                retry_body = resp.read().decode("utf-8", errors="ignore")
                print("telegram_status=ok_plain_fallback")
                print(retry_body[:300])
                return 0
        raise
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
