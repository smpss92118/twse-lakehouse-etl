"""Default HOLIDAY_LLM_CMD: ask Claude (with web search) whether TWSE was closed on a date.

    python -m twse_etl.holiday_llm 2026-10-23
    -> {"is_holiday": true, "reason": "...", "evidence_url": "https://..."}

stdout carries exactly the JSON contract that holiday_fallback.parse_verdict() validates; any
failure exits non-zero with the reason on stderr, which the pipeline treats as "unknown" and
therefore as a data anomaly. Credentials come from the environment (ANTHROPIC_API_KEY in the
gitignored .env); nothing is stored in the repo.
"""

from __future__ import annotations

import json
import os
import sys
from datetime import date

import anthropic

# A yes/no lookup with a cited source: Sonnet at medium effort, with the dynamic-filtering web search.
MODEL = os.environ.get("HOLIDAY_LLM_MODEL", "claude-sonnet-5-5")
MAX_TURNS = 4  # pause_turn continuations for long web-search turns

VERDICT_SCHEMA = {
    "type": "object",
    "properties": {
        "is_holiday": {"type": "boolean"},
        "reason": {"type": "string"},
        "evidence_url": {"type": ["string", "null"]},
    },
    "required": ["is_holiday", "reason", "evidence_url"],
    "additionalProperties": False,
}

PROMPT = """Was the Taiwan Stock Exchange (TWSE, 臺灣證券交易所 集中市場) closed for trading on {day} \
(a {weekday}, Asia/Taipei)?

Context: the official holiday schedule lists this date as a trading day, yet TWSE published no \
closing quotes for it. Unscheduled full-day closures happen, mostly typhoon days: when the Taipei \
City government suspends work for the day, TWSE does not open. Search news and official \
announcements (e.g. "證交所 休市 {day_zh}", "颱風 停止上班 台股 休市") to find out.

Answer is_holiday=true only if a source explicitly says the TWSE market was closed on {day}; put \
that source's URL in evidence_url and summarise the cause in reason (Traditional Chinese, one \
sentence). If you find no such source, answer is_holiday=false, evidence_url=null, and say in \
reason what you checked."""


def ask(day: date) -> dict:
    client = anthropic.Anthropic()
    messages: list[dict] = [
        {
            "role": "user",
            "content": PROMPT.format(
                day=day.isoformat(), weekday=day.strftime("%A"), day_zh=f"{day.month}月{day.day}日"
            ),
        }
    ]
    for _ in range(MAX_TURNS):
        response = client.beta.messages.create(
            model=MODEL,
            max_tokens=16000,
            # On a safety-classifier refusal the API re-runs the request on a fallback model (Claude API only).
            betas=["server-side-fallback-2026-07-01"],
            fallbacks="default",
            tools=[{"type": "web_search_20260209", "name": "web_search", "max_uses": 5}],
            output_config={"effort": "medium", "format": {"type": "json_schema", "schema": VERDICT_SCHEMA}},
            messages=messages,
        )
        if response.stop_reason == "pause_turn":
            messages.append({"role": "assistant", "content": response.content})
            continue
        if response.stop_reason == "refusal":
            raise RuntimeError(f"model declined: {response.stop_details}")
        if response.stop_reason != "end_turn":
            raise RuntimeError(f"unexpected stop_reason {response.stop_reason!r}")
        text = [block.text for block in response.content if block.type == "text"]
        return json.loads(text[-1])
    raise RuntimeError("web search did not finish within the turn limit")


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: python -m twse_etl.holiday_llm YYYY-MM-DD", file=sys.stderr)
        return 2
    try:
        verdict = ask(date.fromisoformat(sys.argv[1]))
    # TypeError: the SDK raises it when no credentials are configured at all
    except (anthropic.AnthropicError, RuntimeError, ValueError, TypeError) as exc:
        print(f"holiday check failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(verdict, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
