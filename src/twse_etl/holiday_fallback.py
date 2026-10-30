"""Unscheduled-closure check for the 'calendar says trading day, TWSE never published' case.

Typhoon closures (e.g. 2024-10-31, 2026-10-23) are announced the evening before and never appear
in the official holiday schedule, and TWSE answers them exactly like a holiday: "no data".
After the data-wait cutoff we ask an LLM command-line tool. It sits behind a fixed JSON contract,
so the vendor can be swapped through HOLIDAY_LLM_CMD without touching the pipeline.

Fail closed: anything other than a well-formed "closed" verdict with an evidence URL is treated
as a data anomaly, never as a holiday.
"""

from __future__ import annotations

import json
import os
import shlex
import subprocess
import sys
from dataclasses import asdict, dataclass
from datetime import date

DEFAULT_CMD = f"{sys.executable} -m twse_etl.holiday_llm"
TIMEOUT_S = 300
CONTRACT_KEYS = {"is_holiday", "reason", "evidence_url"}


class HolidayCheckError(RuntimeError):
    """The LLM could not give a usable answer; treat the missing data as an anomaly."""


@dataclass(frozen=True)
class Verdict:
    is_holiday: bool
    reason: str
    evidence_url: str | None

    def to_dict(self) -> dict:
        return asdict(self)


def parse_verdict(stdout: str) -> Verdict:
    try:
        doc = json.loads(stdout)
    except json.JSONDecodeError as exc:
        raise HolidayCheckError(f"LLM output is not JSON: {stdout[:200]!r}") from exc
    if (
        not isinstance(doc, dict)
        or set(doc) != CONTRACT_KEYS
        or not isinstance(doc["is_holiday"], bool)
        or not isinstance(doc["reason"], str)
        or not (doc["evidence_url"] is None or isinstance(doc["evidence_url"], str))
    ):
        raise HolidayCheckError(f"LLM output violates the contract: {doc!r}")
    return Verdict(**doc)


def ask_llm(trade_date: date, cmd: str | None = None) -> Verdict:
    """Run `<cmd> YYYY-MM-DD`; stdout must be exactly the JSON contract."""
    cmd = cmd or os.environ.get("HOLIDAY_LLM_CMD") or DEFAULT_CMD
    try:
        proc = subprocess.run(
            [*shlex.split(cmd), trade_date.isoformat()],
            capture_output=True,
            text=True,
            timeout=TIMEOUT_S,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise HolidayCheckError(f"LLM command could not run: {exc}") from exc
    if proc.returncode != 0:
        raise HolidayCheckError(f"LLM command exited {proc.returncode}: {proc.stderr.strip()[-300:]}")
    return parse_verdict(proc.stdout.strip())
