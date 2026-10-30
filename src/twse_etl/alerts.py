"""Alert sink. Locally an ALERT log line; swap for Slack/SNS by changing this one function."""

from __future__ import annotations

import json
import logging

log = logging.getLogger("twse_etl.alert")


def alert(event: str, **context: object) -> None:
    log.error("ALERT %s %s", event, json.dumps(context, ensure_ascii=False, default=str))
