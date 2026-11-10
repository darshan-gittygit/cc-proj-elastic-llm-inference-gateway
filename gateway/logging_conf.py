"""
logging_conf.py - one place that configures log output for the gateway.

All decisions are logged through interfaces.EventLog with tags SCHED / ROUTE /
SCALE / HEALTH (ARCHITECTURE.md decision 11). This module only sets the format,
so demo scripts can grep the tags and plots can be built from the timestamps.

Two timestamps per line:
  - wall clock (asctime)  for humans only. WSL2 can step it backwards, so it must
                          NOT be used for elapsed time or event order.
  - mono=<seconds>        time.monotonic(); use this for elapsed time, ordering
                          and plots. Durations inside messages (wait_s, ttft_s,
                          total_s, load_s) are also monotonic.
"""
from __future__ import annotations

import logging
import os
import sys
import time


class _MonotonicStamp(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.mono = time.monotonic()
        return True


def setup_logging(level: str | None = None, logfile: str | None = None) -> None:
    level = (level or os.environ.get("LLMGW_LOG_LEVEL", "INFO")).upper()
    logfile = logfile or os.environ.get("LLMGW_LOG_FILE")
    fmt = logging.Formatter("%(asctime)s.%(msecs)03d mono=%(mono).3f %(levelname)s %(name)s %(message)s",
                            datefmt="%Y-%m-%d %H:%M:%S")
    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    if logfile:
        handlers.append(logging.FileHandler(logfile))
    root = logging.getLogger()
    root.handlers.clear()
    stamp = _MonotonicStamp()
    for h in handlers:
        h.setFormatter(fmt)
        h.addFilter(stamp)
        root.addHandler(h)
    root.setLevel(level)
    # keep third-party noise out of the decision log
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
