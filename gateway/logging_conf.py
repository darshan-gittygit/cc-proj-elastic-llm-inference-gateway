"""
logging_conf.py - one place that configures log output for the gateway.

All decisions are logged through interfaces.EventLog with tags SCHED / ROUTE /
SCALE / HEALTH (ARCHITECTURE.md decision 11). This module only sets the format,
so demo scripts can grep the tags and plots can be built from the timestamps.
"""
from __future__ import annotations

import logging
import os
import sys


def setup_logging(level: str | None = None, logfile: str | None = None) -> None:
    level = (level or os.environ.get("LLMGW_LOG_LEVEL", "INFO")).upper()
    logfile = logfile or os.environ.get("LLMGW_LOG_FILE")
    fmt = logging.Formatter("%(asctime)s.%(msecs)03d %(levelname)s %(name)s %(message)s",
                            datefmt="%Y-%m-%d %H:%M:%S")
    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    if logfile:
        handlers.append(logging.FileHandler(logfile))
    root = logging.getLogger()
    root.handlers.clear()
    for h in handlers:
        h.setFormatter(fmt)
        root.addHandler(h)
    root.setLevel(level)
    # keep third-party noise out of the decision log
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
