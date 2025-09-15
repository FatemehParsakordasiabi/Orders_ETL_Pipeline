from __future__ import annotations
import logging
from logging import Logger

def get_logger(name: str = "etl") -> Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        ch = logging.StreamHandler()
        fmt = logging.Formatter(
            fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        ch.setFormatter(fmt)
        logger.addHandler(ch)
    return logger
