# engine/logging_ct.py
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

CT = ZoneInfo("America/Chicago")

class CTFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, tz=CT)
        return dt.strftime(datefmt or "%Y-%m-%d %H:%M:%S")

def build_logger(name: str = "engine") -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    handler = logging.StreamHandler()
    handler.setFormatter(CTFormatter(fmt="%(asctime)s CT [%(levelname)s] %(message)s"))

    logger.handlers = [handler]
    logger.propagate = False
    return logger
