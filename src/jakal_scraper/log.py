from __future__ import annotations
import json
import sys
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

@dataclass
class Logger:
    stream = sys.stdout

    def event(self, name: str, **fields: Any) -> None:
        payload: Dict[str, Any] = {"ts": time.time(), "event": name, **fields}
        self.stream.write(json.dumps(payload, ensure_ascii=False) + "\n")
        self.stream.flush()

    def info(self, msg: str, **fields: Any) -> None:
        self.event("info", msg=msg, **fields)

    def warn(self, msg: str, **fields: Any) -> None:
        self.event("warn", msg=msg, **fields)

    def error(self, msg: str, **fields: Any) -> None:
        self.event("error", msg=msg, **fields)
