import json
import os
from typing import Any, Dict, Optional

import mysql.connector

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
CONFIG_DIR = os.path.join(ROOT, "configs")


def load_settings() -> Dict[str, Any]:
    path = os.path.join(CONFIG_DIR, "settings.json")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def get_mysql_conn(database: Optional[str] = None):
    cfg = load_settings()["mysql"]
    conn = mysql.connector.connect(
        host=cfg["host"],
        port=int(cfg.get("port", 3306)),
        user=cfg["user"],
        password=cfg["password"],
        database=database or cfg["database"],
        autocommit=False,
        charset="utf8mb4",
        use_unicode=True,
    )
    return conn
