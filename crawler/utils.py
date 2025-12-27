# crawler/utils.py
from __future__ import annotations

import csv
import json
import os
import random
import time
from typing import Any, Dict, List, Optional, Sequence, Union
from urllib.parse import urlparse

import requests

try:
    import yaml  # pyyaml
except Exception:
    yaml = None

JsonType = Union[Dict[str, Any], List[Any]]

# -----------------------------
# FS helpers
# -----------------------------
def ensure_dir(path: str) -> None:
    if path:
        os.makedirs(path, exist_ok=True)


def sleep_sec(sec: float, step: float = 0.25) -> None:
    """
    Sleep in small chunks so Ctrl+C stops quickly.
    """
    try:
        sec = float(sec)
    except Exception:
        sec = 0.0
    if sec <= 0:
        return

    # sleep in slices
    remaining = sec
    while remaining > 0:
        s = min(step, remaining)
        time.sleep(s)
        remaining -= s


def load_json(path: str, default: Optional[Any] = None) -> Any:
    if default is None:
        default = {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def dump_json(path: str, obj: Any, indent: int = 2) -> None:
    ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=indent)


def load_yaml(path: str, default: Optional[Any] = None) -> Any:
    if default is None:
        default = {}
    if yaml is None:
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
            return data if data is not None else default
    except Exception:
        return default


# -----------------------------
# HTTP helpers
# -----------------------------
_DEFAULT_UA_POOL = [
    # rotate a few common UAs (helps a bit against basic anti-bot rules)
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
]


_SESSION: Optional[requests.Session] = None


def _get_session() -> requests.Session:
    global _SESSION
    if _SESSION is None:
        s = requests.Session()
        # Keep-alive by default; Session reuses connections.
        _SESSION = s
    return _SESSION


def _validate_url(url: str) -> None:
    """
    Fail fast for invalid url (e.g. "13000" passed by mistake).
    """
    if not isinstance(url, str) or not url.strip():
        raise ValueError("URL is empty")
    p = urlparse(url)
    if p.scheme not in ("http", "https"):
        raise ValueError(f"Invalid URL '{url}': missing scheme (http/https).")


def http_get(
    url: str,
    params: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    timeout: int = 25,
    retries: int = 6,
    backoff: float = 1.5,
    max_backoff: float = 12.0,
    deadline_sec: Optional[float] = None,
    session: Optional[requests.Session] = None,
    allow_non_json: bool = True,
) -> Any:
    """
    Robust GET:
    - Validate URL early (avoid 'Invalid URL 13000')
    - Short connect timeout (avoid hang)
    - Retry on common transient codes: 429, 403, 5xx
    - Exponential backoff + jitter
    - Optional overall deadline_sec (total wall-time cap)

    Returns: parsed JSON (dict/list) if possible, else {"text": "..."} when allow_non_json=True.
    """
    _validate_url(url)

    # total time cap
    start = time.monotonic()
    if deadline_sec is None:
        # reasonable default: ~ (timeout + backoff bursts) but not too huge
        deadline_sec = max(20.0, float(timeout) * 2.5)

    # requests timeout tuple: (connect, read)
    # NOTE: DNS resolution time isn't perfectly capped by connect timeout in all environments,
    # but this helps avoid long TCP hangs.
    connect_timeout = min(6, max(2, int(timeout * 0.25)))
    read_timeout = max(8, int(timeout))

    h = {
        "User-Agent": random.choice(_DEFAULT_UA_POOL),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
        "Connection": "keep-alive",
        "Referer": "https://www.chotot.com/",
    }
    if headers:
        h.update(headers)

    sess = session or _get_session()

    last_err: Optional[BaseException] = None
    for attempt in range(1, int(retries) + 1):
        # deadline check
        elapsed = time.monotonic() - start
        if deadline_sec is not None and elapsed >= deadline_sec:
            raise TimeoutError(f"http_get deadline exceeded ({deadline_sec}s) for {url}")

        try:
            r = sess.get(
                url,
                params=params,
                headers=h,
                timeout=(connect_timeout, read_timeout),
            )

            # Handle throttling / forbidden / transient errors with retry
            if r.status_code in (429, 403, 500, 502, 503, 504):
                # Some CDNs return html; keep snippet for debugging
                body_snip = (r.text or "")[:200].replace("\n", " ")
                raise requests.HTTPError(
                    f"HTTP {r.status_code} (retryable). body[:200]={body_snip}",
                    response=r,
                )

            r.raise_for_status()

            # JSON first
            ctype = (r.headers.get("Content-Type") or "").lower()
            if "application/json" in ctype:
                return r.json()

            # fallback: try json anyway
            try:
                return r.json()
            except Exception:
                if allow_non_json:
                    return {"text": r.text}
                raise ValueError(f"Non-JSON response from {url}")

        except KeyboardInterrupt:
            # Do not swallow Ctrl+C
            raise
        except Exception as e:
            last_err = e

            if attempt >= retries:
                raise

            # sleep with exponential backoff + jitter
            exp = backoff ** (attempt - 1)
            jitter = random.uniform(0.2, 0.9)
            sleep_s = min(max_backoff, exp + jitter)

            # respect deadline
            elapsed = time.monotonic() - start
            if deadline_sec is not None and elapsed + sleep_s > deadline_sec:
                sleep_s = max(0.0, deadline_sec - elapsed)

            sleep_sec(sleep_s)

    # should never reach
    raise last_err if last_err else RuntimeError("http_get failed without exception")


# -----------------------------
# CSV helpers
# -----------------------------
def append_rows_csv(
    path: str,
    rows: Sequence[Union[Dict[str, Any], Sequence[Any]]],
    header: Optional[List[str]] = None,
    encoding: str = "utf-8",
    **_: Any,  # accept unknown kwargs safely
) -> int:
    if not rows:
        return 0

    ensure_dir(os.path.dirname(path))

    file_exists = os.path.exists(path)
    need_header = True
    if file_exists:
        try:
            need_header = (os.path.getsize(path) == 0)
        except Exception:
            need_header = True

    first = rows[0]
    if header is None and isinstance(first, dict):
        header = list(first.keys())

    mode = "a" if file_exists else "w"

    with open(path, mode, newline="", encoding=encoding) as f:
        if header is not None:
            w = csv.DictWriter(
                f,
                fieldnames=header,
                quoting=csv.QUOTE_MINIMAL,
                escapechar="\\",
                doublequote=True,
            )
            if need_header:
                w.writeheader()

            if isinstance(first, dict):
                for r in rows:  # type: ignore
                    out = {k: (r.get(k, "") if isinstance(r, dict) else "") for k in header}
                    w.writerow(out)
            else:
                for r in rows:  # type: ignore
                    d = {header[i]: (r[i] if i < len(r) else "") for i in range(len(header))}
                    w.writerow(d)
        else:
            w = csv.writer(
                f,
                quoting=csv.QUOTE_MINIMAL,
                escapechar="\\",
                doublequote=True,
            )
            for r in rows:  # type: ignore
                w.writerow(list(r))

    return len(rows)
