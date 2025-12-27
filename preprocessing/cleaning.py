# preprocessing/cleaning.py
from __future__ import annotations

import argparse
import csv
import logging
import math
import re
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd


def get_repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


REPO_ROOT = get_repo_root()
DATA_DIR = REPO_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
CLEAN_DIR = DATA_DIR / "cleaned"
LOG_DIR = REPO_ROOT / "logs"


def setup_logger(log_file: Optional[Path] = None) -> logging.Logger:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("preprocessing.cleaning")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    if log_file:
        fh = logging.FileHandler(str(log_file), encoding="utf-8")
        fh.setFormatter(fmt)
        logger.addHandler(fh)

    return logger


def safe_float(x) -> Optional[float]:
    if x is None:
        return None
    if isinstance(x, float):
        return float(x) if math.isfinite(x) else None
    s = str(x).strip()
    if s == "" or s.lower() in {"nan", "none", "null"}:
        return None
    s = s.replace(",", "").replace(" ", "")
    try:
        v = float(s)
        return float(v) if math.isfinite(v) else None
    except Exception:
        return None


def safe_int(x) -> Optional[int]:
    v = safe_float(x)
    if v is None:
        return None
    try:
        return int(round(v))
    except Exception:
        return None


def parse_latlon_from_location(loc: str) -> Tuple[Optional[float], Optional[float]]:
    if not loc:
        return None, None
    s = str(loc).strip().strip('"').strip("'")
    m = re.search(r"(-?\d+(?:\.\d+)?)\s*,\s*(-?\d+(?:\.\d+)?)", s)
    if not m:
        return None, None
    return safe_float(m.group(1)), safe_float(m.group(2))


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.strip() for c in df.columns]

    # If raw_json missing but last column looks like JSON -> rename it
    if "raw_json" not in df.columns and len(df.columns) >= 1:
        last = df.columns[-1]
        if df[last].astype(str).str.contains(r"^\s*\{", regex=True, na=False).any():
            df = df.rename(columns={last: "raw_json"})

    required = [
        "listing_id",
        "listing_url",
        "brand",
        "model",
        "year",
        "price",
        "mileage_km",
        "fuel",
        "location",
        "lat",
        "lon",
        "color",
        "region_v2",
        "region_name",
        "crawl_time",
        "source",
        "raw_json",
    ]
    for c in required:
        if c not in df.columns:
            df[c] = None

    return df[required].copy()


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = normalize_columns(df)

    # strip strings (avoid SettingWithCopyWarning by using .loc)
    str_cols = ["listing_url", "brand", "model", "fuel", "color", "region_name", "source"]
    for c in str_cols:
        df.loc[:, c] = df[c].where(df[c].notna(), None)
        df.loc[:, c] = df[c].map(lambda x: x.strip() if isinstance(x, str) else x)

    # numeric conversions
    df.loc[:, "listing_id"] = df["listing_id"].map(safe_int).astype("Int64")
    df.loc[:, "year"] = df["year"].map(safe_int).astype("Int64")
    df.loc[:, "price"] = df["price"].map(safe_int).astype("Int64")
    df.loc[:, "mileage_km"] = df["mileage_km"].map(safe_int).astype("Int64")
    df.loc[:, "region_v2"] = df["region_v2"].map(safe_int).astype("Int64")

    df.loc[:, "lat"] = df["lat"].map(safe_float)
    df.loc[:, "lon"] = df["lon"].map(safe_float)

    # fill lat/lon from location if missing
    need_parse = df["lat"].isna() | df["lon"].isna()
    if need_parse.any():
        parsed = df.loc[need_parse, "location"].map(parse_latlon_from_location)
        df.loc[need_parse, "lat"] = [p[0] for p in parsed]
        df.loc[need_parse, "lon"] = [p[1] for p in parsed]

    # crawl_time normalize empty -> None
    df.loc[:, "crawl_time"] = df["crawl_time"].map(
        lambda x: str(x).strip() if pd.notna(x) and str(x).strip() != "" else None
    )

    # raw_json keep as string
    df.loc[:, "raw_json"] = df["raw_json"].map(lambda x: str(x) if pd.notna(x) else None)

    # drop invalid
    df = df[df["listing_id"].notna() & df["listing_url"].notna()].copy()
    return df


def read_csv_robust(path: Path) -> pd.DataFrame:
    try:
        return pd.read_csv(path, dtype=str, encoding="utf-8", on_bad_lines="skip", low_memory=False)
    except UnicodeDecodeError:
        return pd.read_csv(path, dtype=str, encoding="utf-8-sig", on_bad_lines="skip", low_memory=False)


def write_csv_safe(df: pd.DataFrame, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(
        out_path,
        index=False,
        encoding="utf-8-sig",
        quoting=csv.QUOTE_ALL,
        lineterminator="\n",   # ✅ FIX: pandas dùng lineterminator
    )


def run(raw_dir: Path, clean_dir: Path) -> int:
    logger = setup_logger(LOG_DIR / f"cleaning_{pd.Timestamp.now():%Y%m%d}.log")

    logger.info(f"REPO_ROOT={REPO_ROOT}")
    logger.info(f"RAW_DIR={raw_dir}")
    logger.info(f"CLEAN_DIR={clean_dir}")

    raw_dir.mkdir(parents=True, exist_ok=True)
    clean_dir.mkdir(parents=True, exist_ok=True)

    raw_files = sorted(raw_dir.glob("*.csv"))
    if not raw_files:
        logger.info(f"No raw files found in {raw_dir}")
        return 0

    total_in = 0
    total_out = 0

    for f in raw_files:
        logger.info(f"Reading raw: {f.name}")
        df = read_csv_robust(f)
        n_in = len(df)
        total_in += n_in

        df_clean = clean_dataframe(df)
        n_out = len(df_clean)
        total_out += n_out

        out_name = f.name.replace("raw", "cleaned")
        out_path = clean_dir / out_name
        write_csv_safe(df_clean, out_path)

        logger.info(f"Wrote cleaned: {out_path} rows_in={n_in} rows_out={n_out}")

    logger.info(f"DONE total_in={total_in} total_out={total_out}")
    return 0


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw-dir", default=str(RAW_DIR))
    parser.add_argument("--clean-dir", default=str(CLEAN_DIR))
    args = parser.parse_args()
    raise SystemExit(run(Path(args.raw_dir), Path(args.clean_dir)))


if __name__ == "__main__":
    main()
