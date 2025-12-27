# preprocessing/transformation.py
from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import unicodedata
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import numpy as np
import pandas as pd


# =========================
# Utils
# =========================

def _safe_json_loads(x: Any) -> Dict[str, Any]:
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return {}
    if isinstance(x, dict):
        return x
    if isinstance(x, (bytes, bytearray)):
        try:
            x = x.decode("utf-8", errors="ignore")
        except Exception:
            return {}
    if isinstance(x, str):
        s = x.strip()
        if not s:
            return {}
        try:
            return json.loads(s)
        except Exception:
            # Sometimes content is already-like JSON but broken; keep empty to avoid crash
            return {}
    return {}


def _to_int(x: Any) -> Optional[int]:
    try:
        if x is None:
            return None
        if isinstance(x, float) and np.isnan(x):
            return None
        if isinstance(x, str) and x.strip() == "":
            return None
        return int(float(x))
    except Exception:
        return None


def _to_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        if isinstance(x, float) and np.isnan(x):
            return None
        if isinstance(x, str) and x.strip() == "":
            return None
        return float(x)
    except Exception:
        return None


def _normalize_no_accent(text: str) -> str:
    text = text.lower().strip()
    text = unicodedata.normalize("NFD", text)
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
    text = text.replace("đ", "d")
    text = re.sub(r"\s+", " ", text)
    return text


def _coalesce(*vals):
    for v in vals:
        if v is None:
            continue
        if isinstance(v, float) and np.isnan(v):
            continue
        if isinstance(v, str) and v.strip() == "":
            continue
        return v
    return None


# =========================
# Color extraction
# =========================

# Longer phrases first (IMPORTANT)
_COLOR_PHRASES = [
    "xanh đen",
    "xanh duong",
    "xanh dương",
    "xanh la",
    "xanh lá",
    "xanh ngoc",
    "xanh ngọc",
    "xam long chuot",
    "xám lông chuột",
    "ghi",
    "trang",
    "trắng",
    "den",
    "đen",
    "do",
    "đỏ",
    "bac",
    "bạc",
    "xam",
    "xám",
    "vang",
    "vàng",
    "nau",
    "nâu",
    "cam",
    "hong",
    "hồng",
    "tim",
    "tím",
    "be",
    "kem",
]

# Build regex that matches accents/no-accents by normalizing input text first
_COLOR_PATTERNS = sorted({_normalize_no_accent(x) for x in _COLOR_PHRASES}, key=len, reverse=True)


def _extract_color_text(subject: str, body: str) -> Optional[str]:
    """
    Try to find color phrase inside subject/body.
    Returns a human-friendly Vietnamese-ish lower string (keeps spaces),
    using the matched normalized phrase (no-accent form).
    """
    combined = " ".join([subject or "", body or ""]).strip()
    if not combined:
        return None

    norm = _normalize_no_accent(combined)

    # Quick win: subject often ends with color (e.g. "... Xanh den")
    # We'll still do generic match.
    for pat in _COLOR_PATTERNS:
        # word-ish boundary
        if re.search(rf"(?<!\w){re.escape(pat)}(?!\w)", norm):
            return pat  # keep no-accent; stable for analytics
    return None


def _build_colorid_to_name_mapping(df: pd.DataFrame) -> Dict[int, str]:
    """
    Auto-learn mapping per run:
    for each color_id, choose most common extracted color text among rows.
    """
    mapping: Dict[int, str] = {}
    if "color_id" not in df.columns or "color_name" not in df.columns:
        return mapping

    tmp = df[["color_id", "color_name"]].dropna()
    tmp = tmp[tmp["color_id"].apply(lambda x: _to_int(x) is not None)]
    if tmp.empty:
        return mapping

    tmp["color_id"] = tmp["color_id"].apply(_to_int)
    tmp = tmp.dropna(subset=["color_id", "color_name"])

    for cid, g in tmp.groupby("color_id"):
        # most frequent non-empty color_name
        vc = g["color_name"].astype(str).str.strip()
        vc = vc[vc != ""].value_counts()
        if not vc.empty:
            mapping[int(cid)] = vc.index[0]
    return mapping


# =========================
# Fuel normalization
# =========================

def _normalize_fuel(fuel_val: Any, raw: Dict[str, Any]) -> Optional[str]:
    """
    Output: xang / dau / dien / hybrid / khac
    """
    # prefer explicit text in cleaned column
    s = None
    if isinstance(fuel_val, str) and fuel_val.strip():
        s = _normalize_no_accent(fuel_val)

    # if numeric enum in raw_json
    if s is None:
        f = _coalesce(raw.get("fuel"), raw.get("fuel_type"), raw.get("fuel_v2"))
        if isinstance(f, (int, float)) and not (isinstance(f, float) and np.isnan(f)):
            # common: 1 = xang, 2 = dau (from your samples)
            if int(f) == 1:
                return "xang"
            if int(f) == 2:
                return "dau"
        if isinstance(f, str) and f.strip():
            s = _normalize_no_accent(f)

    if not s:
        return None

    if "xang" in s or "petrol" in s or "gas" in s:
        return "xang"
    if "dau" in s or "diesel" in s:
        return "dau"
    if "dien" in s or "electric" in s:
        return "dien"
    if "hybrid" in s:
        return "hybrid"
    return "khac"


# =========================
# IO discover
# =========================

def _project_root() -> Path:
    # .../preprocessing/transformation.py -> project root is parent of "preprocessing"
    return Path(__file__).resolve().parents[1]


def _find_latest_cleaned_csv() -> Path:
    root = _project_root()
    candidates = []
    for rel in ["data/cleaned", "data/clean", "data"]:
        p = root / rel
        if not p.exists():
            continue
        candidates += list(p.glob("chotot_cleaned_*.csv"))

    if not candidates:
        raise FileNotFoundError("Không tìm thấy file input dạng data/**/chotot_cleaned_*.csv")

    def key_fn(fp: Path):
        # parse YYYYMMDD in filename
        m = re.search(r"chotot_cleaned_(\d{8})", fp.name)
        if m:
            try:
                return datetime.strptime(m.group(1), "%Y%m%d")
            except Exception:
                pass
        return datetime.fromtimestamp(fp.stat().st_mtime)

    return sorted(candidates, key=key_fn)[-1]


# =========================
# Main transform
# =========================

@dataclass
class TransformConfig:
    input_csv: Path
    output_csv: Path
    write_master: bool = False
    master_csv: Optional[Path] = None


def transform(cfg: TransformConfig) -> pd.DataFrame:
    df = pd.read_csv(cfg.input_csv)

    # Ensure required cols exist (don't crash if missing)
    for c in [
        "listing_id", "listing_url", "brand", "model", "year", "price",
        "mileage_km", "fuel", "location", "lat", "lon",
        "color", "region_v2", "region_name", "crawl_time", "source", "raw_json"
    ]:
        if c not in df.columns:
            df[c] = np.nan

    # Parse raw_json once
    raw_objs = df["raw_json"].apply(_safe_json_loads)

    # Base typed fields
    df["listing_id"] = df["listing_id"].astype(str).str.replace(r"\.0$", "", regex=True)
    df["year"] = df["year"].apply(_to_int)
    df["price"] = df["price"].apply(_to_int)

    # mileage: prefer cleaned mileage_km else raw_json mileage_v2
    def _mileage_row(i: int) -> Optional[int]:
        v = _to_int(df.at[i, "mileage_km"])
        if v is not None:
            return v
        raw = raw_objs.iat[i]
        return _to_int(_coalesce(raw.get("mileage_v2"), raw.get("mileage")))
    df["mileage_km"] = [ _mileage_row(i) for i in range(len(df)) ]

    # lat/lon cleanup
    df["lat"] = df["lat"].apply(_to_float)
    df["lon"] = df["lon"].apply(_to_float)

    # crawl_date from crawl_time
    def _crawl_date(x: Any) -> Optional[str]:
        if not isinstance(x, str) or not x.strip():
            return None
        # expected: "YYYY-mm-dd HH:MM:SS"
        try:
            return x.strip()[:10]
        except Exception:
            return None
    df["crawl_date"] = df["crawl_time"].apply(_crawl_date)

    # color_id: prefer raw_json["carcolor"], else numeric in df["color"] if present
    def _color_id(i: int) -> Optional[int]:
        raw = raw_objs.iat[i]
        cid = _to_int(raw.get("carcolor"))
        if cid is not None:
            return cid
        return _to_int(df.at[i, "color"])
    df["color_id"] = [ _color_id(i) for i in range(len(df)) ]

    # color_name: extract from subject/body, else later fill by learned mapping
    def _color_name(i: int) -> Optional[str]:
        raw = raw_objs.iat[i]
        subj = raw.get("subject") or ""
        body = raw.get("body") or ""
        txt = _extract_color_text(subj, body)
        return txt  # no-accent, lower (stable)
    df["color_name"] = [ _color_name(i) for i in range(len(df)) ]

    # Learn mapping per color_id, then fill missing color_name
    mapping = _build_colorid_to_name_mapping(df)
    if mapping:
        mask = df["color_name"].isna() | (df["color_name"].astype(str).str.strip() == "")
        df.loc[mask, "color_name"] = df.loc[mask, "color_id"].apply(lambda x: mapping.get(_to_int(x)) if _to_int(x) is not None else None)

    # fuel_norm
    df["fuel_norm"] = [
        _normalize_fuel(df.at[i, "fuel"], raw_objs.iat[i])
        for i in range(len(df))
    ]

    # Optional extra fields you may want later
    df["veh_inspected"] = [ _to_int(raw_objs.iat[i].get("veh_inspected")) for i in range(len(df)) ]

    # Source file
    df["source_file"] = cfg.input_csv.name

    # Choose output columns (keep raw_json at end)
    out_cols = [
        "listing_id", "listing_url", "brand", "model", "year", "price", "mileage_km",
        "fuel", "fuel_norm",
        "location", "lat", "lon",
        "color_id", "color_name",
        "region_v2", "region_name",
        "crawl_time", "crawl_date",
        "source", "veh_inspected",
        "raw_json",
        "source_file",
    ]
    out_cols = [c for c in out_cols if c in df.columns]
    out_df = df[out_cols].copy()

    # Write output safely (quotes to protect commas/newlines in raw_json/body)
    cfg.output_csv.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(
        cfg.output_csv,
        index=False,
        encoding="utf-8-sig",
        quoting=csv.QUOTE_ALL,
        escapechar="\\",
        lineterminator="\n",
    )

    # Append to master if asked
    if cfg.write_master:
        master = cfg.master_csv or (_project_root() / "data" / "master" / "car_price_master.csv")
        master.parent.mkdir(parents=True, exist_ok=True)

        if master.exists():
            # append without duplicating header
            out_df.to_csv(
                master,
                index=False,
                mode="a",
                header=False,
                encoding="utf-8-sig",
                quoting=csv.QUOTE_ALL,
                escapechar="\\",
                lineterminator="\n",
            )
        else:
            out_df.to_csv(
                master,
                index=False,
                encoding="utf-8-sig",
                quoting=csv.QUOTE_ALL,
                escapechar="\\",
                lineterminator="\n",
            )

    return out_df


def _default_output_path(input_csv: Path) -> Path:
    root = _project_root()
    m = re.search(r"chotot_cleaned_(\d{8})", input_csv.name)
    suffix = m.group(1) if m else datetime.now().strftime("%Y%m%d")
    return root / "data" / "transformed" / f"chotot_transformed_{suffix}.csv"


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Transform cleaned ChoTot car data -> transformed CSV")
    p.add_argument("--input", type=str, default="", help="Path to cleaned csv (default: latest chotot_cleaned_*.csv)")
    p.add_argument("--output", type=str, default="", help="Path to output transformed csv")
    p.add_argument("--write-master", action="store_true", help="Append output into data/master/car_price_master.csv")
    p.add_argument("--master", type=str, default="", help="Custom master csv path (optional)")
    return p.parse_args(argv)


def main(argv=None) -> None:
    args = parse_args(argv)

    input_csv = Path(args.input).resolve() if args.input else _find_latest_cleaned_csv()
    output_csv = Path(args.output).resolve() if args.output else _default_output_path(input_csv)

    cfg = TransformConfig(
        input_csv=input_csv,
        output_csv=output_csv,
        write_master=bool(args.write_master),
        master_csv=Path(args.master).resolve() if args.master else None,
    )

    out_df = transform(cfg)

    # Print quick sanity stats
    print(f"[OK] input : {cfg.input_csv}")
    print(f"[OK] output: {cfg.output_csv}")
    if cfg.write_master:
        print(f"[OK] master appended: {cfg.master_csv or (_project_root() / 'data' / 'master' / 'car_price_master.csv')}")
    print(f"rows={len(out_df):,}")
    print("color_name non-null:", int(out_df["color_name"].notna().sum()) if "color_name" in out_df.columns else 0)
    print("sample colors:", out_df[["color_id", "color_name"]].dropna().head(10).to_string(index=False))


if __name__ == "__main__":
    main()
