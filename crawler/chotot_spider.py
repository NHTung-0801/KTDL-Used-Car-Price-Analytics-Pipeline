# crawler/chotot_spider.py
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from .utils import ensure_dir, sleep_sec, load_json, dump_json, http_get, append_rows_csv

DEFAULT_REGIONS_URL = "https://gateway.chotot.com/v1/public/web-proxy-api/loadRegionsV2"
DEFAULT_LISTING_API = "https://gateway.chotot.com/v1/public/ad-listing"

DEFAULT_CATEGORY_CAR = 2010  # Ô tô


# =========================
# Logging
# =========================
def _now_str() -> str:
    # local time
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]


def _today_yyyymmdd() -> str:
    return datetime.now().strftime("%Y%m%d")


def get_repo_root() -> Path:
    # repo_root/ crawler/ chotot_spider.py  -> parents[1] is repo_root
    return Path(__file__).resolve().parents[1]


def setup_logger(log_file: Path):
    ensure_dir(log_file.parent)

    def _log(level: str, msg: str):
        line = f"{_now_str()} | {level.upper():<5} | {msg}"
        print(line)
        try:
            with open(log_file, "a", encoding="utf-8") as f:
                f.write(line + "\n")
        except Exception:
            pass

    class _Logger:
        def info(self, msg: str): _log("INFO", msg)
        def warning(self, msg: str): _log("WARNING", msg)
        def error(self, msg: str): _log("ERROR", msg)

    return _Logger()


# =========================
# Meta (seen ids + cursor)
# =========================
def load_seen_ids(path: Path) -> set[str]:
    if not path.exists():
        return set()
    s: set[str] = set()
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if line:
                s.add(line)
    return s


def save_seen_ids(path: Path, seen_ids: set[str]) -> None:
    ensure_dir(path.parent)
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        for x in sorted(seen_ids):
            f.write(str(x) + "\n")
    os.replace(tmp, path)


def load_cursor(path: Path) -> int:
    if not path.exists():
        return 0
    try:
        obj = load_json(str(path))
        if isinstance(obj, dict) and isinstance(obj.get("cursor"), int):
            return int(obj["cursor"])
    except Exception:
        pass
    return 0


def save_cursor(path: Path, cursor: int) -> None:
    ensure_dir(path.parent)
    payload = {"cursor": int(cursor)}

    # utils.dump_json có thể được định nghĩa theo 1 trong 2 kiểu:
    #   dump_json(path, obj)  hoặc  dump_json(obj, path)
    # nên ta thử kiểu phổ biến trước, nếu lỗi thì fallback.
    try:
        dump_json(str(path), payload)     # ✅ kiểu: dump_json(path, obj)
    except TypeError:
        dump_json(payload, str(path))     # ✅ fallback: dump_json(obj, path)


# =========================
# Regions handling
# =========================
def _is_url(s: str) -> bool:
    return s.startswith("http://") or s.startswith("https://")


def _is_probably_file(s: str) -> bool:
    try:
        return Path(s).exists()
    except Exception:
        return False


def parse_regions_arg(arg: Optional[str]) -> List[int]:
    """
    Parse CLI --regions:
      - "13000" => [13000]
      - "13000,16000" => [13000,16000]
      - "13000 16000" => [13000,16000]
      - "13000;16000" => [13000,16000]
    If not numeric list => return []
    """
    if not arg:
        return []
    s = str(arg).strip()
    if not s:
        return []

    # If it's a URL or an existing file path => not numeric list
    if _is_url(s) or _is_probably_file(s):
        return []

    # normalize separators
    for sep in [";", "|", "\n", "\t"]:
        s = s.replace(sep, ",")
    s = s.replace(" ", ",")

    parts = [p.strip() for p in s.split(",") if p.strip()]
    out: List[int] = []
    for p in parts:
        if p.isdigit():
            out.append(int(p))
        else:
            return []
    return out


@dataclass
class RegionItem:
    id: int
    name: str
    level: Optional[int] = None
    parent_id: Optional[int] = None
    children_count: int = 0


def _collect_region_candidates(obj: Any, out: Dict[int, RegionItem]) -> None:
    """
    Recursively collect objects having (id/name) fields.
    We try multiple key variants because gateway response can change.
    """
    if isinstance(obj, dict):
        # possible id keys
        id_keys = ["id", "value", "region_v2", "regionV2", "region_id", "regionId"]
        name_keys = ["name", "label", "text", "title"]

        rid = None
        rname = None

        for k in id_keys:
            if k in obj and isinstance(obj[k], (int, str)):
                try:
                    rid = int(obj[k])
                    break
                except Exception:
                    rid = None

        for k in name_keys:
            if k in obj and isinstance(obj[k], str) and obj[k].strip():
                rname = obj[k].strip()
                break

        if rid is not None and rname is not None:
            level = None
            for lk in ["level", "lvl", "depth"]:
                if lk in obj and isinstance(obj[lk], (int, str)):
                    try:
                        level = int(obj[lk])
                        break
                    except Exception:
                        level = None

            parent_id = None
            for pk in ["parent_id", "parentId", "parent", "parent_value"]:
                if pk in obj and isinstance(obj[pk], (int, str)):
                    try:
                        parent_id = int(obj[pk])
                        break
                    except Exception:
                        parent_id = None

            children_count = 0
            for ck in ["children", "child", "items", "subs"]:
                if ck in obj and isinstance(obj[ck], list):
                    children_count = len(obj[ck])
                    break

            if rid not in out:
                out[rid] = RegionItem(id=rid, name=rname, level=level, parent_id=parent_id, children_count=children_count)

        for v in obj.values():
            _collect_region_candidates(v, out)

    elif isinstance(obj, list):
        for it in obj:
            _collect_region_candidates(it, out)


def _pick_provinces(cands: List[RegionItem]) -> List[RegionItem]:
    """
    Heuristics to pick top-level provinces/cities.
    """
    if not cands:
        return []

    # 1) if level info exists, prefer level==1 with reasonable size
    lv1 = [c for c in cands if c.level == 1]
    if 40 <= len(lv1) <= 120:
        return lv1

    # 2) if parent_id exists, prefer parent_id in (0, None) with reasonable size
    top_parent = [c for c in cands if c.parent_id in (0, None)]
    if 40 <= len(top_parent) <= 120:
        return top_parent

    # 3) choose by shortest-id-length group that looks like provinces (count 40..120)
    length_groups: Dict[int, List[RegionItem]] = {}
    for c in cands:
        L = len(str(abs(int(c.id))))
        length_groups.setdefault(L, []).append(c)

    for L in sorted(length_groups.keys()):
        grp = length_groups[L]
        if 40 <= len(grp) <= 120:
            return grp

    # 4) fallback filter out obvious lower levels by name prefix
    bad_prefix = ("Quận", "Huyện", "Xã", "Phường", "Thị trấn", "Ấp", "Khu phố")
    filtered = [c for c in cands if not c.name.startswith(bad_prefix)]
    if 40 <= len(filtered) <= 200:
        return filtered

    # 5) last resort: return smallest 100 by id length then id
    cands_sorted = sorted(cands, key=lambda x: (len(str(x.id)), x.id))
    return cands_sorted[:100]


def load_regions_v2_from_url(url: str, logger, timeout: float = 20.0) -> List[Tuple[int, str]]:
    payload = http_get(url, params=None, timeout=timeout)
    candidates: Dict[int, RegionItem] = {}
    _collect_region_candidates(payload, candidates)

    picked = _pick_provinces(list(candidates.values()))
    picked = sorted(picked, key=lambda x: x.id)

    logger.info(f"Loaded regions_v2(provinces) size={len(picked)} from {url}")
    return [(r.id, r.name) for r in picked]


def load_regions_v2_from_file(path: str, logger) -> List[Tuple[int, str]]:
    p = Path(path)
    if not p.exists():
        logger.warning(f"regions file not found: {path}")
        return []
    text = p.read_text(encoding="utf-8", errors="ignore").strip()
    if not text:
        return []

    # allow formats:
    # 13000
    # 13000,HCM
    # 13000,Hồ Chí Minh
    out: List[Tuple[int, str]] = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        parts = [x.strip() for x in line.split(",")]
        if parts[0].isdigit():
            rid = int(parts[0])
            name = parts[1] if len(parts) > 1 else ""
            out.append((rid, name))
    logger.info(f"Loaded regions_v2 from file size={len(out)} path={path}")
    return out


def slice_regions_with_cursor(regions: List[Tuple[int, str]], cursor: int, k: int) -> Tuple[List[Tuple[int, str]], int]:
    if not regions:
        return [], 0
    n = len(regions)
    if k <= 0 or k >= n:
        return regions, (cursor % n)

    cursor = cursor % n
    selected: List[Tuple[int, str]] = []
    for i in range(k):
        selected.append(regions[(cursor + i) % n])
    new_cursor = (cursor + k) % n
    return selected, new_cursor


# =========================
# Listing fetching
# =========================
def _extract_ads(payload: Any) -> List[Dict[str, Any]]:
    """
    Try to find list[dict] of ads from various response shapes.
    """
    if payload is None:
        return []

    if isinstance(payload, list):
        # sometimes already list of ads
        if payload and isinstance(payload[0], dict):
            return payload
        return []

    if isinstance(payload, dict):
        # common direct keys
        for key in ["ads", "ad_list", "items", "results", "data"]:
            v = payload.get(key)
            if isinstance(v, list) and (not v or isinstance(v[0], dict)):
                return v
            if isinstance(v, dict):
                # nested common
                for k2 in ["ads", "ad_list", "items", "results", "list"]:
                    v2 = v.get(k2)
                    if isinstance(v2, list) and (not v2 or isinstance(v2[0], dict)):
                        return v2

        # fallback: deep search largest list[dict] containing list_id
        best: List[Dict[str, Any]] = []

        def walk(o: Any):
            nonlocal best
            if isinstance(o, dict):
                for vv in o.values():
                    walk(vv)
            elif isinstance(o, list):
                if o and isinstance(o[0], dict):
                    score = 0
                    for d in o[:10]:
                        if "list_id" in d or "ad_id" in d:
                            score += 1
                    # choose the list that looks most like ads
                    if score > 0 and len(o) >= len(best):
                        best = o  # type: ignore
                for it in o:
                    walk(it)

        walk(payload)
        return best

    return []


def fetch_listing_page(
    listing_api: str,
    category: int,
    limit: int,
    offset: int,
    region_v2: Optional[int] = None,
    timeout: float = 20.0,
) -> List[Dict[str, Any]]:
    params: Dict[str, Any] = {
        "cg": int(category),
        "limit": int(limit),
        "o": int(offset),
    }
    if region_v2 is not None:
        params["region_v2"] = int(region_v2)

    payload = http_get(listing_api, params=params, timeout=timeout)
    return _extract_ads(payload)


# =========================
# Row mapping
# =========================
FUEL_MAP = {
    1: "Xăng",
    2: "Dầu",
    3: "Điện",
    4: "Hybrid",
    5: "Khác",
}

# very small color heuristic (optional)
COLOR_WORDS = [
    "trắng", "đen", "bạc", "xám", "ghi", "đỏ", "xanh", "vàng", "nâu", "cam", "tím", "hồng",
]
def _guess_color_name(subject: str) -> str:
    s = (subject or "").lower()
    found = []
    for w in COLOR_WORDS:
        if w in s:
            found.append(w)
    # common combos like "xanh đen"
    if "xanh" in found and "đen" in found:
        return "xanh đen"
    if found:
        return " ".join(found[:2])
    return ""


def ad_to_row(
    ad: Dict[str, Any],
    region_v2: Optional[int] = None,
    region_name: Optional[str] = None,
) -> Dict[str, Any]:
    list_id = ad.get("list_id") or ad.get("listId") or ad.get("id") or ad.get("ad_id") or ""
    try:
        list_id_str = str(int(list_id))
    except Exception:
        list_id_str = str(list_id).strip()

    url = ad.get("url")
    if not url and list_id_str:
        url = f"https://www.chotot.com/mua-ban-o-to/{list_id_str}.htm"

    brand = ad.get("carbrand_name") or ad.get("carbrandName") or ""
    model = ad.get("carmodel_name") or ad.get("carmodelName") or ""
    year = ad.get("mfdate") or ad.get("year") or ""
    price = ad.get("price") or ""
    mileage_v2 = ad.get("mileage_v2") or ad.get("mileageV2") or ad.get("mileage") or ""

    fuel_val = ad.get("fuel")
    fuel_name = ""
    if isinstance(fuel_val, int) and fuel_val in FUEL_MAP:
        fuel_name = FUEL_MAP[fuel_val]
    elif isinstance(fuel_val, str):
        fuel_name = fuel_val

    lat = ad.get("latitude", "")
    lng = ad.get("longitude", "")
    loc = ad.get("location", "")
    # sometimes location is "lat,lng"
    if (not lat or not lng) and isinstance(loc, str) and "," in loc:
        parts = [x.strip() for x in loc.split(",")]
        if len(parts) == 2:
            if not lat:
                lat = parts[0]
            if not lng:
                lng = parts[1]

    carcolor = ad.get("carcolor", "")
    carcolor_name = ad.get("carcolor_name") or ad.get("carcolorName") or ""
    if not carcolor_name:
        carcolor_name = _guess_color_name(str(ad.get("subject") or ad.get("title") or ""))

    rv2 = ad.get("region_v2")
    rn = ad.get("region_name")
    if rv2 is None:
        rv2 = region_v2
    if not rn:
        rn = region_name

    crawled_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    return {
        "listing_id": list_id_str,
        "listing_url": url or "",
        "brand": str(brand),
        "model": str(model),
        "year": year,
        "price": price,
        "mileage_v2": mileage_v2,
        "fuel": fuel_name,
        "location": loc if isinstance(loc, str) else "",
        "latitude": lat,
        "longitude": lng,
        "carcolor": carcolor,
        "carcolor_name": carcolor_name,
        "region_v2": rv2 if rv2 is not None else "",
        "region_name": rn or "",
        "crawled_at": crawled_at,
        "source": "chotot",
        "raw_json": json.dumps(ad, ensure_ascii=False),
    }


RAW_FIELDS = [
    "listing_id",
    "listing_url",
    "brand",
    "model",
    "year",
    "price",
    "mileage_v2",
    "fuel",
    "location",
    "latitude",
    "longitude",
    "carcolor",
    "carcolor_name",
    "region_v2",
    "region_name",
    "crawled_at",
    "source",
    "raw_json",
]


# =========================
# Crawl
# =========================
def crawl_region(
    listing_api: str,
    category: int,
    region_v2: Optional[int],
    region_name: str,
    pages: int,
    limit: int,
    delay: float,
    timeout: float,
    write_mode: str,
    seen_ids: set[str],
    out_csv: Path,
    logger,
) -> Tuple[int, int, set[str]]:
    total_new = 0
    newly_seen: set[str] = set()

    for page in range(1, pages + 1):
        offset = (page - 1) * limit
        ads = fetch_listing_page(
            listing_api=listing_api,
            category=category,
            limit=limit,
            offset=offset,
            region_v2=region_v2,
            timeout=timeout,
        )
        logger.info(f"Fetched region={region_v2 if region_v2 is not None else 'NATIONWIDE'} page={page} offset={offset} ads={len(ads)}")

        rows_to_write: List[Dict[str, Any]] = []
        page_new = 0

        for ad in ads:
            row = ad_to_row(ad, region_v2=region_v2, region_name=region_name)

            lid = row.get("listing_id", "")
            if not lid:
                continue

            is_new = lid not in seen_ids

            # write mode:
            # - new: write only new listings
            # - all: write everything (for price history, etc.)
            if write_mode == "all" or is_new:
                rows_to_write.append(row)

            if is_new:
                seen_ids.add(lid)
                newly_seen.add(lid)
                page_new += 1
                total_new += 1

        if rows_to_write:
            append_rows_csv(str(out_csv), rows_to_write, fieldnames=RAW_FIELDS)
            logger.info(f"Rows written in this page: {len(rows_to_write)}")
        else:
            logger.info("No rows written in this page.")

        # polite delay between pages
        if page < pages:
            sleep_sec(delay)

    return total_new, len(seen_ids), newly_seen


def run(args: argparse.Namespace) -> None:
    repo_root = get_repo_root()
    config_dir = repo_root / "configs"
    raw_dir = repo_root / "data" / "raw"
    meta_dir = repo_root / "data" / "meta"
    logs_dir = repo_root / "logs"

    ensure_dir(config_dir)
    ensure_dir(raw_dir)
    ensure_dir(meta_dir)
    ensure_dir(logs_dir)

    log_file = logs_dir / f"chotot_spider_{_today_yyyymmdd()}.log"
    logger = setup_logger(log_file)

    logger.info(f"REPO_ROOT={repo_root}")
    logger.info(f"CONFIG_DIR={config_dir}")
    logger.info(f"RAW_DIR={raw_dir}")
    logger.info(f"META_DIR={meta_dir}")
    logger.info(f"LOG_FILE={log_file}")

    seen_path = meta_dir / "chotot_seen_ids.txt"
    cursor_path = meta_dir / "chotot_regions_cursor.json"

    seen_ids = load_seen_ids(seen_path)
    logger.info(f"Loaded seen_ids={len(seen_ids)} from {seen_path}")

    listing_api = args.listing_api or DEFAULT_LISTING_API
    regions_url = args.regions_url or DEFAULT_REGIONS_URL

    out_csv = raw_dir / f"chotot_raw_{_today_yyyymmdd()}.csv"
    category = int(args.category)

    # ------------- choose mode -------------
    regions_sorted: List[Tuple[int, str]] = []
    mode = "NATIONWIDE"

    regions_from_cli = parse_regions_arg(args.regions)
    if regions_from_cli:
        # Explicit region_v2 list from CLI
        # optionally map names by loading provinces once
        name_map: Dict[int, str] = {}
        try:
            provs = load_regions_v2_from_url(regions_url, logger, timeout=args.timeout)
            name_map = {rid: rname for rid, rname in provs}
        except Exception:
            name_map = {}

        regions_sorted = [(rid, name_map.get(rid, "")) for rid in regions_from_cli]
        mode = "EXPLICIT_REGIONS"

    elif args.regions:
        # If regions is URL or file path
        s = str(args.regions).strip()
        if _is_url(s):
            regions_sorted = load_regions_v2_from_url(s, logger, timeout=args.timeout)
        else:
            regions_sorted = load_regions_v2_from_file(s, logger)
        mode = "REGIONS_SOURCE"

    elif args.all_regions:
        # Load provinces from regions_url
        try:
            regions_sorted = load_regions_v2_from_url(regions_url, logger, timeout=args.timeout)
            mode = "ALL_REGIONS"
        except Exception as e:
            logger.warning(f"Failed to load regions_v2 from {regions_url}: {e}")
            regions_sorted = []
            mode = "NATIONWIDE"

    elif args.nationwide:
        regions_sorted = []
        mode = "NATIONWIDE"

    # ------------- crawl plan -------------
    total_new = 0
    total_seen = len(seen_ids)

    if mode == "ALL_REGIONS" and regions_sorted:
        cursor_before = load_cursor(cursor_path)
        selected, cursor_after = slice_regions_with_cursor(regions_sorted, cursor_before, int(args.max_regions_per_run))
        logger.info(
            f"Mode=ALL_REGIONS total_regions={len(regions_sorted)} crawling_now={len(selected)} "
            f"cursor_before={cursor_before} cursor_after={cursor_after}"
        )
        logger.info(
            "Selected regions: " + ", ".join([f"{rid}:{name}" for rid, name in selected])
        )

        for rid, rname in selected:
            logger.info(f"Crawling region={rid} pages={args.pages} limit={args.limit} delay={args.delay:.2f}s")
            new_cnt, total_seen, _newly = crawl_region(
                listing_api=listing_api,
                category=category,
                region_v2=rid,
                region_name=rname,
                pages=int(args.pages),
                limit=int(args.limit),
                delay=float(args.delay),
                timeout=float(args.timeout),
                write_mode=args.write_mode,
                seen_ids=seen_ids,
                out_csv=out_csv,
                logger=logger,
            )
            total_new += new_cnt
            # delay between regions
            sleep_sec(float(args.delay))

        save_cursor(cursor_path, cursor_after)

    elif mode in ("EXPLICIT_REGIONS", "REGIONS_SOURCE") and regions_sorted:
        logger.info(f"Mode={mode} regions={len(regions_sorted)} pages={args.pages} limit={args.limit} delay={args.delay:.2f}s")

        for rid, rname in regions_sorted:
            logger.info(f"Crawling region={rid} pages={args.pages} limit={args.limit} delay={args.delay:.2f}s")
            new_cnt, total_seen, _newly = crawl_region(
                listing_api=listing_api,
                category=category,
                region_v2=rid,
                region_name=rname,
                pages=int(args.pages),
                limit=int(args.limit),
                delay=float(args.delay),
                timeout=float(args.timeout),
                write_mode=args.write_mode,
                seen_ids=seen_ids,
                out_csv=out_csv,
                logger=logger,
            )
            total_new += new_cnt
            sleep_sec(float(args.delay))

    else:
        # NATIONWIDE
        logger.info("Mode=NATIONWIDE (no region_v2)")
        new_cnt, total_seen, _newly = crawl_region(
            listing_api=listing_api,
            category=category,
            region_v2=None,
            region_name="",
            pages=int(args.pages),
            limit=int(args.limit),
            delay=float(args.delay),
            timeout=float(args.timeout),
            write_mode=args.write_mode,
            seen_ids=seen_ids,
            out_csv=out_csv,
            logger=logger,
        )
        total_new += new_cnt

    # persist seen ids
    save_seen_ids(seen_path, seen_ids)
    logger.info(f"DONE raw_path={out_csv} total_new={total_new} total_seen={len(seen_ids)}")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="ChoTot Car Spider (incremental + safe crawling)")

    mode = p.add_mutually_exclusive_group()
    mode.add_argument("--nationwide", action="store_true", help="crawl nationwide feed (no region_v2)")
    mode.add_argument("--all-regions", action="store_true", help="crawl by provinces/cities from loadRegionsV2")
    # regions can be "13000" or "13000,16000" OR a URL/file
    p.add_argument("--regions", type=str, default=None, help="explicit region_v2 list (e.g. 13000,16000) OR regions source (url/file)")

    p.add_argument("--pages", type=int, default=2)
    p.add_argument("--limit", type=int, default=50)
    p.add_argument("--delay", type=float, default=1.2)
    p.add_argument("--timeout", type=float, default=20.0)

    p.add_argument("--max-regions-per-run", type=int, default=30, help="only for --all-regions, slice regions by cursor")
    p.add_argument("--write-mode", type=str, default="new", choices=["new", "all"], help="new=only unseen, all=write all rows")

    p.add_argument("--category", type=int, default=DEFAULT_CATEGORY_CAR)
    p.add_argument("--listing-api", type=str, default=DEFAULT_LISTING_API)
    p.add_argument("--regions-url", type=str, default=DEFAULT_REGIONS_URL)

    return p


if __name__ == "__main__":
    parser = build_parser()
    run(parser.parse_args())
